package riversharedtest

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/slogtest"
)

// BaseServiceArchetype returns a new base service suitable for use in tests.
// Returns a new instance so that it's not possible to accidentally taint a
// shared object.
func BaseServiceArchetype(tb testing.TB) *baseservice.Archetype {
	tb.Helper()

	return &baseservice.Archetype{
		Logger: Logger(tb),
		Time:   &TimeStub{},
	}
}

// A pool and sync.Once to initialize it, invoked by TestTx. Once open, this
// pool is never explicitly closed, instead closing implicitly as the package
// tests finish.
var (
	dbPool     *pgxpool.Pool //nolint:gochecknoglobals
	dbPoolOnce sync.Once     //nolint:gochecknoglobals
)

// DBPool gets a lazily initialized database pool for `TEST_DATABASE_URL` or
// `river_test` if the former isn't specified.
func DBPool(ctx context.Context, tb testing.TB) *pgxpool.Pool {
	tb.Helper()

	dbPoolOnce.Do(func() {
		var err error
		dbPool, err = pgxpool.New(ctx, cmp.Or(
			os.Getenv("TEST_DATABASE_URL"),
			"postgres://localhost:5432/river_test",
		))
		require.NoError(tb, err)
	})
	require.NotNil(tb, dbPool) // die in case initial connect from another test failed

	return dbPool
}

// Logger returns a logger suitable for use in tests.
//
// Defaults to informational verbosity. If env is set with `RIVER_DEBUG=true`,
// debug level verbosity is activated.
func Logger(tb testing.TB) *slog.Logger {
	tb.Helper()

	if os.Getenv("RIVER_DEBUG") == "1" || os.Getenv("RIVER_DEBUG") == "true" {
		return slogtest.NewLogger(tb, &slog.HandlerOptions{Level: slog.LevelDebug})
	}

	return slogtest.NewLogger(tb, nil)
}

// Logger returns a logger suitable for use in tests which outputs only at warn
// or above. Useful in tests where particularly noisy log output is expected.
func LoggerWarn(tb testing.TB) *slog.Logger {
	tb.Helper()
	return slogtest.NewLogger(tb, &slog.HandlerOptions{Level: slog.LevelWarn})
}

// TestTx starts a test transaction that's rolled back automatically as the test
// case is cleaning itself up.
//
// This variant uses the default database pool from DBPool that points to
// `TEST_DATABASE_URL` or `river_test` if the former wasn't specified.
func TestTx(ctx context.Context, tb testing.TB) pgx.Tx {
	tb.Helper()
	return TestTxPool(ctx, tb, DBPool(ctx, tb))
}

// TestTxPool starts a test transaction that's rolled back automatically as the
// test case is cleaning itself up.
//
// This variant starts the test transaction on the specified database pool.
func TestTxPool(ctx context.Context, tb testing.TB, dbPool *pgxpool.Pool) pgx.Tx {
	tb.Helper()

	tx, err := dbPool.Begin(ctx)
	require.NoError(tb, err)

	tb.Cleanup(func() {
		// Tests may inerit context from `t.Context()` which is cancelled after
		// tests run and before calling clean up. We need a non-cancelled
		// context to issue rollback here, so use a bit of a bludgeon to do so
		// with `context.WithoutCancel()`.
		ctx := context.WithoutCancel(ctx)

		err := tx.Rollback(ctx)

		if err == nil {
			return
		}

		// Try to look for an error on rollback because it does occasionally
		// reveal a real problem in the way a test is written. However, allow
		// tests to roll back their transaction early if they like, so ignore
		// `ErrTxClosed`.
		if errors.Is(err, pgx.ErrTxClosed) {
			return
		}

		// In case of a cancelled context during a database operation, which
		// happens in many tests, pgx seems to not only roll back the
		// transaction, but closes the connection, and returns this error on
		// rollback. Allow this error since it's hard to prevent it in our flows
		// that use contexts heavily.
		if err.Error() == "conn closed" {
			return
		}

		// Similar to the above, but a newly appeared error that wraps the
		// above. As far as I can tell, no error variables are available to use
		// with `errors.Is`.
		if err.Error() == "failed to deallocate cached statement(s): conn closed" {
			return
		}

		require.NoError(tb, err)
	})

	return tx
}

// TimeStub implements baseservice.TimeGeneratorWithStub to allow time to be
// stubbed in tests.
//
// It exists separately from rivertest.TimeStub to avoid a circular dependency.
type TimeStub struct {
	mu     sync.RWMutex
	nowUTC *time.Time
}

func (t *TimeStub) NowUTC() time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.nowUTC == nil {
		return time.Now().UTC()
	}

	return *t.nowUTC
}

func (t *TimeStub) NowUTCOrNil() *time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.nowUTC
}

func (t *TimeStub) StubNowUTC(nowUTC time.Time) time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.nowUTC = &nowUTC
	return nowUTC
}

// WaitOrTimeout tries to wait on the given channel for a value to come through,
// and returns it if one does, but times out after a reasonable amount of time.
// Useful to guarantee that test cases don't hang forever, even in the event of
// something wrong.
func WaitOrTimeout[T any](tb testing.TB, waitChan <-chan T) T {
	tb.Helper()

	timeout := WaitTimeout()

	select {
	case value := <-waitChan:
		return value
	case <-time.After(timeout):
		require.FailNowf(tb, "WaitOrTimeout timed out",
			"WaitOrTimeout timed out after waiting %s", timeout)
	}
	return *new(T) // unreachable
}

// WaitOrTimeoutN tries to wait on the given channel for N values to come
// through, and returns it if they do, but times out after a reasonable amount
// of time.  Useful to guarantee that test cases don't hang forever, even in the
// event of something wrong.
func WaitOrTimeoutN[T any](tb testing.TB, waitChan <-chan T, numValues int) []T {
	tb.Helper()

	var (
		timeout  = WaitTimeout()
		deadline = time.Now().Add(timeout)
		values   = make([]T, 0, numValues)
	)

	for {
		select {
		case value := <-waitChan:
			values = append(values, value)

			if len(values) >= numValues {
				return values
			}

		case <-time.After(time.Until(deadline)):
			require.FailNowf(tb, "WaitOrTimeout timed out",
				"WaitOrTimeout timed out after waiting %s (received %d value(s), wanted %d)", timeout, len(values), numValues)
			return nil
		}
	}
}

// WaitTimeout returns a duration broadly appropriate for waiting on an expected
// event in a test, and which is used for `TestSignal.WaitOrTimeout` in the main
// package and `WaitOrTimeout` above. Its main purpose is to allow a little
// extra leeway in GitHub Actions where we occasionally seem to observe subpar
// performance which leads to timeouts and test intermittency, while still
// keeping a tight a timeout for local test runs where this is never a problem.
func WaitTimeout() time.Duration {
	if os.Getenv("GITHUB_ACTIONS") == "true" {
		return 10 * time.Second
	}

	return 3 * time.Second
}

var IgnoredKnownGoroutineLeaks = []goleak.Option{ //nolint:gochecknoglobals
	// This goroutine contains a 500 ms uninterruptible sleep that may still be
	// running by the time the test suite finishes and cause a failure. This
	// might be something that should be fixed in pgx, but ignore it for the
	// time being lest we have intermittent tests.
	//
	// We opened an issue on pgx, but it may or may not be one that gets fixed:
	//
	// https://github.com/jackc/pgx/issues/1641
	goleak.IgnoreTopFunction("github.com/jackc/pgx/v5/pgxpool.(*Pool).backgroundHealthCheck"),

	// Similar to the above, may be sitting in a sleep when the program finishes
	// and there's not much we can do about it.
	goleak.IgnoreAnyFunction("github.com/jackc/pgx/v5/pgxpool.(*Pool).triggerHealthCheck.func1"),
}

// WrapTestMain performs some common setup and teardown that should be shared
// amongst all packages. e.g. Configures a manager for test databases on setup,
// and checks for no goroutine leaks on teardown.
func WrapTestMain(m *testing.M) {
	status := m.Run()

	if status == 0 {
		if err := goleak.Find(IgnoredKnownGoroutineLeaks...); err != nil {
			fmt.Fprintf(os.Stderr, "goleak: Errors on successful test run: %v\n", err)
			status = 1
		}
	}

	os.Exit(status)
}
