package jobcompleter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/puddle/v2"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/jobstats"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riverpilot"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/startstop"
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivershared/util/ptrutil"
	"github.com/riverqueue/river/rivertype"
)

type partialExecutorMock struct {
	riverdriver.Executor
	JobSetStateIfRunningManyCalled bool
	JobSetStateIfRunningManyFunc   func(ctx context.Context, params *riverdriver.JobSetStateIfRunningManyParams) ([]*rivertype.JobRow, error)
	mu                             sync.Mutex
}

// NewPartialExecutorMock returns a new mock with all mock functions set to call
// down into the given real executor.
func NewPartialExecutorMock(exec riverdriver.Executor) *partialExecutorMock {
	return &partialExecutorMock{
		Executor:                     exec,
		JobSetStateIfRunningManyFunc: exec.JobSetStateIfRunningMany,
	}
}

func (m *partialExecutorMock) Begin(ctx context.Context) (riverdriver.ExecutorTx, error) {
	tx, err := m.Executor.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &partialExecutorTxMock{ExecutorTx: tx, partial: m}, nil
}

func (m *partialExecutorMock) JobSetStateIfRunningMany(ctx context.Context, params *riverdriver.JobSetStateIfRunningManyParams) ([]*rivertype.JobRow, error) {
	m.setCalled(func() { m.JobSetStateIfRunningManyCalled = true })
	return m.JobSetStateIfRunningManyFunc(ctx, params)
}

func (m *partialExecutorMock) setCalled(setCalledFunc func()) {
	m.mu.Lock()
	defer m.mu.Unlock()
	setCalledFunc()
}

type partialExecutorTxMock struct {
	riverdriver.ExecutorTx
	partial *partialExecutorMock
}

func (m *partialExecutorTxMock) JobSetStateIfRunningMany(ctx context.Context, params *riverdriver.JobSetStateIfRunningManyParams) ([]*rivertype.JobRow, error) {
	return m.partial.JobSetStateIfRunningMany(ctx, params)
}

func TestInlineJobCompleter_Complete(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	var (
		tx       = riverinternaltest.TestTx(ctx, t)
		driver   = riverpgxv5.New(nil)
		exec     = driver.UnwrapExecutor(tx)
		execMock = NewPartialExecutorMock(exec)
	)

	var attempt int
	expectedErr := errors.New("an error from the completer")

	execMock.JobSetStateIfRunningManyFunc = func(ctx context.Context, params *riverdriver.JobSetStateIfRunningManyParams) ([]*rivertype.JobRow, error) {
		require.Len(t, params.ID, 1)
		require.Equal(t, int64(1), params.ID[0])
		attempt++
		return nil, expectedErr
	}

	subscribeCh := make(chan []CompleterJobUpdated, 10)
	t.Cleanup(riverinternaltest.DiscardContinuously(subscribeCh))

	completer := NewInlineCompleter(riversharedtest.BaseServiceArchetype(t), execMock, &riverpilot.StandardPilot{}, subscribeCh)
	t.Cleanup(completer.Stop)
	completer.disableSleep = true

	err := completer.JobSetStateIfRunning(ctx, &jobstats.JobStatistics{}, riverdriver.JobSetStateCompleted(1, time.Now(), nil))
	if !errors.Is(err, expectedErr) {
		t.Errorf("expected %v, got %v", expectedErr, err)
	}

	require.True(t, execMock.JobSetStateIfRunningManyCalled)
	require.Equal(t, numRetries, attempt)
}

func TestInlineJobCompleter_Subscribe(t *testing.T) {
	t.Parallel()

	testCompleterSubscribe(t, func(exec riverdriver.Executor, subscribeCh SubscribeChan) JobCompleter {
		return NewInlineCompleter(riversharedtest.BaseServiceArchetype(t), exec, &riverpilot.StandardPilot{}, subscribeCh)
	})
}

func TestInlineJobCompleter_Wait(t *testing.T) {
	t.Parallel()

	testCompleterWait(t, func(exec riverdriver.Executor, subscribeChan SubscribeChan) JobCompleter {
		return NewInlineCompleter(riversharedtest.BaseServiceArchetype(t), exec, &riverpilot.StandardPilot{}, subscribeChan)
	})
}

func TestAsyncJobCompleter_Complete(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type jobInput struct {
		// TODO: Try to get rid of containing the context in struct. It'd be
		// better to pass it forward instead.
		ctx   context.Context //nolint:containedctx
		jobID int64
	}
	inputCh := make(chan jobInput)
	resultCh := make(chan error)

	expectedErr := errors.New("an error from the completer")

	go func() {
		riversharedtest.WaitOrTimeout(t, inputCh)
		resultCh <- expectedErr
	}()

	var (
		db       = riverinternaltest.TestDB(ctx, t)
		driver   = riverpgxv5.New(db)
		execMock = NewPartialExecutorMock(driver.GetExecutor())
	)

	execMock.JobSetStateIfRunningManyFunc = func(ctx context.Context, params *riverdriver.JobSetStateIfRunningManyParams) ([]*rivertype.JobRow, error) {
		require.Len(t, params.ID, 1)
		inputCh <- jobInput{ctx: ctx, jobID: params.ID[0]}
		err := <-resultCh
		if err != nil {
			return nil, err
		}
		return []*rivertype.JobRow{{ID: params.ID[0], State: params.State[0]}}, nil
	}
	subscribeChan := make(chan []CompleterJobUpdated, 10)
	completer := newAsyncCompleterWithConcurrency(riversharedtest.BaseServiceArchetype(t), execMock, &riverpilot.StandardPilot{}, 2, subscribeChan)
	completer.disableSleep = true
	require.NoError(t, completer.Start(ctx))
	t.Cleanup(completer.Stop)

	// launch 4 completions, only 2 can be inline due to the concurrency limit:
	for i := range int64(2) {
		if err := completer.JobSetStateIfRunning(ctx, &jobstats.JobStatistics{}, riverdriver.JobSetStateCompleted(i, time.Now(), nil)); err != nil {
			t.Errorf("expected nil err, got %v", err)
		}
	}
	bgCompletionsStarted := make(chan struct{})
	go func() {
		for i := int64(2); i < 4; i++ {
			if err := completer.JobSetStateIfRunning(ctx, &jobstats.JobStatistics{}, riverdriver.JobSetStateCompleted(i, time.Now(), nil)); err != nil {
				t.Errorf("expected nil err, got %v", err)
			}
		}
		close(bgCompletionsStarted)
	}()

	expectCompletionInFlight := func() {
		select {
		case input := <-inputCh:
			t.Logf("completion for %d in-flight", input.jobID)
		case <-time.After(time.Second):
			t.Fatalf("expected a completion to be in-flight")
		}
	}
	expectNoCompletionInFlight := func() {
		select {
		case input := <-inputCh:
			t.Fatalf("unexpected completion for %d in-flight", input.jobID)
		case <-time.After(500 * time.Millisecond):
		}
	}

	// two completions should be in-flight:
	expectCompletionInFlight()
	expectCompletionInFlight()

	// A 3rd one shouldn't be in-flight due to the concurrency limit:
	expectNoCompletionInFlight()

	// Finish the first two completions:
	resultCh <- nil
	resultCh <- nil

	// The final two completions should now be in-flight:
	<-bgCompletionsStarted
	expectCompletionInFlight()
	expectCompletionInFlight()

	// A 5th one shouldn't be in-flight because we only started 4:
	expectNoCompletionInFlight()

	// Finish the final two completions:
	resultCh <- nil
	resultCh <- nil
}

func TestAsyncJobCompleter_Subscribe(t *testing.T) {
	t.Parallel()

	testCompleterSubscribe(t, func(exec riverdriver.Executor, subscribeCh SubscribeChan) JobCompleter {
		return newAsyncCompleterWithConcurrency(riversharedtest.BaseServiceArchetype(t), exec, &riverpilot.StandardPilot{}, 4, subscribeCh)
	})
}

func TestAsyncJobCompleter_Wait(t *testing.T) {
	t.Parallel()

	testCompleterWait(t, func(exec riverdriver.Executor, subscribeCh SubscribeChan) JobCompleter {
		return newAsyncCompleterWithConcurrency(riversharedtest.BaseServiceArchetype(t), exec, &riverpilot.StandardPilot{}, 4, subscribeCh)
	})
}

func testCompleterSubscribe(t *testing.T, constructor func(riverdriver.Executor, SubscribeChan) JobCompleter) {
	t.Helper()

	ctx := context.Background()

	var (
		db       = riverinternaltest.TestDB(ctx, t)
		driver   = riverpgxv5.New(db)
		execMock = NewPartialExecutorMock(driver.GetExecutor())
	)
	execMock.JobSetStateIfRunningManyFunc = func(ctx context.Context, params *riverdriver.JobSetStateIfRunningManyParams) ([]*rivertype.JobRow, error) {
		return []*rivertype.JobRow{{ID: params.ID[0], State: rivertype.JobStateCompleted}}, nil
	}

	subscribeChan := make(chan []CompleterJobUpdated, 10)
	completer := constructor(execMock, subscribeChan)
	require.NoError(t, completer.Start(ctx))

	// Flatten the slice results from subscribeChan into jobUpdateChan:
	jobUpdateChan := make(chan CompleterJobUpdated, 10)
	go func() {
		defer close(jobUpdateChan)
		for update := range subscribeChan {
			for _, u := range update {
				jobUpdateChan <- u
			}
		}
	}()

	for i := range 4 {
		require.NoError(t, completer.JobSetStateIfRunning(ctx, &jobstats.JobStatistics{}, riverdriver.JobSetStateCompleted(int64(i), time.Now(), nil)))
	}

	completer.Stop() // closes subscribeChan

	updates := riversharedtest.WaitOrTimeoutN(t, jobUpdateChan, 4)
	for range 4 {
		require.Equal(t, rivertype.JobStateCompleted, updates[0].Job.State)
	}
	go completer.Stop()
	// drain all remaining jobs
	for range jobUpdateChan {
	}
}

func testCompleterWait(t *testing.T, constructor func(riverdriver.Executor, SubscribeChan) JobCompleter) {
	t.Helper()

	ctx := context.Background()

	var (
		db       = riverinternaltest.TestDB(ctx, t)
		driver   = riverpgxv5.New(db)
		execMock = NewPartialExecutorMock(driver.GetExecutor())
	)

	resultCh := make(chan struct{})
	completeStartedCh := make(chan struct{})
	execMock.JobSetStateIfRunningManyFunc = func(ctx context.Context, params *riverdriver.JobSetStateIfRunningManyParams) ([]*rivertype.JobRow, error) {
		completeStartedCh <- struct{}{}
		<-resultCh
		results := make([]*rivertype.JobRow, len(params.ID))
		for i := range params.ID {
			results[i] = &rivertype.JobRow{ID: params.ID[i], State: rivertype.JobStateCompleted}
		}
		return results, nil
	}
	subscribeCh := make(chan []CompleterJobUpdated, 100)

	completer := constructor(execMock, subscribeCh)
	require.NoError(t, completer.Start(ctx))

	// launch 4 completions:
	for i := range 4 {
		go func() {
			require.NoError(t, completer.JobSetStateIfRunning(ctx, &jobstats.JobStatistics{}, riverdriver.JobSetStateCompleted(int64(i), time.Now(), nil)))
		}()
		<-completeStartedCh // wait for func to actually start
	}

	// Give one completion a signal to finish, there should be 3 remaining in-flight:
	resultCh <- struct{}{}

	waitDone := make(chan struct{})
	go func() {
		completer.Stop()
		close(waitDone)
	}()

	select {
	case <-waitDone:
		t.Fatalf("expected Wait to block until all jobs are complete, but it returned when there should be three remaining")
	case <-time.After(100 * time.Millisecond):
	}

	// Get us down to one in-flight completion:
	resultCh <- struct{}{}
	resultCh <- struct{}{}

	select {
	case <-waitDone:
		t.Fatalf("expected Wait to block until all jobs are complete, but it returned when there should be one remaining")
	case <-time.After(100 * time.Millisecond):
	}

	// Finish the last one:
	resultCh <- struct{}{}

	select {
	case <-waitDone:
	case <-time.After(100 * time.Millisecond):
		t.Errorf("expected Wait to return after all jobs are complete")
	}
}

func TestAsyncCompleter(t *testing.T) {
	t.Parallel()

	testCompleter(t, func(t *testing.T, exec riverdriver.Executor, pilot riverpilot.Pilot, subscribeCh chan<- []CompleterJobUpdated) *AsyncCompleter {
		t.Helper()
		return NewAsyncCompleter(riversharedtest.BaseServiceArchetype(t), exec, pilot, subscribeCh)
	},
		func(completer *AsyncCompleter) { completer.disableSleep = true },
		func(completer *AsyncCompleter, exec riverdriver.Executor) { completer.exec = exec },
	)
}

func TestBatchCompleter(t *testing.T) {
	t.Parallel()

	testCompleter(t, func(t *testing.T, exec riverdriver.Executor, pilot riverpilot.Pilot, subscribeCh chan<- []CompleterJobUpdated) *BatchCompleter {
		t.Helper()
		return NewBatchCompleter(riversharedtest.BaseServiceArchetype(t), exec, pilot, subscribeCh)
	},
		func(completer *BatchCompleter) { completer.disableSleep = true },
		func(completer *BatchCompleter, exec riverdriver.Executor) { completer.exec = exec },
	)

	ctx := context.Background()

	type testBundle struct {
		exec        riverdriver.Executor
		subscribeCh <-chan []CompleterJobUpdated
	}

	setup := func(t *testing.T) (*BatchCompleter, *testBundle) {
		t.Helper()

		var (
			driver      = riverpgxv5.New(riverinternaltest.TestDB(ctx, t))
			exec        = driver.GetExecutor()
			pilot       = &riverpilot.StandardPilot{}
			subscribeCh = make(chan []CompleterJobUpdated, 10)
			completer   = NewBatchCompleter(riversharedtest.BaseServiceArchetype(t), exec, pilot, subscribeCh)
		)

		require.NoError(t, completer.Start(ctx))
		t.Cleanup(completer.Stop)

		riversharedtest.WaitOrTimeout(t, completer.Started())

		return completer, &testBundle{
			exec:        exec,
			subscribeCh: subscribeCh,
		}
	}

	t.Run("CompletionsCompletedInSubBatches", func(t *testing.T) {
		t.Parallel()

		completer, bundle := setup(t)
		completer.completionMaxSize = 10 // set to something artificially low

		jobUpdateChan := make(chan CompleterJobUpdated, 100)
		go func() {
			defer close(jobUpdateChan)
			for update := range bundle.subscribeCh {
				for _, u := range update {
					jobUpdateChan <- u
				}
			}
		}()

		stopInsertion := doContinuousInsertion(ctx, t, completer, bundle.exec)

		// Wait for some jobs to come through, giving lots of opportunity for
		// the completer to have pooled some completions and being forced to
		// work them in sub-batches with our diminished sub-batch size.
		riversharedtest.WaitOrTimeoutN(t, jobUpdateChan, 100)

		stopInsertion()
		go completer.Stop()
		// drain all remaining jobs
		for range jobUpdateChan {
		}
	})

	t.Run("BacklogWaitAndContinue", func(t *testing.T) {
		t.Parallel()

		completer, bundle := setup(t)
		completer.maxBacklog = 10 // set to something artificially low

		jobUpdateChan := make(chan CompleterJobUpdated, 100)
		go func() {
			defer close(jobUpdateChan)
			for update := range bundle.subscribeCh {
				for _, u := range update {
					jobUpdateChan <- u
				}
			}
		}()

		stopInsertion := doContinuousInsertion(ctx, t, completer, bundle.exec)

		// Wait for some jobs to come through. Waiting for these jobs to come
		// through will provide plenty of opportunity for the completer to back
		// up with our small configured backlog.
		riversharedtest.WaitOrTimeoutN(t, jobUpdateChan, 100)

		stopInsertion()
		go completer.Stop()
		// drain all remaining jobs
		for range jobUpdateChan {
		}
	})
}

func TestInlineCompleter(t *testing.T) {
	t.Parallel()

	testCompleter(t, func(t *testing.T, exec riverdriver.Executor, pilot riverpilot.Pilot, subscribeCh chan<- []CompleterJobUpdated) *InlineCompleter {
		t.Helper()
		return NewInlineCompleter(riversharedtest.BaseServiceArchetype(t), exec, pilot, subscribeCh)
	},
		func(completer *InlineCompleter) { completer.disableSleep = true },
		func(completer *InlineCompleter, exec riverdriver.Executor) { completer.exec = exec })
}

func testCompleter[TCompleter JobCompleter](
	t *testing.T,
	newCompleter func(t *testing.T, exec riverdriver.Executor, pilot riverpilot.Pilot, subscribeCh chan<- []CompleterJobUpdated) TCompleter,

	// These functions are here to help us inject test behavior that's not part
	// of the JobCompleter interface. We could alternatively define a second
	// interface like jobCompleterWithTestFacilities to expose the additional
	// functionality, although that's not particularly beautiful either.
	disableSleep func(completer TCompleter),
	setExec func(completer TCompleter, exec riverdriver.Executor),
) {
	t.Helper()

	ctx := context.Background()

	type testBundle struct {
		exec        riverdriver.Executor
		subscribeCh <-chan []CompleterJobUpdated
	}

	setup := func(t *testing.T) (TCompleter, *testBundle) {
		t.Helper()

		var (
			driver      = riverpgxv5.New(riverinternaltest.TestDB(ctx, t))
			exec        = driver.GetExecutor()
			pilot       = &riverpilot.StandardPilot{}
			subscribeCh = make(chan []CompleterJobUpdated, 10)
			completer   = newCompleter(t, exec, pilot, subscribeCh)
		)

		require.NoError(t, completer.Start(ctx))
		t.Cleanup(completer.Stop)

		return completer, &testBundle{
			exec:        exec,
			subscribeCh: subscribeCh,
		}
	}

	requireJob := func(t *testing.T, exec riverdriver.Executor, jobID int64) *rivertype.JobRow {
		t.Helper()

		job, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: jobID, Schema: ""})
		require.NoError(t, err)
		return job
	}

	requireState := func(t *testing.T, exec riverdriver.Executor, jobID int64, state rivertype.JobState) *rivertype.JobRow {
		t.Helper()

		job := requireJob(t, exec, jobID)
		require.Equal(t, state, job.State)
		return job
	}

	t.Run("CompletesJobs", func(t *testing.T) {
		t.Parallel()

		completer, bundle := setup(t)

		var (
			finalizedAt1 = time.Now().UTC().Add(-1 * time.Minute)
			finalizedAt2 = time.Now().UTC().Add(-2 * time.Minute)
			finalizedAt3 = time.Now().UTC().Add(-3 * time.Minute)

			job1 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})
			job2 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})
			job3 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})
		)

		require.NoError(t, completer.JobSetStateIfRunning(ctx, &jobstats.JobStatistics{}, riverdriver.JobSetStateCompleted(job1.ID, finalizedAt1, nil)))
		require.NoError(t, completer.JobSetStateIfRunning(ctx, &jobstats.JobStatistics{}, riverdriver.JobSetStateCompleted(job2.ID, finalizedAt2, nil)))
		require.NoError(t, completer.JobSetStateIfRunning(ctx, &jobstats.JobStatistics{}, riverdriver.JobSetStateCompleted(job3.ID, finalizedAt3, nil)))

		completer.Stop()

		job1Updated := requireState(t, bundle.exec, job1.ID, rivertype.JobStateCompleted)
		job2Updated := requireState(t, bundle.exec, job2.ID, rivertype.JobStateCompleted)
		job3Updated := requireState(t, bundle.exec, job3.ID, rivertype.JobStateCompleted)

		require.WithinDuration(t, finalizedAt1, *job1Updated.FinalizedAt, time.Microsecond)
		require.WithinDuration(t, finalizedAt2, *job2Updated.FinalizedAt, time.Microsecond)
		require.WithinDuration(t, finalizedAt3, *job3Updated.FinalizedAt, time.Microsecond)
	})

	// Some completers like BatchCompleter have special logic for when they're
	// handling enormous numbers of jobs, so make sure we're covered for cases
	// like that.
	t.Run("CompletesManyJobs", func(t *testing.T) {
		t.Parallel()

		completer, bundle := setup(t)

		const (
			kind    = "many_jobs_kind"
			numJobs = 4_400
		)

		var (
			insertParams = make([]*riverdriver.JobInsertFastParams, numJobs)
			stats        = make([]jobstats.JobStatistics, numJobs)
		)
		for i := range numJobs {
			insertParams[i] = &riverdriver.JobInsertFastParams{
				EncodedArgs: []byte(`{}`),
				Kind:        kind,
				MaxAttempts: rivercommon.MaxAttemptsDefault,
				Priority:    rivercommon.PriorityDefault,
				Queue:       rivercommon.QueueDefault,
				State:       rivertype.JobStateRunning,
			}
		}

		_, err := bundle.exec.JobInsertFastMany(ctx, &riverdriver.JobInsertFastManyParams{
			Jobs:   insertParams,
			Schema: "",
		})
		require.NoError(t, err)

		jobs, err := bundle.exec.JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{
			Kind:   []string{kind},
			Schema: "",
		})
		require.NoError(t, err)

		t.Cleanup(riverinternaltest.DiscardContinuously(bundle.subscribeCh))

		for i := range jobs {
			require.NoError(t, completer.JobSetStateIfRunning(ctx, &stats[i], riverdriver.JobSetStateCompleted(jobs[i].ID, time.Now(), nil)))
		}

		completer.Stop()

		updatedJobs, err := bundle.exec.JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{
			Kind:   []string{kind},
			Schema: "",
		})
		require.NoError(t, err)
		for i := range updatedJobs {
			require.Equal(t, rivertype.JobStateCompleted, updatedJobs[i].State)
		}
	})

	// The minimum time to wait go guarantee a batch of completions from the
	// batch completer. Unless jobs are above a threshold it'll wait a number of
	// ticks before starting completions. 5 ticks @ 50 milliseconds.
	const minBatchCompleterPassDuration = 5 * 50 * time.Millisecond

	t.Run("FastContinuousCompletion", func(t *testing.T) {
		t.Parallel()

		completer, bundle := setup(t)

		t.Cleanup(riverinternaltest.DiscardContinuously(bundle.subscribeCh))
		stopInsertion := doContinuousInsertion(ctx, t, completer, bundle.exec)

		// Give some time for some jobs to be inserted, and a guaranteed pass by
		// the batch completer.
		time.Sleep(minBatchCompleterPassDuration)

		// Signal to stop insertion and wait for the goroutine to return.
		numInserted := stopInsertion()

		require.Positive(t, numInserted)

		numCompleted, err := bundle.exec.JobCountByState(ctx, &riverdriver.JobCountByStateParams{
			State: rivertype.JobStateCompleted,
		})
		require.NoError(t, err)
		t.Logf("Counted %d jobs as completed", numCompleted)
		require.Positive(t, numCompleted)
	})

	t.Run("SlowerContinuousCompletion", func(t *testing.T) {
		t.Parallel()

		completer, bundle := setup(t)

		// Number here is chosen to be a little higher than the batch
		// completer's tick interval so we can make sure that the right thing
		// happens even on an empty tick.
		stopInsertion := doContinuousInsertionInterval(ctx, t, completer, bundle.exec, 30*time.Millisecond)

		// Give some time for some jobs to be inserted, and a guaranteed pass by
		// the batch completer.
		time.Sleep(minBatchCompleterPassDuration)

		// Signal to stop insertion and wait for the goroutine to return.
		numInserted := stopInsertion()

		require.Positive(t, numInserted)

		numCompleted, err := bundle.exec.JobCountByState(ctx, &riverdriver.JobCountByStateParams{
			State: rivertype.JobStateCompleted,
		})
		require.NoError(t, err)
		t.Logf("Counted %d jobs as completed", numCompleted)
		require.Positive(t, numCompleted)
	})

	t.Run("AllJobStates", func(t *testing.T) {
		t.Parallel()

		completer, bundle := setup(t)

		var (
			job1 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})
			job2 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})
			job3 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})
			job4 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})
			job5 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})
			job6 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})
			job7 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})
		)

		require.NoError(t, completer.JobSetStateIfRunning(ctx, &jobstats.JobStatistics{}, riverdriver.JobSetStateCancelled(job1.ID, time.Now(), []byte("{}"), nil)))
		require.NoError(t, completer.JobSetStateIfRunning(ctx, &jobstats.JobStatistics{}, riverdriver.JobSetStateCompleted(job2.ID, time.Now(), nil)))
		require.NoError(t, completer.JobSetStateIfRunning(ctx, &jobstats.JobStatistics{}, riverdriver.JobSetStateDiscarded(job3.ID, time.Now(), []byte("{}"), nil)))
		require.NoError(t, completer.JobSetStateIfRunning(ctx, &jobstats.JobStatistics{}, riverdriver.JobSetStateErrorAvailable(job4.ID, time.Now(), []byte("{}"), nil)))
		require.NoError(t, completer.JobSetStateIfRunning(ctx, &jobstats.JobStatistics{}, riverdriver.JobSetStateErrorRetryable(job5.ID, time.Now(), []byte("{}"), nil)))
		require.NoError(t, completer.JobSetStateIfRunning(ctx, &jobstats.JobStatistics{}, riverdriver.JobSetStateSnoozed(job6.ID, time.Now(), 10, nil)))
		require.NoError(t, completer.JobSetStateIfRunning(ctx, &jobstats.JobStatistics{}, riverdriver.JobSetStateSnoozedAvailable(job7.ID, time.Now(), 10, nil)))

		completer.Stop()

		requireState(t, bundle.exec, job1.ID, rivertype.JobStateCancelled)
		requireState(t, bundle.exec, job2.ID, rivertype.JobStateCompleted)
		requireState(t, bundle.exec, job3.ID, rivertype.JobStateDiscarded)
		requireState(t, bundle.exec, job4.ID, rivertype.JobStateAvailable)
		requireState(t, bundle.exec, job5.ID, rivertype.JobStateRetryable)
		requireState(t, bundle.exec, job6.ID, rivertype.JobStateScheduled)
		requireState(t, bundle.exec, job7.ID, rivertype.JobStateAvailable)
	})

	t.Run("Subscription", func(t *testing.T) {
		t.Parallel()

		completer, bundle := setup(t)

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})

		require.NoError(t, completer.JobSetStateIfRunning(ctx, &jobstats.JobStatistics{}, riverdriver.JobSetStateCompleted(job.ID, time.Now(), nil)))

		completer.Stop()

		jobUpdate := riversharedtest.WaitOrTimeout(t, bundle.subscribeCh)
		require.Len(t, jobUpdate, 1)
		require.Equal(t, rivertype.JobStateCompleted, jobUpdate[0].Job.State)
	})

	t.Run("MultipleCycles", func(t *testing.T) {
		t.Parallel()

		completer, bundle := setup(t)

		{
			job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})

			require.NoError(t, completer.JobSetStateIfRunning(ctx, &jobstats.JobStatistics{}, riverdriver.JobSetStateCompleted(job.ID, time.Now(), nil)))

			completer.Stop()

			requireState(t, bundle.exec, job.ID, rivertype.JobStateCompleted)
		}

		// Completer closes the subscribe channel on stop, so we need to reset it between runs.
		completer.ResetSubscribeChan(make(SubscribeChan, 10))

		{
			require.NoError(t, completer.Start(ctx))

			job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})

			require.NoError(t, completer.JobSetStateIfRunning(ctx, &jobstats.JobStatistics{}, riverdriver.JobSetStateCompleted(job.ID, time.Now(), nil)))

			completer.Stop()

			requireState(t, bundle.exec, job.ID, rivertype.JobStateCompleted)
		}
	})

	t.Run("CompletionFailure", func(t *testing.T) {
		t.Parallel()

		completer, bundle := setup(t)

		// The completers will do an exponential backoff sleep while retrying.
		// Make sure to disable it for this test case so the tests stay fast.
		disableSleep(completer)

		var numCalls int
		maybeError := func() error {
			numCalls++
			switch numCalls {
			case 1:
				fallthrough
			case 2:
				return fmt.Errorf("error from executor %d", numCalls)
			}
			return nil
		}

		execMock := NewPartialExecutorMock(bundle.exec)
		execMock.JobSetStateIfRunningManyFunc = func(ctx context.Context, params *riverdriver.JobSetStateIfRunningManyParams) ([]*rivertype.JobRow, error) {
			if err := maybeError(); err != nil {
				return nil, err
			}
			return bundle.exec.JobSetStateIfRunningMany(ctx, params)
		}
		setExec(completer, execMock)

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})

		require.NoError(t, completer.JobSetStateIfRunning(ctx, &jobstats.JobStatistics{}, riverdriver.JobSetStateCompleted(job.ID, time.Now(), nil)))

		completer.Stop()

		// Make sure our mocks were really called.
		require.True(t, execMock.JobSetStateIfRunningManyCalled)

		// Job still managed to complete despite the errors.
		requireState(t, bundle.exec, job.ID, rivertype.JobStateCompleted)
	})

	t.Run("CompletionImmediateFailureOnContextCanceled", func(t *testing.T) { //nolint:dupl
		t.Parallel()

		completer, bundle := setup(t)

		// The completers will do an exponential backoff sleep while retrying.
		// Make sure to disable it for this test case so the tests stay fast.
		disableSleep(completer)

		execMock := NewPartialExecutorMock(bundle.exec)
		execMock.JobSetStateIfRunningManyFunc = func(ctx context.Context, params *riverdriver.JobSetStateIfRunningManyParams) ([]*rivertype.JobRow, error) {
			return nil, context.Canceled
		}
		setExec(completer, execMock)

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})

		err := completer.JobSetStateIfRunning(ctx, &jobstats.JobStatistics{}, riverdriver.JobSetStateCompleted(job.ID, time.Now(), nil))

		// The error returned will be nil for asynchronous completers, but
		// returned immediately for synchronous ones.
		require.True(t, err == nil || errors.Is(err, context.Canceled))

		completer.Stop()

		// Make sure our mocks were really called.
		require.True(t, execMock.JobSetStateIfRunningManyCalled)

		// Job is still running because the completer is forced to give up
		// immediately on certain types of errors like where a pool is closed.
		requireState(t, bundle.exec, job.ID, rivertype.JobStateRunning)
	})

	t.Run("CompletionImmediateFailureOnErrClosedPool", func(t *testing.T) { //nolint:dupl
		t.Parallel()

		completer, bundle := setup(t)

		// The completers will do an exponential backoff sleep while retrying.
		// Make sure to disable it for this test case so the tests stay fast.
		disableSleep(completer)

		execMock := NewPartialExecutorMock(bundle.exec)
		execMock.JobSetStateIfRunningManyFunc = func(ctx context.Context, params *riverdriver.JobSetStateIfRunningManyParams) ([]*rivertype.JobRow, error) {
			return nil, puddle.ErrClosedPool
		}
		setExec(completer, execMock)

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})

		err := completer.JobSetStateIfRunning(ctx, &jobstats.JobStatistics{}, riverdriver.JobSetStateCompleted(job.ID, time.Now(), nil))

		// The error returned will be nil for asynchronous completers, but
		// returned immediately for synchronous ones.
		require.True(t, err == nil || errors.Is(err, puddle.ErrClosedPool))

		completer.Stop()

		// Make sure our mocks were really called.
		require.True(t, execMock.JobSetStateIfRunningManyCalled)

		// Job is still running because the completer is forced to give up
		// immediately on certain types of errors like where a pool is closed.
		requireState(t, bundle.exec, job.ID, rivertype.JobStateRunning)
	})

	// The batch completer supports an interface that lets caller wait for it to
	// start. Make sure this works as expected.
	t.Run("WithStartedWaitsForStarted", func(t *testing.T) {
		t.Parallel()

		completer, _ := setup(t)

		var completerInterface JobCompleter = completer
		if withWait, ok := completerInterface.(startstop.Service); ok {
			riversharedtest.WaitOrTimeout(t, withWait.Started())
		}
	})
}

func BenchmarkAsyncCompleter_Concurrency10(b *testing.B) {
	benchmarkCompleter(b, func(b *testing.B, exec riverdriver.Executor, pilot riverpilot.Pilot, subscribeCh chan<- []CompleterJobUpdated) JobCompleter {
		b.Helper()
		return newAsyncCompleterWithConcurrency(riversharedtest.BaseServiceArchetype(b), exec, pilot, 10, subscribeCh)
	})
}

func BenchmarkAsyncCompleter_Concurrency100(b *testing.B) {
	benchmarkCompleter(b, func(b *testing.B, exec riverdriver.Executor, pilot riverpilot.Pilot, subscribeCh chan<- []CompleterJobUpdated) JobCompleter {
		b.Helper()
		return newAsyncCompleterWithConcurrency(riversharedtest.BaseServiceArchetype(b), exec, pilot, 100, subscribeCh)
	})
}

func BenchmarkBatchCompleter(b *testing.B) {
	benchmarkCompleter(b, func(b *testing.B, exec riverdriver.Executor, pilot riverpilot.Pilot, subscribeCh chan<- []CompleterJobUpdated) JobCompleter {
		b.Helper()
		return NewBatchCompleter(riversharedtest.BaseServiceArchetype(b), exec, pilot, subscribeCh)
	})
}

func BenchmarkInlineCompleter(b *testing.B) {
	benchmarkCompleter(b, func(b *testing.B, exec riverdriver.Executor, pilot riverpilot.Pilot, subscribeCh chan<- []CompleterJobUpdated) JobCompleter {
		b.Helper()
		return NewInlineCompleter(riversharedtest.BaseServiceArchetype(b), exec, pilot, subscribeCh)
	})
}

func benchmarkCompleter(
	b *testing.B,
	newCompleter func(b *testing.B, exec riverdriver.Executor, pilot riverpilot.Pilot, subscribeCh chan<- []CompleterJobUpdated) JobCompleter,
) {
	b.Helper()

	ctx := context.Background()

	type testBundle struct {
		exec  riverdriver.Executor
		jobs  []*rivertype.JobRow
		pilot riverpilot.Pilot
		stats []jobstats.JobStatistics
	}

	setup := func(b *testing.B) (JobCompleter, *testBundle) {
		b.Helper()

		var (
			driver      = riverpgxv5.New(riverinternaltest.TestDB(ctx, b))
			exec        = driver.GetExecutor()
			pilot       = &riverpilot.StandardPilot{}
			subscribeCh = make(chan []CompleterJobUpdated, 100)
			completer   = newCompleter(b, exec, pilot, subscribeCh)
		)

		b.Cleanup(riverinternaltest.DiscardContinuously(subscribeCh))

		require.NoError(b, completer.Start(ctx))
		b.Cleanup(completer.Stop)

		if withWait, ok := completer.(startstop.Service); ok {
			riversharedtest.WaitOrTimeout(b, withWait.Started())
		}

		insertParams := make([]*riverdriver.JobInsertFastParams, b.N)
		for i := range b.N {
			insertParams[i] = &riverdriver.JobInsertFastParams{
				EncodedArgs: []byte(`{}`),
				Kind:        "benchmark_kind",
				MaxAttempts: rivercommon.MaxAttemptsDefault,
				Priority:    rivercommon.PriorityDefault,
				Queue:       rivercommon.QueueDefault,
				State:       rivertype.JobStateRunning,
			}
		}

		_, err := exec.JobInsertFastMany(ctx, &riverdriver.JobInsertFastManyParams{
			Jobs:   insertParams,
			Schema: "",
		})
		require.NoError(b, err)

		jobs, err := exec.JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{
			Kind:   []string{"benchmark_kind"},
			Schema: "",
		})
		require.NoError(b, err)

		return completer, &testBundle{
			exec:  exec,
			jobs:  jobs,
			pilot: pilot,
			stats: make([]jobstats.JobStatistics, b.N),
		}
	}

	b.Run("Completion", func(b *testing.B) {
		completer, bundle := setup(b)

		b.ResetTimer()

		for i := range b.N {
			err := completer.JobSetStateIfRunning(ctx, &bundle.stats[i], riverdriver.JobSetStateCompleted(bundle.jobs[i].ID, time.Now(), nil))
			require.NoError(b, err)
		}

		completer.Stop()
	})

	b.Run("RotatingStates", func(b *testing.B) {
		completer, bundle := setup(b)

		b.ResetTimer()

		for i := range b.N {
			switch i % 7 {
			case 0:
				err := completer.JobSetStateIfRunning(ctx, &bundle.stats[i], riverdriver.JobSetStateCancelled(bundle.jobs[i].ID, time.Now(), []byte("{}"), nil))
				require.NoError(b, err)

			case 1:
				err := completer.JobSetStateIfRunning(ctx, &bundle.stats[i], riverdriver.JobSetStateCompleted(bundle.jobs[i].ID, time.Now(), nil))
				require.NoError(b, err)

			case 2:
				err := completer.JobSetStateIfRunning(ctx, &bundle.stats[i], riverdriver.JobSetStateDiscarded(bundle.jobs[i].ID, time.Now(), []byte("{}"), nil))
				require.NoError(b, err)

			case 3:
				err := completer.JobSetStateIfRunning(ctx, &bundle.stats[i], riverdriver.JobSetStateErrorAvailable(bundle.jobs[i].ID, time.Now(), []byte("{}"), nil))
				require.NoError(b, err)

			case 4:
				err := completer.JobSetStateIfRunning(ctx, &bundle.stats[i], riverdriver.JobSetStateErrorRetryable(bundle.jobs[i].ID, time.Now(), []byte("{}"), nil))
				require.NoError(b, err)

			case 5:
				err := completer.JobSetStateIfRunning(ctx, &bundle.stats[i], riverdriver.JobSetStateSnoozed(bundle.jobs[i].ID, time.Now(), 10, nil))
				require.NoError(b, err)

			case 6:
				err := completer.JobSetStateIfRunning(ctx, &bundle.stats[i], riverdriver.JobSetStateSnoozedAvailable(bundle.jobs[i].ID, time.Now(), 10, nil))
				require.NoError(b, err)

			default:
				panic("unexpected modulo result (did you update cases without changing the modulo divider or vice versa?")
			}
		}

		completer.Stop()
	})
}

// Performs continuous job insertion from a background goroutine. Returns a
// function that should be invoked to stop insertion, which will block until
// insertion stops, then return the total number of jobs that were inserted.
func doContinuousInsertion(ctx context.Context, t *testing.T, completer JobCompleter, exec riverdriver.Executor) func() int {
	t.Helper()

	return doContinuousInsertionInterval(ctx, t, completer, exec, 1*time.Millisecond)
}

func doContinuousInsertionInterval(ctx context.Context, t *testing.T, completer JobCompleter, exec riverdriver.Executor, insertInterval time.Duration) func() int {
	t.Helper()

	var (
		insertionStopped = make(chan struct{})
		numInserted      atomic.Int64
		stopInsertion    = make(chan struct{})
		ticker           = time.NewTicker(insertInterval)
	)
	go func() {
		defer close(insertionStopped)

		defer ticker.Stop()

		defer func() {
			t.Logf("Inserted %d jobs", numInserted.Load())
		}()

		for {
			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})
			require.NoError(t, completer.JobSetStateIfRunning(ctx, &jobstats.JobStatistics{}, riverdriver.JobSetStateCompleted(job.ID, time.Now(), nil)))
			numInserted.Add(1)

			select {
			case <-stopInsertion:
				return
			case <-ticker.C:
			}
		}
	}()

	return func() int {
		close(stopInsertion)
		<-insertionStopped
		return int(numInserted.Load())
	}
}
