// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.27.0
// source: copyfrom.go

package dbsqlc

import (
	"context"
)

// iteratorForJobInsertFastManyCopyFrom implements pgx.CopyFromSource.
type iteratorForJobInsertFastManyCopyFrom struct {
	rows                 []*JobInsertFastManyCopyFromParams
	skippedFirstNextCall bool
}

func (r *iteratorForJobInsertFastManyCopyFrom) Next() bool {
	if len(r.rows) == 0 {
		return false
	}
	if !r.skippedFirstNextCall {
		r.skippedFirstNextCall = true
		return true
	}
	r.rows = r.rows[1:]
	return len(r.rows) > 0
}

func (r iteratorForJobInsertFastManyCopyFrom) Values() ([]interface{}, error) {
	return []interface{}{
		r.rows[0].Args,
		r.rows[0].FinalizedAt,
		r.rows[0].Kind,
		r.rows[0].MaxAttempts,
		r.rows[0].Metadata,
		r.rows[0].Priority,
		r.rows[0].Queue,
		r.rows[0].ScheduledAt,
		r.rows[0].State,
		r.rows[0].Tags,
		r.rows[0].UniqueKey,
		r.rows[0].UniqueStates,
	}, nil
}

func (r iteratorForJobInsertFastManyCopyFrom) Err() error {
	return nil
}

func (q *Queries) JobInsertFastManyCopyFrom(ctx context.Context, db DBTX, arg []*JobInsertFastManyCopyFromParams) (int64, error) {
	return db.CopyFrom(ctx, []string{"river_job"}, []string{"args", "finalized_at", "kind", "max_attempts", "metadata", "priority", "queue", "scheduled_at", "state", "tags", "unique_key", "unique_states"}, &iteratorForJobInsertFastManyCopyFrom{rows: arg})
}
