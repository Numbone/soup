// Package store holds every state transition of a job. All of them are single conditional
// UPDATEs (compare-and-set on state/attempt) so they are correct under READ COMMITTED without
// explicit locking, and all time comparisons use the database clock (now()).
package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schemaSQL string

const (
	StateQueued    = "queued"
	StateRunning   = "running"
	StateSucceeded = "succeeded"
	StateFailed    = "failed"
	StateCancelled = "cancelled"
)

var (
	ErrNotFound            = errors.New("job not found")
	ErrIdempotencyConflict = errors.New("idempotency key reused with a different payload")
	// ErrFenced: the caller no longer owns this attempt (lease reclaimed, job cancelled, or already finished).
	ErrFenced = errors.New("fenced: attempt no longer owns the job")
	// ErrNotCancellable: job is already in a terminal state other than cancelled.
	ErrNotCancellable = errors.New("job already finished")
)

type Job struct {
	ID             int64           `json:"id"`
	IdempotencyKey string          `json:"idempotency_key"`
	Payload        json.RawMessage `json:"payload"`
	State          string          `json:"state"`
	Attempt        int             `json:"attempt"`
	MaxAttempts    int             `json:"max_attempts"`
	LeaseOwner     *string         `json:"lease_owner,omitempty"`
	LeaseUntil     *time.Time      `json:"lease_until,omitempty"`
	Error          *string         `json:"error,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
	FinishedAt     *time.Time      `json:"finished_at,omitempty"`
}

const jobCols = `id, idempotency_key, payload, state, attempt, max_attempts, lease_owner, lease_until, error, created_at, updated_at, finished_at`

func scanJob(row pgx.Row) (*Job, error) {
	var j Job
	err := row.Scan(&j.ID, &j.IdempotencyKey, &j.Payload, &j.State, &j.Attempt, &j.MaxAttempts,
		&j.LeaseOwner, &j.LeaseUntil, &j.Error, &j.CreatedAt, &j.UpdatedAt, &j.FinishedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return &j, err
}

type Store struct {
	Pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store { return &Store{Pool: pool} }

func Connect(ctx context.Context, url string, maxConns int32) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	return pgxpool.NewWithConfig(ctx, cfg)
}

func (s *Store) Migrate(ctx context.Context) error {
	_, err := s.Pool.Exec(ctx, schemaSQL)
	return err
}

// CanonicalHash hashes the payload after a decode/encode round trip, so key order and
// whitespace do not matter (encoding/json sorts map keys). UseNumber keeps numbers as their exact
// text: without it 9007199254740993 would become a float64 and silently change to ...992.
// Duplicate object keys collapse to the last one (same as jsonb).
func CanonicalHash(payload json.RawMessage) (string, json.RawMessage, error) {
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return "", nil, fmt.Errorf("payload is not valid JSON: %w", err)
	}
	if dec.More() {
		return "", nil, errors.New("payload is not valid JSON: trailing data")
	}
	canon, err := json.Marshal(v)
	if err != nil {
		return "", nil, err
	}
	sum := sha256.Sum256(canon)
	return hex.EncodeToString(sum[:]), canon, nil
}

// Create inserts a job or returns the existing one for the same idempotency key.
// created=false means this was a replay. Concurrent creates with the same key are serialized by the
// unique index: the loser blocks until the winner commits, then ON CONFLICT DO NOTHING; the follow-up
// SELECT (new snapshot per statement under READ COMMITTED) sees the winner's row.
func (s *Store) Create(ctx context.Context, key string, payload json.RawMessage) (job *Job, created bool, err error) {
	hash, canon, err := CanonicalHash(payload)
	if err != nil {
		return nil, false, err
	}
	for range 3 {
		job, err = scanJob(s.Pool.QueryRow(ctx, `
			INSERT INTO jobs (idempotency_key, request_hash, payload, state)
			VALUES ($1, $2, $3, 'queued')
			ON CONFLICT (idempotency_key) DO NOTHING
			RETURNING `+jobCols, key, hash, canon))
		if err == nil {
			return job, true, nil
		}
		if !errors.Is(err, ErrNotFound) {
			return nil, false, err
		}
		var existingHash string
		job, err = scanJobWithHash(s.Pool.QueryRow(ctx,
			`SELECT `+jobCols+`, request_hash FROM jobs WHERE idempotency_key = $1`, key), &existingHash)
		if errors.Is(err, ErrNotFound) {
			continue // the conflicting row was rolled back between our two statements; try again
		}
		if err != nil {
			return nil, false, err
		}
		if existingHash != hash {
			return job, false, ErrIdempotencyConflict
		}
		return job, false, nil
	}
	return nil, false, errors.New("create: idempotency key kept disappearing")
}

func scanJobWithHash(row pgx.Row, hash *string) (*Job, error) {
	var j Job
	err := row.Scan(&j.ID, &j.IdempotencyKey, &j.Payload, &j.State, &j.Attempt, &j.MaxAttempts,
		&j.LeaseOwner, &j.LeaseUntil, &j.Error, &j.CreatedAt, &j.UpdatedAt, &j.FinishedAt, hash)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return &j, err
}

func (s *Store) Get(ctx context.Context, id int64) (*Job, error) {
	return scanJob(s.Pool.QueryRow(ctx, `SELECT `+jobCols+` FROM jobs WHERE id = $1`, id))
}

// List uses keyset pagination (id > cursor). Unlike OFFSET it never shows a row twice because of
// inserts. It is NOT a consistent snapshot: ids are allocated before commit, so a job whose insert
// commits after a page was served with a higher cursor is never listed by that walk, and with a
// state filter a job that changes state between pages can be skipped. Fine for a UI listing; not
// for "process every job exactly once" consumers.
func (s *Store) List(ctx context.Context, state string, afterID int64, limit int) ([]*Job, error) {
	rows, err := s.Pool.Query(ctx, `
		SELECT `+jobCols+` FROM jobs
		WHERE id > $1 AND ($2 = '' OR state = $2)
		ORDER BY id
		LIMIT $3`, afterID, state, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*Job{}
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// Cancel moves queued/running -> cancelled. For a running job this revokes the lease: the worker's
// next heartbeat or its completion will hit ErrFenced, so a cancelled job can never get an effect.
// Cancelling an already cancelled job is a no-op success.
func (s *Store) Cancel(ctx context.Context, id int64) (*Job, error) {
	job, err := scanJob(s.Pool.QueryRow(ctx, `
		UPDATE jobs SET state = 'cancelled', lease_owner = NULL, lease_until = NULL,
		       finished_at = now(), updated_at = now()
		WHERE id = $1 AND state IN ('queued', 'running')
		RETURNING `+jobCols, id))
	if err == nil {
		return job, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return nil, err
	}
	job, err = s.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if job.State == StateCancelled {
		return job, nil
	}
	return job, ErrNotCancellable
}

// Claimed is what a worker holds while executing: (ID, Attempt) is the fencing token.
type Claimed struct {
	ID      int64
	Attempt int
	Payload json.RawMessage
}

// Claim atomically takes the oldest queued job. SKIP LOCKED lets concurrent workers pass over rows
// another claimer has locked instead of queueing behind it; the row lock plus the state='queued'
// re-check (EvalPlanQual under READ COMMITTED) guarantees a single winner per attempt.
// The job_runs row is written in the same transaction, so every execution start is counted.
func (s *Store) Claim(ctx context.Context, workerID string, lease time.Duration) (*Claimed, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	var c Claimed
	err = tx.QueryRow(ctx, `
		WITH next AS (
			SELECT id FROM jobs
			WHERE state = 'queued'
			ORDER BY id
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		UPDATE jobs j
		SET state = 'running', attempt = j.attempt + 1, lease_owner = $1,
		    lease_until = now() + make_interval(secs => $2), updated_at = now()
		FROM next WHERE j.id = next.id
		RETURNING j.id, j.attempt, j.payload`, workerID, lease.Seconds()).Scan(&c.ID, &c.Attempt, &c.Payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO job_runs (job_id, attempt, worker_id) VALUES ($1, $2, $3)`,
		c.ID, c.Attempt, workerID); err != nil {
		return nil, err
	}
	return &c, tx.Commit(ctx)
}

// Heartbeat extends the lease. ErrFenced means stop working: the job was cancelled or reclaimed.
func (s *Store) Heartbeat(ctx context.Context, c *Claimed, workerID string, lease time.Duration) error {
	tag, err := s.Pool.Exec(ctx, `
		UPDATE jobs SET lease_until = now() + make_interval(secs => $4), updated_at = now()
		WHERE id = $1 AND attempt = $2 AND lease_owner = $3 AND state = 'running'`,
		c.ID, c.Attempt, workerID, lease.Seconds())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrFenced
	}
	return nil
}

// Complete deliberately does not check lease_until: a worker whose lease expired but was not yet
// reaped still holds the only valid attempt, so committing is safe for an in-DB effect.
//
// Complete writes the side effect and marks the job succeeded in ONE transaction, fenced on
// (id, attempt, state='running'). A zombie, a cancelled job, or a double completion updates 0 rows,
// the transaction rolls back and no effect is written. This is what makes the effect exactly-once.
//
// unsafeNoFence exists only so the chaos harness can prove it detects the bug (negative control).
func (s *Store) Complete(ctx context.Context, c *Claimed, workerID string, unsafeNoFence bool) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	q := `UPDATE jobs SET state = 'succeeded', lease_owner = NULL, lease_until = NULL,
	             finished_at = now(), updated_at = now()
	      WHERE id = $1 AND attempt = $2 AND state = 'running'`
	args := []any{c.ID, c.Attempt}
	if unsafeNoFence {
		q = `UPDATE jobs SET state = 'succeeded', lease_owner = NULL, lease_until = NULL,
		            finished_at = now(), updated_at = now()
		     WHERE id = $1`
		args = []any{c.ID}
	}
	tag, err := tx.Exec(ctx, q, args...)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrFenced
	}
	if _, err := tx.Exec(ctx, `INSERT INTO effects (job_id, attempt, worker_id) VALUES ($1, $2, $3)`,
		c.ID, c.Attempt, workerID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Fail marks the job failed (business failure, not retried). Same fencing as Complete.
func (s *Store) Fail(ctx context.Context, c *Claimed, reason string) error {
	tag, err := s.Pool.Exec(ctx, `
		UPDATE jobs SET state = 'failed', error = $3, lease_owner = NULL, lease_until = NULL,
		       finished_at = now(), updated_at = now()
		WHERE id = $1 AND attempt = $2 AND state = 'running'`, c.ID, c.Attempt, reason)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrFenced
	}
	return nil
}

// Reap handles dead workers: running jobs whose lease expired (by the DB clock) go back to queued,
// or to failed once max_attempts is exhausted (poison-job protection). Any worker may run it; it is
// idempotent and safe to run concurrently (each row is a conditional UPDATE).
func (s *Store) Reap(ctx context.Context) (requeued, failed int64, err error) {
	tag, err := s.Pool.Exec(ctx, `
		UPDATE jobs SET state = 'failed', error = 'lease expired: max attempts exhausted',
		       lease_owner = NULL, lease_until = NULL, finished_at = now(), updated_at = now()
		WHERE state = 'running' AND lease_until < now() AND attempt >= max_attempts`)
	if err != nil {
		return 0, 0, err
	}
	failed = tag.RowsAffected()
	tag, err = s.Pool.Exec(ctx, `
		UPDATE jobs SET state = 'queued', lease_owner = NULL, lease_until = NULL, updated_at = now()
		WHERE state = 'running' AND lease_until < now() AND attempt < max_attempts`)
	if err != nil {
		return 0, failed, err
	}
	return tag.RowsAffected(), failed, nil
}

// IsSerializationError reports SQLSTATE 40001/40P01 (only possible above READ COMMITTED or on deadlock).
func IsSerializationError(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && (pgErr.Code == "40001" || pgErr.Code == "40P01")
}
