package store

// Integration tests for the failure modes that produce NO error when they go wrong.
// Each protection is paired with a negative control: a naive implementation run through the same
// test, which MUST show the bug. If a negative control stops failing, the test has lost its teeth.
//
// Requires TEST_DATABASE_URL (see docker-compose `test` service). Tables are wiped.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"regexp"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testStore(t *testing.T, runtimeParams map[string]string) *Store {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 64
	// Own schema per test package: `go test ./...` runs packages in parallel against the same DB,
	// and they must not drop each other's tables (this bit us: see runs/tests-docker-compose-run1-*).
	cfg.ConnConfig.RuntimeParams["search_path"] = "t_store"
	for k, v := range runtimeParams {
		cfg.ConnConfig.RuntimeParams[k] = v
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	s := New(pool)
	if _, err := pool.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS t_store; DROP TABLE IF EXISTS effects, job_runs, jobs CASCADE`); err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return s
}

func mustCreate(t *testing.T, s *Store, n int) []int64 {
	t.Helper()
	ids := make([]int64, n)
	for i := range n {
		j, _, err := s.Create(context.Background(), fmt.Sprintf("k-%d", i), json.RawMessage(`{"duration_ms":1}`))
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = j.ID
	}
	return ids
}

func count(t *testing.T, s *Store, q string, args ...any) int {
	t.Helper()
	var n int
	if err := s.Pool.QueryRow(context.Background(), q, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// ---------------------------------------------------------------- 1. double claim

// 32 concurrent claimers drain 2000 jobs. Every job must be handed out exactly once.
func TestDoubleClaim_SkipLocked(t *testing.T) {
	s := testStore(t, nil)
	mustCreate(t, s, 2000)
	claims := claimConcurrently(t, 32, func(ctx context.Context, w string) (int64, bool, error) {
		c, err := s.Claim(ctx, w, time.Minute)
		if c == nil || err != nil {
			return 0, false, err
		}
		return c.ID, true, nil
	})
	dups := 0
	for _, n := range claims {
		if n > 1 {
			dups++
		}
	}
	t.Logf("claimed %d distinct jobs, %d claimed more than once", len(claims), dups)
	if len(claims) != 2000 || dups != 0 {
		t.Fatalf("want 2000 jobs claimed once each, got %d distinct, %d duplicates", len(claims), dups)
	}
}

// Negative control: read-then-write claim without a row lock and without re-checking state.
func TestDoubleClaim_NaiveSelectThenUpdate_IsBroken(t *testing.T) {
	s := testStore(t, nil)
	mustCreate(t, s, 500)
	claims := claimConcurrently(t, 32, func(ctx context.Context, w string) (int64, bool, error) {
		var id int64
		err := s.Pool.QueryRow(ctx, `SELECT id FROM jobs WHERE state='queued' ORDER BY id LIMIT 1`).Scan(&id)
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, false, nil
		}
		if err != nil {
			return 0, false, err
		}
		_, err = s.Pool.Exec(ctx, `UPDATE jobs SET state='running', attempt=attempt+1, lease_owner=$2,
			lease_until=now()+interval '1 minute' WHERE id=$1`, id, w)
		return id, true, err
	})
	dups := 0
	for _, n := range claims {
		if n > 1 {
			dups++
		}
	}
	t.Logf("NEGATIVE CONTROL: naive claim handed %d of %d jobs to more than one worker", dups, len(claims))
	if dups == 0 {
		t.Fatal("negative control did not reproduce a double claim; the test cannot detect the bug")
	}
}

func claimConcurrently(t *testing.T, workers int, claim func(context.Context, string) (int64, bool, error)) map[int64]int {
	t.Helper()
	var mu sync.Mutex
	got := map[int64]int{}
	var wg sync.WaitGroup
	start := make(chan struct{})
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for {
				id, ok, err := claim(context.Background(), fmt.Sprintf("w%d", w))
				if err != nil {
					t.Error(err)
					return
				}
				if !ok {
					return
				}
				mu.Lock()
				got[id]++
				mu.Unlock()
			}
		}()
	}
	close(start)
	wg.Wait()
	return got
}

// ---------------------------------------------------------------- 2. zombie worker

// Worker A claims, freezes past its lease, the job is reaped and re-claimed by B. A wakes up and
// tries to heartbeat and complete with its stale attempt: both must be rejected and leave no effect.
func TestZombieCannotWriteStaleResult(t *testing.T) {
	s := testStore(t, nil)
	ctx := context.Background()
	mustCreate(t, s, 1)

	a, err := s.Claim(ctx, "A", time.Second)
	if err != nil || a == nil {
		t.Fatal(err)
	}
	time.Sleep(1200 * time.Millisecond) // real expiry by the DB clock, no clock mocking
	if rq, _, err := s.Reap(ctx); err != nil || rq != 1 {
		t.Fatalf("reap: requeued=%d err=%v", rq, err)
	}
	b, err := s.Claim(ctx, "B", time.Minute)
	if err != nil || b == nil || b.Attempt != a.Attempt+1 {
		t.Fatalf("B claim: %+v %v", b, err)
	}

	if err := s.Heartbeat(ctx, a, "A", time.Minute); !errors.Is(err, ErrFenced) {
		t.Fatalf("zombie heartbeat: want ErrFenced, got %v", err)
	}
	if err := s.Complete(ctx, a, "A", false); !errors.Is(err, ErrFenced) {
		t.Fatalf("zombie complete: want ErrFenced, got %v", err)
	}
	if err := s.Fail(ctx, a, "zombie"); !errors.Is(err, ErrFenced) {
		t.Fatalf("zombie fail: want ErrFenced, got %v", err)
	}
	if n := count(t, s, `SELECT count(*) FROM effects`); n != 0 {
		t.Fatalf("zombie wrote %d effects", n)
	}
	if err := s.Complete(ctx, b, "B", false); err != nil {
		t.Fatal(err)
	}
	if n := count(t, s, `SELECT count(*) FROM effects WHERE attempt = $1`, b.Attempt); n != 1 {
		t.Fatalf("want exactly B's effect, got %d", n)
	}
	// and a zombie that comes back AFTER B finished is still rejected
	if err := s.Complete(ctx, a, "A", false); !errors.Is(err, ErrFenced) {
		t.Fatalf("late zombie complete: want ErrFenced, got %v", err)
	}
}

// Negative control: same scenario, completion without the attempt fence -> two effects.
func TestZombie_NoFencing_IsBroken(t *testing.T) {
	s := testStore(t, nil)
	ctx := context.Background()
	mustCreate(t, s, 1)
	a, err := s.Claim(ctx, "A", time.Second)
	if err != nil || a == nil {
		t.Fatalf("claim A: %v", err)
	}
	time.Sleep(1200 * time.Millisecond)
	if _, _, err := s.Reap(ctx); err != nil {
		t.Fatal(err)
	}
	b, err := s.Claim(ctx, "B", time.Minute)
	if err != nil || b == nil {
		t.Fatalf("claim B: %v", err)
	}
	if err := s.Complete(ctx, b, "B", true); err != nil {
		t.Fatal(err)
	}
	_ = s.Complete(ctx, a, "A", true) // zombie
	n := count(t, s, `SELECT count(*) FROM effects`)
	t.Logf("NEGATIVE CONTROL: without fencing the job has %d effects", n)
	if n < 2 {
		t.Fatal("negative control did not reproduce the double effect")
	}
}

// ---------------------------------------------------------------- 3. cancel vs complete race

type raceOutcome struct{ cancelWon, completeWon, both, neither, bad int }

// Fire Cancel and Complete at the same instant, many times. Exactly one must win, and the final
// state must agree with the effects table.
func TestCancelCompleteRace(t *testing.T) {
	s := testStore(t, nil)
	out := runCancelCompleteRace(t, s, 400, s.Cancel, func(ctx context.Context, c *Claimed) error {
		return s.Complete(ctx, c, "w", false)
	})
	t.Logf("cancel won=%d complete won=%d both=%d neither=%d inconsistent=%d", out.cancelWon, out.completeWon, out.both, out.neither, out.bad)
	if out.both != 0 || out.neither != 0 || out.bad != 0 {
		t.Fatalf("race broken: %+v", out)
	}
	if out.cancelWon == 0 || out.completeWon == 0 {
		t.Logf("WARNING: race only ever went one way; timing may not exercise both orders")
	}
}

// Negative control: check-then-act (SELECT state, then UPDATE unconditionally) in both operations.
func TestCancelCompleteRace_CheckThenAct_IsBroken(t *testing.T) {
	s := testStore(t, nil)
	naiveCancel := func(ctx context.Context, id int64) (*Job, error) {
		j, err := s.Get(ctx, id)
		if err != nil {
			return nil, err
		}
		if j.State != StateQueued && j.State != StateRunning {
			return j, ErrNotCancellable
		}
		time.Sleep(time.Millisecond) // widen the window, as a GC pause or slow network would
		_, err = s.Pool.Exec(ctx, `UPDATE jobs SET state='cancelled', lease_owner=NULL, lease_until=NULL, finished_at=now() WHERE id=$1`, id)
		return j, err
	}
	naiveComplete := func(ctx context.Context, c *Claimed) error {
		j, err := s.Get(ctx, c.ID)
		if err != nil {
			return err
		}
		if j.State != StateRunning {
			return ErrFenced
		}
		time.Sleep(time.Millisecond)
		if _, err := s.Pool.Exec(ctx, `INSERT INTO effects (job_id, attempt, worker_id) VALUES ($1,$2,'w')`, c.ID, c.Attempt); err != nil {
			return err
		}
		_, err = s.Pool.Exec(ctx, `UPDATE jobs SET state='succeeded', lease_owner=NULL, lease_until=NULL, finished_at=now() WHERE id=$1`, c.ID)
		return err
	}
	out := runCancelCompleteRace(t, s, 200, naiveCancel, naiveComplete)
	// "both" = the client got 200 cancelled AND the job ran to success: a silently lost cancel.
	t.Logf("NEGATIVE CONTROL: both reported success (lost cancel)=%d, final state disagrees with effects=%d", out.both, out.bad)
	if out.both == 0 && out.bad == 0 {
		t.Fatal("negative control did not reproduce the race")
	}
}

func runCancelCompleteRace(t *testing.T, s *Store, n int,
	cancel func(context.Context, int64) (*Job, error),
	complete func(context.Context, *Claimed) error) raceOutcome {
	t.Helper()
	ctx := context.Background()
	mustCreate(t, s, n)
	var out raceOutcome
	for range n {
		c, err := s.Claim(ctx, "w", time.Minute)
		if err != nil || c == nil {
			t.Fatal(err)
		}
		var cancelErr, completeErr error
		var wg sync.WaitGroup
		start := make(chan struct{})
		wg.Add(2)
		delay := time.Duration(rand.IntN(3000)) * time.Microsecond // vary who gets there first
		go func() { defer wg.Done(); <-start; time.Sleep(delay); _, cancelErr = cancel(ctx, c.ID) }()
		go func() { defer wg.Done(); <-start; completeErr = complete(ctx, c) }()
		close(start)
		wg.Wait()
		for _, e := range []error{cancelErr, completeErr} {
			if e != nil && !errors.Is(e, ErrNotCancellable) && !errors.Is(e, ErrFenced) {
				t.Fatalf("unexpected error: %v", e)
			}
		}
		switch {
		case cancelErr == nil && completeErr == nil:
			out.both++
		case cancelErr == nil:
			out.cancelWon++
		case completeErr == nil:
			out.completeWon++
		default:
			out.neither++
		}
		j, _ := s.Get(ctx, c.ID)
		effects := count(t, s, `SELECT count(*) FROM effects WHERE job_id=$1`, c.ID)
		consistent := (j.State == StateSucceeded && effects == 1) || (j.State == StateCancelled && effects == 0)
		if !consistent {
			out.bad++
		}
	}
	return out
}

// ---------------------------------------------------------------- 4. duplicate on request retry

// 50 concurrent creates with the same key (a client retrying aggressively): one row, one "created".
func TestIdempotentCreate_ConcurrentRetries(t *testing.T) {
	s := testStore(t, nil)
	ctx := context.Background()
	for round := range 50 {
		key := fmt.Sprintf("retry-%d", round)
		var created atomic.Int32
		ids := sync.Map{}
		var wg sync.WaitGroup
		start := make(chan struct{})
		for range 50 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				// different key order + whitespace: must be treated as the same request
				p := `{"a":1,"b":2}`
				if round%2 == 0 {
					p = `{ "b": 2, "a": 1 }`
				}
				j, c, err := s.Create(ctx, key, json.RawMessage(p))
				if err != nil {
					t.Error(err)
					return
				}
				if c {
					created.Add(1)
				}
				ids.Store(j.ID, true)
			}()
		}
		close(start)
		wg.Wait()
		nIDs := 0
		ids.Range(func(any, any) bool { nIDs++; return true })
		if created.Load() != 1 || nIDs != 1 {
			t.Fatalf("key %s: created=%d distinct ids=%d", key, created.Load(), nIDs)
		}
	}
	if n := count(t, s, `SELECT count(*) FROM jobs`); n != 50 {
		t.Fatalf("want 50 rows, got %d", n)
	}
	// same key, different payload must not silently return the old job
	if _, _, err := s.Create(ctx, "retry-0", json.RawMessage(`{"a":999}`)); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("want ErrIdempotencyConflict, got %v", err)
	}
}

// Negative control: "SELECT, and INSERT if missing" without a unique constraint creates duplicates.
func TestIdempotentCreate_CheckThenInsert_IsBroken(t *testing.T) {
	s := testStore(t, nil)
	ctx := context.Background()
	if _, err := s.Pool.Exec(ctx, `DROP TABLE IF EXISTS naive_jobs; CREATE TABLE naive_jobs (key text)`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = s.Pool.Exec(context.Background(), `DROP TABLE IF EXISTS naive_jobs`) })
	// All 50 "requests" do their existence check first, then all insert: a barrier makes the
	// interleaving deterministic instead of hoping the scheduler produces it (an earlier version
	// relied on timing and passed vacuously in some runs - see runs/tests-*).
	var checked, wg sync.WaitGroup
	checked.Add(50)
	release := make(chan struct{})
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var n int
			_ = s.Pool.QueryRow(ctx, `SELECT count(*) FROM naive_jobs WHERE key='k'`).Scan(&n)
			checked.Done()
			<-release
			if n == 0 {
				_, _ = s.Pool.Exec(ctx, `INSERT INTO naive_jobs VALUES ('k')`)
			}
		}()
	}
	checked.Wait()
	close(release)
	wg.Wait()
	n := count(t, s, `SELECT count(*) FROM naive_jobs`)
	t.Logf("NEGATIVE CONTROL: check-then-insert produced %d rows for one key", n)
	if n < 2 {
		t.Fatal("negative control did not reproduce duplicate creation")
	}
}

// ---------------------------------------------------------------- 5. isolation level

// Our transitions are conditional single-statement UPDATEs and rely on READ COMMITTED's
// re-evaluation of the WHERE clause on the latest row version. Under REPEATABLE READ the loser of a
// race gets SQLSTATE 40001 instead of "0 rows". Correctness must still hold (no double effect,
// consistent state), but the loser now sees an error, which the API would turn into a 500.
// This test documents that: the system is correct at RR but noisy; READ COMMITTED is the intended level.
func TestIsolation_RepeatableRead_StillSafeButLoserErrors(t *testing.T) {
	s := testStore(t, map[string]string{"default_transaction_isolation": "repeatable read"})
	ctx := context.Background()
	var lvl string
	_ = s.Pool.QueryRow(ctx, `SHOW transaction_isolation`).Scan(&lvl)
	if lvl != "repeatable read" {
		t.Fatalf("isolation not applied: %s", lvl)
	}
	n := 300
	mustCreate(t, s, n)
	var serialization, bad int
	for range n {
		c, err := s.Claim(ctx, "w", time.Minute)
		if err != nil || c == nil {
			t.Fatal(err)
		}
		var cancelErr, completeErr error
		var wg sync.WaitGroup
		start := make(chan struct{})
		wg.Add(2)
		delay := time.Duration(rand.IntN(3000)) * time.Microsecond
		go func() { defer wg.Done(); <-start; time.Sleep(delay); _, cancelErr = s.Cancel(ctx, c.ID) }()
		go func() { defer wg.Done(); <-start; completeErr = s.Complete(ctx, c, "w", false) }()
		close(start)
		wg.Wait()
		for _, e := range []error{cancelErr, completeErr} {
			if IsSerializationError(e) {
				serialization++
			}
		}
		j, _ := s.Get(ctx, c.ID)
		effects := count(t, s, `SELECT count(*) FROM effects WHERE job_id=$1`, c.ID)
		if !((j.State == StateSucceeded && effects == 1) || (j.State == StateCancelled && effects == 0)) {
			bad++
		}
	}
	t.Logf("REPEATABLE READ: %d/%d races surfaced a serialization error to the loser; inconsistent=%d", serialization, n, bad)
	if bad != 0 {
		t.Fatalf("inconsistent final state under repeatable read: %d", bad)
	}
}

// ---------------------------------------------------------------- 6. clock skew

// Workers never send a timestamp: lease_until is computed and compared with the DB's now().
// Guard: no time.Now() inside SQL-building store code (worker-side time.Now is only used for local
// pacing, never written to the DB).
func TestClockSkew_StoreUsesOnlyDBClock(t *testing.T) {
	src, err := os.ReadFile("store.go")
	if err != nil {
		t.Fatal(err)
	}
	if m := regexp.MustCompile(`time\.Now\(`).FindAll(src, -1); len(m) > 0 {
		t.Fatalf("store.go uses the local clock %d times; leases must use DB now()", len(m))
	}
	// and the lease actually lands relative to the DB clock
	s := testStore(t, nil)
	ctx := context.Background()
	mustCreate(t, s, 1)
	if _, err := s.Claim(ctx, "w", 5*time.Second); err != nil {
		t.Fatal(err)
	}
	var secs float64
	if err := s.Pool.QueryRow(ctx, `SELECT extract(epoch FROM lease_until - now()) FROM jobs`).Scan(&secs); err != nil {
		t.Fatal(err)
	}
	if secs < 4 || secs > 5.5 {
		t.Fatalf("lease relative to DB clock = %.2fs, want ~5s", secs)
	}
}

// ---------------------------------------------------------------- 7. payload canonicalization (found in review)

// Numbers beyond float64 precision must not be silently rounded: that would both corrupt the stored
// payload and make a DIFFERENT payload look like a replay of the same request.
func TestCanonicalHash_KeepsExactNumbers(t *testing.T) {
	h1, c1, err := CanonicalHash(json.RawMessage(`{"n":9007199254740993}`))
	if err != nil {
		t.Fatal(err)
	}
	h2, _, _ := CanonicalHash(json.RawMessage(`{"n":9007199254740992}`))
	if h1 == h2 {
		t.Fatal("different large integers hash equal: precision lost")
	}
	if string(c1) != `{"n":9007199254740993}` {
		t.Fatalf("canonical payload altered: %s", c1)
	}
	h3, _, _ := CanonicalHash(json.RawMessage(` { "n" : 9007199254740993 } `))
	if h1 != h3 {
		t.Fatal("whitespace changed the hash")
	}
	if _, _, err := CanonicalHash(json.RawMessage(`{"a":1} {"b":2}`)); err == nil {
		t.Fatal("trailing data accepted")
	}
}
