package worker

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/soup/jobqueue/internal/store"
)

// Found in review: the reaper used to run only between jobs, so a worker busy with a long job never
// recovered jobs abandoned by dead workers. With long training jobs that means "stuck in running".
func TestReaperRunsWhileWorkerIsBusy(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = "t_worker" // isolated from the store package tests
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	s := store.New(pool)
	if _, err := pool.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS t_worker; DROP TABLE IF EXISTS effects, job_runs, jobs CASCADE`); err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	// the "dead" worker's job: claimed, lease about to expire, nobody will heartbeat it
	if _, _, err := s.Create(ctx, "abandoned", json.RawMessage(`{"duration_ms":1}`)); err != nil {
		t.Fatal(err)
	}
	if c, err := s.Claim(ctx, "dead-worker", 500*time.Millisecond); err != nil || c == nil {
		t.Fatal(err)
	}
	// the live worker's long job
	if _, _, err := s.Create(ctx, "long", json.RawMessage(`{"duration_ms":10000}`)); err != nil {
		t.Fatal(err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	w := &Worker{Store: s, Log: log.New(io.Discard, "", 0), Cfg: Config{
		ID: "live", Lease: 3 * time.Second, PollInterval: 50 * time.Millisecond, ReapInterval: 200 * time.Millisecond,
	}}
	go func() { _ = w.Run(runCtx) }()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var state string
		_ = pool.QueryRow(ctx, `SELECT state FROM jobs WHERE idempotency_key='abandoned'`).Scan(&state)
		var busy bool
		_ = pool.QueryRow(ctx, `SELECT state='running' AND lease_owner='live' FROM jobs WHERE idempotency_key='long'`).Scan(&busy)
		if state == "queued" && busy {
			return // reaped while the only live worker was busy
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("abandoned job was not reaped while the live worker was busy with a long job")
}
