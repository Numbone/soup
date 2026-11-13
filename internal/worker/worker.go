// Package worker runs the claim -> execute -> complete loop.
//
// Log lines with a leading tag (CLAIM, DONE, FENCED, ...) are parsed by the chaos harness.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"time"

	"github.com/soup/jobqueue/internal/store"
)

type Config struct {
	ID                string
	Lease             time.Duration // how long a claim is valid without a heartbeat
	PollInterval      time.Duration // sleep when the queue is empty
	ReapInterval      time.Duration
	UnsafeNoFence     bool // negative control for the harness: complete without checking attempt/state
	UnsafeNoHeartbeat bool // negative control for the harness: never extend the lease
}

// Payload is the emulated "training job".
type Payload struct {
	DurationMS int  `json:"duration_ms"`
	Fail       bool `json:"fail"` // deterministic business failure -> state failed, no effect
}

type Worker struct {
	Store *store.Store
	Cfg   Config
	Log   *log.Logger
}

func (w *Worker) Run(ctx context.Context) error {
	// The reaper runs on its own ticker, independent of job execution: a worker busy with a long job
	// must still recover jobs abandoned by dead workers.
	reapDone := make(chan struct{})
	go func() {
		defer close(reapDone)
		t := time.NewTicker(w.Cfg.ReapInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
			requeued, failed, err := w.Store.Reap(ctx)
			if err != nil && ctx.Err() == nil {
				w.Log.Printf("ERROR reap: %v", err)
			} else if requeued+failed > 0 {
				w.Log.Printf("REAP requeued=%d failed=%d", requeued, failed)
			}
		}
	}()
	defer func() { <-reapDone }()

	for ctx.Err() == nil {
		c, err := w.Store.Claim(ctx, w.Cfg.ID, w.Cfg.Lease)
		if err != nil {
			w.Log.Printf("ERROR claim: %v", err)
			sleep(ctx, w.Cfg.PollInterval)
			continue
		}
		if c == nil {
			// jitter so idle workers do not poll in lockstep
			sleep(ctx, w.Cfg.PollInterval/2+rand.N(w.Cfg.PollInterval))
			continue
		}
		w.handle(ctx, c)
	}
	return ctx.Err()
}

func (w *Worker) handle(ctx context.Context, c *store.Claimed) {
	w.Log.Printf("CLAIM job=%d attempt=%d", c.ID, c.Attempt)

	var p Payload
	if err := json.Unmarshal(c.Payload, &p); err != nil {
		w.finishFail(ctx, c, "bad payload: "+err.Error())
		return
	}

	// Heartbeat in the background. If the lease is lost (cancel or reclaim) we abort execution.
	execCtx, stop := context.WithCancelCause(ctx)
	defer stop(nil)
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		t := time.NewTicker(w.Cfg.Lease / 3)
		defer t.Stop()
		for {
			select {
			case <-execCtx.Done():
				return
			case <-t.C:
				if w.Cfg.UnsafeNoHeartbeat {
					continue
				}
				err := w.Store.Heartbeat(execCtx, c, w.Cfg.ID, w.Cfg.Lease)
				if errors.Is(err, store.ErrFenced) {
					stop(store.ErrFenced)
					return
				}
				if err != nil && execCtx.Err() == nil {
					// transient DB error: keep trying; if it persists the lease expires and fencing protects us
					w.Log.Printf("ERROR heartbeat job=%d: %v", c.ID, err)
				}
			}
		}
	}()

	execErr := execute(execCtx, p)
	stop(nil)
	<-hbDone

	if errors.Is(context.Cause(execCtx), store.ErrFenced) {
		w.Log.Printf("LEASE_LOST job=%d attempt=%d (cancelled or reclaimed, aborting)", c.ID, c.Attempt)
		return
	}
	if ctx.Err() != nil {
		return // shutting down: lease will expire and the job will be reaped
	}
	if execErr != nil {
		w.finishFail(ctx, c, execErr.Error())
		return
	}
	err := w.Store.Complete(ctx, c, w.Cfg.ID, w.Cfg.UnsafeNoFence)
	switch {
	case errors.Is(err, store.ErrFenced):
		w.Log.Printf("FENCED job=%d attempt=%d (stale completion rejected)", c.ID, c.Attempt)
	case err != nil:
		// Unknown outcome (e.g. connection died during COMMIT). Do not retry blindly: the lease
		// expires and the job is reaped; fencing guarantees at most one effect either way.
		w.Log.Printf("ERROR complete job=%d attempt=%d: %v", c.ID, c.Attempt, err)
	default:
		w.Log.Printf("DONE job=%d attempt=%d", c.ID, c.Attempt)
	}
}

func (w *Worker) finishFail(ctx context.Context, c *store.Claimed, reason string) {
	err := w.Store.Fail(ctx, c, reason)
	switch {
	case errors.Is(err, store.ErrFenced):
		w.Log.Printf("FENCED job=%d attempt=%d (stale failure rejected)", c.ID, c.Attempt)
	case err != nil:
		w.Log.Printf("ERROR fail job=%d: %v", c.ID, err)
	default:
		w.Log.Printf("FAILED job=%d attempt=%d", c.ID, c.Attempt)
	}
}

// execute emulates training: it just takes time. The observable side effect is written by Complete.
func execute(ctx context.Context, p Payload) error {
	t := time.NewTimer(time.Duration(p.DurationMS) * time.Millisecond)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
	}
	if p.Fail {
		return fmt.Errorf("training diverged (payload.fail=true)")
	}
	return nil
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
