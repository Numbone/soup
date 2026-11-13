# Design: training-job queue

## Storage: Postgres, not SQLite

Workers run as separate processes and get `kill -9`'d. The design depends on three Postgres features:

- **row locks with `FOR UPDATE SKIP LOCKED`**, so N claimers can work in parallel without blocking each other;
- **concurrent writers.** SQLite allows one writer per file, so 8 workers plus the API would serialize on a single lock;
- **a single authoritative clock (`now()`)** that every lease is measured against.

SQLite would work for a single process. It does not fit "several workers across machines".

## Data model

`jobs(id, idempotency_key UNIQUE, request_hash, payload, state, attempt, max_attempts, lease_owner, lease_until, ...)`

- `attempt` is the **fencing token**. It is incremented on every claim and nowhere else.
- `CHECK ((state='running') = (lease_until IS NOT NULL AND lease_owner IS NOT NULL))`: the database itself rejects a running job without a lease, and a terminal job that still has one.
- `job_runs`: one row per claim, inserted in the claim transaction. This counts real executions.
- `effects`: the emulated side effect, inserted in the same transaction that sets `succeeded`.

States: `queued → running → succeeded | failed | cancelled`. A lease expiry adds one more transition, `running → queued`, until `max_attempts` is reached; after that the job goes to `failed`.

## How a worker claims a job

One transaction:

```sql
WITH next AS (SELECT id FROM jobs WHERE state='queued' ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1)
UPDATE jobs SET state='running', attempt=attempt+1, lease_owner=$me, lease_until=now()+$lease
FROM next WHERE jobs.id = next.id RETURNING id, attempt, payload;
INSERT INTO job_runs(job_id, attempt, worker_id) ...;
```

The row lock makes the claim exclusive. `SKIP LOCKED` lets other claimers move on to the next row instead of waiting. Under READ COMMITTED, Postgres re-checks `state='queued'` against the latest row version after taking the lock, so a row that was claimed or cancelled concurrently is skipped. The worker holds `(id, attempt)`, and every later write is conditioned on it.

## How a dead worker is detected

The worker gets a **lease**: 3 s in the harness, which should be 30–60 s for real training. It heartbeats every `lease/3` with `UPDATE ... SET lease_until = now()+lease WHERE id=$id AND attempt=$attempt AND state='running'`.

A worker that was `kill -9`'d, partitioned, or frozen stops heartbeating. Every worker runs a **reaper** once a second: expired `running` jobs go back to `queued`, or to `failed` once `attempt >= max_attempts` (poison-job protection). All lease arithmetic uses the DB's `now()`. Workers never send a timestamp, so skew between worker clocks has no effect.

Detection is not instant. A dead worker's job waits up to `lease + reap interval` before it is re-queued.

## What happens when a running job is cancelled

`UPDATE jobs SET state='cancelled', lease=NULL WHERE id=$id AND state IN ('queued','running')`. Cancel revokes the lease immediately. The worker learns about it in one of two ways:

- its next heartbeat matches 0 rows, and it aborts the execution (`LEASE_LOST`);
- or it finishes first, and its fenced `Complete` matches 0 rows, so the transaction rolls back without writing the effect (`FENCED`).

Cancel and complete are both compare-and-set on the same row, so exactly one of them wins. Cancelling an already-cancelled job returns 200. Cancelling a succeeded or failed job returns 409, because history is not rewritten.

Cost: the worker may keep computing for up to `lease/3` after the cancel. That compute is wasted but harmless.

## Guarantee

- **Execution is at-least-once.** A job can start more than once: the worker dies mid-run, or freezes past its lease. The harness reports how often this happens (`runs-per-job histogram`).
- **The effect is exactly-once**, for an effect that lives in the same database. It is written in the same transaction as `state='succeeded'`, and that transaction is fenced on `(id, attempt, state='running')`. A zombie, a cancelled job, or a duplicate completion matches 0 rows, and the effect insert never commits.

Why exactly-once is not possible in general: for an effect **outside** this database (writing a model to S3, calling a webhook) the "do it" and the "record that it was done" cannot be made atomic. Then the honest guarantee is at-least-once execution plus an idempotent effect, keyed by `job_id` (for example a deterministic artifact path), with the attempt token checked at commit.

`effects` deliberately has **no** `UNIQUE(job_id)`. The harness has to measure the protocol, not a constraint that would hide a broken one. In production I would add the constraint as a second line of defence.

## Idempotent create

`INSERT ... ON CONFLICT (idempotency_key) DO NOTHING RETURNING`, followed by a `SELECT` on conflict. With a concurrent duplicate, the unique index makes the second inserter wait until the first commits. The second inserter's `SELECT` then sees the winner's row: under READ COMMITTED each statement takes a fresh snapshot. The payload is compared through a canonical hash:

- same key, same payload → 200 with the original job;
- same key, different payload → 409.

## Isolation level

READ COMMITTED. Every transition is a single conditional `UPDATE`, which is safe at this level. At REPEATABLE READ the system is still correct, but the loser of a race gets `40001` instead of "0 rows", and the API would turn that into a 500 (see `TestIsolation_RepeatableRead...`).

## Why no Kafka, and where the outbox is

The core problem here is mutable per-job state: cancel, status, idempotent create, and a commit that must happen exactly once. A log-based broker does not provide that state. It would have to be rebuilt on top, and consumer offsets are committed separately from the effect, which gives at-least-once delivery. At 100 rps the load is about 400 short transactions per second, far below Postgres limits.

A broker is worth adding when DB throughput becomes the bottleneck, or when many independent consumers need job events.

The transactional-outbox idea is already the core of `Complete`: the effect row is committed in the same transaction as the state change. If downstream services need a "job finished" event, the next step is:

- an `outbox` table written in that same transaction;
- a relay that publishes it and then marks it sent. This is at-least-once, because the relay can crash between publish and mark;
- consumers that dedupe by `event_id`.

## Known weak points

- **The DB is a single point of failure.** Failover to a replica with a skewed clock would shift every lease.
- **Polling:** idle workers query every ~100 ms. Fine for 8 workers; for 100s of workers, use `LISTEN/NOTIFY` or back off.
- **No retention.** At 100 rps the tables grow by ~8.6M rows a day.
- **A frozen worker cannot be stopped.** It keeps running until it next talks to the DB. Fencing makes it harmless, but it still burns GPU time.
- **Expired but not yet reaped is still valid.** A worker whose lease expired but was not yet reaped can still commit. That is safe: nobody else holds a newer attempt. For an *external* effect you would also check `lease_until > now()` at commit time.
- **`max_attempts` vs crash rate.** If workers die at rate λ, one attempt of a job of length D survives with probability e^(−λD). Under the harness's kill rate, 6–9 s jobs sometimes needed 3–5 attempts. Real multi-hour training needs checkpointing, and an attempt budget based on crashes of the *job*, not on lease expiries.
- **Keyset pagination is not a snapshot.** Ids are allocated before commit, so a slow insert with a lower id can be missed by a client already past it. The same goes for jobs that change state between pages when filtering by state. This is fine for a UI, but not for a consumer that has to see every job.
- **Ids are not dense.** `INSERT ... ON CONFLICT DO NOTHING` consumes a sequence value even on a replay, so gaps in ids are normal.

## Review findings (independent AI review) that changed the code

- **The reaper only ran between jobs.** A worker busy with a long job therefore never recovered jobs left behind by dead workers, so with long training jobs a job could stay stuck in `running`. Fixed: the reaper now runs on its own ticker. Regression test: `TestReaperRunsWhileWorkerIsBusy`.
- **Payload canonicalization went through float64.** `9007199254740993` was stored as `…992`, and a different payload was treated as a replay. Fixed with `UseNumber`. Test: `TestCanonicalHash_KeepsExactNumbers`.
- **The harness could pass without the chaos doing anything** (no coverage checks), heartbeats were never exercised (all jobs were shorter than one heartbeat tick), and spurious lease losses could hide as "failed: max attempts". Fixed with long jobs, coverage checks `C`, the invariant `I8` (lease expiries ≤ kills + stalls), and a second negative control (`-unsafe-no-heartbeat`).
