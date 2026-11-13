# Training-job queue: will it survive production?

This is a Go + Postgres job queue. It has an idempotent create, a status endpoint, keyset-paginated listing and cancellation, and runs N worker processes. With it come a chaos harness that kills workers with `kill -9`, freezes them into zombies, and kills Postgres, plus integration tests for each silent-failure mode. Every one of those tests has a negative control: a deliberately broken implementation that the test must catch.

- Design note: [DESIGN.md](DESIGN.md)
- Raw output of every run, including the failed ones: [runs/](runs/)
- Where AI was used: [AI_USAGE.md](AI_USAGE.md)

## Run

```bash
docker compose up --build                 # postgres + api on :8080 + 4 workers
docker compose run --rm test              # integration tests (silent failures), needs only docker
scripts/chaos.sh my-label                 # chaos harness: 10k jobs, 8 workers, kill -9 every ~2s
scripts/chaos.sh my-label -jobs 3000 -worker-args "-unsafe-no-fence"   # negative control
```

```bash
curl -XPOST localhost:8080/jobs -H 'Idempotency-Key: k1' -d '{"payload":{"duration_ms":3000}}'  # 201, replay -> 200, other payload -> 409
curl localhost:8080/jobs/1
curl 'localhost:8080/jobs?state=succeeded&limit=50&cursor=<next_cursor>'
curl -XPOST localhost:8080/jobs/1/cancel                                                      # 200, or 409 if already finished
```

**Layout:**

- `internal/store`: all SQL and every state transition, plus the silent-failure tests;
- `internal/worker`: the claim, heartbeat, complete loop and the reaper;
- `internal/api`: HTTP;
- `cmd/chaos`: the harness.

## How it works (short version; details in DESIGN.md)

- **Claim:** `FOR UPDATE SKIP LOCKED` on the oldest `queued` row. `attempt += 1` is the fencing token.
- **Dead worker:** each claim comes with a lease, extended by a heartbeat every lease/3. A reaper on every worker returns expired jobs to `queued`, or sends them to `failed` after `max_attempts`. Only the DB clock is used.
- **Completion:** in one transaction, `UPDATE ... WHERE id AND attempt AND state='running'` and `INSERT INTO effects`. A zombie, a cancelled job or a duplicate matches 0 rows, so there is no effect.
- **Guarantee:** execution is at-least-once, the effect is exactly-once, but only for an effect stored in this DB.

## Chaos harness

`cmd/chaos` starts the API and N workers as child processes against a freshly wiped database. While the run goes on, the following happens concurrently:

- 10,000 jobs are submitted at 150/s. 10% of them go as 3 simultaneous identical POSTs, and 5% are first sent with a ~1 ms client timeout and then retried.
- 30% of keys are replayed later (the response must carry the same id), and 2% of keys are reused with a different payload (must get 409).
- 10% of jobs are cancelled (20% of those twice).
- 4 pollers check that a terminal state never changes.
- A random worker is `kill -9`'d every ~2 s and respawned.
- A random worker is `SIGSTOP`'d for 6 s every ~7 s; the lease is 3 s, so the worker comes back as a zombie.
- 1% of jobs run 2–3× longer than the lease, so heartbeats and cancel-while-running are actually exercised.

After the run drains, zombies are woken and given time to attempt stale writes. Then everything is killed and the harness checks the DB:

| # | Invariant |
|---|---|
| I1 | No lost job: every acknowledged (key, id) is in the DB, and there is exactly one row per key. |
| I2 | At most one effect per job. |
| I3 | Nothing is left in `queued` or `running`. |
| I4 | `succeeded` ⇔ exactly 1 effect; `cancelled`/`failed` ⇒ 0 effects. The effect comes from the final attempt. A 200 cancel ⇒ `cancelled`. `fail=true` ⇒ never `succeeded`. The attempt counter equals the number of claims, and no attempt was claimed twice. |
| I5 | Client-side: the same id for every duplicate, 409 on a conflict, a repeated cancel stays cancelled, no terminal state flips. |
| I6 | No HTTP 5xx. |
| I7 | Every `DONE` a worker logged has its committed effect, so no result was acknowledged and then lost. |
| I8 | Lease expiries ≤ kills + stalls. Every lost lease is explained; there is no silent lease loss. |
| C | Coverage: kills happened and caused re-runs, zombies were rejected, a heartbeat kept a long job alive, a running job was cancelled and aborted. A run where the chaos never hit anything fails. |

## All runs (raw output in `runs/`, nothing removed)

**v1** is the code before the independent review. **v2** is after the review fixes, and its harness also has the long jobs, I7, I8 and the C checks.

| Run | Result | kill -9 | zombies | fenced / lease_lost | jobs run >1× | Notes |
|---|---|---|---|---|---|---|
| v1 smoke (500) | PASS | 5 | 1 | 1 / 0 | 3 | |
| **v1 negative control `-unsafe-no-fence`** (3000) | **FAIL, as intended** | 20 | 6 | 0 / 0 | 23 | 4 double effects and 1 lost cancel caught. The HTTP observer (I5) **missed** the cancelled→succeeded flip; only the DB check caught it. |
| v1 full-10k ×3 | PASS ×3 | 67–69 | 17–20 | 12–15 / **0** | 75–80 | `lease_lost=0`: cancel-while-running and heartbeat were **never exercised** (a gap later found in review). |
| v1 cancel-heavy (1500, slow jobs, 40% cancel) | PASS | 120 | 35 | 110 / 118 | 83 | |
| v1 throughput (0 ms jobs, unthrottled) | PASS | 3 | 0 | 0 / 0 | 0 | **1,658 jobs/s** end-to-end on a laptop. Too short for the chaos to mean much. |
| v1 rps100 (16 workers) | PASS | 52 | 13 | 55 / 0 | 34 | p99 created→succeeded 0.22 s |
| **v1 Postgres `docker kill` ×2** | **FAIL** | 68 | 17 | 9 / 0 | 89 | Data invariants all PASS. **867 HTTP 500s** during the outage (should be 503 + Retry-After), so I5 and I6 failed. Fixed in v2. |
| v1 integration tests run1 | **FAIL** | | | | | The check-then-insert negative control did **not** reproduce the bug: the test had no teeth. |
| v1 integration tests run2 | PASS | | | | | Race window widened; the cancel/complete race was jittered (it had been 399:1 one-sided). |
| v2 integration tests run3 | **FAIL** | | | | | The same negative control flaked again (timing-based). Rewritten with a barrier. |
| v2 integration tests run4–6 | PASS ×3 | | | | | Includes the regression tests for the review bugs. |
| **v2 `docker compose run test` run1** | **FAIL** | | | | | `go test ./...` runs packages in parallel, and both packages dropped each other's tables in the same DB. Locally `-p 1` had hidden this. Fixed: one Postgres schema per test package. A nil-check missing in a negative control also caused a panic. |
| v2 `docker compose run test` run2–4 | PASS ×3 | | | | | |
| **reaper regression test against the OLD worker code** | **FAIL, as intended** | | | | | Proves `TestReaperRunsWhileWorkerIsBusy` actually detects the review bug. |
| v2 smoke (1500) | PASS | 41 | 11 | 6 / 1 | 27 | All C checks satisfied. |
| **v2 negative control `-unsafe-no-fence`** | **FAIL, as intended** | 46 | 13 | 0 / 2 | 39 | 3 double effects, 6 stale effects, 1 lost cancel. |
| **v2 negative control `-unsafe-no-heartbeat`** | **FAIL, as intended** | 157 | 48 | 231 / 0 | 94 | I2 **still passes** (fencing alone prevents double effects). Only I8 (386 expiries vs 205 causes) and the C checks catch it. |
| v2 full-10k ×3 | PASS ×3 | 121–148 | 31–45 | 19–22 / 2–3 | 113–134 | 1–2 jobs per run legitimately exhausted `max_attempts` (long jobs killed repeatedly). |
| v2 cancel-heavy | PASS | 145 | 41 | 115 / 126 | 96 | |
| v2 rps100 (16 workers) | PASS | 68 | 18 | 17 / 1 | 61 | **p50 latency 7.5 s, p99 18 s**: with 1% long jobs, 16 workers cannot keep up with 100/s. Saturation was silent. |
| **v2 Postgres `docker kill` ×2** | **FAIL** | 148 | 37 | 22 / 3 | 126 | Data invariants (I1–I4, I7, I8) PASS. Now 503s, no 500s and no API ERROR lines. The 8 I5 violations are all availability: a conflict probe got 503 instead of 409, and a cancel gave up after 2 s of retries while the DB restarted. |

Total: 29 recorded runs (18 chaos runs, 11 test runs), 9 of them FAIL:

- 4 are intended negative controls;
- 4 exposed real defects that were then fixed: 500 instead of 503 when the DB is down, toothless tests (twice), and test packages clobbering each other;
- 1 (the v2 Postgres kill) is the availability limit described above.

The Postgres-kill runs used an external schedule file: `runs/*db-kill-schedule.txt`.

### What the harness does NOT catch

- **Postgres failover to a replica, network partitions, disk full, API process crash.** Postgres was only killed and restarted in place, and the API was never killed.
- **Effects outside the DB.** The effect is a row in the same database. A real S3 upload or webhook is not covered, and could not be exactly-once anyway.
- **Real durations and sustained load.** Jobs last ≤ 9 s and runs last ≤ 5 min. Hours-long jobs, table bloat, autovacuum and connection churn over days are not tested.
- **Exact crash points.** Kills are random in time. The window between `COMMIT` being sent and acknowledged (an "unknown outcome") is covered by reasoning, not by injection: nobody kills the DB connection exactly at that moment.
- **The HTTP observer samples.** It missed a real terminal flip in the negative control. The DB-side invariants carry the weight.
- **Transient overlap is allowed.** Two workers may execute the same job at once (zombie and new owner). The harness checks effects, not exclusive execution.
- **Everything runs on one host**, with one kernel clock and no real network latency. Clock skew is argued, not injected.
- **Same author bias.** The harness and the system were written by the same tool. The independent review found holes in the harness that every run had missed, which is exactly the point.

### Was 10,000 enough?

A job count does not answer that question. What matters is how often each rare path was hit:

| Path | Events across the 3 v2 full runs |
|---|---|
| Zombie completion rejected (fenced) | ~61 |
| Job re-run after a kill | ~360 |
| Cancel while running (in the full runs) | ~8 |
| Cancel while running (in cancel-heavy) | 126 |

With 0 failures in n independent trials, the 95% upper bound on the per-event failure rate is about 3/n (the rule of three). So ~60 zombie events bound the zombie failure rate only to about 5% per event. That is weak evidence.

The strong evidence comes from the targeted tests, which force the interleaving 300–2000 times each, and from the negative controls, which show the checks fire.

## Silent failures: how each one was checked

| Failure | Protection | Evidence |
|---|---|---|
| **Double claim** | `FOR UPDATE SKIP LOCKED` plus the `state='queued'` re-check, with `attempt` incremented under the row lock. | `TestDoubleClaim_SkipLocked`: 32 claimers, 2000 jobs, 0 duplicates. **Control:** naive SELECT-then-UPDATE gave 491/500 jobs to more than one worker. Harness check I4: every attempt was claimed exactly once. |
| **Zombie writes a stale result** | Every write is fenced on `(id, attempt, state='running')`. | `TestZombieCannotWriteStaleResult`: real lease expiry by the DB clock, reap, re-claim; the zombie's heartbeat, complete and fail are all `ErrFenced`, 0 effects. **Control:** without the fence, 2 effects. Harness: real SIGSTOP zombies, ~20 FENCED per run, I4 "effect from the final attempt". |
| **Cancel/complete race** | Both are compare-and-set on the same row, so exactly one wins. | `TestCancelCompleteRace`: 400 races with jitter, both orders seen (13/387), 0 inconsistent. **Control:** check-then-act gave 117 "both succeeded" (the user saw "cancelled", the job ran anyway) and 94 inconsistent states. |
| **Duplicate on request retry** | Unique key + `ON CONFLICT DO NOTHING` + a canonical payload hash; 409 for a different payload. | `TestIdempotentCreate_ConcurrentRetries`: 50×50 concurrent creates, 1 row and one "created" per key. **Control:** check-then-insert gave 50 rows for one key. Harness: bursts, timeout-then-retry, replays and conflicts, I1/I5. Found in review: float64 rounding made a different payload look like a replay. |
| **Isolation level** | READ COMMITTED + single-statement conditional UPDATEs. | `TestIsolation_RepeatableRead...`: under REPEATABLE READ the result is still correct, but 53/300 race losers get `40001` instead of a clean "0 rows", which the API would report as 500. |
| **Clock skew** | Workers never send time; leases use the DB's `now()`. | `TestClockSkew_StoreUsesOnlyDBClock` checks the code (no `time.Now` in the store) and that the lease lands about 5 s ahead of the DB clock. Not injected. The remaining risk is the DB's own clock jumping, or a failover. |
| **Lease lost without cause** | (none in the design itself) | Found only by I8. The no-heartbeat control shows that the double-effect check (I2) stays green while jobs are quietly re-run and failed. |

## Verdict at 100 rps: **DON'T SHIP yet**

**The correctness core I would ship.** No run lost a job, produced a double effect or left a stuck job, across all non-control runs: ~1,250 `kill -9`s, ~340 SIGSTOP zombies and four Postgres crashes. The queue mechanics handled ~1,650 jobs/s on a laptop, 16× the target.

**What stops it** is the only failure mode actually seen at 100 rps: **silent saturation.** In `v2-rps100`, 1% long jobs pushed p50 latency to 7.5 s with the backlog growing. Nothing alerted, and the API kept returning 201.

**Fix first:** observability.

- Export metrics: queue depth, age of the oldest queued job, reclaim / fenced / lease-expiry rates, attempts-exhausted failures.
- Add alerts on oldest-queued age and on I8-style unexplained expiries.
- Add admission control (429 above a backlog limit).

**Then:**

1. Retention or partitioning: ~8.6M rows a day at 100 rps.
2. Production leases of 30–60 s, plus checkpointing and an attempt budget for long training jobs.
3. A failover test with a replica.
4. For a real external effect, an idempotent write keyed by `job_id`, plus `lease_until > now()` checked at commit.

## What surprised me and what still concerns me

**What surprised me:**

- The bugs that mattered were in the *evidence*, not in the protocol. Two "passing" negative controls had no teeth: they passed because of scheduling luck. And three full 10k runs reported PASS while never once exercising a heartbeat or cancel-while-running.
- Without fencing, the double-effect check still caught the bug. But with a broken heartbeat it **stayed green**: fencing hid the problem, and jobs were quietly re-run and failed by "max attempts". Only an invariant about *causes* (I8) exposed it.
- The independent review found a real stuck-job bug (the reaper was blocked by long jobs), which no run could hit because every job was short.

**What still concerns me:**

- Nothing has been tested at the timescale of real training. Long jobs and leases, `max_attempts` versus crash rate, and vacuum behaviour over days are all unknown.
- The `COMMIT`-in-flight window is covered only by argument.
- I trust the harness more than I did, because the negative controls show it fires. But it was written by the same tool as the system.
