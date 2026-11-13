# AI Usage

I built this project myself, using AI tools to speed up parts of the development process. I mainly used AI for repetitive implementation tasks, generating initial code drafts, writing tests, preparing Docker configuration, and helping with documentation.

However, the architecture, technical decisions, implementation reviews, and validation were under my control. I treated AI as an assistant, not as a source of truth. Every important part of the system had to be reviewed, tested, and verified before I considered it complete.

This document explains how I approached development, where AI helped, and what I discovered during testing.

## Technical Decisions

- **No Kafka.** I considered whether Kafka or another message broker was necessary. After evaluating the architecture, I decided to keep the system PostgreSQL-only. The transactional outbox pattern is already implemented as part of `Complete`, and adding a separate broker would introduce additional complexity without improving the guarantees required by this system. I documented the reasoning in `DESIGN.md`.

- **Independent code review.** Once the initial implementation was ready, I requested an independent review using a separate AI agent with fresh context. I wanted to avoid relying on the same assumptions that shaped the original implementation.

- **Preserving failed test runs.** I kept failed test runs and their logs in `runs/`. I didn't want to hide unsuccessful attempts or present only the results that made the implementation look good.

## How I Validated the Implementation

My main principle was simple: a test passing does not automatically mean the system is correct.

I wanted to make sure that every important protection could actually detect the problem it was designed to prevent.

| Potential problem | Validation approach |
|---|---|
| A test passes without detecting the underlying bug | Added negative controls using deliberately incorrect implementations. |
| The system reports success despite broken worker behavior | Ran the chaos harness against workers with fencing and heartbeat protections disabled. |
| Chaos tests pass without exercising critical scenarios | Added coverage assertions to ensure worker failures, retries, zombie rejection, heartbeats, and cancellation are actually exercised. |
| Regression tests fail to detect old bugs | Ran `TestReaperRunsWhileWorkerIsBusy` against the previous worker implementation. |
| Tests work only in a local development environment | Ran the exact command intended for reviewers: `docker compose run --rm test`. |

Some examples of negative controls included:

- SELECT-then-UPDATE instead of an atomic claim.
- Missing fencing protection.
- Check-then-act cancellation.
- Check-then-insert creation.

These implementations were intentionally incorrect. The corresponding tests had to fail against them.

I also tested the chaos harness with unsafe worker configurations:

- `-unsafe-no-fence` reproduced duplicate effects and a lost cancellation.
- `-unsafe-no-heartbeat` reproduced unexplained lease expirations.

Both scenarios correctly resulted in FAIL.

## Problems Discovered During Development

The initial implementation looked complete, but additional testing and independent review revealed several issues.

Interestingly, most problems were not in the main claim, lease, and fencing protocol. They were in the tests and the evidence used to validate the system.

### 1. Negative controls were not reliable

The check-then-insert negative control initially reported only one row instead of detecting duplicate inserts.

The problem was that goroutines effectively executed sequentially while the database connection pool was warming up.

My first attempt was to increase the timing window, but the test remained unreliable.

I eventually replaced this approach with an explicit synchronization barrier that forces the intended race condition.

Failed attempts are preserved in:
- `runs/tests-silent-failures-run1.txt`
- `runs/tests-silent-failures-run3-after-review.txt`

### 2. Race condition coverage was one-sided

The cancellation race test initially favored cancellation too heavily: cancel won 399 out of 400 races.

This meant the competing completion path was barely tested.

I introduced additional jitter to make the race less predictable. The result became 13 cancellation wins and 387 completion wins.

### 3. Chaos tests were not exercising heartbeats

Three complete chaos runs passed without actually testing heartbeats or cancellation during execution.

The reason was straightforward: all jobs finished before the first heartbeat interval.

I added longer-running jobs that exceed the lease duration and introduced explicit coverage checks.

Now a successful run requires evidence that these scenarios were actually exercised.

### 4. HTTP observation was insufficient

The HTTP observer missed a terminal-state transition that was detected by checking the database directly.

This showed me that sampled HTTP observations are not always reliable enough to prove system invariants.

I kept the HTTP observer as additional evidence, but database-level checks became the primary source of truth.

### 5. Independent review revealed additional bugs

The independent review found several issues that were missed by the initial implementation and multiple successful test runs:

- The reaper only executed between jobs. A worker processing a long-running job could not recover jobs abandoned by dead workers.
- Payload canonicalization used `float64`, which silently changed `9007199254740993` into `9007199254740992`.
- `DESIGN.md` incorrectly described keyset pagination as stable.
- The chaos harness could report PASS without actually exercising important failure scenarios.

I verified each finding against the actual implementation before making changes.

One additional finding concerned `Complete` not checking `lease_until`. I evaluated the implications and documented why this is safe for an effect performed entirely inside the same database transaction rather than changing the implementation unnecessarily.

### 6. Docker Compose exposed a test isolation problem

Initially, I validated the tests using `-p 1`, which forced sequential package execution.

However, reviewers would run:

`docker compose run --rm test`

Under this command, packages executed in parallel and interfered with each other's database tables.

I fixed this by assigning a separate PostgreSQL schema to each test package.

This was a good example of why validating the exact reviewer workflow matters.

### 7. Incorrect HTTP status during database failures

A test that terminated PostgreSQL exposed another issue: the API returned HTTP 500 when the database was unavailable.

I changed this behavior to return HTTP 503 with a `Retry-After` header, which more accurately represents a temporary service dependency failure.

## What I Learned

There are a few important lessons I took from this project.

**1. AI accelerates implementation, but it does not guarantee correctness.**

AI was useful for generating boilerplate, implementing repetitive components, drafting tests, and exploring possible solutions. However, generated code and generated tests both required independent verification.

**2. Green tests are not enough.**

A test suite can pass while completely missing the problem it was designed to detect. Negative controls and deliberate failure scenarios were essential to establishing confidence in the implementation.

**3. Independent review matters.**

A fresh reviewer found real problems that the original implementation and multiple successful test runs had missed. This reinforced the importance of reviewing code without relying on the assumptions made during development.

**4. Evidence is as important as implementation.**

A successful chaos run is meaningful only if the expected failure scenarios actually occurred. Logs, coverage assertions, reproducible failures, and database invariants are much stronger evidence than a simple PASS message.

My biggest takeaway is that AI can significantly reduce development time, but responsibility for architecture, correctness, and reliability still belongs to the engineer.

I don't consider a system verified just because its tests are green. I consider it verified when I can demonstrate that the same tests detect deliberately broken implementations.