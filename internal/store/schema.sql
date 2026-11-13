-- Applied on API startup. Idempotent.

CREATE TABLE IF NOT EXISTS jobs (
    id              bigserial PRIMARY KEY,
    idempotency_key text        NOT NULL UNIQUE,
    request_hash    text        NOT NULL,          -- sha256 of canonical payload; same key + different payload => 409
    payload         jsonb       NOT NULL,
    state           text        NOT NULL CHECK (state IN ('queued','running','succeeded','failed','cancelled')),
    attempt         int         NOT NULL DEFAULT 0, -- fencing token: incremented on every claim
    max_attempts    int         NOT NULL DEFAULT 5,
    lease_owner     text,
    lease_until     timestamptz,                    -- always compared against DB now(), never a worker clock
    error           text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    finished_at     timestamptz,
    -- a running job always has a lease, nothing else has one
    CONSTRAINT lease_iff_running CHECK ((state = 'running') = (lease_until IS NOT NULL AND lease_owner IS NOT NULL))
);

-- claim path: oldest queued job
CREATE INDEX IF NOT EXISTS jobs_queued_idx  ON jobs (id) WHERE state = 'queued';
-- reaper path: expired leases
CREATE INDEX IF NOT EXISTS jobs_running_idx ON jobs (lease_until) WHERE state = 'running';
-- list with state filter + keyset pagination
CREATE INDEX IF NOT EXISTS jobs_state_id_idx ON jobs (state, id);

-- One row per claim = one row per real "execution start". Lets us count how many times a job actually ran.
-- Deliberately NO unique constraint: the harness must observe double claims, not have them rejected.
CREATE TABLE IF NOT EXISTS job_runs (
    id         bigserial PRIMARY KEY,
    job_id     bigint      NOT NULL REFERENCES jobs(id),
    attempt    int         NOT NULL,
    worker_id  text        NOT NULL,
    started_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS job_runs_job_idx ON job_runs (job_id);

-- The emulated side effect. Written in the same transaction as state -> succeeded.
-- Deliberately NO unique(job_id): exactly-once is provided by the fencing protocol and the harness
-- verifies it. In production you would add UNIQUE(job_id) as belt-and-braces (see DESIGN.md).
CREATE TABLE IF NOT EXISTS effects (
    id         bigserial PRIMARY KEY,
    job_id     bigint      NOT NULL REFERENCES jobs(id),
    attempt    int         NOT NULL,
    worker_id  text        NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS effects_job_idx ON effects (job_id);
