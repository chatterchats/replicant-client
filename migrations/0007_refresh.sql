CREATE TABLE IF NOT EXISTS refresh_runs (
    run_id TEXT PRIMARY KEY NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('apply', 'dry_run')),
    requested_phases_json TEXT NOT NULL,
    read_requests_per_minute INTEGER NOT NULL CHECK (read_requests_per_minute BETWEEN 1 AND 60),
    status TEXT NOT NULL CHECK (status IN (
        'queued', 'running', 'backing_off', 'awaiting_approval', 'blocked',
        'completed', 'completed_dry_run', 'cancelled', 'failed'
    )),
    current_phase TEXT,
    retry_not_before_ms INTEGER,
    failure_kind TEXT,
    completed_at_ms INTEGER,
    lease_owner TEXT,
    lease_expires_at_ms INTEGER,
    request_attempts INTEGER NOT NULL DEFAULT 0,
    cancel_requested INTEGER NOT NULL DEFAULT 0 CHECK (cancel_requested IN (0, 1)),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS refresh_phase_checkpoints (
    run_id TEXT NOT NULL,
    phase TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN (
        'pending', 'running', 'backing_off', 'awaiting_approval', 'blocked',
        'complete', 'cancelled', 'failed'
    )),
    checkpoint_json TEXT NOT NULL DEFAULT '{}',
    pages INTEGER NOT NULL DEFAULT 0,
    items INTEGER NOT NULL DEFAULT 0,
    request_attempts INTEGER NOT NULL DEFAULT 0,
    proposed_inserts INTEGER NOT NULL DEFAULT 0,
    proposed_updates INTEGER NOT NULL DEFAULT 0,
    proposed_tombstones INTEGER NOT NULL DEFAULT 0,
    applied_inserts INTEGER NOT NULL DEFAULT 0,
    applied_updates INTEGER NOT NULL DEFAULT 0,
    applied_tombstones INTEGER NOT NULL DEFAULT 0,
    enumeration_complete INTEGER NOT NULL DEFAULT 0 CHECK (enumeration_complete IN (0, 1)),
    unfiltered INTEGER NOT NULL DEFAULT 0 CHECK (unfiltered IN (0, 1)),
    phase_started_at_ms INTEGER,
    local_count INTEGER,
    upstream_count INTEGER,
    membership_digest TEXT CHECK (membership_digest IS NULL OR membership_digest GLOB '[0-9a-f]*'),
    approval_digest TEXT CHECK (approval_digest IS NULL OR approval_digest GLOB '[0-9a-f]*'),
    approved_at_ms INTEGER,
    retry_not_before_ms INTEGER,
    failure_kind TEXT,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (run_id, phase),
    FOREIGN KEY (run_id) REFERENCES refresh_runs(run_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS refresh_stage (
    run_id TEXT NOT NULL,
    phase TEXT NOT NULL,
    item_key TEXT NOT NULL,
    payload_json TEXT,
    disposition TEXT NOT NULL CHECK (disposition IN (
        'insert', 'update', 'unchanged', 'tombstone_candidate'
    )),
    observed_at_ms INTEGER,
    PRIMARY KEY (run_id, phase, item_key),
    FOREIGN KEY (run_id) REFERENCES refresh_runs(run_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS refresh_runs_work
ON refresh_runs(status, retry_not_before_ms, lease_expires_at_ms, updated_at_ms);

CREATE INDEX IF NOT EXISTS refresh_stage_disposition
ON refresh_stage(run_id, phase, disposition);
