CREATE TABLE workflow_work_items (
    id TEXT PRIMARY KEY NOT NULL,
    workflow_id TEXT NOT NULL REFERENCES workflow_instances(id) ON DELETE CASCADE,
    dedupe_key TEXT NOT NULL,
    kind TEXT NOT NULL,
    sort_key TEXT NOT NULL,
    payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
    preconditions_json TEXT NOT NULL CHECK (json_valid(preconditions_json)),
    requirements_json TEXT NOT NULL CHECK (json_valid(requirements_json)),
    deadline_at_ms INTEGER,
    status TEXT NOT NULL CHECK (status IN (
        'pending', 'assigned', 'running', 'waiting',
        'succeeded', 'skipped', 'failed', 'abandoned'
    )),
    checkpoint_json TEXT CHECK (checkpoint_json IS NULL OR json_valid(checkpoint_json)),
    result_json TEXT CHECK (result_json IS NULL OR json_valid(result_json)),
    last_error TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    consecutive_failure_count INTEGER NOT NULL DEFAULT 0 CHECK (consecutive_failure_count >= 0),
    next_attempt_at_ms INTEGER,
    ever_started INTEGER NOT NULL DEFAULT 0 CHECK (ever_started IN (0, 1)),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    revision INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0),
    UNIQUE (workflow_id, dedupe_key)
) STRICT;

CREATE INDEX workflow_work_items_eligibility_idx ON workflow_work_items(
    workflow_id, status, next_attempt_at_ms, sort_key, dedupe_key, id
);

CREATE TABLE workflow_work_item_attempts (
    item_id TEXT NOT NULL REFERENCES workflow_work_items(id) ON DELETE CASCADE,
    attempt_ordinal INTEGER NOT NULL CHECK (attempt_ordinal > 0),
    assignment_id TEXT NOT NULL CHECK (length(assignment_id) > 0),
    worker_identity TEXT NOT NULL CHECK (length(worker_identity) > 0),
    started_at_ms INTEGER NOT NULL,
    ended_at_ms INTEGER,
    outcome TEXT CHECK (outcome IS NULL OR outcome IN (
        'succeeded', 'failed', 'reclaimed', 'cancelled'
    )),
    error TEXT,
    PRIMARY KEY (item_id, attempt_ordinal),
    CHECK ((ended_at_ms IS NULL) = (outcome IS NULL))
) STRICT;

CREATE UNIQUE INDEX workflow_work_item_attempts_open_idx
    ON workflow_work_item_attempts(item_id) WHERE ended_at_ms IS NULL;
