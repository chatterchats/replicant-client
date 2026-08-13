CREATE TABLE workflow_instances (
    id TEXT PRIMARY KEY NOT NULL,
    kind TEXT NOT NULL,
    schema_version INTEGER NOT NULL CHECK (schema_version > 0),
    config_json TEXT NOT NULL CHECK (json_valid(config_json)),
    checkpoint_json TEXT NOT NULL CHECK (json_valid(checkpoint_json)),
    status TEXT NOT NULL CHECK (status IN (
        'queued', 'running', 'waiting', 'paused', 'reconciling',
        'succeeded', 'failed', 'cancelled'
    )),
    current_step TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    last_error TEXT,
    result_json TEXT CHECK (result_json IS NULL OR json_valid(result_json)),
    parent_id TEXT REFERENCES workflow_instances(id),
    revision INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0)
) STRICT;

CREATE INDEX workflow_instances_status_idx ON workflow_instances(status, created_at);
CREATE INDEX workflow_instances_parent_idx ON workflow_instances(parent_id);
