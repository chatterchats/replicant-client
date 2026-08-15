CREATE TABLE automation_triggers (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    condition_json TEXT NOT NULL,
    target_json TEXT NOT NULL,
    enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    last_fired_at INTEGER,
    next_run_at INTEGER,
    last_error TEXT,
    event_cursor TEXT,
    revision INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0)
);

CREATE INDEX automation_triggers_enabled_next_run
    ON automation_triggers(enabled, next_run_at);

CREATE TABLE automation_trigger_firings (
    trigger_id TEXT NOT NULL REFERENCES automation_triggers(id) ON DELETE CASCADE,
    dedupe_key TEXT NOT NULL,
    claimed_at INTEGER NOT NULL,
    error TEXT,
    PRIMARY KEY (trigger_id, dedupe_key)
);
