CREATE TABLE finite_executions (
    id TEXT PRIMARY KEY NOT NULL,
    operation_class TEXT NOT NULL CHECK (operation_class IN ('report', 'action')),
    kind TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('succeeded', 'skipped', 'failed')),
    started_at INTEGER NOT NULL,
    finished_at INTEGER NOT NULL,
    result_json TEXT,
    error TEXT
);

CREATE INDEX finite_executions_finished_at
    ON finite_executions(finished_at DESC, id DESC);
