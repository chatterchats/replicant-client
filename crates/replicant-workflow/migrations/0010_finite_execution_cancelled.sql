ALTER TABLE finite_executions RENAME TO finite_executions_old;

CREATE TABLE finite_executions (
    id TEXT PRIMARY KEY NOT NULL,
    operation_class TEXT NOT NULL CHECK (operation_class IN ('report', 'action')),
    kind TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('running', 'succeeded', 'skipped', 'failed', 'cancelled')),
    started_at INTEGER NOT NULL,
    finished_at INTEGER NOT NULL,
    result_json TEXT,
    error TEXT
);

INSERT INTO finite_executions (
    id, operation_class, kind, status, started_at, finished_at, result_json, error
)
SELECT id, operation_class, kind, status, started_at, finished_at, result_json, error
FROM finite_executions_old;

DROP TABLE finite_executions_old;

CREATE INDEX finite_executions_finished_at
    ON finite_executions(finished_at DESC, id DESC);
