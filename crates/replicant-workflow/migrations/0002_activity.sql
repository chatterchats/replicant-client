CREATE TABLE workflow_activity (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_id TEXT NOT NULL REFERENCES workflow_instances(id),
    created_at INTEGER NOT NULL,
    message TEXT NOT NULL
) STRICT;

CREATE INDEX workflow_activity_workflow_idx
    ON workflow_activity(workflow_id, id);
