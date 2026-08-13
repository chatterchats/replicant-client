CREATE TABLE workflow_resource_claims (
    resource_namespace TEXT NOT NULL,
    resource_key TEXT NOT NULL,
    workflow_id TEXT NOT NULL REFERENCES workflow_instances(id),
    acquired_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (resource_namespace, resource_key)
) STRICT;

CREATE INDEX workflow_resource_claims_workflow_idx
    ON workflow_resource_claims(workflow_id);
