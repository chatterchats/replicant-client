CREATE TABLE workflow_targets (
    workflow_id TEXT NOT NULL REFERENCES workflow_instances(id) ON DELETE CASCADE,
    target_kind TEXT NOT NULL CHECK (length(target_kind) > 0),
    target_key TEXT NOT NULL CHECK (length(target_key) > 0),
    target_json TEXT NOT NULL CHECK (json_valid(target_json)),
    state TEXT NOT NULL CHECK (state IN ('active', 'released')),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (workflow_id, target_kind, target_key)
) STRICT;

CREATE INDEX workflow_targets_reverse_idx
    ON workflow_targets(target_kind, target_key, state, workflow_id);

-- Separate monotonic revision for cross-workflow allocation/target projections. This avoids
-- overloading workflow instance revisions while still giving the daemon a lossless invalidation
-- watermark when resource pools, allocations, or structured targets change.
CREATE TABLE workflow_intelligence_revision (
    singleton INTEGER PRIMARY KEY NOT NULL CHECK (singleton = 1),
    revision INTEGER NOT NULL CHECK (revision >= 0)
) STRICT;
INSERT INTO workflow_intelligence_revision (singleton, revision) VALUES (1, 0);

CREATE TRIGGER workflow_resource_pools_intelligence_update
AFTER UPDATE ON workflow_resource_pools
WHEN EXISTS (
    SELECT 1 FROM workflow_resource_allocations allocation
    WHERE allocation.pool_key = NEW.pool_key AND allocation.state = 'active'
)
AND (OLD.kind IS NOT NEW.kind
  OR OLD.capabilities_json IS NOT NEW.capabilities_json
  OR OLD.location_json IS NOT NEW.location_json)
BEGIN
    UPDATE workflow_intelligence_revision SET revision = revision + 1 WHERE singleton = 1;
END;

CREATE TRIGGER workflow_resource_allocations_intelligence_insert
AFTER INSERT ON workflow_resource_allocations
BEGIN
    UPDATE workflow_intelligence_revision SET revision = revision + 1 WHERE singleton = 1;
END;
CREATE TRIGGER workflow_resource_allocations_intelligence_update
AFTER UPDATE ON workflow_resource_allocations
BEGIN
    UPDATE workflow_intelligence_revision SET revision = revision + 1 WHERE singleton = 1;
END;
CREATE TRIGGER workflow_resource_allocations_intelligence_delete
AFTER DELETE ON workflow_resource_allocations
BEGIN
    UPDATE workflow_intelligence_revision SET revision = revision + 1 WHERE singleton = 1;
END;

CREATE TRIGGER workflow_targets_intelligence_insert
AFTER INSERT ON workflow_targets
BEGIN
    UPDATE workflow_intelligence_revision SET revision = revision + 1 WHERE singleton = 1;
END;
CREATE TRIGGER workflow_targets_intelligence_update
AFTER UPDATE ON workflow_targets
BEGIN
    UPDATE workflow_intelligence_revision SET revision = revision + 1 WHERE singleton = 1;
END;
CREATE TRIGGER workflow_targets_intelligence_delete
AFTER DELETE ON workflow_targets
BEGIN
    UPDATE workflow_intelligence_revision SET revision = revision + 1 WHERE singleton = 1;
END;
