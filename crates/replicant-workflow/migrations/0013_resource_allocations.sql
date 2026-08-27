CREATE TABLE workflow_resource_pools (
    pool_key TEXT PRIMARY KEY NOT NULL,
    resource_namespace TEXT NOT NULL,
    resource_key TEXT NOT NULL,
    kind TEXT NOT NULL,
    capabilities_json TEXT NOT NULL CHECK (json_valid(capabilities_json)),
    location_json TEXT CHECK (location_json IS NULL OR json_valid(location_json)),
    available_quantity INTEGER NOT NULL CHECK (available_quantity >= 0),
    observed_revision INTEGER NOT NULL CHECK (observed_revision >= 0),
    observed_at_ms INTEGER NOT NULL,
    UNIQUE (resource_namespace, resource_key)
) STRICT;

CREATE TABLE workflow_resource_allocations (
    id TEXT PRIMARY KEY NOT NULL,
    item_id TEXT NOT NULL REFERENCES workflow_work_items(id) ON DELETE CASCADE,
    requirement_key TEXT NOT NULL,
    pool_key TEXT NOT NULL REFERENCES workflow_resource_pools(pool_key),
    resource_namespace TEXT NOT NULL,
    resource_key TEXT NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    state TEXT NOT NULL CHECK (state IN ('active', 'dead', 'released')),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
) STRICT;

CREATE INDEX workflow_resource_allocations_item_idx
    ON workflow_resource_allocations(item_id, state, requirement_key, id);
CREATE INDEX workflow_resource_allocations_pool_idx
    ON workflow_resource_allocations(pool_key, state);
CREATE UNIQUE INDEX workflow_resource_allocations_active_identity_idx
    ON workflow_resource_allocations(
        item_id, requirement_key, resource_namespace, resource_key
    ) WHERE state = 'active';

CREATE TABLE workflow_assignments (
    id TEXT PRIMARY KEY NOT NULL CHECK (length(id) > 0),
    item_id TEXT NOT NULL REFERENCES workflow_work_items(id) ON DELETE CASCADE,
    worker_namespace TEXT NOT NULL,
    worker_key TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('active', 'reclaim_requested', 'released')),
    reclaim_requested_at_ms INTEGER,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    CHECK ((state = 'reclaim_requested') = (reclaim_requested_at_ms IS NOT NULL))
) STRICT;

CREATE UNIQUE INDEX workflow_assignments_active_item_idx
    ON workflow_assignments(item_id) WHERE state != 'released';
CREATE UNIQUE INDEX workflow_assignments_active_worker_idx
    ON workflow_assignments(worker_namespace, worker_key) WHERE state != 'released';
