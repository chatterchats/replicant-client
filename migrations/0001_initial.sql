-- Fresh schema version 1 for replicant-client. It intentionally has no
-- compatibility path from any predecessor database.
CREATE TABLE schema_metadata (
    key TEXT PRIMARY KEY NOT NULL,
    value TEXT NOT NULL
);
CREATE TABLE account_binding (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    account_id TEXT NOT NULL,
    bound_at TEXT NOT NULL
);
-- Reserved provenance scaffold: source_documents and the generic payload tables
-- were created before typed managed projections became the durable design.
-- Messages now use their table directly; blueprints, BobNet messages,
-- achievements, reputation, species, trades, resource sites, location events,
-- directory profiles, discovery data, and freshness remain intentionally
-- unwritten. Keep the scaffold because populated tables still carry foreign
-- keys to source_documents; removing it requires tested SQLite table rebuilds.
CREATE TABLE source_documents (
    id TEXT PRIMARY KEY NOT NULL,
    operation TEXT NOT NULL,
    request_id TEXT,
    captured_at TEXT NOT NULL
);
CREATE TABLE accounts (
    realm TEXT NOT NULL,
    account_id TEXT NOT NULL,
    observation_json TEXT NOT NULL,
    source_document_id TEXT,
    PRIMARY KEY (realm, account_id),
    FOREIGN KEY (source_document_id) REFERENCES source_documents(id)
);
CREATE TABLE replicants (
    realm TEXT NOT NULL,
    replicant_id TEXT NOT NULL,
    observation_json TEXT NOT NULL,
    source_document_id TEXT,
    PRIMARY KEY (realm, replicant_id),
    FOREIGN KEY (source_document_id) REFERENCES source_documents(id)
);
CREATE TABLE public_directory_profiles (
    replicant_id TEXT PRIMARY KEY NOT NULL,
    profile_json TEXT NOT NULL,
    source_document_id TEXT,
    FOREIGN KEY (source_document_id) REFERENCES source_documents(id)
);
CREATE TABLE devices (
    realm TEXT NOT NULL,
    device_id TEXT NOT NULL,
    device_type TEXT,
    status TEXT,
    location_realm TEXT,
    location_id TEXT,
    access_scope TEXT NOT NULL,
    observed_at INTEGER NOT NULL,
    observation_json TEXT NOT NULL,
    source_document_id TEXT,
    PRIMARY KEY (realm, device_id),
    FOREIGN KEY (source_document_id) REFERENCES source_documents(id)
);
CREATE TABLE device_relationships (
    realm TEXT NOT NULL,
    device_id TEXT NOT NULL,
    relationship TEXT NOT NULL,
    target_realm TEXT NOT NULL,
    target_id TEXT NOT NULL,
    PRIMARY KEY (realm, device_id, relationship),
    FOREIGN KEY (realm, device_id) REFERENCES devices(realm, device_id) ON DELETE CASCADE
);
CREATE TABLE locations (
    realm TEXT NOT NULL,
    location_id TEXT NOT NULL,
    observation_json TEXT NOT NULL,
    source_document_id TEXT,
    PRIMARY KEY (realm, location_id),
    FOREIGN KEY (source_document_id) REFERENCES source_documents(id)
);
CREATE TABLE inventories (
    realm TEXT NOT NULL,
    owner_kind TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    inventory_json TEXT NOT NULL,
    source_document_id TEXT,
    PRIMARY KEY (realm, owner_kind, owner_id),
    FOREIGN KEY (source_document_id) REFERENCES source_documents(id)
);
CREATE TABLE discovery_data (
    realm TEXT NOT NULL,
    kind TEXT NOT NULL,
    item_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    source_document_id TEXT,
    PRIMARY KEY (realm, kind, item_id),
    FOREIGN KEY (source_document_id) REFERENCES source_documents(id)
);
CREATE TABLE stars (
    realm TEXT NOT NULL,
    star_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    source_document_id TEXT,
    PRIMARY KEY (realm, star_id),
    FOREIGN KEY (source_document_id) REFERENCES source_documents(id)
);
CREATE TABLE catalogue_metadata (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    generated_at TEXT
);
CREATE TABLE replicant_star_knowledge (
    realm TEXT NOT NULL,
    replicant_id TEXT NOT NULL,
    star_id TEXT NOT NULL,
    observation_json TEXT NOT NULL,
    PRIMARY KEY (realm, replicant_id, star_id)
);
CREATE TABLE resource_sites (
    realm TEXT NOT NULL,
    site_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    source_document_id TEXT,
    PRIMARY KEY (realm, site_id),
    FOREIGN KEY (source_document_id) REFERENCES source_documents(id)
);
CREATE TABLE location_events (
    realm TEXT NOT NULL,
    event_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    source_document_id TEXT,
    PRIMARY KEY (realm, event_id),
    FOREIGN KEY (source_document_id) REFERENCES source_documents(id)
);
CREATE TABLE messages (
    message_id TEXT PRIMARY KEY NOT NULL,
    payload_json TEXT NOT NULL,
    source_document_id TEXT,
    FOREIGN KEY (source_document_id) REFERENCES source_documents(id)
);
CREATE TABLE bobnet_messages (
    message_id TEXT PRIMARY KEY NOT NULL,
    payload_json TEXT NOT NULL,
    source_document_id TEXT,
    FOREIGN KEY (source_document_id) REFERENCES source_documents(id)
);
CREATE TABLE blueprints (
    blueprint_id TEXT PRIMARY KEY NOT NULL,
    payload_json TEXT NOT NULL,
    source_document_id TEXT,
    FOREIGN KEY (source_document_id) REFERENCES source_documents(id)
);
CREATE TABLE achievements (
    achievement_id TEXT PRIMARY KEY NOT NULL,
    payload_json TEXT NOT NULL,
    source_document_id TEXT,
    FOREIGN KEY (source_document_id) REFERENCES source_documents(id)
);
CREATE TABLE reputation (
    realm TEXT NOT NULL,
    species_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    source_document_id TEXT,
    PRIMARY KEY (realm, species_id),
    FOREIGN KEY (source_document_id) REFERENCES source_documents(id)
);
CREATE TABLE species (
    species_id TEXT PRIMARY KEY NOT NULL,
    payload_json TEXT NOT NULL,
    source_document_id TEXT,
    FOREIGN KEY (source_document_id) REFERENCES source_documents(id)
);
CREATE TABLE trades (
    realm TEXT NOT NULL,
    trade_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    source_document_id TEXT,
    PRIMARY KEY (realm, trade_id),
    FOREIGN KEY (source_document_id) REFERENCES source_documents(id)
);
CREATE TABLE simulations (
    simulation_id INTEGER PRIMARY KEY NOT NULL,
    payload_json TEXT NOT NULL,
    source_document_id TEXT,
    FOREIGN KEY (source_document_id) REFERENCES source_documents(id)
);
CREATE TABLE event_journal (
    event_id TEXT PRIMARY KEY NOT NULL,
    realm TEXT,
    event_json TEXT NOT NULL,
    appended_at TEXT NOT NULL
);
CREATE TABLE event_cursors (
    stream TEXT PRIMARY KEY NOT NULL,
    cursor TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE operation_journal (
    operation_id TEXT PRIMARY KEY NOT NULL,
    state TEXT NOT NULL,
    target_realm TEXT,
    target_kind TEXT,
    target_id TEXT,
    intent_json TEXT NOT NULL,
    projection_json TEXT,
    submission_attempt_id TEXT,
    submitted_at TEXT,
    submission_cursor TEXT,
    updated_at TEXT NOT NULL
);
CREATE TABLE reconciliation_queue (
    work_id TEXT PRIMARY KEY NOT NULL,
    realm TEXT NOT NULL,
    kind TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    not_before TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL DEFAULT 'queued'
);
CREATE TABLE reconciliation_leader (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    owner TEXT NOT NULL,
    lease_until INTEGER NOT NULL
);
CREATE TABLE reconciliation_runs (
    run_id TEXT PRIMARY KEY NOT NULL,
    work_id TEXT NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    outcome TEXT,
    FOREIGN KEY (work_id) REFERENCES reconciliation_queue(work_id)
);
CREATE TABLE freshness (
    realm TEXT NOT NULL,
    scope TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    PRIMARY KEY (realm, scope)
);
CREATE TABLE tombstones (
    realm TEXT NOT NULL,
    kind TEXT NOT NULL,
    item_id TEXT NOT NULL,
    removed_at TEXT NOT NULL,
    evidence TEXT NOT NULL,
    PRIMARY KEY (realm, kind, item_id)
);
CREATE INDEX devices_realm_type_status ON devices(realm, device_type, status);
CREATE INDEX devices_realm_location ON devices(realm, location_realm, location_id);
CREATE INDEX devices_realm_access ON devices(realm, access_scope);
CREATE INDEX replicants_realm ON replicants(realm, replicant_id);
CREATE INDEX replicant_star_knowledge_star ON replicant_star_knowledge(realm, star_id);
CREATE INDEX inventories_realm_owner ON inventories(realm, owner_kind, owner_id);
CREATE INDEX device_relationship_targets ON device_relationships(target_realm, target_id);
CREATE INDEX event_journal_realm ON event_journal(realm, appended_at);
CREATE INDEX reconciliation_ready ON reconciliation_queue(state, not_before);
CREATE INDEX operation_journal_target ON operation_journal(target_realm, target_kind, target_id, state);
CREATE INDEX operation_journal_state ON operation_journal(state);
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
