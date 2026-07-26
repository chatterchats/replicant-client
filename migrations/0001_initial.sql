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
CREATE INDEX inventories_realm_owner ON inventories(realm, owner_kind, owner_id);
CREATE INDEX device_relationship_targets ON device_relationships(target_realm, target_id);
CREATE INDEX event_journal_realm ON event_journal(realm, appended_at);
CREATE INDEX reconciliation_ready ON reconciliation_queue(state, not_before);
CREATE INDEX operation_journal_target ON operation_journal(target_realm, target_kind, target_id, state);
CREATE INDEX operation_journal_state ON operation_journal(state);
