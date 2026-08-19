CREATE TABLE IF NOT EXISTS history_schema_metadata (
    key TEXT PRIMARY KEY NOT NULL,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS event_history (
    event_id TEXT PRIMARY KEY NOT NULL,
    realm TEXT,
    event_name TEXT NOT NULL,
    category TEXT NOT NULL,
    device_code TEXT,
    replicant_code TEXT,
    star_id TEXT,
    location_id TEXT,
    occurred_at TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    appended_at TEXT NOT NULL,
    applied_at TEXT
);
INSERT INTO history_schema_metadata(key, value) VALUES ('schema_version', '1')
ON CONFLICT(key) DO NOTHING;
