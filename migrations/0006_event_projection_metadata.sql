CREATE TABLE event_projection_metadata (
    projection TEXT PRIMARY KEY NOT NULL,
    version INTEGER NOT NULL,
    last_history_rowid INTEGER NOT NULL,
    high_water_rowid INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending', 'running', 'complete')),
    coverage TEXT NOT NULL CHECK (coverage = 'retained_only'),
    updated_at TEXT NOT NULL
);
