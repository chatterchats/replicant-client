CREATE TABLE IF NOT EXISTS reconciliation_leader (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    owner TEXT NOT NULL,
    lease_until INTEGER NOT NULL
);
