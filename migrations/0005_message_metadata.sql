CREATE TABLE message_metadata (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    last_cursor INTEGER,
    unread_count INTEGER,
    refreshed_at INTEGER
);
