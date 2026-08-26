ALTER TABLE event_history ADD COLUMN archived_only INTEGER NOT NULL DEFAULT 0
CHECK (archived_only IN (0, 1));
ALTER TABLE event_history ADD COLUMN stream_millis INTEGER;
ALTER TABLE event_history ADD COLUMN stream_sequence INTEGER;

CREATE INDEX IF NOT EXISTS event_history_stream_order
ON event_history(stream_millis, stream_sequence);
