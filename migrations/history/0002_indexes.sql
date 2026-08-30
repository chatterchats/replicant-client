CREATE INDEX IF NOT EXISTS event_history_occurred_at ON event_history(occurred_at DESC);
CREATE INDEX IF NOT EXISTS event_history_name_time ON event_history(event_name, occurred_at DESC);
CREATE INDEX IF NOT EXISTS event_history_device_time ON event_history(device_code, occurred_at DESC) WHERE device_code IS NOT NULL;
CREATE INDEX IF NOT EXISTS event_history_staged ON event_history(event_id) WHERE applied_at IS NULL;
CREATE INDEX IF NOT EXISTS event_history_retained_stream_order
ON event_history(stream_millis, stream_sequence, event_id)
WHERE applied_at IS NOT NULL OR archived_only = 1;
CREATE INDEX IF NOT EXISTS event_history_retained_name_stream_order
ON event_history(event_name, stream_millis, stream_sequence, event_id)
WHERE applied_at IS NOT NULL OR archived_only = 1;
CREATE INDEX IF NOT EXISTS event_history_retained_device_stream_order
ON event_history(device_code, stream_millis, stream_sequence, event_id)
WHERE applied_at IS NOT NULL OR archived_only = 1;
