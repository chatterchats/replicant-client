CREATE INDEX IF NOT EXISTS event_history_occurred_at ON event_history(occurred_at DESC);
CREATE INDEX IF NOT EXISTS event_history_name_time ON event_history(event_name, occurred_at DESC);
CREATE INDEX IF NOT EXISTS event_history_device_time ON event_history(device_code, occurred_at DESC) WHERE device_code IS NOT NULL;
CREATE INDEX IF NOT EXISTS event_history_staged ON event_history(event_id) WHERE applied_at IS NULL;
