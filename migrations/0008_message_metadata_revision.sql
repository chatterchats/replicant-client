ALTER TABLE message_metadata ADD COLUMN revision INTEGER NOT NULL DEFAULT 0;
ALTER TABLE message_metadata ADD COLUMN last_error TEXT;
