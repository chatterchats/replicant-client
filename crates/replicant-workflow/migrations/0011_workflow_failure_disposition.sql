ALTER TABLE workflow_instances
ADD COLUMN failure_disposition TEXT
CHECK (
    failure_disposition IS NULL
    OR failure_disposition IN ('retryable', 'permanent')
);
