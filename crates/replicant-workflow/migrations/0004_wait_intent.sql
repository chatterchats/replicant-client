ALTER TABLE workflow_instances ADD COLUMN wait_intent_json TEXT
    CHECK (wait_intent_json IS NULL OR json_valid(wait_intent_json));
