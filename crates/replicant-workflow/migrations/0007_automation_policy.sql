CREATE TABLE automation_policy (
    singleton INTEGER PRIMARY KEY NOT NULL CHECK (singleton = 1),
    automatic_triggers_enabled INTEGER NOT NULL CHECK (automatic_triggers_enabled IN (0, 1)),
    workflows_paused INTEGER NOT NULL CHECK (workflows_paused IN (0, 1))
) STRICT;

INSERT INTO automation_policy (singleton, automatic_triggers_enabled, workflows_paused)
VALUES (1, 1, 0);
