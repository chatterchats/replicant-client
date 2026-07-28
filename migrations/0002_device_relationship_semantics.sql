-- Version 1 called the owner/operator relationship "hosted_by".  The
-- normalized snapshot JSON is rewritten by Store::migrate because SQLite's
-- JSON extension is not part of this crate's portability contract.
DELETE FROM device_relationships
WHERE relationship = 'hosted_by'
  AND EXISTS (
      SELECT 1
      FROM device_relationships AS assigned
      WHERE assigned.realm = device_relationships.realm
        AND assigned.device_id = device_relationships.device_id
        AND assigned.relationship = 'assigned_replicant'
  );

UPDATE device_relationships
SET relationship = 'assigned_replicant'
WHERE relationship = 'hosted_by';
