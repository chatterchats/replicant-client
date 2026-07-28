# Device relationship semantics

`DeviceRelationships::assigned_replicant` is the owner/operator reported by
`replicant_code`. `hosting_replicant` is the matrix physically hosted by a
vessel, reported by `hosting_replicant`. They are independent: a drone can be
assigned without hosting a matrix, and a vessel may host a different
replicant's matrix.

Schema version 2 migrates every version-1 `device_relationships.hosted_by`
row and serialized `DeviceRelationships.hosted_by` value to
`assigned_replicant`. This is an explicit, atomic data migration because the
version-1 adapter populated that field exclusively from `replicant_code`; it
does not infer a hosted matrix from historical state. No value is invented for
`hosting_replicant`.

The old public `hosted_by` field is removed rather than retained as an alias:
its name is ambiguous now that the API has both assignment and physical
hosting. This is a source-breaking managed API change and therefore requires a
major-version release for already published 1.x consumers. Saved SQLite data
remains compatible through schema version 2.

Public device observations preserve the two relationship values from an owned
observation. Local `assigned_to` and `hosting_replicant` queries only filter
the published state and never fetch from the network.
