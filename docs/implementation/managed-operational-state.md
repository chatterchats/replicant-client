# Managed operational state

The managed client is the default application surface. Raw access is an
explicit transport escape hatch, not a prerequisite for ordinary automation.
A successful managed read normalizes the complete operational fields needed by
controllers, commits them to SQLite, publishes the new revision, and only then
returns.

## Device state retained by managed snapshots

`Device` now retains:

- assignment and hosting relationships;
- attachment, controller, and stow relationships in both directions;
- attach/stow capacity and current stow usage;
- the current AMI directive, directive status, and forward-compatible detail
  fields;
- active travel origin, destination, stage, arrival times, and route ETA.

`DeviceQuery::stowed_in` now evaluates the stow relationship rather than
aliasing attachment. `DevicesGateway::refresh_many` performs an explicit,
paginated remote refresh and commits each page before advancing its cursor.
Filtered traversal never infers deletion from absence; only a fully traversed,
unfiltered owned-device collection can reconcile membership.

## Replicant and location state

Owned `Replicant` snapshots retain active travel, allowing restart logic to
recognize an in-progress route without a raw status read.

`LocationsGateway::get_for_replicant` commits replicant-scoped location data,
including aggregate planet/moon scan counts and the `moons_total_estimated`
flag. System-survey automation therefore needs one managed location read rather
than enumerating planetary bodies or reading a raw DTO.

## Event continuity

`EventsGateway::catch_up` explicitly traverses the unfiltered account event log
through the managed event engine. Events are deduplicated, reduced, journaled,
and cursor-advanced atomically before the method returns.

`EventsGateway::history` is a local-only query over that durable journal, and
`EventsGateway::cursor` returns the last durably applied cursor. Together with
`EventsGateway::watch`, this gives applications a managed live path plus a
managed gap-recovery path; no direct raw event-log query is required.

## Raw boundary

`client.raw()` remains available when an application intentionally needs:

- transport response metadata or headers;
- exact endpoint DTO shapes;
- newly introduced fields not yet promoted into the normalized domain;
- unsupported or diagnostic contract operations.

Managed examples should not use raw merely to recover information discarded by
normalization. Any such need is a managed-domain coverage defect and should be
fixed in the projection or gateway.
