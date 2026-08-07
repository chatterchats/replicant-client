# replicant-transport

Reusable point-to-point logistics for Replicant Space.

The crate owns generic delivery mechanics: source resolution, payload selection,
transport selection, resource collection/deposit, device attachment/detachment,
travel, repeated trips, and optional modular-device unfurling. Event-specific
logic such as requirement progress, achievements, beacons, and reward recovery
stays outside this crate.

The companion `replicant-transport` CLI accepts either a system-wide origin
(`SCEPTURUM`) or an exact source location (`SCEPTURUM-BELT-1`).

Device payloads can be selected either by type/count or by tag. A tag selector
includes every eligible device carrying that tag inside the origin scope.
Repeated tag/type selectors are deduplicated by device code.
