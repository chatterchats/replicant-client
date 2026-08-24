# replicant-protocol

Stable, versioned DTOs shared by `replicantd` and its local frontends. Types
only — no transport, no logic.

`PROTOCOL_VERSION` is the local application protocol version. Bump it when a
change is not backward compatible for an already-running frontend.

## Scope boundary

This crate carries **only** the application's normalized local protocol. Raw
upstream Replicant Space events, DTOs, and authentication data do not belong
here — those live in `replicant-client`'s `raw` and `events` modules. Adding an
upstream type here couples the frontend to the game API and defeats the
normalization layer.

## Core types

| Type | Role |
| --- | --- |
| `RuntimeSnapshot` | The bootstrap payload: snapshot metadata, managed sync state, global automation state, workflow summaries, requirement summaries, notifications, and `slice_revisions`. |
| `DomainSlice` | Identifies a projection domain (Universe, Devices, Inventory, Autofactories, Workflows, Operations, …). |
| `LiveDelta` | The WebSocket update envelope: snapshot, entity upsert/remove, domain invalidation, workflow and operation updates, notifications, automation and sync status changes. |

`RuntimeSnapshot` is deliberately compact. It carries summaries and revisions,
not the full entity set or every page projection — **keep it that way.** A
reconnecting client compares `slice_revisions` to decide which projections are
stale instead of discarding everything and refetching.

Page data belongs in typed projection endpoints (the `GET /api/galaxy-scene`
and `/api/system-scene/:system` pattern), not in the snapshot.

## Device claim tags

`replicant-protocol` is the single authority for the device-tag prefixes that
mark a device as claimed by a running workflow. These previously existed as
verbatim copies in the transport and other crates; do not reintroduce a local
copy.

## Tests

```sh
cargo test -p replicant-protocol --all-features
```

Serde round-trip coverage lives in `tests/finite_execution.rs`.
