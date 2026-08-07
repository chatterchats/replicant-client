# Replicant Space 2.4.0 contract refresh

This record captures the client-visible changes in the verified Replicant
Space 2.4.0 OpenAPI and rendered-document corpus generated on 2026-08-07.
Unlike the preceding rendered-only releases, the checked-in OpenAPI document
and documentation now describe the same 2.4.0 release.

## Corpus and operation surface

- OpenAPI SHA-256:
  `9f5498b2229e9aea6c20840b61267b09a0fffcc7ac87804b6c80db98fc058e31`
- Documentation manifest SHA-256:
  `54d5b394a5eb9f0ba54afb9eb1aae9bbad4d55a634a4299c1e7ab6c97c601f15`
- Rendered documentation pages: 84
- OpenAPI paths: 72
- OpenAPI operations: 86
- OpenAPI schemas: 160
- Callable operation inventory: 79 supported, 5 deprecated, 2 administrative

No route was added or removed. `policy/documented-operation-deltas.json`
therefore remains empty. The OpenAPI schema count increased by one because
`POST /v1/devices/{device_code}` gained the typed `triangulate` command shape.

## Wire-shape changes

### Galactic Observatory triangulation

The device command union now accepts:

```json
{
  "command": "triangulate",
  "signature": "a3f7c2e8b1d94f06",
  "target": [5000, 14000, 100]
}
```

The accepted response can include `status`, `signature`, `target`,
`started_at`, and `completes_at`. Completion is asynchronous and is reported
through one of these events:

- `triangulation.started`: `signature`, `target`, `completes_at`
- `triangulation.complete`: `signature`, `target`, `direction`
- `triangulation.failed`: `signature`, `target`, `reason`

`reason` is open for forward compatibility; the documented value is currently
`signature_not_found`.

The rendered Galactic Observatory page is authoritative for the response's
`signature` and `target` fields. The 2.4.0 OpenAPI request union includes the
new command schema, but its shared `DeviceCommandResponseSchema` still omits
those two response properties.

### Modular-device lifecycle events

The event catalogue adds:

- `device.compacting` with `completes_at`
- `device.compacted` with an empty payload
- `device.unfurling` with `completes_at`
- `device.unfurled` with an empty payload

### Printing events

- `print.started` adds `tags`.
- `print.completed` adds `tags` and `consumed_device_codes`.
- Replicant/vessel `PrintRequestSchema` exposes `flatpack`; both explicit
  `true` and explicit `false` are valid request values.

Consumed component codes are explicit removal evidence. The managed event
applier now tombstones those devices atomically with journaling the
`print.completed` event, preventing already-consumed components from remaining
available in the local cache.

## Behavior-only changes

The release also corrects mining-controller retargeting, relay-capable BobNet
routing, uncatalogued-location visibility, cancelled-travel achievements, and
deactivation of active prospecting or triangulation. These changes alter
server behavior without adding another client request field or route.

The intervening 2.3.6 release broadened BobNet routing to every device with the
open `relay` feature, established the Deep Space Relay Station's active
10-light-year range, and corrected blueprint directives on event device
rewards. Existing feature-driven/open models already cover those changes.

## Client implementation

- Raw device commands expose `DeviceCommand::Triangulate` and preserve the
  triangulation response fields.
- Raw replicant print requests preserve an explicitly supplied
  `flatpack: false` value rather than omitting it.
- The managed device gateway provides `triangulate(signature, [x, y, z])`.
- Durable triangulation operations wait for matching completion evidence and
  become `failed` when a matching `triangulation.failed` event arrives.
- Compact and unfurl operations use their new terminal events as durable
  completion evidence.
- Typed event helpers decode all seven newly documented event names and the
  expanded print payloads while retaining unknown fields.
- Raw and domain open vocabularies recognize the new event and command names
  without sacrificing unknown-value preservation.
