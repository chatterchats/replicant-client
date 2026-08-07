# replicant-transport

Reusable point-to-point resource and device delivery for Replicant Space. The
crate separates what to move from event, mining, relay, and bootstrap-specific
completion logic.

Unlike the pure planners, this package uses the managed client for discovery,
travel, inventory operations, and durable mutations. It is unpublished.

## Use locally

From another workspace crate:

```toml
[dependencies]
replicant-transport = { path = "../replicant-transport" }
replicant-client = { path = "../.." }
```

```rust,ignore
use replicant_transport::{DeliveryOptions, DeliveryRequest, plan_delivery, execute_delivery};

let request = DeliveryRequest {
    origin: "SCEPTURUM-BELT-1".into(),
    destination: "THYFFAWFF-BELT-1".into(),
    resources: [("iron".into(), 500)].into_iter().collect(),
    devices: vec![],
    device_tags: vec![],
    carrier: None,
};

let plan = plan_delivery(&client, &request).await?;
let report = execute_delivery(&client, &plan, DeliveryOptions::default()).await?;
println!("{report:?}");
```

## Planning

`plan_delivery` resolves a location or system origin into concrete resource
pickup locations, payload device codes, and suitable cargo/device carriers.

- `DeliveryRequest` declares resource quantities, device quantities, optional
  device tags, and carrier preferences.
- `DeviceRequest` identifies device types and counts.
- `CarrierPreference` restricts carrier type/count when required.
- `DeliveryPlan` is explicit and serializable for inspection or persistence.

Planning reads live state but performs no gameplay mutation.

## Execution

`execute_delivery` collects, carries, delivers, optionally unfolds modular
payload, and optionally returns transports. `DeliveryOptions` controls wait
timeouts, polling, unfolding, and return behavior. `DeliveryReport` records
the resources and devices delivered.

`deliver_resources_with` and `deliver_devices_with` are narrower convenience
entry points when the caller already owns the surrounding workflow.

Mutations use `replicant-client` durable operations. `TransportError` preserves
planning, managed-client, timeout, inventory, carrier, and operation failures.

Use `replicant-cli transport` for mission-file persistence and restart-safe
command-line orchestration.

## Verify

```sh
cargo test -p replicant-transport
cargo clippy -p replicant-transport --all-targets -- -D warnings
```
