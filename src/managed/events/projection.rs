use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use crate::Result;
use crate::domain::{
    self, AccessScope, ActiveDeviceDirective, Blueprint, BlueprintId, Device, DeviceDirective,
    DeviceId, DeviceKey, Event, IncomingObject, IncomingObjectId, IncomingObjectKey,
    IncomingObjectStatus, LocationEvent, LocationEventId, LocationEventKey, LocationId,
    LocationKey, Message, Observation, ObservationAuthority, ObservationMetadata,
    ObservationSource, ObservationTime, Reachability, Realm, ReplicantId, ReplicantKey,
    ResourceSite, ResourceSiteId, ResourceSiteKey, Simulation, SimulationId, SimulationLifecycle,
    SourceDocument, StarId, StarKey, Trade, TradeId, TradeKey, TradeStatus, TravelState,
};
use crate::managed::Client;

use super::super::store::{EventProjectionBatch, ProjectionTombstone, ReconciliationTarget};

fn metadata(event: &Event) -> ObservationMetadata {
    ObservationMetadata {
        source: ObservationSource::EventLog,
        authority: ObservationAuthority::EventDelta,
        observed_at: ObservationTime::from(event.occurred_at.clone()),
        access: AccessScope::Owned,
        reachability: Reachability::Reachable,
        stale: false,
        source_document: SourceDocument {
            operation: format!("event:{}", event.name.as_str()),
            request_id: None,
            document_id: Some(event.id.as_str().to_owned()),
        },
    }
}

fn observed<T>(event: &Event, value: T) -> Observation<T> {
    Observation {
        value,
        metadata: metadata(event),
    }
}

fn extra(payload: &BTreeMap<String, Value>, typed: &[&str]) -> BTreeMap<String, Value> {
    payload
        .iter()
        .filter(|(key, _)| !typed.contains(&key.as_str()))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn strings(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect()
}

fn object_map(value: Option<&Value>) -> BTreeMap<String, Value> {
    value
        .and_then(Value::as_object)
        .map(|value| {
            value
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect()
        })
        .unwrap_or_default()
}

fn integer_map(value: Option<&Value>) -> BTreeMap<String, i64> {
    value
        .and_then(Value::as_object)
        .into_iter()
        .flat_map(|value| value.iter())
        .filter_map(|(key, value)| value.as_i64().map(|value| (key.clone(), value)))
        .collect()
}

fn narrow_reconciliation(event: &Event) -> ReconciliationTarget {
    let realm = event.realm.clone().unwrap_or_default();
    if let Some(device) = &event.device {
        return device_reconciliation(device);
    }
    if let Some(replicant) = &event.replicant {
        return ReconciliationTarget {
            work_id: format!("replicant:{}", replicant.id.as_str()),
            realm,
            kind: "replicant",
            payload: serde_json::json!({"id": replicant.id.as_str()}),
        };
    }
    if let Some(location) = &event.location {
        return location_reconciliation(location);
    }
    ReconciliationTarget {
        work_id: "account:event".to_owned(),
        realm,
        kind: "account",
        payload: serde_json::json!({"id": "account"}),
    }
}

fn device_reconciliation(key: &DeviceKey) -> ReconciliationTarget {
    ReconciliationTarget {
        work_id: format!("device:{}", key.id.as_str()),
        realm: key.realm.clone(),
        kind: "device",
        payload: serde_json::json!({"id": key.id.as_str()}),
    }
}

fn location_reconciliation(key: &LocationKey) -> ReconciliationTarget {
    ReconciliationTarget {
        work_id: format!("location:{}", key.id.as_str()),
        realm: key.realm.clone(),
        kind: "location",
        payload: serde_json::json!({"id": key.id.as_str()}),
    }
}

fn device(client: &Client, key: &DeviceKey) -> Option<Observation<Device>> {
    client.managed_state().device(key)
}

fn push_device(batch: &mut EventProjectionBatch, observation: Observation<Device>) {
    if let Some(existing) = batch
        .devices
        .iter_mut()
        .find(|existing| existing.value.key == observation.value.key)
    {
        *existing = observation;
    } else {
        batch.devices.push(observation);
    }
}

fn require_device(
    client: &Client,
    batch: &mut EventProjectionBatch,
    key: &DeviceKey,
) -> Option<Observation<Device>> {
    let value = device(client, key);
    if value.is_none()
        && !batch
            .reconciliation
            .iter()
            .any(|work| work.work_id == format!("device:{}", key.id.as_str()))
    {
        batch.reconciliation.push(device_reconciliation(key));
    }
    value
}

pub(super) fn projection_history_only(_: &Client, _: &Event) -> Result<EventProjectionBatch> {
    Ok(EventProjectionBatch::default())
}

pub(super) fn projection_reconciliation_only(
    _: &Client,
    event: &Event,
) -> Result<EventProjectionBatch> {
    if event.realm.is_none() {
        return Ok(EventProjectionBatch::default());
    }
    Ok(EventProjectionBatch {
        reconciliation: vec![narrow_reconciliation(event)],
        ..EventProjectionBatch::default()
    })
}

pub(super) fn projection_automation_primitives(
    client: &Client,
    event: &Event,
) -> Result<EventProjectionBatch> {
    let Some(realm) = event.realm.clone() else {
        return projection_reconciliation_only(client, event);
    };
    let mut batch = EventProjectionBatch::default();
    match event.name.as_str() {
        "salvage.discovered" => {
            let Some(designation) = event.payload.get("designation").and_then(Value::as_str) else {
                return projection_reconciliation_only(client, event);
            };
            let location = event
                .payload
                .get("location")
                .and_then(Value::as_str)
                .map(|id| LocationKey::in_realm(realm.clone(), LocationId::new(id)));
            batch.resource_sites.push(observed(
                event,
                ResourceSite {
                    key: ResourceSiteKey::in_realm(realm, ResourceSiteId::new(designation)),
                    location,
                    site_type: event
                        .payload
                        .get("salvage_type")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    name: event
                        .payload
                        .get("name")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    resources: object_map(event.payload.get("resources")),
                    extra: extra(
                        &event.payload,
                        &[
                            "designation",
                            "location",
                            "salvage_type",
                            "name",
                            "resources",
                        ],
                    ),
                },
            ));
        }
        "salvage.depleted" => {
            let Some(site) = event.payload.get("site").and_then(Value::as_str) else {
                return projection_reconciliation_only(client, event);
            };
            batch.deletions.push(ProjectionTombstone {
                realm,
                kind: "resource_site",
                item_id: site.to_owned(),
                evidence: "salvage.depleted",
            });
        }
        "system.object_detected" => {
            let Some(designation) = event
                .payload
                .get("object_designation")
                .and_then(Value::as_str)
            else {
                return projection_reconciliation_only(client, event);
            };
            let key =
                IncomingObjectKey::in_realm(realm.clone(), IncomingObjectId::new(designation));
            batch.incoming_objects.push(observed(
                event,
                IncomingObject {
                    key,
                    star: event.star.clone(),
                    size_class: event
                        .payload
                        .get("size_class")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    impact_target: event
                        .payload
                        .get("impact_target")
                        .and_then(Value::as_str)
                        .map(|id| LocationKey::in_realm(realm, LocationId::new(id))),
                    impact_eta: event
                        .payload
                        .get("impact_eta")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    discovery_source: event
                        .payload
                        .get("discovery_source")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    status: IncomingObjectStatus::Detected,
                    propulsor: None,
                    extra: extra(
                        &event.payload,
                        &[
                            "object_designation",
                            "size_class",
                            "impact_target",
                            "impact_eta",
                            "discovery_source",
                        ],
                    ),
                },
            ));
        }
        name if name.starts_with("diversion.") => {
            let current = client.locations().incoming_objects()?;
            if name == "diversion.deactivated" {
                let Some(code) = event.payload.get("device_code").and_then(Value::as_str) else {
                    return projection_reconciliation_only(client, event);
                };
                let key = DeviceKey::in_realm(realm, DeviceId::new(code));
                let mut matches = current
                    .into_iter()
                    .filter(|object| {
                        object.propulsor.as_ref() == Some(&key)
                            && matches!(
                                object.status,
                                IncomingObjectStatus::DiversionActive
                                    | IncomingObjectStatus::Partial
                            )
                    })
                    .collect::<Vec<_>>();
                if matches.len() != 1 {
                    batch.reconciliation.push(device_reconciliation(&key));
                } else {
                    if let Some(mut object) = matches.pop() {
                        object.propulsor = None;
                        batch.incoming_objects.push(observed(event, object));
                    }
                }
            } else {
                let Some(designation) = event
                    .payload
                    .get("object_designation")
                    .and_then(Value::as_str)
                else {
                    return projection_reconciliation_only(client, event);
                };
                let key =
                    IncomingObjectKey::in_realm(realm.clone(), IncomingObjectId::new(designation));
                let mut object = current
                    .into_iter()
                    .find(|object| object.key == key)
                    .unwrap_or(IncomingObject {
                        key,
                        star: event.star.clone(),
                        size_class: None,
                        impact_target: None,
                        impact_eta: None,
                        discovery_source: None,
                        status: IncomingObjectStatus::Detected,
                        propulsor: None,
                        extra: BTreeMap::new(),
                    });
                object.status = match name {
                    "diversion.activated" => IncomingObjectStatus::DiversionActive,
                    "diversion.partial" => IncomingObjectStatus::Partial,
                    "diversion.diverted" => IncomingObjectStatus::Diverted,
                    "diversion.impacted" => IncomingObjectStatus::Impacted,
                    _ => object.status,
                };
                if name == "diversion.activated" {
                    object.size_class = event
                        .payload
                        .get("size_class")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                        .or(object.size_class);
                    object.propulsor = event.device.clone();
                }
                object.extra.extend(extra(
                    &event.payload,
                    &["object_designation", "size_class", "outcome"],
                ));
                batch.incoming_objects.push(observed(event, object));
            }
        }
        _ => return projection_reconciliation_only(client, event),
    }
    Ok(batch)
}

pub(super) fn projection_device_movement(
    client: &Client,
    event: &Event,
) -> Result<EventProjectionBatch> {
    let Some(realm) = event.realm.clone() else {
        return projection_reconciliation_only(client, event);
    };
    let mut batch = EventProjectionBatch::default();
    match event.name.as_str() {
        "device.decommissioned" => {
            let Some(key) = event.device.clone() else {
                return projection_reconciliation_only(client, event);
            };
            batch.deletions.push(ProjectionTombstone {
                realm,
                kind: "device",
                item_id: key.id.as_str().to_owned(),
                evidence: "device.decommissioned",
            });
        }
        "device.attached" | "device.detached" => {
            let Some(carrier_key) = event.device.clone() else {
                return projection_reconciliation_only(client, event);
            };
            let Some(target) = event.payload.get("target_code").and_then(Value::as_str) else {
                return projection_reconciliation_only(client, event);
            };
            let child_key = DeviceKey::in_realm(realm, DeviceId::new(target));
            let carrier = require_device(client, &mut batch, &carrier_key);
            let child = require_device(client, &mut batch, &child_key);
            if let (Some(mut carrier), Some(mut child)) = (carrier, child) {
                if event.name.as_str() == "device.attached" {
                    if !carrier
                        .value
                        .relationships
                        .attached_devices
                        .contains(&child_key)
                    {
                        carrier
                            .value
                            .relationships
                            .attached_devices
                            .push(child_key.clone());
                        carrier.value.relationships.attached_devices.sort();
                    }
                    child.value.relationships.attached_to = Some(carrier_key);
                } else {
                    carrier
                        .value
                        .relationships
                        .attached_devices
                        .retain(|key| key != &child_key);
                    child.value.relationships.attached_to = None;
                }
                carrier.metadata = metadata(event);
                child.metadata = metadata(event);
                push_device(&mut batch, carrier);
                push_device(&mut batch, child);
                batch.reconciliation.clear();
            }
        }
        "device.stowed" | "device.deployed" => {
            let Some(child_key) = event.device.clone() else {
                return projection_reconciliation_only(client, event);
            };
            let field = if event.name.as_str() == "device.stowed" {
                "stowed_in_device_code"
            } else {
                "deployed_from_device_code"
            };
            let Some(carrier_code) = event.payload.get(field).and_then(Value::as_str) else {
                return projection_reconciliation_only(client, event);
            };
            let carrier_key = DeviceKey::in_realm(realm, DeviceId::new(carrier_code));
            let child = require_device(client, &mut batch, &child_key);
            let carrier = require_device(client, &mut batch, &carrier_key);
            if let (Some(mut child), Some(mut carrier)) = (child, carrier) {
                if event.name.as_str() == "device.stowed" {
                    child.value.relationships.stowed_in = Some(carrier_key.clone());
                    if !carrier
                        .value
                        .relationships
                        .stowed_devices
                        .contains(&child_key)
                    {
                        carrier
                            .value
                            .relationships
                            .stowed_devices
                            .push(child_key.clone());
                        carrier.value.relationships.stowed_devices.sort();
                    }
                } else {
                    child.value.relationships.stowed_in = None;
                    carrier
                        .value
                        .relationships
                        .stowed_devices
                        .retain(|key| key != &child_key);
                }
                child.metadata = metadata(event);
                carrier.metadata = metadata(event);
                push_device(&mut batch, child);
                push_device(&mut batch, carrier);
                batch.reconciliation.clear();
            }
        }
        "device.changed_owner" => {
            let Some(key) = event.device.clone() else {
                return projection_reconciliation_only(client, event);
            };
            if let Some(mut observation) = require_device(client, &mut batch, &key) {
                observation.value.relationships.assigned_replicant = event
                    .payload
                    .get("to_replicant")
                    .and_then(Value::as_str)
                    .map(|id| ReplicantKey::in_realm(realm, ReplicantId::new(id)));
                observation.metadata = metadata(event);
                push_device(&mut batch, observation);
                batch.reconciliation.clear();
            }
        }
        "travel.departed" | "travel.arrived" | "travel.cancelled" => {
            let Some(carrier_key) = event.device.clone() else {
                return projection_reconciliation_only(client, event);
            };
            let attached = strings(event.payload.get("attached_devices"));
            let keys = std::iter::once(carrier_key)
                .chain(
                    attached
                        .into_iter()
                        .map(|id| DeviceKey::in_realm(realm.clone(), DeviceId::new(id))),
                )
                .collect::<BTreeSet<_>>();
            for key in keys {
                let Some(mut observation) = require_device(client, &mut batch, &key) else {
                    continue;
                };
                match event.name.as_str() {
                    "travel.departed" => {
                        observation.value.location = None;
                        observation.value.travel =
                            Some(TravelState {
                                arrives_at: event
                                    .payload
                                    .get("arrives_at")
                                    .and_then(Value::as_str)
                                    .map(str::to_owned),
                                departed_at: Some(event.occurred_at.clone()),
                                destination: event
                                    .payload
                                    .get("destination")
                                    .and_then(Value::as_str)
                                    .map(|id| {
                                        LocationKey::in_realm(realm.clone(), LocationId::new(id))
                                    }),
                                origin: event.payload.get("origin").and_then(Value::as_str).map(
                                    |id| LocationKey::in_realm(realm.clone(), LocationId::new(id)),
                                ),
                                stage: Some("traveling".to_owned()),
                                travel_type: event
                                    .payload
                                    .get("travel_type")
                                    .and_then(Value::as_str)
                                    .map(str::to_owned),
                                ..TravelState::default()
                            });
                    }
                    "travel.arrived" => {
                        observation.value.location = event
                            .payload
                            .get("destination")
                            .and_then(Value::as_str)
                            .map(|id| LocationKey::in_realm(realm.clone(), LocationId::new(id)));
                        observation.value.travel = None;
                    }
                    _ => {
                        observation.value.location = event
                            .payload
                            .get("origin")
                            .and_then(Value::as_str)
                            .map(|id| LocationKey::in_realm(realm.clone(), LocationId::new(id)));
                        observation.value.travel = None;
                    }
                }
                observation.metadata = metadata(event);
                push_device(&mut batch, observation);
            }
            if !batch.devices.is_empty() {
                batch.reconciliation.clear();
            }
        }
        "replicant.transferred" | "teleport.completed" => {
            let Some(replicant_key) = event.replicant.clone() else {
                return projection_reconciliation_only(client, event);
            };
            let host_field = if event.name.as_str() == "replicant.transferred" {
                "new_host"
            } else {
                "new_host_code"
            };
            let Some(host_code) = event.payload.get(host_field).and_then(Value::as_str) else {
                return projection_reconciliation_only(client, event);
            };
            let host_key = DeviceKey::in_realm(realm, DeviceId::new(host_code));
            if let Some(mut replicant) = client.managed_state().replicant(&replicant_key) {
                replicant.value.hosted_device = Some(host_key.clone());
                replicant.metadata = metadata(event);
                batch.replicants.push(replicant);
            } else {
                batch.reconciliation.push(narrow_reconciliation(event));
            }
            if let Some(mut host) = device(client, &host_key) {
                host.value.relationships.hosting_replicant = Some(replicant_key);
                host.metadata = metadata(event);
                push_device(&mut batch, host);
            } else {
                batch.reconciliation.push(device_reconciliation(&host_key));
            }
        }
        _ => return projection_reconciliation_only(client, event),
    }
    Ok(batch)
}

fn resource_site(
    event: &Event,
    realm: Realm,
    location: Option<LocationKey>,
    value: &serde_json::Map<String, Value>,
) -> Option<Observation<ResourceSite>> {
    let designation = value
        .get("designation")
        .or_else(|| value.get("site"))?
        .as_str()?;
    Some(observed(
        event,
        ResourceSite {
            key: ResourceSiteKey::in_realm(realm, ResourceSiteId::new(designation)),
            location,
            site_type: value
                .get("site_type")
                .and_then(Value::as_str)
                .map(str::to_owned),
            name: value.get("name").and_then(Value::as_str).map(str::to_owned),
            resources: object_map(value.get("resources")),
            extra: value
                .iter()
                .filter(|(key, _)| {
                    !["designation", "site", "site_type", "name", "resources"]
                        .contains(&key.as_str())
                })
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        },
    ))
}

fn update_star_flag(
    client: &Client,
    event: &Event,
    star_id: &str,
    hub: Option<bool>,
    ward: Option<bool>,
    entry_point: Option<&str>,
) -> EventProjectionBatch {
    let realm = event.realm.clone().unwrap_or_default();
    let key = StarKey::in_realm(realm.clone(), StarId::new(star_id));
    let Some(mut star) = client
        .managed_state()
        .catalogue()
        .into_iter()
        .find(|star| star.value.key == key)
    else {
        return EventProjectionBatch {
            reconciliation: vec![ReconciliationTarget {
                work_id: format!("location:{star_id}"),
                realm,
                kind: "location",
                payload: serde_json::json!({"id": star_id}),
            }],
            ..EventProjectionBatch::default()
        };
    };
    if let Some(hub) = hub {
        star.value.has_hub = Some(hub);
    }
    if let Some(ward) = ward {
        star.value.has_ward = Some(ward);
    }
    if let Some(entry_point) = entry_point {
        star.value.entry_point = Some(LocationKey::in_realm(realm, LocationId::new(entry_point)));
    }
    star.metadata = metadata(event);
    EventProjectionBatch {
        stars: vec![star],
        ..EventProjectionBatch::default()
    }
}

pub(super) fn projection_world_lifecycle(
    client: &Client,
    event: &Event,
) -> Result<EventProjectionBatch> {
    let Some(realm) = event.realm.clone() else {
        return projection_reconciliation_only(client, event);
    };
    let mut batch = EventProjectionBatch::default();
    match event.name.as_str() {
        "scan.completed" | "ami.survey.digest" => {
            let (locations, fallbacks) = super::scan_projection(event);
            batch.locations = locations;
            batch
                .reconciliation
                .extend(fallbacks.into_iter().map(|(realm, id)| {
                    location_reconciliation(&LocationKey::in_realm(realm, LocationId::new(id)))
                }));
            let scan_entries = if event.name.as_str() == "scan.completed" {
                vec![event.payload.clone()]
            } else {
                event
                    .payload
                    .get("report")
                    .and_then(|value| value.get("scans"))
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_object)
                    .map(|value| value.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                    .collect()
            };
            for scan in scan_entries {
                let location = scan
                    .get("scan_target")
                    .and_then(Value::as_str)
                    .map(|id| LocationKey::in_realm(realm.clone(), LocationId::new(id)));
                let sites = scan
                    .get("report")
                    .and_then(Value::as_object)
                    .and_then(|report| report.get("belt"))
                    .and_then(Value::as_object)
                    .and_then(|belt| belt.get("resource_sites"))
                    .and_then(Value::as_array);
                for site in sites.into_iter().flatten().filter_map(Value::as_object) {
                    if let Some(site) = resource_site(event, realm.clone(), location.clone(), site)
                    {
                        batch.resource_sites.push(site);
                    }
                }
            }
        }
        "search.completed" => {
            let location = event
                .payload
                .get("search_target")
                .and_then(Value::as_str)
                .map(|id| LocationKey::in_realm(realm.clone(), LocationId::new(id)));
            if let Some(report) = event.payload.get("report").and_then(Value::as_object)
                && let Some(site) = resource_site(event, realm, location, report)
            {
                batch.resource_sites.push(site);
            }
            if batch.resource_sites.is_empty() {
                return projection_reconciliation_only(client, event);
            }
        }
        "site.depleted" => {
            let Some(site) = event.payload.get("site").and_then(Value::as_str) else {
                return projection_reconciliation_only(client, event);
            };
            batch.deletions.push(ProjectionTombstone {
                realm,
                kind: "resource_site",
                item_id: site.to_owned(),
                evidence: "site.depleted",
            });
        }
        "event.discovered" => {
            let Some(designation) = event.payload.get("designation").and_then(Value::as_str) else {
                return projection_reconciliation_only(client, event);
            };
            batch.location_events.push(observed(
                event,
                LocationEvent {
                    key: LocationEventKey::in_realm(
                        realm.clone(),
                        LocationEventId::new(designation),
                    ),
                    location: event
                        .payload
                        .get("location")
                        .and_then(Value::as_str)
                        .map(|id| LocationKey::in_realm(realm, LocationId::new(id))),
                    event_type: event
                        .payload
                        .get("event_type")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    tier: event.payload.get("tier").and_then(Value::as_i64),
                    title: event
                        .payload
                        .get("title")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    description: event
                        .payload
                        .get("description")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    criteria: event
                        .payload
                        .get("criteria")
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten()
                        .filter_map(Value::as_object)
                        .map(|value| value.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                        .collect(),
                    extra: extra(
                        &event.payload,
                        &[
                            "designation",
                            "location",
                            "event_type",
                            "tier",
                            "title",
                            "description",
                            "criteria",
                        ],
                    ),
                },
            ));
        }
        "event.completed" => {
            let Some(designation) = event.payload.get("designation").and_then(Value::as_str) else {
                return projection_reconciliation_only(client, event);
            };
            batch.deletions.push(ProjectionTombstone {
                realm,
                kind: "location_event",
                item_id: designation.to_owned(),
                evidence: "event.completed",
            });
        }
        "hub.activated" | "hub.destroyed" => {
            let Some(star) = event.payload.get("star").and_then(Value::as_str) else {
                return projection_reconciliation_only(client, event);
            };
            batch = update_star_flag(
                client,
                event,
                star,
                Some(event.name.as_str() == "hub.activated"),
                None,
                None,
            );
        }
        "ward.activated" | "ward.deactivated" => {
            let Some(star) = event.star.as_ref() else {
                return projection_reconciliation_only(client, event);
            };
            batch = update_star_flag(
                client,
                event,
                star.id.as_str(),
                None,
                Some(event.name.as_str() == "ward.activated"),
                None,
            );
        }
        "system.entry_point_set" => {
            let Some(star) = event.payload.get("star").and_then(Value::as_str) else {
                return projection_reconciliation_only(client, event);
            };
            let Some(entry) = event.payload.get("entry_point").and_then(Value::as_str) else {
                return projection_reconciliation_only(client, event);
            };
            batch = update_star_flag(client, event, star, None, None, Some(entry));
        }
        "system.body_renamed" => {
            let Some(designation) = event.payload.get("designation").and_then(Value::as_str) else {
                return projection_reconciliation_only(client, event);
            };
            let key = LocationKey::in_realm(realm, LocationId::new(designation));
            let Some(mut location) = client.managed_state().location(&key) else {
                return Ok(EventProjectionBatch {
                    reconciliation: vec![location_reconciliation(&key)],
                    ..EventProjectionBatch::default()
                });
            };
            location.value.custom_name = event
                .payload
                .get("new_name")
                .and_then(Value::as_str)
                .map(str::to_owned);
            location.metadata = metadata(event);
            batch.locations.push(location);
        }
        "hub.warning" | "hub.maintained" => {
            let Some(key) = event.device.clone() else {
                return projection_reconciliation_only(client, event);
            };
            let Some(mut device) = require_device(client, &mut batch, &key) else {
                return Ok(batch);
            };
            device.value.operational_capacity = event
                .payload
                .get("capacity")
                .and_then(Value::as_f64)
                .and_then(domain::OperationalCapacity::new);
            device.metadata = metadata(event);
            push_device(&mut batch, device);
            batch.reconciliation.clear();
        }
        _ => return projection_reconciliation_only(client, event),
    }
    Ok(batch)
}

pub(super) fn projection_account_content(
    client: &Client,
    event: &Event,
) -> Result<EventProjectionBatch> {
    let realm = event.realm.clone().unwrap_or_default();
    let mut batch = EventProjectionBatch::default();
    match event.name.as_str() {
        "blueprint.unlocked" => {
            let Some(device_type) = event.payload.get("device_type").and_then(Value::as_str) else {
                return projection_reconciliation_only(client, event);
            };
            let mut unknown = extra(
                &event.payload,
                &[
                    "device_type",
                    "short_description",
                    "description",
                    "resources",
                    "components",
                    "print_time",
                ],
            );
            if let Some(value) = event.payload.get("requires_autofactory") {
                unknown.insert("requires_autofactory".to_owned(), value.clone());
            }
            batch.blueprints.push(observed(
                event,
                Blueprint {
                    id: BlueprintId::new(device_type),
                    device_type: Some(domain::DeviceType::from(device_type)),
                    short_description: event
                        .payload
                        .get("short_description")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    description: event
                        .payload
                        .get("description")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    print_time_seconds: event.payload.get("print_time").and_then(Value::as_f64),
                    resources: integer_map(event.payload.get("resources")),
                    components: integer_map(event.payload.get("components")),
                    features: Vec::new(),
                    directives: Vec::new(),
                    cargo_capacity: None,
                    attach_capacity: None,
                    stow_capacity: None,
                    queue_size: None,
                    unknown,
                },
            ));
        }
        "message.new" => {
            batch.messages.push(observed(
                event,
                Message {
                    id: event.payload.get("message_id").and_then(Value::as_i64),
                    title: event
                        .payload
                        .get("title")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    body: event
                        .payload
                        .get("body")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    subcategory: None,
                    category: None,
                    message_type: event
                        .payload
                        .get("message_type")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    is_read: Some(false),
                    created_at: Some(event.occurred_at.clone()),
                },
            ));
        }
        "trade.created" | "trade.completed" => {
            let Some(controller) = event.device.clone() else {
                return projection_reconciliation_only(client, event);
            };
            if event.name.as_str() == "trade.completed" {
                batch
                    .reconciliation
                    .push(device_reconciliation(&controller));
                for code in strings(event.payload.get("new_device_codes")) {
                    batch
                        .reconciliation
                        .push(device_reconciliation(&DeviceKey::in_realm(
                            realm.clone(),
                            DeviceId::new(code),
                        )));
                }
                for outcome in ["rewards_received", "criteria_received"] {
                    for code in strings(
                        event
                            .payload
                            .get(outcome)
                            .and_then(Value::as_object)
                            .and_then(|value| value.get("devices")),
                    ) {
                        batch
                            .reconciliation
                            .push(device_reconciliation(&DeviceKey::in_realm(
                                realm.clone(),
                                DeviceId::new(code),
                            )));
                    }
                }
            }
            let Some(code) = event.payload.get("trade_code").and_then(Value::as_str) else {
                return if event.name.as_str() == "trade.completed" {
                    Ok(batch)
                } else {
                    projection_reconciliation_only(client, event)
                };
            };
            batch.trades.push(observed(
                event,
                Trade {
                    key: TradeKey::in_realm(realm, TradeId::new(code)),
                    controller,
                    status: Some(TradeStatus::from(
                        if event.name.as_str() == "trade.created" {
                            "active"
                        } else {
                            "completed"
                        },
                    )),
                    name: event
                        .payload
                        .get("name")
                        .or_else(|| event.payload.get("trade_name"))
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    remaining_stock: event
                        .payload
                        .get("remaining_stock")
                        .or_else(|| event.payload.get("stock"))
                        .and_then(Value::as_i64),
                    extra: extra(
                        &event.payload,
                        &[
                            "trade_code",
                            "name",
                            "trade_name",
                            "remaining_stock",
                            "stock",
                        ],
                    ),
                },
            ));
        }
        "trade.deleted" => {
            let Some(code) = event.payload.get("trade_code").and_then(Value::as_str) else {
                return projection_reconciliation_only(client, event);
            };
            batch.deletions.push(ProjectionTombstone {
                realm,
                kind: "trade",
                item_id: code.to_owned(),
                evidence: "trade.deleted",
            });
        }
        "megastructure.contributed" => {
            let values = event
                .payload
                .get("contributed_devices")
                .and_then(Value::as_array);
            for code in values
                .into_iter()
                .flatten()
                .filter_map(Value::as_object)
                .filter_map(|value| value.get("device_code"))
                .filter_map(Value::as_str)
            {
                batch.deletions.push(ProjectionTombstone {
                    realm: realm.clone(),
                    kind: "device",
                    item_id: code.to_owned(),
                    evidence: "megastructure.contributed",
                });
            }
            batch.reconciliation.push(narrow_reconciliation(event));
        }
        _ => return projection_reconciliation_only(client, event),
    }
    Ok(batch)
}

pub(super) fn projection_operational_lifecycle(
    client: &Client,
    event: &Event,
) -> Result<EventProjectionBatch> {
    let realm = event.realm.clone().unwrap_or_default();
    let mut batch = EventProjectionBatch::default();
    match event.name.as_str() {
        "ami.adopted" | "ami.released" => {
            let Some(controller_key) = event.device.clone() else {
                return projection_reconciliation_only(client, event);
            };
            let child_keys = event
                .payload
                .get("devices")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_object)
                .filter_map(|value| value.get("device_code"))
                .filter_map(Value::as_str)
                .map(|id| DeviceKey::in_realm(realm.clone(), DeviceId::new(id)))
                .collect::<Vec<_>>();
            let controller = require_device(client, &mut batch, &controller_key);
            if let Some(mut controller) = controller {
                for child_key in &child_keys {
                    if let Some(mut child) = require_device(client, &mut batch, child_key) {
                        if event.name.as_str() == "ami.adopted" {
                            child.value.relationships.controller = Some(controller_key.clone());
                            if !controller
                                .value
                                .relationships
                                .controlled_devices
                                .contains(child_key)
                            {
                                controller
                                    .value
                                    .relationships
                                    .controlled_devices
                                    .push(child_key.clone());
                            }
                        } else {
                            child.value.relationships.controller = None;
                            controller
                                .value
                                .relationships
                                .controlled_devices
                                .retain(|key| key != child_key);
                        }
                        child.metadata = metadata(event);
                        push_device(&mut batch, child);
                    }
                }
                controller.value.relationships.controlled_devices.sort();
                controller.metadata = metadata(event);
                push_device(&mut batch, controller);
                if batch.devices.len() == child_keys.len() + 1 {
                    batch.reconciliation.clear();
                }
            }
        }
        name if name.starts_with("directive.") => {
            let Some(key) = event.device.clone() else {
                return projection_reconciliation_only(client, event);
            };
            let Some(mut device) = require_device(client, &mut batch, &key) else {
                return Ok(batch);
            };
            match name {
                "directive.cleared" => device.value.active_directive = None,
                _ => {
                    let directive = event
                        .payload
                        .get("directive")
                        .and_then(Value::as_str)
                        .map(DeviceDirective::from);
                    let status = Some(
                        match name {
                            "directive.completed" => "completed",
                            "directive.paused" => "paused",
                            "directive.resumed" | "directive.set" => "active",
                            _ => "unknown",
                        }
                        .to_owned(),
                    );
                    device.value.active_directive = Some(ActiveDeviceDirective {
                        directive,
                        status,
                        details: object_map(event.payload.get("configuration")),
                    });
                }
            }
            device.metadata = metadata(event);
            push_device(&mut batch, device);
            batch.reconciliation.clear();
        }
        "print.completed" => {
            for code in strings(event.payload.get("consumed_device_codes")) {
                batch.deletions.push(ProjectionTombstone {
                    realm: realm.clone(),
                    kind: "device",
                    item_id: code,
                    evidence: "print.completed",
                });
            }
            if let Some(controller) = event.device.as_ref() {
                batch.reconciliation.push(device_reconciliation(controller));
            }
            if let Some(code) = event.payload.get("new_device_code").and_then(Value::as_str) {
                batch
                    .reconciliation
                    .push(device_reconciliation(&DeviceKey::in_realm(
                        realm,
                        DeviceId::new(code),
                    )));
            }
        }
        "simulation.started" => {
            let Some(id) = event.payload.get("simulation_id").and_then(Value::as_i64) else {
                return projection_reconciliation_only(client, event);
            };
            batch.simulations.push(observed(
                event,
                Simulation {
                    id: SimulationId::new(id),
                    scenario_code: event
                        .payload
                        .get("scenario_code")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    scenario_name: None,
                    starting_location: None,
                    starting_star: event
                        .payload
                        .get("starting_star")
                        .and_then(Value::as_str)
                        .map(|star| {
                            StarKey::in_realm(
                                Realm::Simulation(SimulationId::new(id)),
                                StarId::new(star),
                            )
                        }),
                    is_mine: true,
                    started_at: Some(event.occurred_at.clone()),
                    completed_at: None,
                    lifecycle: SimulationLifecycle::Active,
                    seed_failures: Vec::new(),
                    replicant_code: event
                        .replicant
                        .as_ref()
                        .map(|key| key.id.as_str().to_owned()),
                },
            ));
        }
        "simulation.abandoned" | "simulation.completed" | "simulation.expired" => {
            let Some(id) = event.payload.get("simulation_id").and_then(Value::as_i64) else {
                return projection_reconciliation_only(client, event);
            };
            let id = SimulationId::new(id);
            if let Some(mut simulation) = client
                .managed_state()
                .simulations()
                .into_iter()
                .find(|simulation| simulation.value.id == id)
            {
                simulation.value.lifecycle = SimulationLifecycle::Ended;
                simulation.value.completed_at = Some(event.occurred_at.clone());
                simulation.metadata = metadata(event);
                batch.simulations.push(simulation);
            }
            for device in client
                .managed_state()
                .devices()
                .into_iter()
                .filter(|device| device.value.key.realm == Realm::Simulation(id))
            {
                batch.deletions.push(ProjectionTombstone {
                    realm: Realm::Simulation(id),
                    kind: "device",
                    item_id: device.value.key.id.as_str().to_owned(),
                    evidence: "simulation-terminal-event",
                });
            }
        }
        "transport.collected" | "transport.delivered" => {
            let Some(key) = event.device.clone() else {
                return projection_reconciliation_only(client, event);
            };
            let Some(mut device) = require_device(client, &mut batch, &key) else {
                return Ok(batch);
            };
            let resources = integer_map(event.payload.get("resources"));
            for (resource, quantity) in resources {
                let current = device
                    .value
                    .cargo
                    .get(&resource)
                    .copied()
                    .unwrap_or_default();
                let updated = if event.name.as_str() == "transport.collected" {
                    current.saturating_add(quantity)
                } else {
                    current.saturating_sub(quantity)
                };
                device.value.cargo.insert(resource, updated);
            }
            device.value.cargo_capacity = event
                .payload
                .get("cargo_capacity")
                .and_then(Value::as_i64)
                .or(device.value.cargo_capacity);
            device.metadata = metadata(event);
            push_device(&mut batch, device);
            batch.reconciliation.clear();
        }
        _ => return projection_reconciliation_only(client, event),
    }
    Ok(batch)
}
