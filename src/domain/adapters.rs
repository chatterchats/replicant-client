use std::collections::BTreeMap;

use serde_json::Value;
use tracing::{trace, warn};

use super::*;
use crate::{events::GameEvent, raw};

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum NormalizeError {
    MissingIdentity(&'static str),
    InvalidScanReport,
}

impl core::fmt::Display for NormalizeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::MissingIdentity(field) => {
                write!(f, "response omitted required identity `{field}`")
            }
            Self::InvalidScanReport => {
                write!(f, "scan report is incomplete or has an unsupported shape")
            }
        }
    }
}

/// Normalizes the documented body portion of a `scan.completed` report.
///
/// Both direct scan events and AMI survey digests carry this exact shape.  The
/// report is intentionally retained as evidence because it is open-ended.
pub fn scan_report_location(
    scan_target: &str,
    scan_type: &str,
    report: &serde_json::Map<String, Value>,
    realm: Realm,
    observed_at: impl Into<ObservationTime>,
    event_id: &str,
) -> Result<Observation<Location>, NormalizeError> {
    if !matches!(scan_type, "planet" | "moon") {
        return Err(NormalizeError::InvalidScanReport);
    }
    let body = report
        .get(scan_type)
        .and_then(Value::as_object)
        .filter(|body| body.get("designation").and_then(Value::as_str) == Some(scan_target))
        .ok_or(NormalizeError::InvalidScanReport)?;
    let raw: raw::locations::Location = serde_json::from_value(serde_json::json!({
        "location": scan_target,
        "location_type": scan_type,
        "scanned": true,
        scan_type: body,
    }))
    .map_err(|_| NormalizeError::InvalidScanReport)?;
    let mut observation = location_detail(&raw, realm, observed_at)?;
    observation.metadata.source = ObservationSource::EventLog;
    observation.metadata.authority = ObservationAuthority::EventDelta;
    observation.metadata.source_document = SourceDocument {
        operation: "event:scan.completed".into(),
        request_id: None,
        document_id: Some(event_id.into()),
    };
    observation.value.unknown.insert(
        "event_scan_report".into(),
        sanitize_scan_evidence(&Value::Object(report.clone())),
    );
    Ok(observation)
}

impl std::error::Error for NormalizeError {}

fn metadata(
    operation: &str,
    observed_at: impl Into<ObservationTime>,
    source: ObservationSource,
    authority: ObservationAuthority,
    access: AccessScope,
    reachability: Reachability,
) -> ObservationMetadata {
    trace!(
        target: "replicant_client::domain",
        "normalizing observation operation={operation} source={source:?} authority={authority:?} access={access:?} reachability={reachability:?}"
    );
    ObservationMetadata {
        source,
        authority,
        observed_at: observed_at.into(),
        access,
        reachability,
        stale: false,
        source_document: SourceDocument {
            operation: operation.into(),
            request_id: None,
            document_id: None,
        },
    }
}

fn required(value: Option<&String>, field: &'static str) -> Result<String, NormalizeError> {
    value.cloned().ok_or_else(|| {
        warn!(
            target: "replicant_client::domain",
            "normalization rejected response missing_identity={field}"
        );
        NormalizeError::MissingIdentity(field)
    })
}

fn sanitize_scan_evidence(value: &Value) -> Value {
    match value {
        Value::Object(object) => Value::Object(
            object
                .iter()
                .map(|(key, value)| {
                    let key_lower = key.to_ascii_lowercase();
                    let value = if ["authorization", "password", "secret", "token"]
                        .iter()
                        .any(|sensitive| key_lower.contains(sensitive))
                    {
                        Value::String("<redacted>".into())
                    } else {
                        sanitize_scan_evidence(value)
                    };
                    (key.clone(), value)
                })
                .collect(),
        ),
        Value::Array(values) => Value::Array(values.iter().map(sanitize_scan_evidence).collect()),
        _ => value.clone(),
    }
}

fn knowledge<T>(value: Option<T>) -> Knowledge<T> {
    value.map_or(Knowledge::Unknown, Knowledge::Present)
}

pub fn account_me(
    raw: &raw::accounts::AccountMeResponse,
    id: AccountId,
    observed_at: impl Into<ObservationTime>,
) -> Observation<Account> {
    Observation {
        value: Account {
            id,
            name: raw.name.clone(),
            email: raw.email.clone(),
            timezone: raw.timezone.clone(),
            status: raw.status.clone(),
            experience_points_total: raw.experience_points_total,
        },
        metadata: metadata(
            "GET /v1/accounts/me",
            observed_at,
            ObservationSource::RestDetail,
            ObservationAuthority::EntitySnapshot,
            AccessScope::Owned,
            Reachability::Reachable,
        ),
    }
}

fn device(
    raw: &raw::devices::DeviceStatus,
    realm: Realm,
    access: AccessScope,
) -> Result<Device, NormalizeError> {
    let device_id = DeviceId::new(required(raw.device_code.as_ref(), "device_code")?);
    let location = raw
        .location
        .as_ref()
        .map(|value| WorldKey::in_realm(realm.clone(), LocationId::new(value)));
    let assigned_replicant = raw
        .replicant_code
        .as_ref()
        .map(|value| WorldKey::in_realm(realm.clone(), ReplicantId::new(value)));
    let hosting_replicant = raw
        .hosting_replicant
        .as_ref()
        .map(|value| WorldKey::in_realm(realm.clone(), ReplicantId::new(value)));
    let related = |value: &Option<String>| {
        value
            .as_ref()
            .map(|id| WorldKey::in_realm(realm.clone(), DeviceId::new(id)))
    };
    Ok(Device {
        key: WorldKey::in_realm(realm.clone(), device_id),
        device_type: raw.device_type.clone().map(DeviceType::from),
        status: raw.status.clone().map(DeviceStatus::from),
        location,
        features: raw
            .features
            .iter()
            .cloned()
            .map(DeviceFeature::from)
            .collect(),
        available_commands: raw
            .available_commands
            .iter()
            .cloned()
            .map(DeviceCommand::from)
            .collect(),
        available_directives: raw
            .available_directives
            .iter()
            .cloned()
            .map(DeviceDirective::from)
            .collect(),
        tags: raw.tags.clone(),
        relationships: DeviceRelationships {
            attached_to: related(&raw.attached_to_device_code),
            controller: related(&raw.controller_device_code),
            assigned_replicant,
            hosting_replicant,
        },
        access,
    })
}

pub fn device_detail(
    raw: &raw::devices::DeviceStatus,
    realm: Realm,
    access: AccessScope,
    observed_at: impl Into<ObservationTime>,
) -> Result<Observation<Device>, NormalizeError> {
    let value = device(raw, realm, access.clone())?;
    Ok(Observation {
        value,
        metadata: metadata(
            "GET /v1/devices/{device_code}",
            observed_at,
            ObservationSource::RestDetail,
            ObservationAuthority::EntitySnapshot,
            access,
            Reachability::Reachable,
        ),
    })
}

pub fn device_list_member(
    raw: &raw::devices::DeviceStatus,
    realm: Realm,
    access: AccessScope,
    observed_at: impl Into<ObservationTime>,
) -> Result<Observation<Device>, NormalizeError> {
    let value = device(raw, realm, access.clone())?;
    Ok(Observation {
        value,
        metadata: metadata(
            "GET /v1/devices",
            observed_at,
            ObservationSource::RestCollection,
            ObservationAuthority::CollectionMember,
            access,
            Reachability::Reachable,
        ),
    })
}

pub fn device_collection(
    raw: &raw::devices::DeviceListResponse,
    realm: Realm,
    filtered: bool,
    fully_traversed: bool,
    observed_at: impl Into<ObservationTime>,
) -> Result<CollectionObservation<Device>, NormalizeError> {
    let observed_at = observed_at.into();
    let members = raw
        .devices
        .iter()
        .map(|device| device_list_member(device, realm.clone(), AccessScope::Owned, observed_at))
        .collect::<Result<Vec<_>, _>>()?;
    let completeness = if !filtered && fully_traversed {
        CollectionCompleteness::Complete
    } else if filtered {
        CollectionCompleteness::Filtered
    } else {
        CollectionCompleteness::PartialPage
    };
    let authority = if completeness.can_reconcile_membership() {
        ObservationAuthority::CompleteCollection
    } else {
        ObservationAuthority::CollectionMember
    };
    Ok(CollectionObservation {
        members,
        completeness,
        metadata: metadata(
            "GET /v1/devices",
            observed_at,
            ObservationSource::RestCollection,
            authority,
            AccessScope::Owned,
            Reachability::Reachable,
        ),
    })
}

pub fn replicant_device_collection(
    raw: &raw::devices::DeviceListResponse,
    realm: Realm,
    access: AccessScope,
    observed_at: impl Into<ObservationTime>,
) -> Result<CollectionObservation<Device>, NormalizeError> {
    let observed_at = observed_at.into();
    let members = raw
        .devices
        .iter()
        .map(|device| device_list_member(device, realm.clone(), access.clone(), observed_at))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(CollectionObservation {
        members,
        completeness: CollectionCompleteness::RangeScoped,
        metadata: metadata(
            "GET /v1/replicants/{replicant_code}/devices",
            observed_at,
            ObservationSource::RestCollection,
            ObservationAuthority::CollectionMember,
            access,
            Reachability::OutOfRange,
        ),
    })
}

fn replicant(
    raw: &raw::replicants::ReplicantStatus,
    realm: Realm,
    access: AccessScope,
    owned: bool,
) -> Result<Replicant, NormalizeError> {
    let key = WorldKey::in_realm(
        realm.clone(),
        ReplicantId::new(required(raw.replicant_code.as_ref(), "replicant_code")?),
    );
    let location = raw
        .location
        .as_ref()
        .map(|id| WorldKey::in_realm(realm.clone(), LocationId::new(id)));
    let hosted_device = raw
        .hosted_device_code
        .as_ref()
        .map(|id| WorldKey::in_realm(realm, DeviceId::new(id)));
    let private = owned.then(|| OwnedReplicantData {
        description: raw.description.clone(),
        pronouns: raw.pronouns.clone(),
        experience_points: raw.experience_points,
        plan: raw.plan.clone(),
        cohort_permission: raw.cohort_permission.clone(),
    });
    Ok(Replicant {
        key,
        name: raw.name.clone(),
        is_npc: raw.is_npc,
        status: raw.status.clone().map(ReplicantStatus::from),
        location,
        hosted_device,
        private,
        access,
    })
}

pub fn owned_replicant_detail(
    raw: &raw::replicants::ReplicantStatus,
    realm: Realm,
    observed_at: impl Into<ObservationTime>,
) -> Result<Observation<Replicant>, NormalizeError> {
    Ok(Observation {
        value: replicant(raw, realm, AccessScope::Owned, true)?,
        metadata: metadata(
            "GET /v1/replicants/{replicant_code}",
            observed_at,
            ObservationSource::RestDetail,
            ObservationAuthority::EntitySnapshot,
            AccessScope::Owned,
            Reachability::Reachable,
        ),
    })
}

pub fn public_replicant_detail(
    raw: &raw::replicants::ReplicantStatus,
    realm: Realm,
    observed_at: impl Into<ObservationTime>,
) -> Result<Observation<Replicant>, NormalizeError> {
    Ok(Observation {
        value: replicant(raw, realm, AccessScope::Public, false)?,
        metadata: metadata(
            "GET /v1/replicants/{replicant_code}",
            observed_at,
            ObservationSource::RestDetail,
            ObservationAuthority::PublicProfile,
            AccessScope::Public,
            Reachability::Reachable,
        ),
    })
}

pub fn directory_profile(
    raw: &raw::replicants::ReplicantSearchItem,
    observed_at: impl Into<ObservationTime>,
) -> Result<Observation<DirectoryProfile>, NormalizeError> {
    let value = DirectoryProfile {
        id: ReplicantId::new(required(raw.replicant_code.as_ref(), "replicant_code")?),
        name: raw.name.clone(),
        last_location: raw.last_location.clone().map(LocationId::new),
        is_npc: raw.is_npc,
    };
    Ok(Observation {
        value,
        metadata: metadata(
            "GET /v1/replicants",
            observed_at,
            ObservationSource::RestCollection,
            ObservationAuthority::PublicProfile,
            AccessScope::Public,
            Reachability::Historical,
        ),
    })
}

pub fn location_detail(
    raw: &raw::locations::Location,
    realm: Realm,
    observed_at: impl Into<ObservationTime>,
) -> Result<Observation<Location>, NormalizeError> {
    let body = match raw.location_type.as_deref() {
        Some("planet") => raw.planet.as_ref(),
        Some("moon") => raw.moon.as_ref(),
        _ => raw.planet.as_ref().or(raw.moon.as_ref()),
    };
    let life_stage = body.and_then(|body| body.life_stage.clone());
    let survey_environment_evidence = body.is_some_and(|body| {
        body.atmosphere.is_some() || body.magnetic_field.is_some() || body.axial_tilt_deg.is_some()
    });
    let scanned = raw
        .scanned
        .or_else(|| survey_environment_evidence.then_some(true));
    let surveyed = scanned == Some(true);
    let value = Location {
        key: WorldKey::in_realm(
            realm.clone(),
            LocationId::new(required(raw.location.as_ref(), "location")?),
        ),
        location_type: raw.location_type.clone().map(LocationType::from),
        scanned,
        system_scanned: raw.system_scanned,
        system_tags: raw.system_tags.clone(),
        system: raw.system.clone(),
        parent: raw
            .parent
            .as_ref()
            .map(|parent| WorldKey::in_realm(realm.clone(), LocationId::new(parent))),
        environment: LocationEnvironment {
            atmosphere: knowledge(
                body.and_then(|body| body.atmosphere.clone())
                    .map(Atmosphere::from),
            ),
            magnetic_field: knowledge(body.and_then(|body| body.magnetic_field)),
            gravity_g: knowledge(
                body.and_then(|body| body.surface_gravity)
                    .filter(|value| value.is_finite()),
            ),
            surface_temp_c: knowledge(
                body.and_then(|body| body.surface_temp_c)
                    .filter(|value| value.is_finite()),
            ),
            in_habitable_zone: knowledge(body.and_then(|body| body.in_habitable_zone)),
            axial_tilt_deg: knowledge(
                body.and_then(|body| body.axial_tilt_deg)
                    .filter(|value| value.is_finite()),
            ),
            life_stage: match life_stage {
                Some(stage) => Knowledge::Present(LifeStage::from(stage)),
                None if surveyed && body.is_some() => Knowledge::Absent,
                None => Knowledge::Unknown,
            },
            ..LocationEnvironment::default()
        },
        unknown: raw.unknown.clone().into_iter().collect(),
    };
    Ok(Observation {
        value,
        metadata: metadata(
            "GET /v1/locations/{designation}",
            observed_at,
            ObservationSource::RestDetail,
            ObservationAuthority::EntitySnapshot,
            AccessScope::Owned,
            Reachability::Reachable,
        ),
    })
}

pub fn location_overview(
    raw: &raw::locations::LocationSystemMap,
    realm: Realm,
    observed_at: impl Into<ObservationTime>,
) -> CollectionObservation<LocationOverview> {
    let observed_at = observed_at.into();
    let members = raw
        .locations
        .iter()
        .map(|(id, count)| Observation {
            value: LocationOverview {
                key: WorldKey::in_realm(realm.clone(), LocationId::new(id)),
                device_count: count.devices.unwrap_or_default(),
                replicant_count: count.replicants.unwrap_or_default(),
            },
            metadata: metadata(
                "GET /v1/locations",
                observed_at,
                ObservationSource::RestCollection,
                ObservationAuthority::Discovery,
                AccessScope::Owned,
                Reachability::Reachable,
            ),
        })
        .collect();
    CollectionObservation {
        members,
        completeness: CollectionCompleteness::DiscoveryLimited,
        metadata: metadata(
            "GET /v1/locations",
            observed_at,
            ObservationSource::RestCollection,
            ObservationAuthority::Discovery,
            AccessScope::Owned,
            Reachability::Reachable,
        ),
    }
}

pub fn location_inventory(
    raw: &raw::inventory::LocationInventory,
    owner: InventoryOwner,
    realm: Realm,
    observed_at: impl Into<ObservationTime>,
) -> Result<Observation<Inventory>, NormalizeError> {
    let location = WorldKey::in_realm(
        realm,
        LocationId::new(required(raw.location.as_ref(), "location")?),
    );
    let items = raw
        .items
        .iter()
        .filter_map(|item| {
            Some(InventoryItem {
                resource: item.resource_type.clone()?,
                quantity: item.quantity?,
            })
        })
        .collect();
    Ok(Observation {
        value: Inventory {
            owner,
            location: Some(location),
            items,
        },
        metadata: metadata(
            "GET /v1/inventory",
            observed_at,
            ObservationSource::RestDetail,
            ObservationAuthority::EntitySnapshot,
            AccessScope::Owned,
            Reachability::Reachable,
        ),
    })
}

pub fn catalogue_star(
    raw: &raw::galaxy::CatalogueStar,
    realm: Realm,
    observed_at: impl Into<ObservationTime>,
) -> Result<Observation<Star>, NormalizeError> {
    let value = Star {
        key: WorldKey::in_realm(
            realm.clone(),
            StarId::new(required(raw.designation.as_ref(), "designation")?),
        ),
        name: raw.name.clone(),
        spectral_type: raw.spectral_type.clone(),
        entry_point: raw
            .entry_point
            .as_ref()
            .map(|id| WorldKey::in_realm(realm, LocationId::new(id))),
        position: raw.position.and_then(position),
        has_hub: raw.has_hub,
        region: raw.region.clone(),
    };
    Ok(Observation {
        value,
        metadata: metadata(
            "GET /v1/stars",
            observed_at,
            ObservationSource::RestCollection,
            ObservationAuthority::CompleteCollection,
            AccessScope::Owned,
            Reachability::Historical,
        ),
    })
}

fn position(raw: raw::Position) -> Option<GalacticPosition> {
    (raw.x.is_finite() && raw.y.is_finite() && raw.z.is_finite()).then_some(GalacticPosition {
        x: raw.x,
        y: raw.y,
        z: raw.z,
    })
}

/// Normalizes one paged star listing without claiming catalogue authority or
/// membership completeness.
pub fn replicant_star_knowledge(
    raw: &raw::galaxy::StarItem,
    replicant: ReplicantKey,
    realm: Realm,
    observed_at: impl Into<ObservationTime>,
) -> Result<Observation<StarKnowledge>, NormalizeError> {
    let star = WorldKey::in_realm(
        realm.clone(),
        StarId::new(required(raw.designation.as_ref(), "designation")?),
    );
    Ok(Observation {
        value: StarKnowledge {
            replicant,
            star,
            position: raw.position.and_then(position),
            spectral_type: raw.spectral_type.clone(),
            entry_point: raw
                .entry_point
                .as_ref()
                .map(|id| WorldKey::in_realm(realm, LocationId::new(id))),
            explored: raw.explored,
            has_hub: raw.has_hub,
            has_life: raw.has_life,
            region: raw.region.clone(),
            distance_from_replicant: raw
                .distance_from_replicant
                .filter(|value| value.is_finite()),
            estimated_travel_time: raw.estimated_travel_time,
        },
        metadata: metadata(
            "GET /v1/replicants/{replicant_code}/stars",
            observed_at,
            ObservationSource::RestCollection,
            ObservationAuthority::Discovery,
            AccessScope::Owned,
            Reachability::Historical,
        ),
    })
}

pub fn account_event(
    raw: &GameEvent,
    realm: Option<Realm>,
    observed_at: impl Into<ObservationTime>,
) -> Observation<Event> {
    // An unknown realm is deliberately not Live: entity keys would otherwise
    // let an unresolved simulation event mutate a same-code live projection.
    let device = realm
        .as_ref()
        .zip(raw.device_code.as_ref())
        .map(|(realm, id)| WorldKey::in_realm(realm.clone(), DeviceId::new(id)));
    let replicant = realm
        .as_ref()
        .zip(raw.replicant_code.as_ref())
        .map(|(realm, id)| WorldKey::in_realm(realm.clone(), ReplicantId::new(id)));
    let location = realm
        .as_ref()
        .zip(raw.location.as_ref())
        .map(|(realm, id)| WorldKey::in_realm(realm.clone(), LocationId::new(id)));
    let star = realm
        .as_ref()
        .zip(raw.star.as_ref())
        .map(|(realm, id)| WorldKey::in_realm(realm.clone(), StarId::new(id)));
    let value = Event {
        id: EventId::new(&raw.id),
        realm,
        name: EventName::from(raw.event.clone()),
        category: EventCategory::from(raw.category.clone()),
        device,
        replicant,
        location,
        star,
        occurred_at: raw.created_at.clone(),
        payload: raw
            .payload
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<BTreeMap<_, Value>>(),
    };
    Observation {
        value,
        metadata: metadata(
            "GET /v1/events",
            observed_at,
            ObservationSource::EventLog,
            ObservationAuthority::EventDelta,
            AccessScope::Owned,
            Reachability::Historical,
        ),
    }
}

pub fn simulation_start(
    raw: &raw::simulations::SimulationEnterResponse,
    observed_at: impl Into<ObservationTime>,
) -> Result<Observation<Simulation>, NormalizeError> {
    let id = raw
        .simulation_id
        .map(SimulationId::new)
        .ok_or(NormalizeError::MissingIdentity("simulation_id"))?;
    let realm = Realm::Simulation(id);
    let value = Simulation {
        id,
        scenario_code: raw.scenario_code.clone(),
        scenario_name: raw.scenario_name.clone(),
        starting_location: raw
            .starting_location
            .as_ref()
            .map(|location| WorldKey::in_realm(realm.clone(), LocationId::new(location))),
        starting_star: raw
            .starting_star
            .as_ref()
            .map(|star| WorldKey::in_realm(realm, StarId::new(star))),
        is_mine: true,
        started_at: None,
        completed_at: None,
        lifecycle: SimulationLifecycle::Synchronizing,
        seed_failures: Vec::new(),
        replicant_code: None,
    };
    Ok(Observation {
        value,
        metadata: metadata(
            "POST /v1/devices/{device_code}/simulate",
            observed_at,
            ObservationSource::CommandResponse,
            ObservationAuthority::OperationResult,
            AccessScope::Owned,
            Reachability::Reachable,
        ),
    })
}

/// Normalizes one owned run from the complete account simulation history.
/// History is additive: an absent entry is never evidence that a local run was
/// deleted.
pub fn simulation_history(
    raw: &raw::simulations::SimulationHistoryEntry,
    observed_at: impl Into<ObservationTime>,
) -> Result<Observation<Simulation>, NormalizeError> {
    let id = raw
        .id
        .map(SimulationId::new)
        .ok_or(NormalizeError::MissingIdentity("id"))?;
    let completed_at = raw
        .completed_at
        .clone()
        .or_else(|| raw.abandoned_at.clone())
        .or_else(|| raw.timed_out_at.clone());
    Ok(Observation {
        value: Simulation {
            id,
            scenario_code: raw.scenario_code.clone(),
            scenario_name: raw.scenario_name.clone(),
            starting_location: None,
            starting_star: None,
            is_mine: true,
            started_at: raw.started_at.clone(),
            completed_at,
            lifecycle: SimulationLifecycle::Ended,
            seed_failures: Vec::new(),
            replicant_code: None,
        },
        metadata: metadata(
            "GET /v1/accounts/simulations",
            observed_at,
            ObservationSource::RestCollection,
            ObservationAuthority::EntitySnapshot,
            AccessScope::Owned,
            Reachability::Historical,
        ),
    })
}

#[cfg(test)]
mod location_tests {
    use super::*;

    #[test]
    fn nested_planet_environment_normalizes_without_losing_unknown_fields() {
        let raw: raw::locations::Location = serde_json::from_str(include_str!(
            "../../reference/replicant-space/fixtures/location-ilphard-3-sanitized.json"
        ))
        .expect("fixture decodes");
        let observation =
            location_detail(&raw, Realm::Live, ObservationTime::now()).expect("normalizes");
        assert_eq!(observation.value.scanned, Some(true));
        assert!(matches!(
            observation.value.environment.atmosphere,
            Knowledge::Present(Atmosphere::Dense)
        ));
        assert!(
            matches!(observation.value.environment.gravity_g, Knowledge::Present(value) if value == 2.06)
        );
        assert!(
            matches!(observation.value.environment.surface_temp_c, Knowledge::Present(value) if value == 125.0)
        );
        assert!(matches!(
            observation.value.environment.life_stage,
            Knowledge::Present(LifeStage::Intelligent)
        ));
        assert!(matches!(
            raw.planet
                .as_ref()
                .and_then(|planet| planet.unknown.get("future_environment")),
            Some(Value::Object(_))
        ));
    }
}
