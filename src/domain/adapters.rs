use std::collections::BTreeMap;

use serde_json::Value;
use tracing::{trace, warn};

use super::*;
use crate::{events::GameEvent, raw};

const LOCATION_PROMOTED_FIELDS: &[&str] = &[
    "location",
    "location_type",
    "moons_scanned",
    "moons_total",
    "moons_total_estimated",
    "parent",
    "planets_scanned",
    "planets_total",
    "scanned",
    "system",
    "system_scanned",
    "system_tags",
];

const LOCATION_PASSTHROUGH_FIELDS: &[&str] = &[
    "active_location_events",
    "asteroid_belt",
    "belt",
    "devices",
    "entry_point",
    "estimated_travel_time",
    "inventory",
    "kuiper",
    "lagrange",
    "life_detected",
    "location_event",
    "megastructure",
    "mining_bonus_pct",
    "moon",
    "moons",
    "object",
    "oort",
    "outer_system",
    "planet",
    "planets",
    "resource_sites",
    "shops",
    "star",
    "system_objects",
];

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

fn related_device_list(values: &[raw::JsonObject], realm: &Realm) -> Vec<DeviceKey> {
    let mut devices = values
        .iter()
        .filter_map(|value| {
            value
                .get("device_code")
                .or_else(|| value.get("code"))
                .and_then(Value::as_str)
        })
        .map(|id| WorldKey::in_realm(realm.clone(), DeviceId::new(id)))
        .collect::<Vec<_>>();
    devices.sort();
    devices.dedup();
    devices
}

fn location_key(value: &Option<String>, realm: &Realm) -> Option<LocationKey> {
    value
        .as_ref()
        .map(|id| WorldKey::in_realm(realm.clone(), LocationId::new(id)))
}

fn whole_seconds(value: Option<f64>) -> Option<i64> {
    value.filter(|value| value.is_finite()).and_then(|value| {
        let rounded = value.round();
        ((value - rounded).abs() <= f64::EPSILON
            && rounded >= i64::MIN as f64
            && rounded <= i64::MAX as f64)
            .then_some(rounded as i64)
    })
}

fn travel_state(travel: &Option<raw::status::TravelInfo>, realm: &Realm) -> Option<TravelState> {
    travel.as_ref().map(|travel| {
        let mut details = BTreeMap::new();
        for (key, value) in [
            (
                "destination_name",
                serde_json::json!(travel.destination_name),
            ),
            (
                "destination_type",
                serde_json::json!(travel.destination_type),
            ),
            ("distance_au", serde_json::json!(travel.distance_au)),
            ("distance_ly", serde_json::json!(travel.distance_ly)),
            (
                "final_destination_name",
                serde_json::json!(travel.final_destination_name),
            ),
            ("origin_name", serde_json::json!(travel.origin_name)),
            (
                "progress_percent",
                serde_json::json!(travel.progress_percent),
            ),
            ("route", serde_json::json!(travel.route)),
            (
                "route_progress_percent",
                serde_json::json!(travel.route_progress_percent),
            ),
            (
                "total_distance_ly",
                serde_json::json!(travel.total_distance_ly),
            ),
            (
                "total_time_seconds",
                serde_json::json!(travel.total_time_seconds),
            ),
        ] {
            if !value.is_null() && !matches!(&value, Value::Array(values) if values.is_empty()) {
                details.insert(key.to_owned(), value);
            }
        }
        TravelState {
            arrives_at: travel.arrives_at.clone(),
            departed_at: travel.departed_at.clone(),
            destination: location_key(&travel.destination, realm),
            eta_seconds: whole_seconds(travel.eta_seconds),
            final_arrives_at: travel.final_arrives_at.clone(),
            final_destination: location_key(&travel.final_destination, realm),
            origin: location_key(&travel.origin, realm),
            route_eta_seconds: whole_seconds(travel.route_eta_seconds),
            stage: travel.stage.clone(),
            travel_type: travel.r#type.clone(),
            details,
        }
    })
}

fn activity_value<T: serde::Serialize>(value: &Option<T>) -> Option<Value> {
    value
        .as_ref()
        .and_then(|value| serde_json::to_value(value).ok())
}

fn device_runtime(raw: &raw::devices::DeviceStatus) -> DeviceRuntimeState {
    DeviceRuntimeState {
        created_at: raw.created_at.clone(),
        short_description: raw.short_description.clone(),
        description: raw.description.clone(),
        printing: activity_value(&raw.printing),
        mining: activity_value(&raw.mining),
        prospect: activity_value(&raw.prospect),
        repair: activity_value(&raw.repair),
        scan: activity_value(&raw.scan),
        waiting_for: raw.waiting_for.clone().map(Value::Object),
        print_queue: raw
            .print_queue
            .iter()
            .map(|item| item.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .collect(),
        queue_size: raw.queue_size,
        taxi_mode: raw.taxi_mode.clone(),
        tracking_site_id: raw.tracking_site_id,
        beacon_only: raw.beacon_only,
        welcome_message: raw.welcome_message.clone(),
        repair_paid_pct: raw.repair_paid_pct.map(|value| serde_json::json!(value)),
    }
}

fn active_device_directive(raw: &raw::devices::DeviceStatus) -> Option<ActiveDeviceDirective> {
    if raw.ami_directive.is_none() && raw.ami_directive_status.is_none() {
        return None;
    }
    let details = raw
        .ami_directive
        .clone()
        .unwrap_or_default()
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    let directive = details
        .get("directive")
        .or_else(|| details.get("name"))
        .and_then(Value::as_str)
        .map(DeviceDirective::from);
    Some(ActiveDeviceDirective {
        directive,
        status: raw.ami_directive_status.clone(),
        details,
    })
}

fn device(
    raw: &raw::devices::DeviceStatus,
    realm: Realm,
    access: AccessScope,
) -> Result<Device, NormalizeError> {
    let device_id = DeviceId::new(required(raw.device_code.as_ref(), "device_code")?);
    let location = location_key(&raw.location, &realm);
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
        deployed_at: raw.deployed_at.clone(),
        in_control_range: raw.in_control_range,
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
        settings: raw
            .settings
            .as_ref()
            .map(|settings| {
                settings
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect()
            })
            .unwrap_or_default(),
        relationships: DeviceRelationships {
            attached_to: related(&raw.attached_to_device_code),
            stowed_in: related(&raw.stowed_in_device_code),
            controller: related(&raw.controller_device_code),
            linked_device: related(&raw.linked_device),
            attached_devices: related_device_list(&raw.attached_devices, &realm),
            controlled_devices: related_device_list(&raw.controlled_devices, &realm),
            stowed_devices: related_device_list(&raw.stowed_devices, &realm),
            assigned_replicant,
            hosting_replicant,
        },
        cargo: raw
            .cargo
            .iter()
            .filter_map(|item| Some((item.resource_type.clone()?, item.quantity?)))
            .collect(),
        cargo_capacity: raw.cargo_capacity,
        attach_capacity: raw.attach_capacity,
        stow_capacity: raw.stow_capacity,
        stow_used: raw.stow_used,
        operational_capacity: raw.operational_capacity.and_then(OperationalCapacity::new),
        grace_period_remaining: raw.grace_period_remaining,
        upkeep_requirements: raw
            .upkeep_requirements
            .iter()
            .map(|value| {
                value
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect()
            })
            .collect(),
        system_status: raw.system_status.as_ref().map(|value| {
            value
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect()
        }),
        active_directive: active_device_directive(raw),
        travel: travel_state(&raw.travel, &realm),
        runtime: device_runtime(raw),
        access,
    })
}

/// Normalizes one unlocked account blueprint into the managed domain.
///
/// Blueprint resource/component maps are schema-backed integer quantities;
/// unrelated open response fields remain in `unknown`.
pub fn blueprint(raw: &raw::blueprints::Blueprint) -> Result<Blueprint, NormalizeError> {
    let device_type = required(raw.device_type.as_ref(), "device_type")?;
    let mut unknown: BTreeMap<String, Value> = raw
        .extra
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();

    let resources = raw.resources.clone().unwrap_or_default();
    let components = raw.components.clone().unwrap_or_default();
    if let Some(strength) = raw.strength {
        unknown.insert("strength".to_owned(), serde_json::json!(strength));
    }
    if let Some(current_hubs) = raw.current_hubs {
        unknown.insert("current_hubs".to_owned(), serde_json::json!(current_hubs));
    }

    Ok(Blueprint {
        id: BlueprintId::new(device_type.clone()),
        device_type: Some(DeviceType::from(device_type)),
        short_description: raw.short_description.clone(),
        description: raw.description.clone(),
        print_time_seconds: raw.print_time,
        resources,
        components,
        features: raw
            .features
            .as_deref()
            .unwrap_or_default()
            .iter()
            .cloned()
            .map(DeviceFeature::from)
            .collect(),
        directives: raw
            .directives
            .as_deref()
            .unwrap_or_default()
            .iter()
            .cloned()
            .map(DeviceDirective::from)
            .collect(),
        cargo_capacity: raw.cargo_capacity,
        attach_capacity: raw.attach_capacity,
        stow_capacity: raw.stow_capacity,
        queue_size: raw.queue_size,
        unknown,
    })
}

/// Normalizes one account inbox message.
pub fn message(
    raw: raw::messages::Message,
    observed_at: impl Into<ObservationTime>,
) -> Observation<Message> {
    Observation {
        value: Message {
            id: raw.id,
            title: raw.title,
            body: raw.body,
            category: raw.category,
            subcategory: raw.subcategory,
            message_type: raw.message_type,
            is_read: raw.is_read,
            created_at: raw.created_at,
        },
        metadata: metadata(
            "GET /v1/messages",
            observed_at,
            ObservationSource::RestCollection,
            ObservationAuthority::CollectionMember,
            AccessScope::Owned,
            Reachability::Reachable,
        ),
    }
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
        .map(|id| WorldKey::in_realm(realm.clone(), DeviceId::new(id)));
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
        travel: travel_state(&raw.travel, &realm),
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
    debug_assert!(!LOCATION_PROMOTED_FIELDS.is_empty() && !LOCATION_PASSTHROUGH_FIELDS.is_empty());
    let body = match raw.location_type.as_deref() {
        Some("planet") => raw.planet.as_ref(),
        Some("moon") => raw.moon.as_ref(),
        _ => raw.planet.as_ref().or(raw.moon.as_ref()),
    };
    let life_stage = body.and_then(|body| body.life_stage.clone());
    let survey_environment_evidence = body.is_some_and(|body| {
        body.atmosphere.is_some()
            || body.magnetic_field.is_some()
            || body.axial_tilt_deg.is_some()
            || body.atmo_pressure_atm.is_some()
            || body.atmo_o2_pct.is_some()
            || body.atmo_toxicity.is_some()
            || body.hydrosphere_pct.is_some()
            || body.tectonic_index.is_some()
            || body.biosphere_index.is_some()
            || body.has_subsurface_ocean.is_some()
    });
    let scanned = raw
        .scanned
        .or_else(|| body.and_then(|body| body.scanned))
        .or_else(|| survey_environment_evidence.then_some(true));
    let surveyed = scanned == Some(true);
    let unknown = raw.open_fields();
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
        custom_name: None,
        survey_progress: LocationSurveyProgress {
            planets_total: raw.planets_total,
            planets_scanned: raw.planets_scanned,
            moons_total: raw.moons_total,
            moons_scanned: raw.moons_scanned,
            moons_total_estimated: raw.moons_total_estimated,
            survey_system_complete: None,
        },
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
            surface_temp_k: knowledge(
                body.and_then(|body| body.surface_temp_k)
                    .filter(|value| value.is_finite()),
            ),
            atmo_pressure_atm: knowledge(
                body.and_then(|body| body.atmo_pressure_atm)
                    .filter(|value| value.is_finite()),
            ),
            atmo_o2_pct: knowledge(
                body.and_then(|body| body.atmo_o2_pct)
                    .filter(|value| value.is_finite()),
            ),
            atmo_toxicity: knowledge(
                body.and_then(|body| body.atmo_toxicity)
                    .filter(|value| value.is_finite()),
            ),
            hydrosphere_pct: knowledge(
                body.and_then(|body| body.hydrosphere_pct)
                    .filter(|value| value.is_finite()),
            ),
            tectonic_index: knowledge(
                body.and_then(|body| body.tectonic_index)
                    .filter(|value| value.is_finite()),
            ),
            biosphere_index: knowledge(
                body.and_then(|body| body.biosphere_index)
                    .filter(|value| value.is_finite()),
            ),
            has_subsurface_ocean: knowledge(body.and_then(|body| body.has_subsurface_ocean)),
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
        unknown: unknown.into_iter().collect(),
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
        has_ward: Some(raw.has_ward.unwrap_or(false)),
        knowledge_observed: false,
        explored: None,
        has_life: None,
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
            has_ward: Some(raw.has_ward.unwrap_or(false)),
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

/// Collapses one Replicant-scoped star observation into the account-shared star projection.
pub fn account_star_from_knowledge(knowledge: Observation<StarKnowledge>) -> Observation<Star> {
    Observation {
        value: Star {
            key: knowledge.value.star,
            name: None,
            spectral_type: knowledge.value.spectral_type,
            entry_point: knowledge.value.entry_point,
            position: knowledge.value.position,
            has_hub: knowledge.value.has_hub,
            has_ward: knowledge.value.has_ward,
            knowledge_observed: true,
            explored: knowledge.value.explored,
            has_life: knowledge.value.has_life,
            region: knowledge.value.region,
        },
        metadata: knowledge.metadata,
    }
}

/// Builds the legacy Replicant-scoped compatibility view from account-shared star state.
pub fn star_knowledge_view(
    star: Observation<Star>,
    replicant: ReplicantKey,
) -> Observation<StarKnowledge> {
    Observation {
        value: StarKnowledge {
            replicant,
            star: star.value.key,
            position: star.value.position,
            spectral_type: star.value.spectral_type,
            entry_point: star.value.entry_point,
            explored: star.value.explored,
            has_hub: star.value.has_hub,
            has_ward: star.value.has_ward,
            has_life: star.value.has_life,
            region: star.value.region,
            distance_from_replicant: None,
            estimated_travel_time: None,
        },
        metadata: star.metadata,
    }
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
        let raw: raw::locations::Location = serde_json::from_value(serde_json::json!({
            "location": "ILPHARD-3",
            "location_type": "planet",
            "planet": {
                "scanned": true,
                "atmosphere": "dense",
                "surface_gravity": 2.06,
                "surface_temp_c": 125.0,
                "life_stage": "intelligent",
                "future_environment": {}
            }
        }))
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

    #[test]
    fn nested_body_scanned_flag_is_normalized_even_when_false() {
        let raw: raw::locations::Location = serde_json::from_value(serde_json::json!({
            "location": "TEST-2",
            "location_type": "moon",
            "moon": {
                "scanned": false,
                "atmosphere": "thin"
            }
        }))
        .expect("location should decode");
        let observation = location_detail(&raw, Realm::Live, ObservationTime::now())
            .expect("location should normalize");
        assert_eq!(observation.value.scanned, Some(false));
    }

    #[test]
    fn every_location_field_is_promoted_or_preserved() {
        let raw: raw::locations::Location = serde_json::from_value(serde_json::json!({
            "location": "TEST-1",
            "location_type": "planet",
            "scanned": true,
            "system_scanned": true,
            "system_tags": ["settled"],
            "system": "TEST",
            "parent": "TEST-STAR",
            "planets_total": 2,
            "planets_scanned": 1,
            "moons_total": 3,
            "moons_scanned": 2,
            "moons_total_estimated": true,
            "active_location_events": [{"designation": "EVENT-1"}],
            "asteroid_belt": {"designation": "BELT-1"},
            "belt": {"designation": "BELT-0"},
            "devices": [{"device_code": "D1"}],
            "entry_point": "TEST-ENTRY",
            "estimated_travel_time": 12,
            "inventory": [{"resource_type": "carbon"}],
            "kuiper": {"designation": "KUIPER-1"},
            "lagrange": {"designation": "L1"},
            "life_detected": true,
            "location_event": {"designation": "EVENT-1"},
            "megastructure": {"designation": "MEGA-1"},
            "mining_bonus_pct": 12.5,
            "moon": {"scanned": false, "future_moon": "kept"},
            "moons": [{"designation": "MOON-1"}],
            "object": {"designation": "OBJECT-1"},
            "oort": {"designation": "OORT-1"},
            "outer_system": {"designation": "OUTER-1"},
            "planet": {"scanned": true, "future_planet": "kept"},
            "planets": [{"designation": "PLANET-1"}],
            "resource_sites": [{"designation": "SITE-1"}],
            "shops": [{"designation": "SHOP-1"}],
            "star": {"designation": "TEST"},
            "system_objects": [{"designation": "OBJECT-1"}],
            "future_top_level": {"kept": true}
        }))
        .expect("complete location fixture should decode");

        let observation = location_detail(&raw, Realm::Live, ObservationTime::now())
            .expect("complete location fixture should normalize");
        let expected = [
            "active_location_events",
            "asteroid_belt",
            "belt",
            "devices",
            "entry_point",
            "estimated_travel_time",
            "inventory",
            "kuiper",
            "lagrange",
            "life_detected",
            "location_event",
            "megastructure",
            "mining_bonus_pct",
            "moon",
            "moons",
            "object",
            "oort",
            "outer_system",
            "planet",
            "planets",
            "resource_sites",
            "shops",
            "star",
            "system_objects",
            "future_top_level",
        ]
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
        let actual = observation
            .value
            .unknown
            .keys()
            .map(String::as_str)
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(actual, expected);
        assert_eq!(observation.value.unknown["planet"]["future_planet"], "kept");
        assert_eq!(observation.value.unknown["moon"]["future_moon"], "kept");
        assert_eq!(observation.value.unknown["future_top_level"]["kept"], true);
        assert_eq!(observation.value.system.as_deref(), Some("TEST"));
        assert_eq!(observation.value.survey_progress.planets_total, Some(2));
    }

    #[test]
    fn partial_location_merge_preserves_prior_passthrough() {
        let first: raw::locations::Location = serde_json::from_value(serde_json::json!({
            "location": "TEST-1",
            "location_type": "belt",
            "belt": {"designation": "BELT-1"},
            "future_first": true
        }))
        .expect("first location should decode");
        let second: raw::locations::Location = serde_json::from_value(serde_json::json!({
            "location": "TEST-1",
            "life_detected": true,
            "future_second": true
        }))
        .expect("second location should decode");
        let mut merged = location_detail(&first, Realm::Live, ObservationTime::now())
            .expect("first location should normalize")
            .value;
        let later = location_detail(&second, Realm::Live, ObservationTime::now())
            .expect("second location should normalize")
            .value;
        merged.merge_from(&later);
        assert_eq!(merged.unknown["belt"]["designation"], "BELT-1");
        assert_eq!(merged.unknown["future_first"], true);
        assert_eq!(merged.unknown["life_detected"], true);
        assert_eq!(merged.unknown["future_second"], true);
    }
    #[test]
    fn device_operational_state_is_retained_by_managed_normalization() {
        let raw: raw::devices::DeviceStatus = serde_json::from_value(serde_json::json!({
            "device_code": "DRONE",
            "device_type": "survey_drone",
            "status": "recalling",
            "deployed_at": "2026-07-29T10:00:00Z",
            "in_control_range": false,
            "replicant_code": "R1",
            "location": null,
            "stowed_in_device_code": "VESSEL",
            "controller_device_code": "CTRL",
            "attached_devices": [{"device_code": "ATTACHED"}],
            "controlled_devices": [{"device_code": "CONTROLLED"}],
            "stowed_devices": [{"device_code": "STOWED"}],
            "cargo": [{"resource_type": "conductive", "quantity": 12}],
            "cargo_capacity": 500,
            "attach_capacity": 2,
            "stow_capacity": 5,
            "stow_used": 3,
            "operational_capacity": 19.5,
            "grace_period_remaining": 7200,
            "upkeep_requirements": [
                {"resource": "structural", "required": 400, "missing": 120}
            ],
            "system_status": {"maintenance": "waiting_for_resources"},
            "available_commands": ["withdraw", "stow"],
            "ami_directive": {
                "directive": "survey_system",
                "planets": "all",
                "moons": "all"
            },
            "ami_directive_status": "active",
            "travel": {
                "origin": "SOL-1-L4",
                "destination": "SOL-4-L4",
                "final_destination": "SOL-4-L4",
                "arrives_at": "2026-07-29T12:00:00Z",
                "final_arrives_at": "2026-07-29T12:00:00Z",
                "eta_seconds": 42,
                "route_eta_seconds": 42,
                "stage": "recalling",
                "type": "local"
            }
        }))
        .expect("device status should decode");

        let observation = device_detail(
            &raw,
            Realm::Live,
            AccessScope::Owned,
            ObservationTime::now(),
        )
        .expect("device should normalize");
        let device = observation.value;
        assert_eq!(device.deployed_at.as_deref(), Some("2026-07-29T10:00:00Z"));
        assert_eq!(device.in_control_range, Some(false));

        assert_eq!(
            device
                .relationships
                .stowed_in
                .as_ref()
                .map(|key| key.id.as_str()),
            Some("VESSEL")
        );
        assert_eq!(
            device
                .relationships
                .controller
                .as_ref()
                .map(|key| key.id.as_str()),
            Some("CTRL")
        );
        assert_eq!(
            device
                .relationships
                .attached_devices
                .iter()
                .map(|key| key.id.as_str())
                .collect::<Vec<_>>(),
            ["ATTACHED"]
        );
        assert_eq!(
            device
                .relationships
                .controlled_devices
                .iter()
                .map(|key| key.id.as_str())
                .collect::<Vec<_>>(),
            ["CONTROLLED"]
        );
        assert_eq!(
            device
                .relationships
                .stowed_devices
                .iter()
                .map(|key| key.id.as_str())
                .collect::<Vec<_>>(),
            ["STOWED"]
        );
        assert_eq!(device.cargo.get("conductive"), Some(&12));
        assert_eq!(device.cargo_capacity, Some(500));
        assert_eq!(device.attach_capacity, Some(2));
        assert_eq!(device.stow_capacity, Some(5));
        assert_eq!(device.stow_used, Some(3));
        assert_eq!(
            device.operational_capacity.map(OperationalCapacity::raw),
            Some(19.5)
        );
        assert_eq!(device.grace_period_remaining, Some(7200));
        assert_eq!(device.upkeep_requirements.len(), 1);
        assert_eq!(
            device.upkeep_requirements[0]
                .get("missing")
                .and_then(Value::as_i64),
            Some(120)
        );
        assert_eq!(
            device
                .system_status
                .as_ref()
                .and_then(|status| status.get("maintenance"))
                .and_then(Value::as_str),
            Some("waiting_for_resources")
        );
        let directive = device
            .active_directive
            .as_ref()
            .expect("active directive should be retained");
        assert_eq!(
            directive.directive.as_ref().map(DeviceDirective::as_str),
            Some("survey_system")
        );
        assert_eq!(directive.status.as_deref(), Some("active"));
        assert_eq!(
            directive.details.get("planets").and_then(Value::as_str),
            Some("all")
        );
        let travel = device.travel.as_ref().expect("travel should be retained");
        assert_eq!(
            travel.destination.as_ref().map(|key| key.id.as_str()),
            Some("SOL-4-L4")
        );
        assert_eq!(travel.eta_seconds, Some(42));
        assert_eq!(travel.stage.as_deref(), Some("recalling"));
    }

    #[test]
    fn device_placement_facts_remain_unknown_when_raw_status_omits_them() {
        let raw: raw::devices::DeviceStatus =
            serde_json::from_value(serde_json::json!({"device_code": "UNKNOWN"}))
                .expect("sparse device status should decode");
        let observation = device_detail(
            &raw,
            Realm::Live,
            AccessScope::Owned,
            ObservationTime::now(),
        )
        .expect("sparse device should normalize");

        assert_eq!(observation.value.deployed_at, None);
        assert_eq!(observation.value.in_control_range, None);
    }

    #[test]
    fn blueprint_normalization_types_quantities_and_preserves_open_fields() {
        let raw: raw::blueprints::Blueprint = serde_json::from_value(serde_json::json!({
            "device_type": "deep_space_relay_station",
            "short_description": "Long-range relay",
            "description": "A relay intended for sparse frontier links.",
            "print_time": 1800,
            "resources": {"structural": 900},
            "components": {"compute_core": 2},
            "features": ["travel", "future_feature"],
            "directives": ["future_directive"],
            "cargo_capacity": 10,
            "future_field": {"range_ly": 10.0}
        }))
        .expect("blueprint should decode");

        let blueprint = blueprint(&raw).expect("blueprint should normalize");
        assert_eq!(blueprint.id.as_str(), "deep_space_relay_station");
        assert_eq!(
            blueprint.device_type.as_ref().map(DeviceType::as_str),
            Some("deep_space_relay_station")
        );
        assert_eq!(blueprint.print_time_seconds, Some(1800.0));
        assert_eq!(blueprint.resources.get("structural"), Some(&900));
        assert_eq!(blueprint.components.get("compute_core"), Some(&2));
        assert!(blueprint.unknown.contains_key("future_field"));
    }

    #[test]
    fn replicant_travel_is_retained_by_managed_normalization() {
        let raw: raw::replicants::ReplicantStatus = serde_json::from_value(serde_json::json!({
            "replicant_code": "R1",
            "status": "traveling",
            "location": null,
            "travel": {
                "origin": "SOL",
                "destination": "KRUKKRAK",
                "arrives_at": "2026-07-29T13:00:00Z",
                "eta_seconds": 120,
                "stage": "interstellar",
                "type": "direct"
            }
        }))
        .expect("replicant status should decode");

        let observation = owned_replicant_detail(&raw, Realm::Live, ObservationTime::now())
            .expect("replicant should normalize");
        let travel = observation
            .value
            .travel
            .as_ref()
            .expect("travel should be retained");
        assert_eq!(
            travel.destination.as_ref().map(|key| key.id.as_str()),
            Some("KRUKKRAK")
        );
        assert_eq!(travel.eta_seconds, Some(120));
        assert_eq!(travel.stage.as_deref(), Some("interstellar"));
    }

    #[test]
    fn aggregate_system_survey_progress_is_retained() {
        let raw: raw::locations::Location = serde_json::from_value(serde_json::json!({
            "location": "KRUKKRAK",
            "location_type": "star",
            "planets_total": 10,
            "planets_scanned": 10,
            "moons_total": 195,
            "moons_scanned": 195,
            "moons_total_estimated": false
        }))
        .expect("location should decode");

        let observation = location_detail(&raw, Realm::Live, ObservationTime::now())
            .expect("location should normalize");
        assert_eq!(observation.value.survey_progress.planets_total, Some(10));
        assert_eq!(observation.value.survey_progress.planets_scanned, Some(10));
        assert_eq!(observation.value.survey_progress.moons_total, Some(195));
        assert_eq!(observation.value.survey_progress.moons_scanned, Some(195));
        assert_eq!(
            observation.value.survey_progress.moons_total_estimated,
            Some(false)
        );
    }

    #[test]
    fn belt_metadata_is_retained_in_managed_location_evidence() {
        let raw: raw::locations::Location = serde_json::from_value(serde_json::json!({
            "location": "TARAZEDAR-BELT-1",
            "location_type": "belt",
            "belt": {
                "density": "dense",
                "designation": "TARAZEDAR-BELT-1",
                "inner_radius_au": 0.6,
                "outer_radius_au": 0.9,
                "resources": {"carbon": "rich"}
            },
            "mining_bonus_pct": 12.5,
            "active_location_events": [{"event_code": "E1"}],
            "resource_sites": [{"site_code": "R1"}]
        }))
        .expect("belt location should decode");

        let observation = location_detail(&raw, Realm::Live, ObservationTime::now())
            .expect("belt location should normalize");
        assert_eq!(
            observation.value.unknown["belt"]["density"],
            Value::String("dense".into())
        );
        assert_eq!(
            observation.value.unknown["mining_bonus_pct"],
            Value::from(12.5)
        );
        assert_eq!(
            observation.value.unknown["active_location_events"][0]["event_code"],
            "E1"
        );
        assert_eq!(
            observation.value.unknown["resource_sites"][0]["site_code"],
            "R1"
        );
    }

    #[test]
    fn old_device_json_without_operational_fields_still_decodes() {
        let device: Device = serde_json::from_value(serde_json::json!({
            "key": {"realm": "Live", "id": "D1"},
            "device_type": "survey_drone",
            "status": "idle",
            "location": null,
            "features": [],
            "available_commands": [],
            "available_directives": [],
            "tags": [],
            "relationships": {
                "attached_to": null,
                "controller": null,
                "assigned_replicant": null,
                "hosting_replicant": null
            },
            "access": "Owned"
        }))
        .expect("legacy device JSON should decode");

        assert!(device.relationships.stowed_in.is_none());
        assert!(device.relationships.attached_devices.is_empty());
        assert!(device.operational_capacity.is_none());
        assert_eq!(device.deployed_at, None);
        assert_eq!(device.in_control_range, None);
        assert!(device.active_directive.is_none());
        assert!(device.travel.is_none());
    }
}
