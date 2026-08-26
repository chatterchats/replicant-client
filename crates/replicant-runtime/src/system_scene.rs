//! Frontend-safe system scene derived from managed projections and runtime state.

use std::collections::{BTreeMap, BTreeSet};

use replicant_client::{Client, Device, Knowledge, Location, Replicant, TravelState};
use replicant_protocol::{
    EntityId, EntityKind, EntityRef, OperationKind, SystemMarker, SystemMarkerKind, SystemPoint,
    SystemSceneSnapshot, SystemTravel, SystemWorkflowMarker, WorkflowId,
};
use replicant_workflow::WorkflowInstance;

use crate::workflows::{RelayWorkflowConfig, SurveyWorkflowConfig};

const CENTER: f64 = 500.0;

/// Builds one system scene from committed managed state without upstream I/O.
pub async fn system_scene(
    client: &Client,
    workflows: &[WorkflowInstance],
    system: &str,
    revision: u64,
    generated_at_ms: i64,
) -> Result<SystemSceneSnapshot, crate::ApplicationError> {
    let locations = client
        .locations()
        .find()
        .in_system(system)
        .collect()
        .await?;
    let devices = device_snapshots(client).await?;
    let replicants = replicant_snapshots(client).await?;
    Ok(build_scene(
        system,
        locations,
        devices,
        replicants,
        workflows,
        revision,
        generated_at_ms,
    ))
}

async fn device_snapshots(client: &Client) -> Result<Vec<Device>, crate::ApplicationError> {
    let handles = client.devices().find().collect().await?;
    let mut devices = Vec::with_capacity(handles.len());
    for handle in handles {
        devices.push(handle.snapshot().await?);
    }
    Ok(devices)
}

async fn replicant_snapshots(client: &Client) -> Result<Vec<Replicant>, crate::ApplicationError> {
    let handles = client.replicants().find().owned().collect().await?;
    let mut replicants = Vec::with_capacity(handles.len());
    for handle in handles {
        replicants.push(handle.snapshot().await?);
    }
    Ok(replicants)
}

fn build_scene(
    system: &str,
    mut locations: Vec<Location>,
    devices: Vec<Device>,
    replicants: Vec<Replicant>,
    workflows: &[WorkflowInstance],
    revision: u64,
    generated_at_ms: i64,
) -> SystemSceneSnapshot {
    locations.sort_by(|left, right| left.id().cmp(right.id()));
    locations.retain(|location| location.id().as_str() != system);
    let mut known = locations
        .iter()
        .map(|location| location.id().to_string())
        .collect::<BTreeSet<_>>();
    known.insert(system.to_owned());
    let mut markers = vec![marker(
        system,
        system,
        SystemMarkerKind::Star,
        EntityKind::System,
        system,
        None,
        SystemPoint {
            x: CENTER,
            y: CENTER,
        },
        1,
    )];
    let mut positions = BTreeMap::from([(
        system.to_owned(),
        SystemPoint {
            x: CENTER,
            y: CENTER,
        },
    )]);

    let mut top_level_index = 0;
    for location in &locations {
        let id = location.id().to_string();
        let kind = location_kind(location);
        let parent = location
            .parent
            .as_ref()
            .map(|value| value.id.to_string())
            .or_else(|| inferred_parent(&id, kind));
        let orbit = top_level_index;
        if parent.is_none() {
            top_level_index += 1;
        }
        let position = location_position(&id, parent.as_deref(), orbit, &positions);
        positions.insert(id.clone(), position);
        let mut body = marker(
            &id,
            &id,
            kind,
            EntityKind::Location,
            &id,
            parent,
            position,
            1,
        );
        body.in_habitable_zone = match location.in_habitable_zone() {
            Knowledge::Present(value) => Some(*value),
            _ => None,
        };
        markers.push(body);
        append_known_content(&mut markers, location, position);
    }

    for device in devices.iter().filter(|device| {
        device
            .location
            .as_ref()
            .is_some_and(|location| known.contains(location.id.as_str()))
    }) {
        let location = device
            .location
            .as_ref()
            .expect("filtered location")
            .id
            .as_str();
        let id = device.key.id.as_str();
        markers.push(marker(
            id,
            id,
            device_kind(device),
            EntityKind::Device,
            location,
            Some(location.to_owned()),
            offset(positions.get(location).copied().unwrap_or_default(), id),
            1,
        ));
    }

    for replicant in replicants.iter().filter(|replicant| {
        replicant
            .location
            .as_ref()
            .is_some_and(|location| known.contains(location.id.as_str()))
    }) {
        let location = replicant
            .location
            .as_ref()
            .expect("filtered location")
            .id
            .as_str();
        let id = replicant.key.id.as_str();
        markers.push(marker(
            id,
            replicant.name.as_deref().unwrap_or(id),
            SystemMarkerKind::Vessel,
            EntityKind::Replicant,
            location,
            Some(location.to_owned()),
            offset(positions.get(location).copied().unwrap_or_default(), id),
            1,
        ));
    }

    let mut active_travel = Vec::new();
    for replicant in &replicants {
        push_travel(
            &mut active_travel,
            EntityKind::Replicant,
            replicant.key.id.as_str(),
            replicant.travel.as_ref(),
            &known,
        );
    }
    for device in &devices {
        push_travel(
            &mut active_travel,
            EntityKind::Device,
            device.key.id.as_str(),
            device.travel.as_ref(),
            &known,
        );
    }
    markers.sort_by(|left, right| left.id.cmp(&right.id));
    active_travel.sort_by(|left, right| left.entity.id.cmp(&right.entity.id));

    SystemSceneSnapshot {
        system: system.to_owned(),
        revision,
        generated_at_ms,
        markers,
        active_travel,
        workflow_markers: workflow_markers(workflows, system),
    }
}

fn location_kind(location: &Location) -> SystemMarkerKind {
    if location.unknown.contains_key("megastructure") {
        return SystemMarkerKind::Megastructure;
    }
    match location.location_type.as_ref().map(|value| value.as_str()) {
        Some("planet") => SystemMarkerKind::Planet,
        Some("moon") => SystemMarkerKind::Moon,
        Some("belt") => SystemMarkerKind::Belt,
        Some("lagrange") => SystemMarkerKind::Lagrange,
        Some("megastructure") => SystemMarkerKind::Megastructure,
        _ => SystemMarkerKind::Location,
    }
}

fn inferred_parent(id: &str, kind: SystemMarkerKind) -> Option<String> {
    match kind {
        SystemMarkerKind::Moon => id.rsplit_once('-').map(|(parent, _)| parent.to_owned()),
        SystemMarkerKind::Lagrange => id
            .strip_suffix("-L4")
            .or_else(|| id.strip_suffix("-L5"))
            .map(str::to_owned),
        _ => None,
    }
}

fn device_kind(device: &Device) -> SystemMarkerKind {
    let kind = device
        .device_type
        .as_ref()
        .map_or("", |value| value.as_str());
    if kind.contains("factory") || kind.contains("autofac") || kind.contains("printer") {
        SystemMarkerKind::Factory
    } else if kind.contains("relay") || kind == "system_hub" || kind.contains("beacon") {
        SystemMarkerKind::Relay
    } else if kind.contains("vessel") || kind.contains("ship") || kind.contains("carrier") {
        SystemMarkerKind::Vessel
    } else {
        SystemMarkerKind::Device
    }
}

fn append_known_content(
    markers: &mut Vec<SystemMarker>,
    location: &Location,
    position: SystemPoint,
) {
    for (field, kind, label) in [
        ("active_location_events", SystemMarkerKind::Event, "event"),
        (
            "resource_sites",
            SystemMarkerKind::ResourceSite,
            "resource site",
        ),
    ] {
        let count = location
            .unknown
            .get(field)
            .and_then(serde_json::Value::as_array)
            .map_or(0, Vec::len);
        if count > 0 {
            let id = format!("{}:{field}", location.id());
            markers.push(marker(
                &id,
                label,
                kind,
                EntityKind::Location,
                location.id().as_str(),
                Some(location.id().to_string()),
                offset(position, &id),
                count as u32,
            ));
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn marker(
    id: &str,
    label: &str,
    kind: SystemMarkerKind,
    entity_kind: EntityKind,
    location: &str,
    parent: Option<String>,
    position: SystemPoint,
    count: u32,
) -> SystemMarker {
    let aggregate_location = entity_kind == EntityKind::Location && id.contains(':');
    SystemMarker {
        id: id.to_owned(),
        label: label.to_owned(),
        kind,
        entity: EntityRef {
            kind: entity_kind,
            id: EntityId(if aggregate_location {
                location.to_owned()
            } else {
                id.to_owned()
            }),
        },
        location: location.to_owned(),
        parent,
        in_habitable_zone: None,
        position,
        count,
    }
}

fn location_position(
    id: &str,
    parent: Option<&str>,
    index: usize,
    positions: &BTreeMap<String, SystemPoint>,
) -> SystemPoint {
    let center = parent
        .and_then(|parent| positions.get(parent).copied())
        .unwrap_or(SystemPoint {
            x: CENTER,
            y: CENTER,
        });
    let radius = if parent.is_some() {
        34.0
    } else {
        68.0 + index as f64 * 32.0
    };
    let angle = hash(id) as f64 / u32::MAX as f64 * std::f64::consts::TAU;
    SystemPoint {
        x: center.x + radius * angle.cos(),
        y: center.y + radius * angle.sin(),
    }
}

fn offset(point: SystemPoint, id: &str) -> SystemPoint {
    let angle = hash(id) as f64 / u32::MAX as f64 * std::f64::consts::TAU;
    SystemPoint {
        x: point.x + 18.0 * angle.cos(),
        y: point.y + 18.0 * angle.sin(),
    }
}

fn hash(value: &str) -> u32 {
    value.bytes().fold(2_166_136_261, |hash, byte| {
        hash.wrapping_mul(16_777_619) ^ u32::from(byte)
    })
}

fn push_travel(
    output: &mut Vec<SystemTravel>,
    kind: EntityKind,
    id: &str,
    travel: Option<&TravelState>,
    known: &BTreeSet<String>,
) {
    let Some(travel) = travel else { return };
    let Some(from) = travel.origin.as_ref().map(|value| value.id.to_string()) else {
        return;
    };
    let Some(to) = travel
        .final_destination
        .as_ref()
        .or(travel.destination.as_ref())
        .map(|value| value.id.to_string())
    else {
        return;
    };
    if known.contains(&from) && known.contains(&to) {
        output.push(SystemTravel {
            entity: EntityRef {
                kind,
                id: EntityId(id.to_owned()),
            },
            from,
            to,
            started_at: travel.departed_at.clone(),
            arrives_at: travel
                .final_arrives_at
                .clone()
                .or_else(|| travel.arrives_at.clone()),
        });
    }
}

fn workflow_markers(workflows: &[WorkflowInstance], system: &str) -> Vec<SystemWorkflowMarker> {
    workflows
        .iter()
        .filter(|workflow| !workflow.status.is_terminal())
        .filter_map(|workflow| {
            let location = match workflow.kind.as_str() {
                "survey.route" => workflow
                    .config::<SurveyWorkflowConfig>()
                    .ok()
                    .and_then(|config| (config.options.center == system).then_some(system)),
                "relay.expansion" => {
                    workflow
                        .config::<RelayWorkflowConfig>()
                        .ok()
                        .and_then(|config| {
                            (config.request.hub.starts_with(system)
                                || config.request.targets.iter().any(|target| target == system))
                            .then_some(system)
                        })
                }
                _ => None,
            }?;
            Some(SystemWorkflowMarker {
                workflow_id: WorkflowId(workflow.id.to_string()),
                workflow_kind: OperationKind(workflow.kind.to_string()),
                location: location.to_owned(),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use replicant_client::{
        DeviceId, DeviceKey, DeviceRelationships, DeviceType, LocationId, LocationKey,
        LocationSurveyProgress,
        domain::{AccessScope, DeviceFeature, LocationEnvironment, LocationType},
    };

    use super::*;

    fn location(id: &str, kind: LocationType, parent: Option<&str>) -> Location {
        Location {
            key: LocationKey::live(LocationId::from(id)),
            location_type: Some(kind),
            scanned: Some(true),
            system_scanned: Some(true),
            system_tags: Vec::new(),
            system: Some("SOL".to_owned()),
            parent: parent.map(|value| LocationKey::live(LocationId::from(value))),
            custom_name: None,
            survey_progress: LocationSurveyProgress::default(),
            environment: LocationEnvironment::default(),
            unknown: BTreeMap::new(),
        }
    }

    fn device(id: &str, kind: DeviceType, at: &str) -> Device {
        Device {
            key: DeviceKey::live(DeviceId::from(id)),
            device_type: Some(kind),
            status: None,
            location: Some(LocationKey::live(LocationId::from(at))),
            features: Vec::<DeviceFeature>::new(),
            available_commands: Vec::new(),
            available_directives: Vec::new(),
            tags: Vec::new(),
            relationships: DeviceRelationships::default(),
            cargo: Default::default(),
            cargo_capacity: None,
            attach_capacity: None,
            stow_capacity: None,
            stow_used: None,
            operational_capacity: None,
            grace_period_remaining: None,
            upkeep_requirements: Vec::new(),
            system_status: None,
            active_directive: None,
            travel: None,
            access: AccessScope::Owned,
        }
    }

    #[test]
    fn scene_maps_bodies_and_device_categories_to_typed_markers() {
        let mut planet = location("SOL-1", LocationType::Planet, None);
        planet.environment.in_habitable_zone = Knowledge::Present(true);
        planet.unknown.insert(
            "active_location_events".to_owned(),
            serde_json::json!([{"event_code": "E1"}]),
        );
        planet.unknown.insert(
            "resource_sites".to_owned(),
            serde_json::json!([{"site_code": "R1"}]),
        );
        let scene = build_scene(
            "SOL",
            vec![
                planet,
                location("SOL-1-A", LocationType::Moon, Some("SOL-1")),
                location("SOL-1-L4", LocationType::from("lagrange"), None),
                location("SOL-BELT", LocationType::Belt, None),
            ],
            vec![
                device("FACTORY", DeviceType::from("autofactory"), "SOL-1"),
                device("RELAY", DeviceType::FtlRelay, "SOL-BELT"),
            ],
            Vec::new(),
            &[],
            7,
            8,
        );

        assert_eq!(scene.system, "SOL");
        assert_eq!(scene.revision, 7);
        for kind in [
            SystemMarkerKind::Star,
            SystemMarkerKind::Planet,
            SystemMarkerKind::Moon,
            SystemMarkerKind::Lagrange,
            SystemMarkerKind::Belt,
            SystemMarkerKind::Factory,
            SystemMarkerKind::Relay,
            SystemMarkerKind::Event,
            SystemMarkerKind::ResourceSite,
        ] {
            assert!(scene.markers.iter().any(|marker| marker.kind == kind));
        }
        let moon = scene
            .markers
            .iter()
            .find(|marker| marker.id == "SOL-1-A")
            .unwrap();
        assert_eq!(moon.parent.as_deref(), Some("SOL-1"));
        assert_eq!(moon.entity.kind, EntityKind::Location);
        assert_eq!(
            scene
                .markers
                .iter()
                .find(|marker| marker.id == "SOL-1-L4")
                .unwrap()
                .parent
                .as_deref(),
            Some("SOL-1")
        );
        assert_eq!(
            scene
                .markers
                .iter()
                .find(|marker| marker.id == "SOL-1")
                .unwrap()
                .in_habitable_zone,
            Some(true)
        );
    }
}
