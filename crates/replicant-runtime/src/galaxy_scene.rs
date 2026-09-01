//! Frontend-safe galaxy scene derived from managed projections and runtime state.

use std::collections::{BTreeMap, BTreeSet};

use replicant_client::{Client, Device, Location, Replicant, Star, TravelState};
use replicant_protocol::{
    EntityId, EntityKind, EntityRef, GalaxyEdge, GalaxyExploration, GalaxyHighlight, GalaxyOverlay,
    GalaxyOverlayKind, GalaxyPoint, GalaxySceneSnapshot, GalaxySignal, GalaxyStar, GalaxyTravel,
    GalaxyWorkflowTarget, OperationKind, WorkflowId,
};
use replicant_workflow::WorkflowInstance;

use crate::workflows::{RelayWorkflowConfig, SurveyWorkflowConfig};

const RELAY_RANGE_LY: f64 = 7.499;

/// Builds a galaxy scene from local managed state without upstream I/O.
pub async fn galaxy_scene(
    client: &Client,
    workflows: &[WorkflowInstance],
    revision: u64,
    generated_at_ms: i64,
) -> Result<GalaxySceneSnapshot, crate::ApplicationError> {
    let stars = client.galaxy().catalogue();
    let replicant_handles = client.replicants().find().owned().collect().await?;
    let mut replicants = Vec::with_capacity(replicant_handles.len());
    for handle in replicant_handles {
        replicants.push(handle.snapshot().await?);
    }
    let device_handles = client.devices().find().owned().collect().await?;
    let mut devices = Vec::with_capacity(device_handles.len());
    for handle in device_handles {
        devices.push(handle.snapshot().await?);
    }
    let locations = client.locations().find().collect().await?;

    Ok(build_scene(SceneInputs {
        stars,
        locations,
        devices,
        replicants,
        targets: workflow_targets(workflows),
        revision,
        generated_at_ms,
    }))
}

struct SceneInputs {
    stars: Vec<Star>,
    locations: Vec<Location>,
    devices: Vec<Device>,
    replicants: Vec<Replicant>,
    targets: Vec<Target>,
    revision: u64,
    generated_at_ms: i64,
}

#[derive(Clone)]
struct Target {
    workflow_id: WorkflowId,
    workflow_kind: OperationKind,
    anchor: String,
    systems: Vec<String>,
}

fn workflow_targets(workflows: &[WorkflowInstance]) -> Vec<Target> {
    workflows
        .iter()
        .filter(|workflow| !workflow.status.is_terminal())
        .filter_map(|workflow| match workflow.kind.as_str() {
            "survey.route" => workflow
                .config::<SurveyWorkflowConfig>()
                .ok()
                .map(|config| Target {
                    workflow_id: WorkflowId(workflow.id.to_string()),
                    workflow_kind: OperationKind(workflow.kind.to_string()),
                    anchor: config.center.clone(),
                    systems: vec![config.center],
                }),
            "relay.expansion" => {
                workflow
                    .config::<RelayWorkflowConfig>()
                    .ok()
                    .map(|config| Target {
                        workflow_id: WorkflowId(workflow.id.to_string()),
                        workflow_kind: OperationKind(workflow.kind.to_string()),
                        anchor: config.hub,
                        systems: config.targets,
                    })
            }
            _ => None,
        })
        .collect()
}

fn build_scene(inputs: SceneInputs) -> GalaxySceneSnapshot {
    let SceneInputs {
        stars,
        locations,
        devices,
        replicants,
        targets,
        revision,
        generated_at_ms,
    } = inputs;
    let positions = stars
        .iter()
        .filter_map(|star| {
            star.position
                .map(|position| (star.key.id.to_string(), point(position)))
        })
        .collect::<BTreeMap<_, _>>();
    let known_systems = positions.keys().cloned().collect::<BTreeSet<_>>();
    let mut explored = BTreeSet::new();
    let mut partial = BTreeSet::new();
    let mut life = BTreeSet::new();
    for star in &stars {
        let system = star.key.id.to_string();
        if star.knowledge_observed {
            partial.insert(system.clone());
        }
        if star.explored == Some(true) {
            explored.insert(system.clone());
        }
        if star.has_life == Some(true) {
            life.insert(system);
        }
    }

    let mut current = BTreeSet::new();
    let mut travel = Vec::new();
    for replicant in &replicants {
        if let Some(location) = &replicant.location
            && let Some(system) = resolve_system(location.id.as_str(), &known_systems)
        {
            current.insert(system);
        }
        push_travel(
            &mut travel,
            EntityKind::Replicant,
            replicant.key.id.as_str(),
            replicant.travel.as_ref(),
            &known_systems,
        );
    }

    let megastructure_systems = locations
        .iter()
        .filter(|location| {
            location.unknown.contains_key("megastructure")
                || location
                    .location_type
                    .as_ref()
                    .is_some_and(|value| value.as_str() == "megastructure")
        })
        .filter_map(|location| {
            location
                .system
                .clone()
                .filter(|system| known_systems.contains(system))
                .or_else(|| resolve_system(location.id().as_str(), &known_systems))
        })
        .collect::<BTreeSet<_>>();

    let mut device_counts = BTreeMap::<String, u32>::new();
    let mut relay_systems = BTreeSet::new();
    for device in &devices {
        let system = device
            .location
            .as_ref()
            .and_then(|location| resolve_system(location.id.as_str(), &known_systems));
        if let Some(system) = system {
            *device_counts.entry(system.clone()).or_default() += 1;
            if is_active_relay(device) {
                relay_systems.insert(system);
            }
        }
        push_travel(
            &mut travel,
            EntityKind::Device,
            device.key.id.as_str(),
            device.travel.as_ref(),
            &known_systems,
        );
    }

    let mut scene_stars = stars
        .into_iter()
        .filter_map(|star| {
            let id = star.key.id.to_string();
            let position = positions.get(&id).copied()?;
            Some(GalaxyStar {
                name: star.name,
                spectral_type: star.spectral_type,
                region: star.region,
                position,
                exploration: if explored.contains(&id) {
                    GalaxyExploration::Explored
                } else if partial.contains(&id) {
                    GalaxyExploration::Partial
                } else {
                    GalaxyExploration::Undiscovered
                },
                current: current.contains(&id),
                has_hub: star.has_hub == Some(true),
                has_life: life.contains(&id),
                has_relay: relay_systems.contains(&id),
                has_megastructure: megastructure_systems.contains(&id),
                id,
            })
        })
        .collect::<Vec<_>>();
    scene_stars.sort_by(|left, right| left.id.cmp(&right.id));
    travel.sort_by(|left, right| left.entity.id.cmp(&right.entity.id));

    let relay_nodes = relay_systems.iter().collect::<Vec<_>>();
    let mut relay_edges = Vec::new();
    for (index, from) in relay_nodes.iter().enumerate() {
        for to in relay_nodes.iter().skip(index + 1) {
            if distance(positions[*from], positions[*to]) <= RELAY_RANGE_LY {
                relay_edges.push(GalaxyEdge {
                    from: (*from).clone(),
                    to: (*to).clone(),
                });
            }
        }
    }

    let mut overlays = life
        .iter()
        .filter_map(|system| overlay(GalaxyOverlayKind::Life, system, 1, &positions))
        .collect::<Vec<_>>();
    overlays.extend(device_counts.iter().filter_map(|(system, count)| {
        overlay(GalaxyOverlayKind::Device, system, *count, &positions)
    }));
    overlays.extend(
        relay_systems
            .iter()
            .filter_map(|system| overlay(GalaxyOverlayKind::Influence, system, 1, &positions)),
    );

    let workflow_targets = targets
        .iter()
        .flat_map(|target| {
            target
                .systems
                .iter()
                .filter(|system| positions.contains_key(*system))
                .map(|system| GalaxyWorkflowTarget {
                    workflow_id: target.workflow_id.clone(),
                    workflow_kind: target.workflow_kind.clone(),
                    system: system.clone(),
                })
        })
        .collect();
    let position_index = &positions;
    let highlights = targets
        .iter()
        .flat_map(|target| {
            let anchor = resolve_system(&target.anchor, &known_systems);
            target.systems.iter().filter_map(move |system| {
                let from = anchor.clone()?;
                (from != *system && position_index.contains_key(system)).then(|| GalaxyHighlight {
                    workflow_id: target.workflow_id.clone(),
                    from,
                    to: system.clone(),
                })
            })
        })
        .collect();

    GalaxySceneSnapshot {
        revision,
        generated_at_ms,
        stars: scene_stars,
        relay_edges,
        active_travel: travel,
        signals: discovered_signals(),
        highlights,
        overlays,
        workflow_targets,
    }
}

// Player-discovered coordinates carried forward from the supplied, authorized reference UI.
fn discovered_signals() -> Vec<GalaxySignal> {
    [
        ("Alpha", 175.0, 85.0, 30.0),
        ("Beta", 155.0, -215.0, -140.0),
        ("Gamma", -350.0, 160.0, -80.0),
        ("GalacticCenter", 27_000.0, 0.0, 0.0),
    ]
    .into_iter()
    .map(|(id, x, y, z)| GalaxySignal {
        id: id.to_owned(),
        label: Some(id.to_owned()),
        position: GalaxyPoint { x, y, z },
    })
    .collect()
}

fn point(position: replicant_client::domain::GalacticPosition) -> GalaxyPoint {
    GalaxyPoint {
        x: position.x,
        y: position.y,
        z: position.z,
    }
}

fn resolve_system(location: &str, systems: &BTreeSet<String>) -> Option<String> {
    systems
        .iter()
        .filter(|system| location == system.as_str() || location.starts_with(&format!("{system}-")))
        .max_by_key(|system| system.len())
        .cloned()
}

fn push_travel(
    output: &mut Vec<GalaxyTravel>,
    kind: EntityKind,
    id: &str,
    travel: Option<&TravelState>,
    systems: &BTreeSet<String>,
) {
    let Some(travel) = travel else { return };
    let Some(from) = travel
        .origin
        .as_ref()
        .and_then(|value| resolve_system(value.id.as_str(), systems))
    else {
        return;
    };
    let destination = travel
        .final_destination
        .as_ref()
        .or(travel.destination.as_ref());
    let Some(to) = destination.and_then(|value| resolve_system(value.id.as_str(), systems)) else {
        return;
    };
    output.push(GalaxyTravel {
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

fn is_active_relay(device: &Device) -> bool {
    let relay_capable = device.device_type.as_ref().is_some_and(|kind| {
        matches!(
            kind.as_str(),
            "ftl_relay" | "system_hub" | "deep_space_relay_station"
        )
    }) || device
        .features
        .iter()
        .any(|feature| feature.as_str() == "relay");
    relay_capable
        && device.relationships.stowed_in.is_none()
        && device.relationships.attached_to.is_none()
        && (device
            .status
            .as_ref()
            .is_some_and(|status| matches!(status.as_str(), "active" | "relaying"))
            || device
                .available_commands
                .iter()
                .any(|command| command.as_str() == "deactivate"))
}

fn overlay(
    kind: GalaxyOverlayKind,
    system: &str,
    count: u32,
    positions: &BTreeMap<String, GalaxyPoint>,
) -> Option<GalaxyOverlay> {
    Some(GalaxyOverlay {
        kind,
        system: system.to_owned(),
        position: *positions.get(system)?,
        count,
    })
}

fn distance(left: GalaxyPoint, right: GalaxyPoint) -> f64 {
    ((left.x - right.x).powi(2) + (left.y - right.y).powi(2) + (left.z - right.z).powi(2)).sqrt()
}

#[cfg(test)]
mod tests {
    use replicant_client::{
        DeviceCommand, DeviceId, DeviceKey, DeviceRelationships, DeviceStatus, DeviceType,
        LocationId, LocationKey, StarId,
        domain::{AccessScope, DeviceFeature, GalacticPosition, StarKey},
    };

    use super::*;

    fn star(id: &str, x: f64) -> Star {
        Star {
            key: StarKey::live(StarId::from(id)),
            name: None,
            spectral_type: Some("G".to_owned()),
            entry_point: None,
            position: Some(GalacticPosition { x, y: 0.0, z: 0.0 }),
            has_hub: Some(false),
            has_ward: None,
            knowledge_observed: false,
            explored: None,
            has_life: None,
            region: None,
        }
    }

    fn relay(id: &str, location: &str) -> Device {
        Device {
            key: DeviceKey::live(DeviceId::from(id)),
            device_type: Some(DeviceType::FtlRelay),
            status: Some(DeviceStatus::Active),
            location: Some(LocationKey::live(LocationId::from(location))),
            deployed_at: None,
            in_control_range: None,
            features: Vec::<DeviceFeature>::new(),
            available_commands: Vec::<DeviceCommand>::new(),
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
    fn scene_maps_relay_devices_and_workflow_targets_without_raw_shapes() {
        let target = Target {
            workflow_id: WorkflowId("workflow-1".to_owned()),
            workflow_kind: OperationKind("relay.expansion".to_owned()),
            anchor: "SOL-HUB".to_owned(),
            systems: vec!["ALPHA".to_owned()],
        };
        let scene = build_scene(SceneInputs {
            stars: vec![star("SOL", 0.0), star("ALPHA", 7.0)],
            locations: Vec::new(),
            devices: vec![relay("R1", "SOL-1"), relay("R2", "ALPHA-1")],
            replicants: Vec::new(),
            targets: vec![target],
            revision: 9,
            generated_at_ms: 10,
        });

        assert_eq!(scene.revision, 9);
        assert_eq!(
            scene.relay_edges,
            vec![GalaxyEdge {
                from: "ALPHA".to_owned(),
                to: "SOL".to_owned()
            }]
        );
        assert_eq!(scene.workflow_targets[0].system, "ALPHA");
        assert_eq!(scene.highlights[0].from, "SOL");
        assert_eq!(
            scene
                .overlays
                .iter()
                .filter(|item| item.kind == GalaxyOverlayKind::Device)
                .count(),
            2
        );
    }
}
