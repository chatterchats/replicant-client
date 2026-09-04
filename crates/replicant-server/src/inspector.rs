use std::collections::BTreeMap;

use replicant_client::domain::{
    Knowledge, Location, Observation, ObservationMetadata, Reachability, Star,
};
use replicant_protocol::{
    EntityCollectionSummary, EntityGroupSummary, EntityId, EntityKind, EntityProvenance, EntityRef,
    EntityStatusCount, EntitySummary, GalaxyPoint, LocationEnvironmentSummary,
    LocationInspectorSummary, LocationSurveySummary, SystemInspectorSummary,
};
use serde::Serialize;

pub(crate) const INSPECTOR_INLINE_COLLECTION_LIMIT: usize = 8;

#[derive(Debug)]
pub(crate) struct InspectorError(&'static str);

impl std::fmt::Display for InspectorError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for InspectorError {}

pub(crate) type Result<T, E = InspectorError> = std::result::Result<T, E>;

pub(crate) fn collection(mut items: Vec<EntitySummary>) -> Result<EntityCollectionSummary> {
    items.sort_by(|left, right| left.entity.cmp(&right.entity));
    let total = u32::try_from(items.len()).map_err(|_| InspectorError("entity count overflow"))?;
    if items.len() <= INSPECTOR_INLINE_COLLECTION_LIMIT {
        return Ok(EntityCollectionSummary {
            total,
            items,
            groups: Vec::new(),
        });
    }

    let mut grouped =
        BTreeMap::<(EntityKind, Option<String>), BTreeMap<Option<String>, usize>>::new();
    for item in items {
        *grouped
            .entry((item.entity.kind, item.entity_type))
            .or_default()
            .entry(item.status)
            .or_default() += 1;
    }
    let groups = grouped
        .into_iter()
        .map(|((entity_kind, entity_type), statuses)| {
            let count = statuses.values().try_fold(0_u32, |total, count| {
                let count =
                    u32::try_from(*count).map_err(|_| InspectorError("group count overflow"))?;
                total
                    .checked_add(count)
                    .ok_or(InspectorError("group count overflow"))
            })?;
            let statuses = statuses
                .into_iter()
                .map(|(status, count)| {
                    Ok(EntityStatusCount {
                        status,
                        count: u32::try_from(count)
                            .map_err(|_| InspectorError("status count overflow"))?,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(EntityGroupSummary {
                entity_kind,
                entity_type,
                count,
                statuses,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(EntityCollectionSummary {
        total,
        items: Vec::new(),
        groups,
    })
}

pub(crate) fn provenance(metadata: &ObservationMetadata) -> EntityProvenance {
    EntityProvenance {
        observed_at_ms: metadata.observed_at.unix_millis(),
        stale: metadata.stale,
        reachability: match metadata.reachability {
            Reachability::Reachable => "reachable",
            Reachability::OutOfRange => "out_of_range",
            Reachability::AccessRevoked => "access_revoked",
            Reachability::Historical => "historical",
            _ => "unknown",
        }
        .to_owned(),
        source_operation: metadata.source_document.operation.clone(),
    }
}

fn object_field(
    fields: &BTreeMap<String, serde_json::Value>,
    key: &str,
) -> BTreeMap<String, serde_json::Value> {
    fields
        .get(key)
        .and_then(serde_json::Value::as_object)
        .map(|object| object.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        .unwrap_or_default()
}

fn object_array(
    fields: &BTreeMap<String, serde_json::Value>,
    key: &str,
) -> Vec<BTreeMap<String, serde_json::Value>> {
    fields
        .get(key)
        .and_then(serde_json::Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_object())
                .map(|object| object.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                .collect()
        })
        .unwrap_or_default()
}

fn value_count(fields: &BTreeMap<String, serde_json::Value>, key: &str) -> Option<u32> {
    fields
        .get(key)
        .and_then(serde_json::Value::as_array)
        .and_then(|items| u32::try_from(items.len()).ok())
}

pub(crate) fn system_detail(
    star: Option<&Observation<Star>>,
    locations: &[Observation<Location>],
) -> Result<SystemInspectorSummary> {
    let children = locations
        .iter()
        .map(|observation| location_entity_summary(&observation.value))
        .collect();
    let richest = locations
        .iter()
        .max_by_key(|observation| observation.value.unknown.len());
    let fields = richest.map(|observation| &observation.value.unknown);
    let empty = BTreeMap::new();
    let fields = fields.unwrap_or(&empty);
    Ok(SystemInspectorSummary {
        name: star.and_then(|observation| observation.value.name.clone()),
        spectral_type: star.and_then(|observation| observation.value.spectral_type.clone()),
        region: star.and_then(|observation| observation.value.region.clone()),
        entry_point: star.and_then(|observation| {
            observation
                .value
                .entry_point
                .as_ref()
                .map(|location| location.id.to_string())
        }),
        position: star.and_then(|observation| {
            observation.value.position.map(|position| GalaxyPoint {
                x: position.x,
                y: position.y,
                z: position.z,
            })
        }),
        explored: star.and_then(|observation| observation.value.explored),
        has_hub: star.and_then(|observation| observation.value.has_hub),
        has_ward: star.and_then(|observation| observation.value.has_ward),
        has_life: star.and_then(|observation| observation.value.has_life),
        tags: richest
            .map(|observation| observation.value.system_tags.clone())
            .unwrap_or_default(),
        stellar: object_field(fields, "star"),
        asteroid_belt: object_field(fields, "asteroid_belt"),
        outer_system: {
            let mut outer = object_field(fields, "outer_system");
            for key in ["kuiper", "oort"] {
                if let Some(value) = fields.get(key) {
                    outer.insert(key.to_owned(), value.clone());
                }
            }
            outer
        },
        mining_bonus_percent: fields
            .get("mining_bonus_pct")
            .and_then(serde_json::Value::as_f64),
        shop_count: value_count(fields, "shops"),
        active_event_count: value_count(fields, "active_location_events"),
        object_count: value_count(fields, "system_objects"),
        children: collection(children)?,
    })
}

pub(crate) fn location_detail(
    location: &Location,
    contents: Vec<EntitySummary>,
) -> Result<LocationInspectorSummary> {
    Ok(LocationInspectorSummary {
        location_type: wire_value(location.location_type.as_ref()),
        custom_name: location.custom_name.clone(),
        system: location.system.clone(),
        parent: location.parent.as_ref().map(|parent| parent.id.to_string()),
        scanned: location.scanned,
        system_scanned: location.system_scanned,
        system_tags: location.system_tags.clone(),
        survey: LocationSurveySummary {
            system_complete: location.survey_progress.system_survey_complete(),
            planets_total: location.survey_progress.planets_total,
            planets_scanned: location.survey_progress.planets_scanned,
            moons_total: location.survey_progress.moons_total,
            moons_scanned: location.survey_progress.moons_scanned,
            moons_total_estimated: location.survey_progress.moons_total_estimated,
        },
        environment: LocationEnvironmentSummary {
            atmosphere: knowledge_wire(&location.environment.atmosphere),
            magnetic_field: knowledge_value(&location.environment.magnetic_field),
            gravity_g: knowledge_value(&location.environment.gravity_g),
            surface_temperature_c: knowledge_value(&location.environment.surface_temp_c),
            surface_temperature_k: knowledge_value(&location.environment.surface_temp_k),
            atmospheric_pressure_atm: knowledge_value(&location.environment.atmo_pressure_atm),
            oxygen_percent: knowledge_value(&location.environment.atmo_o2_pct),
            atmospheric_toxicity: knowledge_value(&location.environment.atmo_toxicity),
            hydrosphere_percent: knowledge_value(&location.environment.hydrosphere_pct),
            tectonic_index: knowledge_value(&location.environment.tectonic_index),
            biosphere_index: knowledge_value(&location.environment.biosphere_index),
            subsurface_ocean: knowledge_value(&location.environment.has_subsurface_ocean),
            habitable_zone: knowledge_value(&location.environment.in_habitable_zone),
            life_stage: match &location.environment.life_stage {
                Knowledge::Present(value) => wire_value(Some(value)),
                Knowledge::Absent => Some("none".to_owned()),
                Knowledge::Unknown => None,
                _ => None,
            },
            axial_tilt_degrees: knowledge_value(&location.environment.axial_tilt_deg),
            rotation_state: knowledge_value(&location.environment.rotation_state),
            star_spectral_type: knowledge_value(&location.environment.star_spectral_type),
            nearby_belt_richness: knowledge_value(&location.environment.nearby_belt_richness),
            distance_from_sol_light_years: knowledge_value(
                &location.environment.distance_from_sol_ly,
            ),
        },
        physical: {
            let key = match wire_value(location.location_type.as_ref()).as_deref() {
                Some("planet") => "planet",
                Some("moon") => "moon",
                Some("star") => "star",
                _ => "object",
            };
            object_field(&location.unknown, key)
        },
        belt: {
            let mut belt = object_field(&location.unknown, "asteroid_belt");
            if belt.is_empty() {
                belt = object_field(&location.unknown, "belt");
            }
            belt
        },
        lagrange: object_field(&location.unknown, "lagrange"),
        outer_system: {
            let mut outer = object_field(&location.unknown, "outer_system");
            for key in ["kuiper", "oort"] {
                if let Some(value) = location.unknown.get(key) {
                    outer.insert(key.to_owned(), value.clone());
                }
            }
            outer
        },
        incoming_object: object_field(&location.unknown, "object"),
        megastructure: object_field(&location.unknown, "megastructure"),
        resource_sites: object_array(&location.unknown, "resource_sites"),
        inventory: object_array(&location.unknown, "inventory"),
        advanced: location
            .unknown
            .iter()
            .filter(|(key, _)| {
                !matches!(
                    key.as_str(),
                    "planet"
                        | "moon"
                        | "star"
                        | "object"
                        | "asteroid_belt"
                        | "belt"
                        | "lagrange"
                        | "outer_system"
                        | "kuiper"
                        | "oort"
                        | "megastructure"
                        | "resource_sites"
                        | "inventory"
                )
            })
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
        contents: collection(contents)?,
    })
}

pub(crate) fn location_entity_summary(location: &Location) -> EntitySummary {
    let id = location.id().to_string();
    let status = location.scanned.map(|scanned| {
        if scanned {
            "scanned".to_owned()
        } else {
            "unscanned".to_owned()
        }
    });
    EntitySummary {
        entity: EntityRef {
            kind: EntityKind::Location,
            id: EntityId(id.clone()),
        },
        label: id.clone(),
        secondary_label: wire_value(location.location_type.as_ref()),
        system: location.system.clone(),
        location: Some(id),
        entity_type: wire_value(location.location_type.as_ref()),
        status,
    }
}

fn knowledge_value<T: Clone>(knowledge: &Knowledge<T>) -> Option<T> {
    match knowledge {
        Knowledge::Present(value) => Some(value.clone()),
        Knowledge::Unknown | Knowledge::Absent => None,
        _ => None,
    }
}

fn knowledge_wire<T: Serialize>(knowledge: &Knowledge<T>) -> Option<String> {
    match knowledge {
        Knowledge::Present(value) => wire_value(Some(value)),
        Knowledge::Unknown | Knowledge::Absent => None,
        _ => None,
    }
}

fn wire_value<T: Serialize>(value: Option<&T>) -> Option<String> {
    value
        .and_then(|value| serde_json::to_value(value).ok())
        .and_then(|value| value.as_str().map(str::to_owned))
}
