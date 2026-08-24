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

pub(crate) fn system_detail(
    star: Option<&Observation<Star>>,
    locations: &[Observation<Location>],
) -> Result<SystemInspectorSummary> {
    let children = locations
        .iter()
        .map(|observation| location_entity_summary(&observation.value))
        .collect();
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
        children: collection(children)?,
    })
}

pub(crate) fn location_detail(
    location: &Location,
    contents: Vec<EntitySummary>,
) -> Result<LocationInspectorSummary> {
    Ok(LocationInspectorSummary {
        location_type: wire_value(location.location_type.as_ref()),
        system: location.system.clone(),
        parent: location.parent.as_ref().map(|parent| parent.id.to_string()),
        scanned: location.scanned,
        system_scanned: location.system_scanned,
        system_tags: location.system_tags.clone(),
        survey: LocationSurveySummary {
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
