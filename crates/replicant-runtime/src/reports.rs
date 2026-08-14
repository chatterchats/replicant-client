//! Read-only application reports.

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    io,
};

use futures::{StreamExt, stream};
use replicant_client::{
    Client, Realm,
    domain::{GalacticPosition, Location},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::ReportResult;

/// Reusable system and location values for smart frontend selectors.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct EntityIndex {
    /// Catalogue system designations.
    pub systems: Vec<String>,
    /// Known location and entry-point designations.
    pub locations: Vec<String>,
}

/// Builds a smart-selector index from managed catalogue and location state.
pub async fn entity_index(client: &Client) -> ReportResult<EntityIndex> {
    client.galaxy().refresh_catalogue().await?;
    let catalogue = client.galaxy().catalogue();
    let known_locations = client.locations().find().collect().await?;

    let systems = catalogue
        .iter()
        .map(|star| star.key.id.as_str().to_ascii_uppercase())
        .collect::<BTreeSet<_>>();
    let locations = catalogue
        .iter()
        .filter_map(|star| star.entry_point.as_ref())
        .map(|location| location.id.as_str().to_ascii_uppercase())
        .chain(
            known_locations
                .iter()
                .map(|location| location.id().as_str().to_ascii_uppercase()),
        )
        .collect::<BTreeSet<_>>();

    Ok(EntityIndex {
        systems: systems.into_iter().collect(),
        locations: locations.into_iter().collect(),
    })
}

/// Default number of concurrent system refreshes for a nearby-belt report.
pub const DEFAULT_BELT_REPORT_CONCURRENCY: usize = 4;
/// Maximum number of concurrent system refreshes for a nearby-belt report.
pub const MAX_BELT_REPORT_CONCURRENCY: usize = 16;

/// Inputs for [`nearby_belt_report`].
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct NearbyBeltReportRequest {
    /// Catalogue system at the center of the search.
    pub origin: String,
    /// Maximum straight-line catalogue distance in light years.
    pub radius_ly: f64,
    /// Maximum number of concurrent managed location refreshes.
    pub concurrency: usize,
}

impl NearbyBeltReportRequest {
    /// Creates a request with the default refresh concurrency.
    #[must_use]
    pub fn new(origin: impl Into<String>, radius_ly: f64) -> Self {
        Self {
            origin: origin.into().to_ascii_uppercase(),
            radius_ly,
            concurrency: DEFAULT_BELT_REPORT_CONCURRENCY,
        }
    }
}

/// One asteroid belt in a nearby explored system.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct NearbyBelt {
    /// System containing the belt.
    pub system: String,
    /// Belt designation.
    pub designation: String,
    /// Distance from the requested origin in light years.
    pub distance_ly: f64,
    /// Reported belt density.
    pub density: String,
    /// Inner orbital radius in astronomical units.
    pub inner_radius_au: Option<f64>,
    /// Outer orbital radius in astronomical units.
    pub outer_radius_au: Option<f64>,
    /// Resource name to reported scarcity.
    pub resources: BTreeMap<String, String>,
}

impl NearbyBelt {
    fn density_rank(&self) -> u8 {
        match self.density.to_ascii_lowercase().as_str() {
            "dense" => 3,
            "moderate" => 2,
            "sparse" => 1,
            _ => 0,
        }
    }
}

/// A system that could not be refreshed while building a report.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SystemRefreshFailure {
    /// System designation.
    pub system: String,
    /// Human-readable managed-client error.
    pub error: String,
}

/// Typed result of a nearby-belt report.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct NearbyBeltReport {
    /// Requested origin system.
    pub origin: String,
    /// Requested radius in light years.
    pub radius_ly: f64,
    /// Number of nearby explored systems examined.
    pub examined_systems: usize,
    /// Belts ordered from dense to sparse, then distance and designation.
    pub belts: Vec<NearbyBelt>,
    /// Systems whose managed refresh failed, making the report incomplete.
    pub failures: Vec<SystemRefreshFailure>,
}

/// Builds a read-only report of belts in explored systems near an origin star.
pub async fn nearby_belt_report(
    client: &Client,
    request: &NearbyBeltReportRequest,
) -> ReportResult<NearbyBeltReport> {
    validate_request(request)?;
    client.ready().await?;

    let replicants = client
        .replicants()
        .find()
        .in_realm(Realm::Live)
        .owned()
        .collect()
        .await?;
    if replicants.is_empty() {
        return Err(io::Error::other("the account has no owned replicants").into());
    }

    let mut explored = BTreeSet::new();
    for replicant in replicants {
        let report = client
            .galaxy()
            .sync_replicant_stars(replicant.id().as_str())
            .await?;
        explored.extend(
            report
                .explored_designations()
                .iter()
                .map(|designation| designation.as_str().to_owned()),
        );
    }

    let mut catalogue = client.galaxy().catalogue();
    if catalogue.is_empty() {
        client.galaxy().refresh_catalogue().await?;
        catalogue = client.galaxy().catalogue();
    }
    let origin_position = catalogue
        .iter()
        .find(|star| star.key.id.as_str() == request.origin)
        .ok_or_else(|| {
            io::Error::other(format!(
                "origin system `{}` is absent from the star catalogue",
                request.origin
            ))
        })?
        .position
        .ok_or_else(|| {
            io::Error::other(format!(
                "origin system `{}` has no catalogue position",
                request.origin
            ))
        })?;

    let mut nearby = catalogue
        .iter()
        .filter_map(|star| {
            let designation = star.key.id.as_str();
            let position = star.position?;
            let distance_ly = position_distance(origin_position, position);
            (explored.contains(designation) && distance_ly <= request.radius_ly).then(|| {
                NearbySystem {
                    designation: designation.to_owned(),
                    distance_ly,
                }
            })
        })
        .collect::<Vec<_>>();
    nearby.sort_by(|left, right| left.designation.cmp(&right.designation));

    let fetched = stream::iter(nearby.into_iter().map(|system| {
        let locations = client.locations();
        async move {
            let result = locations.get(&system.designation).await;
            (system, result)
        }
    }))
    .buffer_unordered(request.concurrency)
    .collect::<Vec<_>>()
    .await;

    let examined_systems = fetched.len();
    let mut failures = Vec::new();
    let mut belts = Vec::new();
    for (system, result) in fetched {
        match result {
            Ok(location) => belts.extend(belts_from_location(&system, &location)),
            Err(error) => failures.push(SystemRefreshFailure {
                system: system.designation,
                error: error.to_string(),
            }),
        }
    }
    sort_belts(&mut belts);

    Ok(NearbyBeltReport {
        origin: request.origin.clone(),
        radius_ly: request.radius_ly,
        examined_systems,
        belts,
        failures,
    })
}

fn validate_request(request: &NearbyBeltReportRequest) -> ReportResult<()> {
    if request.origin.is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "origin must not be empty").into());
    }
    if !request.radius_ly.is_finite() || request.radius_ly < 0.0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "radius_ly must be a non-negative finite number",
        )
        .into());
    }
    if request.concurrency == 0 || request.concurrency > MAX_BELT_REPORT_CONCURRENCY {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("concurrency must be between 1 and {MAX_BELT_REPORT_CONCURRENCY}"),
        )
        .into());
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct NearbySystem {
    designation: String,
    distance_ly: f64,
}

fn position_distance(left: GalacticPosition, right: GalacticPosition) -> f64 {
    let dx = left.x - right.x;
    let dy = left.y - right.y;
    let dz = left.z - right.z;
    (dx * dx + dy * dy + dz * dz).sqrt()
}

fn belts_from_location(system: &NearbySystem, location: &Location) -> Vec<NearbyBelt> {
    let Some(asteroid_belt) = location.unknown.get("asteroid_belt") else {
        return Vec::new();
    };
    let values = asteroid_belt
        .get("belts")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_else(|| std::slice::from_ref(asteroid_belt));
    values
        .iter()
        .filter_map(|value| parse_belt(system, value))
        .collect()
}

fn parse_belt(system: &NearbySystem, value: &Value) -> Option<NearbyBelt> {
    let object = value.as_object()?;
    let resources = object
        .get("resources")
        .and_then(Value::as_object)
        .map(|resources| {
            resources
                .iter()
                .map(|(resource, scarcity)| {
                    (
                        resource.clone(),
                        scarcity
                            .as_str()
                            .map(str::to_owned)
                            .unwrap_or_else(|| scarcity.to_string()),
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    Some(NearbyBelt {
        system: system.designation.clone(),
        designation: object.get("designation")?.as_str()?.to_owned(),
        distance_ly: system.distance_ly,
        density: object
            .get("density")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_owned(),
        inner_radius_au: object.get("inner_radius_au").and_then(Value::as_f64),
        outer_radius_au: object.get("outer_radius_au").and_then(Value::as_f64),
        resources,
    })
}

fn sort_belts(belts: &mut [NearbyBelt]) {
    belts.sort_by(|left, right| {
        right
            .density_rank()
            .cmp(&left.density_rank())
            .then_with(|| {
                left.distance_ly
                    .partial_cmp(&right.distance_ly)
                    .unwrap_or(Ordering::Equal)
            })
            .then_with(|| left.designation.cmp(&right.designation))
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use replicant_client::domain::{LocationEnvironment, LocationKey};
    use serde_json::json;

    #[test]
    fn extracts_and_orders_typed_belts() {
        let system = NearbySystem {
            designation: "TEST".into(),
            distance_ly: 2.0,
        };
        let location = Location {
            key: LocationKey::live("TEST".into()),
            location_type: None,
            scanned: None,
            system_scanned: None,
            system_tags: Vec::new(),
            system: None,
            parent: None,
            survey_progress: Default::default(),
            environment: LocationEnvironment::default(),
            unknown: BTreeMap::from([(
                "asteroid_belt".into(),
                json!({"belts": [
                    {"designation": "SPARSE", "density": "sparse"},
                    {"designation": "DENSE", "density": "dense", "resources": {"carbon": "common"}}
                ]}),
            )]),
        };

        let mut belts = belts_from_location(&system, &location);
        sort_belts(&mut belts);

        assert_eq!(
            belts
                .iter()
                .map(|belt| belt.designation.as_str())
                .collect::<Vec<_>>(),
            ["DENSE", "SPARSE"]
        );
        assert_eq!(
            belts[0].resources.get("carbon").map(String::as_str),
            Some("common")
        );
    }

    #[test]
    fn rejects_invalid_report_bounds() {
        let mut request = NearbyBeltReportRequest::new("TEST", f64::NAN);
        assert!(validate_request(&request).is_err());
        request.radius_ly = 1.0;
        request.concurrency = 0;
        assert!(validate_request(&request).is_err());
    }
}
