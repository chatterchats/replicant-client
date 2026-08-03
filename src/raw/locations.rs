//! Location detail and the galaxy system map (`GET /v1/locations`,
//! `GET /v1/locations/{designation}`,
//! `POST /v1/locations/{designation}/contribute`).
//!
//! The contract declares most of [`Location`]'s fields as opaque objects
//! (`{}`, no nested schema) rather than typed structures, so this module
//! mirrors that faithfully with [`JsonObject`] instead of inventing shapes
//! the corrected docs/OpenAPI do not define.

use std::collections::HashMap;

use reqwest::Method;
use serde::{Deserialize, Serialize};

use crate::error::Error;
use crate::raw::common::{encode_path_segment, with_query};
use crate::raw::{Client, JsonObject, RawResponse, RequestSafety};

/// Per-location content counts, as summarized in the system map.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct LocationCounts {
    /// Devices present at this location.
    pub devices: Option<i64>,
    /// Location events discovered here.
    pub location_events: Option<i64>,
    /// Replicants present at this location.
    pub replicants: Option<i64>,
    /// Resource extraction sites at this location.
    pub resource_sites: Option<i64>,
    /// Distinct resource types available at this location.
    pub resources: Option<i64>,
}

/// Response body for `GET /v1/locations`: a galaxy-wide map of location
/// designation to summary counts.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct LocationSystemMap {
    /// Summary counts, keyed by location designation.
    #[serde(default)]
    pub locations: HashMap<String, LocationCounts>,
}

/// Surveyed environment details for a planet or moon.
///
/// The location contract leaves this object open, so only fields verified by
/// the sanitized `ILPHARD-3` response are modeled here. Remaining fields are
/// retained for forward compatibility.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct PlanetaryBody {
    /// Whether this planet or moon has been scanned.
    pub scanned: Option<bool>,
    /// Reported atmospheric classification.
    pub atmosphere: Option<String>,
    /// Whether the body is inside the star's habitable zone.
    pub in_habitable_zone: Option<bool>,
    /// Highest life stage reported for the body.
    pub life_stage: Option<String>,
    /// Whether a magnetic field is reported.
    pub magnetic_field: Option<bool>,
    /// Axial tilt in degrees.
    pub axial_tilt_deg: Option<f64>,
    /// Earth gravities (`g`).
    pub surface_gravity: Option<f64>,
    /// Degrees Celsius.
    pub surface_temp_c: Option<f64>,
    /// Additional response fields retained without interpretation.
    #[serde(flatten)]
    pub unknown: JsonObject,
}

/// Response body for `GET /v1/locations/{designation}`. Most fields are
/// open-shaped per the contract; only the scalars it actually types are
/// modeled directly.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct Location {
    /// Location events currently active here.
    pub active_location_events: Option<Vec<JsonObject>>,
    /// Asteroid belt detail, if this location is a belt.
    pub asteroid_belt: Option<JsonObject>,
    /// Parent belt summary, if this location sits within one.
    pub belt: Option<JsonObject>,
    /// Devices present at this location.
    pub devices: Option<Vec<JsonObject>>,
    /// Entry-point designation for interstellar travel, if any.
    pub entry_point: Option<String>,
    /// Estimated travel time, in seconds, from the requesting replicant.
    pub estimated_travel_time: Option<i64>,
    /// Resources present at this location.
    pub inventory: Option<Vec<JsonObject>>,
    /// Kuiper-belt detail, if applicable.
    pub kuiper: Option<JsonObject>,
    /// Lagrange-point detail, if applicable.
    pub lagrange: Option<JsonObject>,
    /// Whether life has been detected here.
    pub life_detected: Option<bool>,
    /// This location's own designation.
    #[serde(alias = "code")]
    pub location: Option<String>,
    /// The single most relevant active location event here, if any.
    pub location_event: Option<JsonObject>,
    /// Location type, e.g. `"planet"`, `"belt"`, `"station"`.
    pub location_type: Option<String>,
    /// Megastructure detail, if one is under construction or complete here.
    pub megastructure: Option<JsonObject>,
    /// Mining yield bonus percentage at this location.
    pub mining_bonus_pct: Option<f64>,
    /// Moon detail, if this location is a moon.
    pub moon: Option<PlanetaryBody>,
    /// Moons orbiting this location.
    pub moons: Option<Vec<JsonObject>>,
    /// Moons scanned so far.
    pub moons_scanned: Option<i64>,
    /// Total moons at this location.
    pub moons_total: Option<i64>,
    /// Whether `moons_total` is an estimate rather than an exact count.
    pub moons_total_estimated: Option<bool>,
    /// Generic catalogue object detail, for location types with no more
    /// specific field.
    pub object: Option<JsonObject>,
    /// Oort-cloud detail, if applicable.
    pub oort: Option<JsonObject>,
    /// Outer-system detail, if applicable.
    pub outer_system: Option<JsonObject>,
    /// Planet detail, if this location is a planet.
    pub planet: Option<PlanetaryBody>,
    /// Planets in this system.
    pub planets: Option<Vec<JsonObject>>,
    /// Planets scanned so far.
    pub planets_scanned: Option<i64>,
    /// Total planets in this system.
    pub planets_total: Option<i64>,
    /// Resource extraction sites at this location.
    pub resource_sites: Option<Vec<JsonObject>>,
    /// Whether this location has been scanned.
    #[serde(alias = "surveyed")]
    pub scanned: Option<bool>,
    /// Parent location designation, when the response identifies one.
    pub parent: Option<String>,
    /// Shops trading at this location.
    pub shops: Option<Vec<JsonObject>>,
    /// Parent star summary.
    pub star: Option<JsonObject>,
    /// Every catalogued object in this system.
    pub system_objects: Option<Vec<JsonObject>>,
    /// Whether the whole system has been scanned.
    pub system_scanned: Option<bool>,
    /// Parent system designation, when the response identifies one.
    pub system: Option<String>,
    /// Descriptive system tags.
    #[serde(default)]
    pub system_tags: Vec<String>,
    /// Verified and future top-level fields not yet modeled by this DTO.
    #[serde(flatten)]
    pub unknown: JsonObject,
}

/// Request body for `POST /v1/locations/{designation}/contribute`.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct LocationContributionRequest {
    /// Device codes contributing resources toward this location's active
    /// megastructure or location event.
    pub devices: Vec<String>,
}

/// Typed client for `/v1/locations*`.
#[derive(Clone, Debug)]
pub struct LocationsClient {
    client: Client,
}

impl LocationsClient {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    /// Fetches the galaxy-wide location summary map.
    pub async fn system_map(&self) -> Result<RawResponse<LocationSystemMap>, Error> {
        self.client
            .execute(Method::GET, "v1/locations", true, RequestSafety::SafeRead)
            .await
    }

    /// Fetches a single location's detail, optionally scoped to a specific
    /// replicant's perspective.
    pub async fn get(
        &self,
        designation: &str,
        replicant: Option<&str>,
    ) -> Result<RawResponse<Location>, Error> {
        let base = format!("v1/locations/{}", encode_path_segment(designation));
        let path = with_query(&base, &[("replicant", replicant.map(str::to_string))]);
        self.client
            .execute(Method::GET, &path, true, RequestSafety::SafeRead)
            .await
    }

    /// Contributes devices' resources toward a location's active
    /// megastructure or location event. This is an unsafe mutation: if the
    /// response is lost after transmission, resources may already have been
    /// consumed server-side. The raw client never retries it automatically.
    pub async fn contribute(
        &self,
        designation: &str,
        request: &LocationContributionRequest,
    ) -> Result<RawResponse<serde_json::Value>, Error> {
        let path = format!(
            "v1/locations/{}/contribute",
            encode_path_segment(designation)
        );
        self.client
            .execute_json(Method::POST, &path, true, RequestSafety::Mutating, request)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::Location;

    #[test]
    fn planet_and_moon_scanned_flags_are_retained() {
        let location: Location = serde_json::from_value(serde_json::json!({
            "location": "TEST-1",
            "planet": {"scanned": true},
            "moon": {"scanned": false}
        }))
        .unwrap();
        assert_eq!(location.planet.unwrap().scanned, Some(true));
        assert_eq!(location.moon.unwrap().scanned, Some(false));
    }
}
