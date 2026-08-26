//! Galaxy-wide star catalogue (`GET /v1/stars`,
//! `GET /v1/locations/{star_designation}/stars`).
//!
//! This is deliberately the *non-replicant-scoped* half of the star data:
//! nearby/visible stars for a specific replicant live on
//! [`crate::raw::replicants::ReplicantsClient`] instead.

use reqwest::Method;
use serde::Deserialize;

use crate::error::Error;
use crate::raw::common::{encode_path_segment, with_query};
use crate::raw::{Client, Position, RawResponse, RequestSafety};

/// One star as listed near a given location.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize, serde::Serialize)]
pub struct StarItem {
    /// Display color.
    pub color: Option<String>,
    /// Stable star designation.
    pub designation: Option<String>,
    /// Distance from the viewing replicant's current location.
    pub distance_from_replicant: Option<f64>,
    /// Entry-point designation for interstellar travel, if this star has one.
    pub entry_point: Option<String>,
    /// Estimated number of planets.
    pub estimated_planets: Option<i64>,
    /// Estimated travel time, in seconds, from the replicant's location.
    pub estimated_travel_time: Option<i64>,
    /// Whether this star has been explored.
    pub explored: Option<bool>,
    /// Whether this star hosts a system hub.
    pub has_hub: Option<bool>,
    /// Whether this star hosts an active system ward.
    pub has_ward: Option<bool>,
    /// Whether this star is known to host life.
    pub has_life: Option<bool>,
    /// Catalogue region, e.g. `solzone`, `alpha`, `beta`, or `gamma`.
    pub region: Option<String>,
    /// Galactic coordinates.
    pub position: Option<Position>,
    /// Spectral classification.
    pub spectral_type: Option<String>,
}

/// Response body for `GET /v1/locations/{star_designation}/stars`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct StarListResponse {
    /// Current page number.
    pub page: Option<i64>,
    /// Page size.
    pub per_page: Option<i64>,
    /// The viewing replicant's current galactic coordinates.
    pub replicant_position: Option<Position>,
    /// Stars on this page.
    #[serde(default)]
    pub stars: Vec<StarItem>,
    /// Total stars matching this listing.
    pub total: Option<i64>,
    /// Total pages available.
    pub total_pages: Option<i64>,
    /// Total stars in the galaxy.
    pub total_stars: Option<i64>,
}

/// Query parameters for [`GalaxyClient::stars_near`].
#[derive(Clone, Debug, Default, PartialEq)]
pub struct StarListQuery {
    /// Page number to fetch.
    pub page: Option<i64>,
    /// Page size.
    pub per_page: Option<i64>,
}

/// One star in the full galaxy catalogue.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct CatalogueStar {
    /// Display color.
    pub color: Option<String>,
    /// Stable star designation.
    pub designation: Option<String>,
    /// Entry-point designation for interstellar travel.
    pub entry_point: Option<String>,
    /// Estimated number of planets.
    pub estimated_planets: Option<i64>,
    /// Whether this star hosts a megastructure hub.
    pub has_hub: Option<bool>,
    /// Whether this star hosts an active system ward.
    pub has_ward: Option<bool>,
    /// Display name.
    pub name: Option<String>,
    /// Catalogue region, e.g. `solzone`, `alpha`, `beta`, or `gamma`.
    pub region: Option<String>,
    /// Galactic coordinates.
    pub position: Option<Position>,
    /// Spectral classification.
    pub spectral_type: Option<String>,
}

/// Response body for `GET /v1/stars`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct CatalogueResponse {
    /// When this catalogue snapshot was generated, RFC3339.
    pub generated_at: Option<String>,
    /// Every star in the galaxy.
    #[serde(default)]
    pub stars: Vec<CatalogueStar>,
    /// Total star count.
    pub total: Option<i64>,
}

/// Typed client for the galaxy-wide star catalogue.
#[derive(Clone, Debug)]
pub struct GalaxyClient {
    client: Client,
}

impl GalaxyClient {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    /// Lists stars near a given star, from the perspective of the
    /// authenticated account's replicant there.
    pub async fn stars_near(
        &self,
        star_designation: &str,
        query: &StarListQuery,
    ) -> Result<RawResponse<StarListResponse>, Error> {
        let base = format!(
            "v1/locations/{}/stars",
            encode_path_segment(star_designation)
        );
        let path = with_query(
            &base,
            &[
                ("page", query.page.map(|value| value.to_string())),
                ("per_page", query.per_page.map(|value| value.to_string())),
            ],
        );
        self.client
            .execute(Method::GET, &path, true, RequestSafety::SafeRead)
            .await
    }

    /// Downloads the full star catalogue as a single response.
    pub async fn catalogue(&self) -> Result<RawResponse<CatalogueResponse>, Error> {
        self.client
            .execute_with_response_limit(
                Method::GET,
                "v1/stars",
                true,
                RequestSafety::SafeRead,
                self.client.max_star_catalogue_response_body_bytes(),
            )
            .await
    }
}
