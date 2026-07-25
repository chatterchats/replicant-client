//! Public civilisation species directory (`GET /v1/species`).

use reqwest::Method;
use serde::Deserialize;

use crate::error::Error;
use crate::raw::{Client, JsonObject, RawResponse, RequestSafety};

/// One species discovered in the game world.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct Species {
    /// Stable species identifier.
    pub species_key: Option<String>,
    /// Display name.
    pub name: Option<String>,
    /// Flavor description.
    pub description: Option<String>,
    /// Government archetype.
    pub government: Option<String>,
    /// First-contact greeting text.
    pub greeting: Option<String>,
    /// Typical homeworld type.
    pub homeworld_type: Option<String>,
    /// Technological affinity descriptor.
    pub tech_affinity: Option<String>,
    /// Dominant species trait descriptor.
    pub r#trait: Option<String>,
    /// Any other fields the server returns.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Response body for `GET /v1/species`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct SpeciesListResponse {
    /// All discovered species.
    #[serde(default)]
    pub species: Vec<Species>,
}

/// Typed client for `/v1/species`.
#[derive(Clone, Debug)]
pub struct SpeciesClient {
    client: Client,
}

impl SpeciesClient {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    /// Lists all species discovered in the game world.
    pub async fn list(&self) -> Result<RawResponse<SpeciesListResponse>, Error> {
        self.client
            .execute(Method::GET, "v1/species", true, RequestSafety::SafeRead)
            .await
    }
}
