//! Replicant species reputation (`GET /v1/replicants/{replicant_code}/reputation`).
//!
//! The account-level aggregate (`GET /v1/accounts/reputation`) is a sibling
//! schema but lives on [`crate::raw::accounts::AccountsClient::reputation`],
//! matching the server's own route grouping; its response type
//! ([`AccountReputationResponse`]) is defined here since both endpoints share
//! the `app_schemas_species_*` schema family.

use reqwest::Method;
use serde::Deserialize;

use crate::error::Error;
use crate::raw::common::encode_path_segment;
use crate::raw::{Client, RawResponse, RequestSafety};

/// One species reputation entry, aggregated across all of an account's
/// replicants.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct AccountReputationEntry {
    /// Flavor description of the standing.
    pub description: Option<String>,
    /// Species display name.
    pub name: Option<String>,
    /// Species identifier.
    pub species_key: Option<String>,
    /// Aggregated reputation value across all replicants.
    pub total_reputation: Option<f64>,
    /// Dominant species trait descriptor.
    pub r#trait: Option<String>,
}

/// Response body for `GET /v1/accounts/reputation`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct AccountReputationResponse {
    /// Reputation entries, one per species.
    #[serde(default)]
    pub reputation: Vec<AccountReputationEntry>,
}

/// One species reputation entry scoped to a single replicant.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct ReplicantReputationEntry {
    /// Species display name.
    pub name: Option<String>,
    /// Reputation value with this species.
    pub reputation: Option<f64>,
    /// Species identifier.
    pub species_key: Option<String>,
}

/// Response body for `GET /v1/replicants/{replicant_code}/reputation`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct ReplicantReputationResponse {
    /// Reputation entries, one per species.
    #[serde(default)]
    pub reputation: Vec<ReplicantReputationEntry>,
}

/// Typed client for reputation operations.
#[derive(Clone, Debug)]
pub struct ReputationClient {
    client: Client,
}

impl ReputationClient {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    /// Fetches a single replicant's per-species reputation breakdown.
    pub async fn for_replicant(
        &self,
        replicant_code: &str,
    ) -> Result<RawResponse<ReplicantReputationResponse>, Error> {
        let path = format!(
            "v1/replicants/{}/reputation",
            encode_path_segment(replicant_code)
        );
        self.client
            .execute(Method::GET, &path, true, RequestSafety::SafeRead)
            .await
    }
}
