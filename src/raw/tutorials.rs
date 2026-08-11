//! Authenticated tutorial progress (`GET /v1/tutorials*`).
//!
//! Replicant Space 2.5.0 adds these routes to OpenAPI without declaring a
//! successful response schema. The response models below therefore follow the
//! rendered 2.5.0 tutorial documentation and retain unknown fields so the raw
//! layer remains forward compatible if the server expands tutorial metadata.

use reqwest::Method;
use serde::Deserialize;

use crate::error::Error;
use crate::raw::common::encode_path_segment;
use crate::raw::{Client, JsonObject, RawResponse, RequestSafety};

/// Progress summary for one tutorial in `GET /v1/tutorials`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct TutorialSummary {
    /// Stable tutorial slug, for example `bootstrap`.
    pub slug: Option<String>,
    /// Human-readable tutorial name.
    pub name: Option<String>,
    /// Short tutorial description.
    pub description: Option<String>,
    /// One-based tutorial ordering within the onboarding sequence.
    pub order: Option<i64>,
    /// Whether every step in this tutorial is complete.
    pub completed: Option<bool>,
    /// Zero-based index of the current step as reported by the server.
    pub current_step: Option<i64>,
    /// Total number of steps in this tutorial.
    pub total_steps: Option<i64>,
    /// Future tutorial-summary fields not yet modeled by this client.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Response body for `GET /v1/tutorials`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct TutorialListResponse {
    /// Tutorials and their current progress, in server-defined order.
    #[serde(default)]
    pub tutorials: Vec<TutorialSummary>,
    /// Future list-level fields not yet modeled by this client.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// One objective within a tutorial.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct TutorialStep {
    /// Stable objective key.
    pub key: Option<String>,
    /// Human-readable objective description.
    pub description: Option<String>,
    /// API-oriented hint for completing this objective.
    pub hint: Option<String>,
    /// Whether this objective has been completed.
    pub completed: Option<bool>,
    /// Whether this is the objective the account is currently working on.
    pub current: Option<bool>,
    /// Future tutorial-step fields not yet modeled by this client.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Detailed progress for one tutorial from `GET /v1/tutorials/{slug}`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct TutorialDetail {
    /// Stable tutorial slug.
    pub slug: Option<String>,
    /// Human-readable tutorial name.
    pub name: Option<String>,
    /// Short tutorial description.
    pub description: Option<String>,
    /// One-based tutorial ordering when supplied by the server.
    pub order: Option<i64>,
    /// Zero-based index of the current step as reported by the server.
    pub current_step: Option<i64>,
    /// Total number of steps when supplied by the server.
    pub total_steps: Option<i64>,
    /// Whether every step in this tutorial is complete.
    pub completed: Option<bool>,
    /// Tutorial objectives in execution order.
    #[serde(default)]
    pub steps: Vec<TutorialStep>,
    /// Future tutorial-detail fields not yet modeled by this client.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed client for the authenticated tutorial-progress endpoints.
#[derive(Clone, Debug)]
pub struct TutorialsClient {
    client: Client,
}

impl TutorialsClient {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    /// Lists all tutorials with current account progress.
    pub async fn list(&self) -> Result<RawResponse<TutorialListResponse>, Error> {
        self.client
            .execute(Method::GET, "v1/tutorials", true, RequestSafety::SafeRead)
            .await
    }

    /// Fetches detailed progress and objective steps for one tutorial slug.
    pub async fn get(&self, slug: &str) -> Result<RawResponse<TutorialDetail>, Error> {
        let path = format!("v1/tutorials/{}", encode_path_segment(slug));
        self.client
            .execute(Method::GET, &path, true, RequestSafety::SafeRead)
            .await
    }
}
