//! Feedback submission (`POST /v1/feedback`).

use reqwest::Method;
use serde::{Deserialize, Serialize};

use crate::error::Error;
use crate::raw::{Client, RawResponse, RequestSafety};

/// Request body for `POST /v1/feedback`.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct FeedbackSubmitRequest {
    /// Feedback category, e.g. `"typo"`, `"bug"`, or `"idea"`.
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub category: Option<String>,
    /// Feedback message (max 2000 characters).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
}

/// Response body for `POST /v1/feedback`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct FeedbackSubmitResponse {
    /// Submission status.
    pub status: Option<String>,
}

/// Typed client for `/v1/feedback`.
#[derive(Clone, Debug)]
pub struct FeedbackClient {
    client: Client,
}

impl FeedbackClient {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    /// Submits feedback for the authenticated account.
    pub async fn submit(
        &self,
        request: &FeedbackSubmitRequest,
    ) -> Result<RawResponse<FeedbackSubmitResponse>, Error> {
        self.client
            .execute_json(
                Method::POST,
                "v1/feedback",
                true,
                RequestSafety::Mutating,
                request,
            )
            .await
    }
}
