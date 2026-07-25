//! Location-scoped event discovery and resolution
//! (`GET /v1/locations/{location_code}/events`,
//! `POST /v1/locations/{location_code}/events/{designation}`).
//!
//! Response shapes are shared with the account-wide event list; see
//! [`crate::raw::events::LocationEventListResponse`].

use reqwest::Method;

use crate::error::Error;
use crate::raw::common::{encode_path_segment, with_query};
use crate::raw::events::LocationEventListResponse;
use crate::raw::{Client, RawResponse, RequestSafety};

/// Typed client for location-scoped event operations.
#[derive(Clone, Debug)]
pub struct LocationEventsClient {
    client: Client,
}

impl LocationEventsClient {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    /// Lists events at a location that the account has discovered.
    pub async fn list(
        &self,
        location_code: &str,
        status: Option<&str>,
    ) -> Result<RawResponse<LocationEventListResponse>, Error> {
        let base = format!("v1/locations/{}/events", encode_path_segment(location_code));
        let path = with_query(&base, &[("status", status.map(str::to_string))]);
        self.client
            .execute(Method::GET, &path, true, RequestSafety::SafeRead)
            .await
    }

    /// Resolves a single location event for the caller. Requires the account
    /// to have discovered the event and to have a replicant at its location;
    /// the resolving replicant is server-selected (most recently arrived).
    /// The contract documents no response schema for this endpoint, so the
    /// raw body is returned as-is.
    pub async fn resolve(
        &self,
        location_code: &str,
        designation: &str,
    ) -> Result<RawResponse<serde_json::Value>, Error> {
        let path = format!(
            "v1/locations/{}/events/{}",
            encode_path_segment(location_code),
            encode_path_segment(designation)
        );
        self.client
            .execute(Method::POST, &path, true, RequestSafety::Mutating)
            .await
    }
}
