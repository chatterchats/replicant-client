//! Shared location/account event DTOs.
//!
//! `GET /v1/accounts/events` and `GET /v1/locations/{location_code}/events`
//! both return the same location-event shape (a discovered or resolved
//! location event, not to be confused with the account-wide device/game
//! event log at `GET /v1/events`, which lives behind the `events` feature).
//! This module defines that shape once so [`crate::raw::accounts`] and
//! [`crate::raw::location_events`] do not each define their own copy.

use serde::Deserialize;

use crate::raw::JsonObject;

/// A location event discovered or completed by a replicant: a first-contact
/// beacon, a megastructure contribution milestone, and similar site-scoped
/// story content.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct LocationEvent {
    /// The event's own designation (its identifier within its location).
    pub designation: Option<String>,
    /// The location this event occurs at.
    pub location: Option<String>,
    /// Human-readable location name.
    pub location_name: Option<String>,
    /// Event category, e.g. `"first_contact"`, `"megastructure"`.
    pub event_type: Option<String>,
    /// Display title.
    pub title: Option<String>,
    /// Display description.
    pub description: Option<String>,
    /// Event tier (difficulty/reward scale).
    pub tier: Option<i64>,
    /// Current status, e.g. `"active"`, `"completed"`.
    pub status: Option<String>,
    /// Message broadcast to nearby replicants when this event resolves.
    pub broadcast_message: Option<String>,
    /// When the event was discovered, RFC3339.
    pub discovered_at: Option<String>,
    /// When the event was completed, RFC3339.
    pub completed_at: Option<String>,
    /// Resolution criteria, open-shaped.
    pub criteria: Option<Vec<JsonObject>>,
    /// Current progress toward resolution, open-shaped.
    pub progress: Option<JsonObject>,
    /// Rewards granted on resolution, open-shaped.
    pub rewards: Option<JsonObject>,
    /// Any other fields the server returns.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Response body for `GET /v1/accounts/events`, addressed by an opaque
/// integer cursor.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct LocationEventListResponse {
    /// Events on this page.
    #[serde(default)]
    pub events: Vec<LocationEvent>,
    /// Cursor for the next page, or `None` if this is the last page.
    pub next_cursor: Option<i64>,
}
