//! Raw account event history and Server-Sent Events.
//!
//! Event names and payloads are intentionally open strings/JSON objects:
//! servers may add events without requiring a client release. The SSE
//! endpoint always applies account mute patterns; history applies them only
//! when [`crate::events::EventLogQuery::filtered`] is `Some(true)`.

use std::{collections::BTreeMap, pin::Pin};

use eventsource_stream::Eventsource as _;
use futures::{Stream, StreamExt as _};
use reqwest::Method;
use serde::{Deserialize, de::DeserializeOwned};

use crate::{
    Error,
    raw::{Client, JsonObject, RawResponse, RequestSafety, common::with_query},
};

/// Shared activity summary carried by an AMI digest.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct AmiDigestActivity {
    /// Event counts keyed by open dotted event name.
    #[serde(default)]
    pub counts: BTreeMap<String, i64>,
    /// Total buffered events represented by this digest.
    pub event_count: Option<i64>,
    /// First and last event timestamps in the digest window.
    #[serde(default)]
    pub window: Vec<String>,
    /// Future activity fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// One AMI-managed device's digest summary.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct AmiDigestDevice {
    /// Stable device code.
    pub device_code: Option<String>,
    /// Events emitted by this device in the digest window.
    pub events: Option<i64>,
    /// Most recent open event name.
    pub last_event: Option<String>,
    /// Current status label.
    pub status: Option<String>,
    /// Future device-summary fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Survey progress within an `ami.survey.digest`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct AmiSurveyProgress {
    /// Bodies still awaiting a scan.
    pub remaining: Option<i64>,
    /// Bodies scanned so far.
    pub scanned: Option<i64>,
    /// Total bodies in the directive.
    pub total: Option<i64>,
    /// Future progress fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// One full body scan collated into an `ami.survey.digest` report.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct AmiSurveyScan {
    /// Device that performed the scan.
    pub device_code: Option<String>,
    /// Scanned planet, moon, or belt designation.
    pub scan_target: Option<String>,
    /// Open scan type, currently `planet`, `moon`, or `belt`.
    pub scan_type: Option<String>,
    /// Full type-keyed scan report.
    #[serde(default)]
    pub report: JsonObject,
    /// Future scan-entry fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Survey-specific report carried by an `ami.survey.digest`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct AmiSurveyReport {
    /// Devices assigned during this evaluation tick.
    pub assigned_this_tick: Option<i64>,
    /// Busy survey devices.
    pub busy: Option<i64>,
    /// Idle survey devices.
    pub idle: Option<i64>,
    /// Overall body-scan progress.
    pub progress: Option<AmiSurveyProgress>,
    /// Full scan results completed since the previous digest.
    #[serde(default)]
    pub scans: Vec<AmiSurveyScan>,
    /// Future survey-report fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `ami.survey.digest`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct AmiSurveyDigestPayload {
    /// Active controller directive.
    pub directive: Option<String>,
    /// Buffered activity summary.
    pub activity: Option<AmiDigestActivity>,
    /// Current state of each managed device.
    #[serde(default)]
    pub devices: Vec<AmiDigestDevice>,
    /// Survey progress and collated full scan results.
    pub report: Option<AmiSurveyReport>,
    /// Future digest fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `blueprint.unlocked`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct BlueprintUnlockedPayload {
    /// Newly unlocked device type.
    pub device_type: Option<String>,
    /// Short description.
    pub short_description: Option<String>,
    /// Full description.
    pub description: Option<String>,
    /// Resource cost by resource type.
    #[serde(default)]
    pub resources: JsonObject,
    /// Component cost by component type.
    pub components: Option<JsonObject>,
    /// Print time in seconds. Replicant Space 2.3.3 emits an integer JSON
    /// number; `f64` also accepts that representation.
    pub print_time: Option<f64>,
    /// Whether an autofactory is required.
    pub requires_autofactory: Option<bool>,
    /// Future blueprint fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// One account-wide game event.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct GameEvent {
    /// Opaque event ID, also used as the history and reconnect cursor.
    ///
    /// The SSE `data:` payload never repeats the frame's own `id:` line (see
    /// the event stream documentation), so this must deserialize even when
    /// absent; [`EventsClient::stream`] then fills it in from the SSE frame.
    #[serde(default)]
    pub id: String,
    /// Envelope version.
    pub version: i64,
    /// Open event category.
    pub category: String,
    /// Open dotted event name.
    pub event: String,
    /// Replicant associated with the event.
    pub replicant_code: Option<String>,
    /// Device associated with the event.
    pub device_code: Option<String>,
    /// Open device type.
    pub device_type: Option<String>,
    /// Star designation.
    pub star: Option<String>,
    /// Location designation.
    pub location: Option<String>,
    /// Event-specific, forward-compatible payload.
    #[serde(default)]
    pub payload: JsonObject,
    /// Event timestamp.
    pub created_at: String,
    /// Future envelope fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

impl GameEvent {
    fn decode_payload<T: DeserializeOwned>(&self, event_name: &str) -> Result<Option<T>, Error> {
        if self.event != event_name {
            return Ok(None);
        }
        serde_json::from_value(serde_json::Value::Object(self.payload.clone()))
            .map(Some)
            .map_err(|error| Error::Decode {
                message: format!("{event_name} payload failed to decode: {error}"),
                status: None,
                source: Some(Box::new(error)),
            })
    }

    /// Decodes this event as an `ami.survey.digest`, returning `None` for a
    /// different event name.
    pub fn ami_survey_digest(&self) -> Result<Option<AmiSurveyDigestPayload>, Error> {
        self.decode_payload("ami.survey.digest")
    }

    /// Decodes this event as `blueprint.unlocked`, returning `None` for a
    /// different event name.
    pub fn blueprint_unlocked(&self) -> Result<Option<BlueprintUnlockedPayload>, Error> {
        self.decode_payload("blueprint.unlocked")
    }
}

/// Query parameters for `GET /v1/events`.
#[derive(Clone, Debug, Default)]
pub struct EventLogQuery {
    /// Read after this string event ID.
    pub cursor: Option<String>,
    /// Page size, at most 100.
    pub limit: Option<u32>,
    /// Apply account mute patterns.
    pub filtered: Option<bool>,
    /// Restrict to a device.
    pub device_code: Option<String>,
    /// Restrict to an exact open event name.
    pub event: Option<String>,
    /// Restrict to an open category.
    pub category: Option<String>,
    /// Inclusive ISO-8601 lower timestamp bound.
    pub after: Option<String>,
    /// Exclusive ISO-8601 upper timestamp bound.
    pub before: Option<String>,
}

/// One page from the account event log.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct EventLogResponse {
    /// Events in chronological order.
    #[serde(default)]
    pub events: Vec<GameEvent>,
    /// String event-ID cursor for the next page.
    pub next_cursor: Option<String>,
}

/// Raw account event operations.
#[derive(Clone, Debug)]
pub struct EventsClient {
    client: Client,
}

impl EventsClient {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    /// Fetches one event-history page.
    pub async fn list(
        &self,
        query: &EventLogQuery,
    ) -> Result<RawResponse<EventLogResponse>, Error> {
        let path = with_query(
            "v1/events",
            &[
                ("cursor", query.cursor.clone()),
                ("limit", query.limit.map(|value| value.to_string())),
                ("filtered", query.filtered.map(|value| value.to_string())),
                ("device_code", query.device_code.clone()),
                ("event", query.event.clone()),
                ("category", query.category.clone()),
                ("after", query.after.clone()),
                ("before", query.before.clone()),
            ],
        );
        self.client
            .execute(Method::GET, &path, true, RequestSafety::SafeRead)
            .await
    }

    /// Opens the filtered account SSE stream.
    ///
    /// Pass the last received [`GameEvent::id`] as `cursor` when
    /// reconnecting. Keepalive comments are ignored by the SSE parser.
    pub async fn stream(
        &self,
        cursor: Option<&str>,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<GameEvent, Error>> + Send>>, Error> {
        let response = self.client.event_stream_response(cursor).await?;
        Ok(Box::pin(response.bytes_stream().eventsource().map(
            |frame| {
                let frame = frame.map_err(|error| Error::Transport {
                    message: format!("event stream failed: {error}"),
                    source: Some(Box::new(error)),
                })?;
                let mut event =
                    serde_json::from_str::<GameEvent>(&frame.data).map_err(|error| {
                        Error::Decode {
                            message: format!("event stream JSON failed: {error}"),
                            status: Some(200),
                            source: Some(Box::new(error)),
                        }
                    })?;
                if event.id.is_empty() {
                    event.id = frame.id;
                }
                if event.event.is_empty() {
                    event.event = frame.event;
                }
                Ok(event)
            },
        )))
    }
}

impl Client {
    /// Account event history and filtered SSE operations.
    #[must_use]
    pub fn events(&self) -> EventsClient {
        EventsClient::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::GameEvent;

    #[test]
    fn unknown_event_and_payload_fields_round_trip_through_json_value() {
        let event: GameEvent = serde_json::from_value(serde_json::json!({
            "id": "1-0", "version": 2, "category": "future",
            "event": "future.arrived", "replicant_code": null,
            "device_code": null, "device_type": null, "star": null,
            "location": null, "payload": {"new_shape": [1, 2]},
            "created_at": "2026-01-01T00:00:00Z", "future_envelope": true
        }))
        .unwrap();
        assert_eq!(event.event, "future.arrived");
        assert_eq!(event.payload["new_shape"][1], 2);
        assert_eq!(event.extra["future_envelope"], true);
    }
}
