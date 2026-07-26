//! Raw account event history and Server-Sent Events.
//!
//! Event names and payloads are intentionally open strings/JSON objects:
//! servers may add events without requiring a client release. The SSE
//! endpoint always applies account mute patterns; history applies them only
//! when [`crate::events::EventLogQuery::filtered`] is `Some(true)`.

use std::pin::Pin;

use eventsource_stream::Eventsource as _;
use futures::{Stream, StreamExt as _};
use reqwest::Method;
use serde::Deserialize;

use crate::{
    Error,
    raw::{Client, JsonObject, RawResponse, RequestSafety, common::with_query},
};

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
