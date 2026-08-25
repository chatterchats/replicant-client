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

/// Per-resource assignment state in an `ami.mining.digest` report.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct AmiMiningResourceReport {
    /// Number of mining drones currently assigned to this resource.
    pub actual: Option<i64>,
    /// Total resource quantity available across matching sites.
    pub capacity: Option<i64>,
    /// Number of mining drones the controller wants assigned.
    pub desired: Option<i64>,
    /// Whether this resource is fully depleted.
    pub exhausted: Option<bool>,
    /// Future per-resource report fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Directive-specific report carried by an `ami.mining.digest`.
///
/// Known resource-allocation fields are typed. New directive shapes, including
/// the Replicant Space 2.3.5 `gather_salvage` report, remain available through
/// [`Self::extra`] until their complete schema is documented.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct AmiMiningReport {
    /// Location currently coordinated by the mining controller.
    pub location: Option<String>,
    /// Resource assignment state keyed by resource type.
    #[serde(default)]
    pub resources: BTreeMap<String, AmiMiningResourceReport>,
    /// Directive-specific and future report fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `ami.mining.digest`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct AmiMiningDigestPayload {
    /// Active controller directive.
    pub directive: Option<String>,
    /// Buffered activity summary.
    pub activity: Option<AmiDigestActivity>,
    /// Current state of each managed device.
    #[serde(default)]
    pub devices: Vec<AmiDigestDevice>,
    /// Mining progress and directive-specific report data.
    pub report: Option<AmiMiningReport>,
    /// Future digest fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `print.started`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct PrintStartedPayload {
    /// Device type being printed.
    pub device_type: Option<String>,
    /// Open print origin, currently `vessel` or `autofactory`.
    pub print_mode: Option<String>,
    /// When the print is expected to finish, RFC3339.
    pub completes_at: Option<String>,
    /// Tags requested for the printed device.
    #[serde(default)]
    pub tags: Vec<String>,
    /// Future print-start fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `print.completed`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct PrintCompletedPayload {
    /// Device type that finished printing.
    pub device_type: Option<String>,
    /// Newly created device code.
    pub new_device_code: Option<String>,
    /// Open print origin, currently `vessel` or `autofactory`.
    pub print_mode: Option<String>,
    /// Whether the device was printed compacted as a flatpack.
    pub compacted: Option<bool>,
    /// Component device codes consumed by the print.
    #[serde(default)]
    pub consumed_device_codes: Vec<String>,
    /// Tags applied to the new device.
    #[serde(default)]
    pub tags: Vec<String>,
    /// Future print-completion fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for terminal modular-device transitions with no current fields.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct DeviceTransitionCompletedPayload {
    /// Future transition-completion fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `device.compacting` and `device.unfurling`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct DeviceTransitionStartedPayload {
    /// When the modular transition is expected to finish, RFC3339.
    pub completes_at: Option<String>,
    /// Future transition fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `triangulation.started`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct TriangulationStartedPayload {
    /// Spectral signature hash being tracked.
    pub signature: Option<String>,
    /// Reference point coordinates `[x, y, z]`.
    #[serde(default)]
    pub target: Vec<f64>,
    /// When the observation is expected to finish, RFC3339.
    pub completes_at: Option<String>,
    /// Future triangulation fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `triangulation.complete`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct TriangulationCompletedPayload {
    /// Spectral signature hash that was located.
    pub signature: Option<String>,
    /// Reference point coordinates `[x, y, z]`.
    #[serde(default)]
    pub target: Vec<f64>,
    /// Direction vector from the reference point toward the source.
    #[serde(default)]
    pub direction: Vec<f64>,
    /// Future triangulation fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `triangulation.failed`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct TriangulationFailedPayload {
    /// Spectral signature hash that could not be located.
    pub signature: Option<String>,
    /// Reference point coordinates `[x, y, z]`.
    #[serde(default)]
    pub target: Vec<f64>,
    /// Open failure reason, currently `signature_not_found`.
    pub reason: Option<String>,
    /// Future triangulation fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Resources and device codes exchanged in a completed trade.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct TradeReceivedItems {
    /// Resource quantities keyed by resource type.
    #[serde(default)]
    pub resources: JsonObject,
    /// Device codes transferred by the trade.
    #[serde(default)]
    pub devices: Vec<String>,
    /// Future trade-outcome fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `trade.completed` for either participant role.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct TradeCompletedPayload {
    /// Stable trade code.
    pub trade_code: Option<String>,
    /// Human-readable trade name.
    pub trade_name: Option<String>,
    /// Participant role, currently `buyer` or `seller`.
    pub role: Option<String>,
    /// Remaining stock, present for the seller.
    pub remaining_stock: Option<i64>,
    /// Rewards transferred to the buyer.
    pub rewards_received: Option<TradeReceivedItems>,
    /// Criteria transferred to the seller.
    pub criteria_received: Option<TradeReceivedItems>,
    /// Device codes created or transferred by older event variants.
    #[serde(default)]
    pub new_device_codes: Vec<String>,
    /// Future trade-completion fields.
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

/// Typed payload for `ward.activated` and `ward.deactivated`.
///
/// Replicant Space 2.5.0 currently documents these events with an empty
/// payload. Unknown fields are retained so future ward metadata does not
/// require a breaking decoder change.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct WardTransitionPayload {
    /// Future ward-transition fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `hub.warning`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct HubWarningPayload {
    /// Hub capacity when the threshold was crossed.
    pub capacity: Option<f64>,
    /// Warning class, currently `wear` or `inactive`.
    pub warning_type: Option<String>,
    /// Future warning fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `hub.maintained`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct HubMaintainedPayload {
    /// Resource quantities consumed by the repair.
    #[serde(default)]
    pub resources_consumed: JsonObject,
    /// Hub capacity after maintenance.
    pub capacity: Option<f64>,
    /// Future maintenance fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload shared by multiplayer Replicant presence events.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct MultiplayerReplicantPresencePayload {
    /// Replicant entering or leaving the locally observed system.
    pub replicant_code: Option<String>,
    /// Display name for that Replicant.
    pub replicant_name: Option<String>,
    /// Future multiplayer-presence fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `system.object_detected`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct SystemObjectDetectedPayload {
    /// Designation assigned to the incoming object.
    pub object_designation: Option<String>,
    /// Open object size class, such as `large`.
    pub size_class: Option<String>,
    /// Location expected to be impacted.
    pub impact_target: Option<String>,
    /// Expected impact time, RFC3339.
    pub impact_eta: Option<String>,
    /// Open detection source, currently `hub` or `beacon`.
    pub discovery_source: Option<String>,
    /// Future object-detection fields.
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

    /// Decodes this event as an `ami.mining.digest`, returning `None` for a
    /// different event name.
    pub fn ami_mining_digest(&self) -> Result<Option<AmiMiningDigestPayload>, Error> {
        self.decode_payload("ami.mining.digest")
    }

    /// Decodes this event as `blueprint.unlocked`, returning `None` for a
    /// different event name.
    pub fn blueprint_unlocked(&self) -> Result<Option<BlueprintUnlockedPayload>, Error> {
        self.decode_payload("blueprint.unlocked")
    }

    /// Decodes this event as `print.started`, returning `None` for a
    /// different event name.
    pub fn print_started(&self) -> Result<Option<PrintStartedPayload>, Error> {
        self.decode_payload("print.started")
    }

    /// Decodes this event as `print.completed`, returning `None` for a
    /// different event name.
    pub fn print_completed(&self) -> Result<Option<PrintCompletedPayload>, Error> {
        self.decode_payload("print.completed")
    }

    /// Decodes this event as `device.compacting`, returning `None` for a
    /// different event name.
    pub fn device_compacting(&self) -> Result<Option<DeviceTransitionStartedPayload>, Error> {
        self.decode_payload("device.compacting")
    }

    /// Decodes this event as `device.compacted`, returning `None` for a
    /// different event name.
    pub fn device_compacted(&self) -> Result<Option<DeviceTransitionCompletedPayload>, Error> {
        self.decode_payload("device.compacted")
    }

    /// Decodes this event as `device.unfurling`, returning `None` for a
    /// different event name.
    pub fn device_unfurling(&self) -> Result<Option<DeviceTransitionStartedPayload>, Error> {
        self.decode_payload("device.unfurling")
    }

    /// Decodes this event as `device.unfurled`, returning `None` for a
    /// different event name.
    pub fn device_unfurled(&self) -> Result<Option<DeviceTransitionCompletedPayload>, Error> {
        self.decode_payload("device.unfurled")
    }

    /// Decodes this event as `triangulation.started`, returning `None` for a
    /// different event name.
    pub fn triangulation_started(&self) -> Result<Option<TriangulationStartedPayload>, Error> {
        self.decode_payload("triangulation.started")
    }

    /// Decodes this event as `triangulation.complete`, returning `None` for a
    /// different event name.
    pub fn triangulation_completed(&self) -> Result<Option<TriangulationCompletedPayload>, Error> {
        self.decode_payload("triangulation.complete")
    }

    /// Decodes this event as `triangulation.failed`, returning `None` for a
    /// different event name.
    pub fn triangulation_failed(&self) -> Result<Option<TriangulationFailedPayload>, Error> {
        self.decode_payload("triangulation.failed")
    }

    /// Decodes this event as `ward.activated`, returning `None` for a
    /// different event name.
    pub fn ward_activated(&self) -> Result<Option<WardTransitionPayload>, Error> {
        self.decode_payload("ward.activated")
    }

    /// Decodes this event as `ward.deactivated`, returning `None` for a
    /// different event name.
    pub fn ward_deactivated(&self) -> Result<Option<WardTransitionPayload>, Error> {
        self.decode_payload("ward.deactivated")
    }

    /// Decodes this event as `hub.warning`, returning `None` for a different
    /// event name.
    pub fn hub_warning(&self) -> Result<Option<HubWarningPayload>, Error> {
        self.decode_payload("hub.warning")
    }

    /// Decodes this event as `hub.maintained`, returning `None` for a
    /// different event name.
    pub fn hub_maintained(&self) -> Result<Option<HubMaintainedPayload>, Error> {
        self.decode_payload("hub.maintained")
    }

    /// Decodes this event as `system.object_detected`, returning `None` for a
    /// different event name.
    pub fn system_object_detected(&self) -> Result<Option<SystemObjectDetectedPayload>, Error> {
        self.decode_payload("system.object_detected")
    }

    /// Decodes this event as `multiplayer.replicant_entered`, returning
    /// `None` for a different event name.
    pub fn multiplayer_replicant_entered(
        &self,
    ) -> Result<Option<MultiplayerReplicantPresencePayload>, Error> {
        self.decode_payload("multiplayer.replicant_entered")
    }

    /// Decodes this event as `multiplayer.replicant_left`, returning `None`
    /// for a different event name.
    pub fn multiplayer_replicant_left(
        &self,
    ) -> Result<Option<MultiplayerReplicantPresencePayload>, Error> {
        self.decode_payload("multiplayer.replicant_left")
    }

    /// Decodes this event as `trade.completed`, returning `None` for a
    /// different event name.
    pub fn trade_completed(&self) -> Result<Option<TradeCompletedPayload>, Error> {
        self.decode_payload("trade.completed")
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

    fn event(name: &str, payload: serde_json::Value) -> GameEvent {
        serde_json::from_value(serde_json::json!({
            "id": "1-0",
            "version": 2,
            "category": "test",
            "event": name,
            "replicant_code": null,
            "device_code": null,
            "device_type": null,
            "star": null,
            "location": null,
            "payload": payload,
            "created_at": "2026-08-02T00:00:00Z"
        }))
        .unwrap()
    }

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

    #[test]
    fn print_started_exposes_completes_at_and_tags() {
        let event = event(
            "print.started",
            serde_json::json!({
                "device_type": "autofactory",
                "print_mode": "autofactory",
                "completes_at": "2026-08-02T00:05:00Z",
                "tags": ["fleet-a", "miner"]
            }),
        );
        let payload = event.print_started().unwrap().unwrap();
        assert_eq!(payload.device_type.as_deref(), Some("autofactory"));
        assert_eq!(
            payload.completes_at.as_deref(),
            Some("2026-08-02T00:05:00Z")
        );
        assert_eq!(payload.tags, ["fleet-a".to_owned(), "miner".to_owned()]);
    }

    #[test]
    fn print_completed_exposes_compacted_state_consumed_devices_and_tags() {
        let event = event(
            "print.completed",
            serde_json::json!({
                "device_type": "parallax_array",
                "new_device_code": "A1B2C3D4",
                "print_mode": "autofactory",
                "compacted": true,
                "consumed_device_codes": ["E5F6G7H8", "J9K0L1M2"],
                "tags": ["fleet-a"]
            }),
        );
        let payload = event.print_completed().unwrap().unwrap();
        assert_eq!(payload.new_device_code.as_deref(), Some("A1B2C3D4"));
        assert_eq!(payload.compacted, Some(true));
        assert_eq!(
            payload.consumed_device_codes,
            ["E5F6G7H8".to_owned(), "J9K0L1M2".to_owned()]
        );
        assert_eq!(payload.tags, ["fleet-a".to_owned()]);
    }

    #[test]
    fn modular_transition_payloads_decode_started_and_completed_events() {
        let compacting = event(
            "device.compacting",
            serde_json::json!({"completes_at": "2026-08-06T15:22:00Z"}),
        );
        assert_eq!(
            compacting
                .device_compacting()
                .unwrap()
                .unwrap()
                .completes_at
                .as_deref(),
            Some("2026-08-06T15:22:00Z")
        );
        assert!(
            event("device.compacted", serde_json::json!({}))
                .device_compacted()
                .unwrap()
                .is_some()
        );
        assert!(
            event(
                "device.unfurling",
                serde_json::json!({"completes_at": "2026-08-06T15:23:00Z"}),
            )
            .device_unfurling()
            .unwrap()
            .is_some()
        );
        assert!(
            event("device.unfurled", serde_json::json!({}))
                .device_unfurled()
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn system_object_detected_exposes_impact_eta() {
        let event = event(
            "system.object_detected",
            serde_json::json!({
                "object_designation": "SOL-OBJ-2",
                "size_class": "large",
                "impact_target": "SOL-4",
                "impact_eta": "2026-08-26T09:30:00",
                "discovery_source": "hub"
            }),
        );
        let payload = event.system_object_detected().unwrap().unwrap();
        assert_eq!(payload.object_designation.as_deref(), Some("SOL-OBJ-2"));
        assert_eq!(payload.impact_eta.as_deref(), Some("2026-08-26T09:30:00"));
        assert_eq!(payload.discovery_source.as_deref(), Some("hub"));
    }

    #[test]
    fn triangulation_payloads_decode_vectors_and_failure_reason() {
        let started = event(
            "triangulation.started",
            serde_json::json!({
                "signature": "a3f7c2e8b1d94f06",
                "target": [5000, 14000, 100],
                "completes_at": "2026-08-06T16:22:00Z"
            }),
        )
        .triangulation_started()
        .unwrap()
        .unwrap();
        assert_eq!(started.target, [5000.0, 14_000.0, 100.0]);

        let completed = event(
            "triangulation.complete",
            serde_json::json!({
                "signature": "a3f7c2e8b1d94f06",
                "target": [5000, 14000, 100],
                "direction": [0.4, 0.9, 0.0]
            }),
        )
        .triangulation_completed()
        .unwrap()
        .unwrap();
        assert_eq!(completed.target, [5000.0, 14_000.0, 100.0]);
        assert_eq!(completed.direction, [0.4, 0.9, 0.0]);

        let failed = event(
            "triangulation.failed",
            serde_json::json!({
                "signature": "a3f7c2e8b1d94f06",
                "target": [5000, 14000, 100],
                "reason": "signature_not_found"
            }),
        )
        .triangulation_failed()
        .unwrap()
        .unwrap();
        assert_eq!(failed.reason.as_deref(), Some("signature_not_found"));
    }

    #[test]
    fn trade_completed_decodes_role_specific_outcomes() {
        let buyer = event(
            "trade.completed",
            serde_json::json!({
                "trade_code": "T1",
                "role": "buyer",
                "rewards_received": {
                    "resources": {"conductive": 12},
                    "devices": ["D1"]
                }
            }),
        )
        .trade_completed()
        .unwrap()
        .unwrap();
        assert_eq!(buyer.role.as_deref(), Some("buyer"));
        assert_eq!(
            buyer.rewards_received.as_ref().unwrap().devices,
            ["D1".to_owned()]
        );

        let seller = event(
            "trade.completed",
            serde_json::json!({
                "trade_code": "T1",
                "role": "seller",
                "remaining_stock": 3,
                "criteria_received": {
                    "resources": {"carbon": 25},
                    "devices": []
                }
            }),
        )
        .trade_completed()
        .unwrap()
        .unwrap();
        assert_eq!(seller.role.as_deref(), Some("seller"));
        assert_eq!(seller.remaining_stock, Some(3));
        assert_eq!(
            seller.criteria_received.as_ref().unwrap().resources["carbon"],
            25
        );
    }

    #[test]
    fn mining_digest_types_known_resources_and_preserves_new_report_shapes() {
        let event = event(
            "ami.mining.digest",
            serde_json::json!({
                "directive": "gather_salvage",
                "report": {
                    "location": "SCEPTURUM-BELT-1",
                    "resources": {
                        "conductive": {"actual": 2, "desired": 4, "exhausted": false}
                    },
                    "salvage": {"remaining": 9}
                }
            }),
        );
        let payload = event.ami_mining_digest().unwrap().unwrap();
        let report = payload.report.unwrap();
        assert_eq!(report.resources["conductive"].desired, Some(4));
        assert_eq!(report.extra["salvage"]["remaining"], 9);
    }
}
