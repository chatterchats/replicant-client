//! Replicant directory and control operations (`/v1/replicants*`).
//!
//! Replicant-scoped inventory, reputation, and trade-visibility live on
//! sibling families instead of here: [`crate::raw::inventory`],
//! [`crate::raw::reputation`], [`crate::raw::trading`]. The near-star and
//! star-detail views share their response schema with the non-replicant
//! galaxy catalogue in [`crate::raw::galaxy`], reused directly here.
//!
//! `mine`, `print`, `scan`, `teleport`, `transfer`, and `travel` are unsafe
//! mutations: the raw client never retries them automatically after a
//! request may have reached the server.

use reqwest::Method;
use serde::{Deserialize, Serialize};

use crate::error::Error;
use crate::raw::common::{Position, encode_path_segment, with_query};
use crate::raw::devices::DeviceListResponse;
use crate::raw::galaxy::{StarItem, StarListQuery, StarListResponse};
use crate::raw::{Client, JsonObject, RawResponse, RequestSafety};

pub use crate::raw::status::{MiningInfo, PrintingInfo, TravelInfo};

/// Query parameters for `GET /v1/replicants`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ReplicantListQuery {
    /// Opaque integer cursor from a previous page's `next_cursor`.
    pub cursor: Option<i64>,
    /// Maximum number of replicants to return.
    pub limit: Option<i64>,
    /// Return only the single latest entry.
    pub latest: Option<bool>,
    /// Filter by (partial) display name.
    pub name: Option<String>,
}

/// One replicant in a directory search.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct ReplicantSearchItem {
    /// Whether this replicant is an NPC.
    pub is_npc: Option<bool>,
    /// Last known location designation.
    pub last_location: Option<String>,
    /// Display name.
    pub name: Option<String>,
    /// Stable replicant code.
    pub replicant_code: Option<String>,
}

/// Response body for `GET /v1/replicants`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct ReplicantListResponse {
    /// Cursor for the next page, or `None` if this is the last page.
    pub next_cursor: Option<i64>,
    /// Replicants on this page.
    #[serde(default)]
    pub replicants: Vec<ReplicantSearchItem>,
}

/// A replicant's in-progress teleport.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct TeleportInfo {
    /// When the teleport completes, RFC3339.
    pub completes_at: Option<String>,
    /// Estimated seconds remaining.
    pub eta_seconds: Option<i64>,
    /// When the teleport started, RFC3339.
    pub started_at: Option<String>,
    /// The destination matrix code.
    pub target_matrix_code: Option<String>,
}

/// Full status of a single replicant.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct ReplicantStatus {
    /// Devices currently attached to this replicant, open-shaped.
    #[serde(default)]
    pub attached_devices: Vec<JsonObject>,
    /// Carried resource stacks, open-shaped.
    #[serde(default)]
    pub cargo: Vec<JsonObject>,
    /// This replicant's cohort permission level.
    pub cohort_permission: Option<String>,
    /// Free-text flavor description.
    pub description: Option<String>,
    /// Accumulated experience points.
    pub experience_points: Option<i64>,
    /// The device this replicant is currently hosted in, if any.
    pub hosted_device_code: Option<String>,
    /// Whether this replicant is an NPC.
    pub is_npc: Option<bool>,
    /// Current location designation.
    pub location: Option<String>,
    /// Human-readable current location name.
    pub location_name: Option<String>,
    /// In-progress mining operation, if any.
    pub mining: Option<MiningInfo>,
    /// Display name.
    pub name: Option<String>,
    /// Subscription plan.
    pub plan: Option<String>,
    /// Galactic coordinates.
    pub position: Option<Position>,
    /// Queued print jobs, open-shaped.
    #[serde(default)]
    pub print_queue: Vec<JsonObject>,
    /// In-progress print job, if any.
    pub printing: Option<PrintingInfo>,
    /// Associated project name.
    pub project: Option<String>,
    /// Preferred pronouns.
    pub pronouns: Option<String>,
    /// Stable replicant code.
    pub replicant_code: Option<String>,
    /// Replicant status, e.g. `"active"`, `"offline"`.
    pub status: Option<String>,
    /// Devices currently stowed by this replicant, open-shaped.
    #[serde(default)]
    pub stowed_devices: Vec<JsonObject>,
    /// In-progress teleport, if any.
    pub teleport: Option<TeleportInfo>,
    /// In-progress travel, if any.
    pub travel: Option<TravelInfo>,
}

/// Request body for `PATCH /v1/replicants/{replicant_code}`.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct ReplicantUpdateRequest {
    /// New cohort permission level.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cohort_permission: Option<String>,
    /// New flavor description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// New NPC flag.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_npc: Option<bool>,
    /// New display name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// New subscription plan.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan: Option<String>,
    /// New associated project name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    /// New preferred pronouns.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pronouns: Option<String>,
}

/// Response body for `PATCH /v1/replicants/{replicant_code}`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct ReplicantUpdateResponse {
    /// Cohort permission level after the update.
    pub cohort_permission: Option<String>,
    /// Flavor description after the update.
    pub description: Option<String>,
    /// NPC flag after the update.
    pub is_npc: Option<bool>,
    /// Display name after the update.
    pub name: Option<String>,
    /// Subscription plan after the update.
    pub plan: Option<String>,
    /// Associated project name after the update.
    pub project: Option<String>,
    /// Preferred pronouns after the update.
    pub pronouns: Option<String>,
    /// The updated replicant's code.
    pub replicant_code: Option<String>,
    /// Replicant status after the update.
    pub status: Option<String>,
}

/// Query parameters for `GET /v1/replicants/{replicant_code}/devices`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ReplicantDevicesQuery {
    /// Restrict to a single location designation.
    pub location: Option<String>,
    /// Restrict to a single device type.
    pub device_type: Option<String>,
    /// Opaque integer cursor from a previous page's `next_cursor`.
    pub cursor: Option<i64>,
    /// Maximum number of devices to return.
    pub limit: Option<i64>,
}

/// Request body for `POST /v1/replicants/{replicant_code}/message`.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct ReplicantMessageRequest {
    /// The channel to broadcast on.
    pub channel: String,
    /// The message body.
    pub text: String,
}

/// Response body for `POST /v1/replicants/{replicant_code}/message`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct ReplicantMessageResponse {
    /// The channel broadcast on.
    pub channel: Option<String>,
    /// The replicant's current star.
    pub current_star: Option<String>,
    /// ID of the sent message.
    pub id: Option<i64>,
    /// The message body.
    pub message: Option<String>,
    /// The relaying device's code, if relayed.
    pub relay_code: Option<String>,
    /// The sending replicant's code.
    pub replicant_code: Option<String>,
    /// The sending replicant's display name.
    pub replicant_name: Option<String>,
    /// Send status.
    pub status: Option<String>,
    /// When the message was sent, RFC3339.
    pub time: Option<String>,
}

/// Request body for `POST /v1/replicants/{replicant_code}/mine`.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct MineRequest {
    /// Notification preferences for this mining operation, open-shaped.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notify: Option<JsonObject>,
    /// The resource type to mine.
    pub resource_type: String,
    /// The specific site to mine, if not the current location.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
}

/// Response body for `POST /v1/replicants/{replicant_code}/mine`.
///
/// Per `policy/normalization-aliases.json`, this endpoint's wire format may
/// still use two deprecated field names: `belt` (legacy for `location`) and
/// `designation` (legacy for `site`). Both are deserialized transparently
/// via `serde(alias)` into their current, normalized field; the legacy
/// names are never exposed on this type.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct MineResponse {
    /// Current resource availability at the mining site.
    pub availability: Option<String>,
    /// Seconds per mining cycle.
    pub cycle_time_seconds: Option<f64>,
    /// Resource density at the site.
    pub density: Option<String>,
    /// The device performing the mining, if device-hosted.
    pub device_code: Option<String>,
    /// The mining site's location designation. Deserializes from either the
    /// current `location` key or the deprecated `belt` key.
    #[serde(alias = "belt")]
    pub location: Option<String>,
    /// The mining site's location type.
    pub location_type: Option<String>,
    /// Resource type being mined.
    pub resource_type: Option<String>,
    /// Salvage type, for salvage-mining operations.
    pub salvage_type: Option<String>,
    /// The mining site's own designation. Deserializes from either the
    /// current `site` key or the deprecated `designation` key.
    #[serde(alias = "designation")]
    pub site: Option<String>,
    /// Mining status.
    pub status: Option<String>,
}

/// Request body for `POST /v1/replicants/{replicant_code}/print`.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct PrintRequest {
    /// The print sub-command, if the server requires one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    /// The device type to print.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_type: Option<String>,
    /// Prints a modular device in its compacted transport state when `true`.
    /// Supplying `false` explicitly is supported by Replicant Space 2.4.0.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flatpack: Option<bool>,
    /// Notification preferences for this print job, open-shaped.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notify: Option<JsonObject>,
}

/// Response body for `POST /v1/replicants/{replicant_code}/print`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct PrintResponse {
    /// When the print completes, RFC3339.
    pub completes_at: Option<String>,
    /// Device type being printed.
    pub device_type: Option<String>,
    /// Print time, in seconds.
    pub print_time_seconds: Option<i64>,
    /// Whether resources were refunded (e.g. on a failed print).
    pub resources_refunded: Option<bool>,
    /// When the print started, RFC3339.
    pub started_at: Option<String>,
    /// Print status.
    pub status: Option<String>,
}

/// One location event visible in a system scan.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct ScanLocationEvent {
    /// The event's own designation.
    pub designation: Option<String>,
    /// Event type.
    pub event_type: Option<String>,
    /// The location this event occurs at.
    pub location: Option<String>,
    /// Event tier.
    pub tier: Option<i64>,
    /// Display title.
    pub title: Option<String>,
}

/// One asteroid belt detail within a scan's asteroid belt summary.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct ScanAsteroidBeltDetail {
    /// Resource density.
    pub density: Option<String>,
    /// Belt designation.
    pub designation: Option<String>,
    /// Inner radius, in AU.
    pub inner_radius_au: Option<f64>,
    /// Outer radius, in AU.
    pub outer_radius_au: Option<f64>,
    /// Resource availability by type, open-shaped.
    pub resources: Option<JsonObject>,
}

/// A scan's asteroid belt summary.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct ScanAsteroidBelt {
    /// Individual belts present.
    #[serde(default)]
    pub belts: Vec<ScanAsteroidBeltDetail>,
    /// Whether any asteroid belt is present in this system.
    pub present: Option<bool>,
}

/// One resource stack in a scanned planet's surface inventory.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct ScanInventoryItem {
    /// Quantity available.
    pub quantity: Option<f64>,
    /// Resource type.
    pub resource_type: Option<String>,
}

/// One planet visible in a system scan.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct ScanPlanetSummary {
    /// Planet designation.
    pub designation: Option<String>,
    /// Whether the planet lies in its star's habitable zone.
    pub in_habitable_zone: Option<bool>,
    /// Surface resource inventory, if scanned.
    #[serde(default)]
    pub inventory: Vec<ScanInventoryItem>,
    /// Number of moons.
    pub moon_count: Option<i64>,
    /// Display name.
    pub name: Option<String>,
    /// Orbital distance, in AU.
    pub orbital_distance_au: Option<f64>,
    /// Surface salvage sites, open-shaped.
    #[serde(default)]
    pub salvage: Vec<JsonObject>,
    /// Whether this planet has been surface-scanned.
    pub scanned: Option<bool>,
    /// Planet type.
    pub r#type: Option<String>,
}

/// One trade on a shop visible in a system scan.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct ScanShopTrade {
    /// Trade criteria, open-shaped.
    pub criteria: Option<JsonObject>,
    /// Units of stock remaining.
    pub current_stock: Option<i64>,
    /// Trade display name.
    pub name: Option<String>,
    /// Trade rewards, open-shaped.
    pub rewards: Option<JsonObject>,
    /// Stable trade code.
    pub trade_code: Option<String>,
}

/// One shop visible in a system scan.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct ScanShopSummary {
    /// The controlling device's code.
    pub controller_code: Option<String>,
    /// Flavor description.
    pub description: Option<String>,
    /// The shop's location designation.
    pub location: Option<String>,
    /// Human-readable location name.
    pub location_name: Option<String>,
    /// Owning replicant's display name.
    pub owner_name: Option<String>,
    /// Owning replicant's code.
    pub owner_replicant_code: Option<String>,
    /// Shop display name.
    pub shop_name: Option<String>,
    /// Trades currently offered.
    #[serde(default)]
    pub trades: Vec<ScanShopTrade>,
}

/// A scanned star's habitable zone.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct ScanHabitableZone {
    /// Inner boundary, in AU.
    pub inner_au: Option<f64>,
    /// Outer boundary, in AU.
    pub outer_au: Option<f64>,
}

/// Full detail of the scanned star itself.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct ScanStarDetail {
    /// Age, in millions of years.
    pub age_my: Option<f64>,
    /// Display color.
    pub color: Option<String>,
    /// Star designation.
    pub designation: Option<String>,
    /// Habitable zone boundaries.
    pub habitable_zone: Option<ScanHabitableZone>,
    /// Luminosity, in solar units.
    pub luminosity_solar: Option<f64>,
    /// Mass, in solar units.
    pub mass_solar: Option<f64>,
    /// Display name.
    pub name: Option<String>,
    /// Galactic coordinates.
    pub position: Option<Position>,
    /// Spectral classification.
    pub spectral_type: Option<String>,
    /// Surface temperature, in Kelvin.
    pub temperature_k: Option<i64>,
}

/// Response body for `POST /v1/replicants/{replicant_code}/scan`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct SystemScanResponse {
    /// Location events active in this system.
    #[serde(default)]
    pub active_location_events: Vec<ScanLocationEvent>,
    /// This system's asteroid belt summary.
    pub asteroid_belt: Option<ScanAsteroidBelt>,
    /// Entry-point designation for interstellar travel, if any.
    pub entry_point: Option<String>,
    /// Whether life was detected in this system.
    pub life_detected: Option<bool>,
    /// Mining yield bonus, as a percentage.
    pub mining_bonus_pct: Option<f64>,
    /// Outer-system detail, open-shaped.
    pub outer_system: Option<JsonObject>,
    /// Planets in this system.
    #[serde(default)]
    pub planets: Vec<ScanPlanetSummary>,
    /// Other replicants visible in this system, open-shaped.
    #[serde(default)]
    pub replicants: Vec<JsonObject>,
    /// Shops visible in this system.
    #[serde(default)]
    pub shops: Vec<ScanShopSummary>,
    /// The scanned star itself.
    pub star: Option<ScanStarDetail>,
    /// Other system objects, open-shaped.
    #[serde(default)]
    pub system_objects: Vec<JsonObject>,
    /// System-level descriptive tags.
    #[serde(default)]
    pub system_tags: Vec<String>,
}

/// Query parameters for `GET /v1/replicants/{replicant_code}/scan/devices`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ScanDevicesQuery {
    /// Restrict to a single device type.
    pub device_type: Option<String>,
    /// Restrict to devices owned by a single replicant.
    pub owner_replicant_code: Option<String>,
    /// Opaque integer cursor from a previous page's `next_cursor`.
    pub cursor: Option<i64>,
    /// Maximum number of devices to return.
    pub limit: Option<i64>,
}

/// Response body for `GET /v1/replicants/{replicant_code}/stars/{star_designation}`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct StarDetailResponse {
    /// The viewing replicant's current galactic coordinates.
    pub replicant_position: Option<Position>,
    /// The requested star's detail.
    pub star: Option<StarItem>,
}

/// Request body for `POST /v1/replicants/{replicant_code}/teleport`.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct TeleportRequest {
    /// Destination empty replicant matrix code, or an FTL slingshot device
    /// code whose configured `linked_device` identifies the destination
    /// matrix.
    pub target: String,
}

/// Response body for `POST /v1/replicants/{replicant_code}/teleport`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct TeleportResponse {
    /// When the teleport completes, RFC3339.
    pub completes_at: Option<String>,
    /// The destination star.
    pub destination_star: Option<String>,
    /// Error detail, if the teleport failed.
    pub error: Option<String>,
    /// Offline duration incurred, in seconds.
    pub offline_seconds: Option<i64>,
    /// The source star.
    pub source_star: Option<String>,
    /// When the teleport started, RFC3339.
    pub started_at: Option<String>,
    /// Teleport status.
    pub status: Option<String>,
    /// The destination matrix code.
    pub target_matrix_code: Option<String>,
}

/// Request body for `POST /v1/replicants/{replicant_code}/transfer`.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct TransferRequest {
    /// The receiving account or replicant code.
    pub target: String,
}

/// Response body for `POST /v1/replicants/{replicant_code}/transfer`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct TransferResponse {
    /// Error detail, if the transfer failed.
    pub error: Option<String>,
    /// The new hosting device or account.
    pub new_host: Option<String>,
    /// The previous hosting device or account.
    pub old_host: Option<String>,
    /// Transfer status.
    pub status: Option<String>,
}

/// Request body for `POST /v1/replicants/{replicant_code}/travel`.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct TravelRequest {
    /// The destination location or star designation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub destination: Option<String>,
    /// If `true`, computes and returns the route without departing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dry_run: Option<bool>,
    /// Notification preferences for this travel, open-shaped.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notify: Option<JsonObject>,
    /// Server-defined routing hint.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub via: Option<serde_json::Value>,
}

/// One leg of a travel route.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct RouteLeg {
    /// Whether this leg is the currently active one.
    pub active: Option<bool>,
    /// Distance, in AU.
    pub distance_au: Option<f64>,
    /// Distance, in light-years.
    pub distance_ly: Option<f64>,
    /// Leg origin.
    pub from: Option<String>,
    /// Human-readable leg origin name.
    pub from_name: Option<String>,
    /// Leg index within the route.
    pub leg: Option<i64>,
    /// Leg duration, in seconds.
    pub time_seconds: Option<f64>,
    /// Leg destination.
    pub to: Option<String>,
    /// Human-readable leg destination name.
    pub to_name: Option<String>,
    /// Leg type.
    pub r#type: Option<String>,
}

/// Response body for `POST /v1/replicants/{replicant_code}/travel`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct TravelResponse {
    /// When this leg arrives, RFC3339.
    pub arrives_at: Option<String>,
    /// When travel departed, RFC3339.
    pub departed_at: Option<String>,
    /// This leg's destination.
    pub destination: Option<String>,
    /// Human-readable destination name.
    pub destination_name: Option<String>,
    /// Destination type.
    pub destination_type: Option<String>,
    /// The traveling device's code, for device-hosted travel.
    pub device_code: Option<String>,
    /// The overall route's final destination.
    pub final_destination: Option<String>,
    /// Human-readable final destination name.
    pub final_destination_name: Option<String>,
    /// Whether a hub bonus applied to this travel.
    pub hub_bonus: Option<bool>,
    /// This leg's origin.
    pub origin: Option<String>,
    /// Completion percentage.
    pub progress_percent: Option<f64>,
    /// Remaining route legs.
    #[serde(default)]
    pub route: Vec<RouteLeg>,
    /// Current star.
    pub star: Option<String>,
    /// Travel status.
    pub status: Option<String>,
    /// Total route distance, in light-years.
    pub total_distance_ly: Option<f64>,
    /// Total route time, in seconds.
    pub total_time_seconds: Option<f64>,
}

/// Typed client for `/v1/replicants*`.
#[derive(Clone, Debug)]
pub struct ReplicantsClient {
    client: Client,
}

impl ReplicantsClient {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    /// Searches the replicant directory.
    pub async fn list(
        &self,
        query: &ReplicantListQuery,
    ) -> Result<RawResponse<ReplicantListResponse>, Error> {
        let path = with_query(
            "v1/replicants",
            &[
                ("cursor", query.cursor.map(|value| value.to_string())),
                ("limit", query.limit.map(|value| value.to_string())),
                ("latest", query.latest.map(|value| value.to_string())),
                ("name", query.name.clone()),
            ],
        );
        self.client
            .execute(Method::GET, &path, true, RequestSafety::SafeRead)
            .await
    }

    /// Fetches a single replicant's full status.
    pub async fn get(&self, replicant_code: &str) -> Result<RawResponse<ReplicantStatus>, Error> {
        let path = format!("v1/replicants/{}", encode_path_segment(replicant_code));
        self.client
            .execute(Method::GET, &path, true, RequestSafety::SafeRead)
            .await
    }

    /// Updates a replicant's profile fields.
    pub async fn update(
        &self,
        replicant_code: &str,
        request: &ReplicantUpdateRequest,
    ) -> Result<RawResponse<ReplicantUpdateResponse>, Error> {
        let path = format!("v1/replicants/{}", encode_path_segment(replicant_code));
        self.client
            .execute_json(Method::PATCH, &path, true, RequestSafety::Mutating, request)
            .await
    }

    /// Lists devices owned by a replicant.
    pub async fn devices(
        &self,
        replicant_code: &str,
        query: &ReplicantDevicesQuery,
    ) -> Result<RawResponse<DeviceListResponse>, Error> {
        let base = format!(
            "v1/replicants/{}/devices",
            encode_path_segment(replicant_code)
        );
        let path = with_query(
            &base,
            &[
                ("location", query.location.clone()),
                ("device_type", query.device_type.clone()),
                ("cursor", query.cursor.map(|value| value.to_string())),
                ("limit", query.limit.map(|value| value.to_string())),
            ],
        );
        self.client
            .execute(Method::GET, &path, true, RequestSafety::SafeRead)
            .await
    }

    /// Broadcasts a BobNet message from a replicant.
    pub async fn message(
        &self,
        replicant_code: &str,
        request: &ReplicantMessageRequest,
    ) -> Result<RawResponse<ReplicantMessageResponse>, Error> {
        let path = format!(
            "v1/replicants/{}/message",
            encode_path_segment(replicant_code)
        );
        self.client
            .execute_json(Method::POST, &path, true, RequestSafety::Mutating, request)
            .await
    }

    /// Stops a replicant's current mining operation. The contract documents
    /// no response schema for this endpoint's success case, so the raw body
    /// is returned as-is.
    pub async fn stop_mining(
        &self,
        replicant_code: &str,
    ) -> Result<RawResponse<serde_json::Value>, Error> {
        let path = format!("v1/replicants/{}/mine", encode_path_segment(replicant_code));
        self.client
            .execute(Method::DELETE, &path, true, RequestSafety::Mutating)
            .await
    }

    /// Begins a replicant mining operation.
    pub async fn mine(
        &self,
        replicant_code: &str,
        request: &MineRequest,
    ) -> Result<RawResponse<MineResponse>, Error> {
        let path = format!("v1/replicants/{}/mine", encode_path_segment(replicant_code));
        self.client
            .execute_json(Method::POST, &path, true, RequestSafety::Mutating, request)
            .await
    }

    /// Queues a device to be printed by a replicant.
    pub async fn print(
        &self,
        replicant_code: &str,
        request: &PrintRequest,
    ) -> Result<RawResponse<PrintResponse>, Error> {
        let path = format!(
            "v1/replicants/{}/print",
            encode_path_segment(replicant_code)
        );
        self.client
            .execute_json(Method::POST, &path, true, RequestSafety::Mutating, request)
            .await
    }

    /// Runs a full system scan from a replicant's current location.
    pub async fn scan(
        &self,
        replicant_code: &str,
    ) -> Result<RawResponse<SystemScanResponse>, Error> {
        let path = format!("v1/replicants/{}/scan", encode_path_segment(replicant_code));
        self.client
            .execute(Method::POST, &path, true, RequestSafety::Mutating)
            .await
    }

    /// Lists devices visible to a replicant's scan. The contract documents
    /// no response schema for this endpoint, so the raw body is returned
    /// as-is.
    pub async fn scan_devices(
        &self,
        replicant_code: &str,
        query: &ScanDevicesQuery,
    ) -> Result<RawResponse<serde_json::Value>, Error> {
        let base = format!(
            "v1/replicants/{}/scan/devices",
            encode_path_segment(replicant_code)
        );
        let path = with_query(
            &base,
            &[
                ("device_type", query.device_type.clone()),
                ("owner_replicant_code", query.owner_replicant_code.clone()),
                ("cursor", query.cursor.map(|value| value.to_string())),
                ("limit", query.limit.map(|value| value.to_string())),
            ],
        );
        self.client
            .execute(Method::GET, &path, true, RequestSafety::SafeRead)
            .await
    }

    /// Lists stars visible to a replicant, from its current location.
    pub async fn stars(
        &self,
        replicant_code: &str,
        query: &StarListQuery,
    ) -> Result<RawResponse<StarListResponse>, Error> {
        let base = format!(
            "v1/replicants/{}/stars",
            encode_path_segment(replicant_code)
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

    /// Fetches detail on a single star, from a replicant's perspective.
    pub async fn star(
        &self,
        replicant_code: &str,
        star_designation: &str,
    ) -> Result<RawResponse<StarDetailResponse>, Error> {
        let path = format!(
            "v1/replicants/{}/stars/{}",
            encode_path_segment(replicant_code),
            encode_path_segment(star_designation)
        );
        self.client
            .execute(Method::GET, &path, true, RequestSafety::SafeRead)
            .await
    }

    /// Teleports a replicant to a new matrix, incurring offline time.
    pub async fn teleport(
        &self,
        replicant_code: &str,
        request: &TeleportRequest,
    ) -> Result<RawResponse<TeleportResponse>, Error> {
        let path = format!(
            "v1/replicants/{}/teleport",
            encode_path_segment(replicant_code)
        );
        self.client
            .execute_json(Method::POST, &path, true, RequestSafety::Mutating, request)
            .await
    }

    /// Transfers a replicant to a new hosting device or account.
    pub async fn transfer(
        &self,
        replicant_code: &str,
        request: &TransferRequest,
    ) -> Result<RawResponse<TransferResponse>, Error> {
        let path = format!(
            "v1/replicants/{}/transfer",
            encode_path_segment(replicant_code)
        );
        self.client
            .execute_json(Method::POST, &path, true, RequestSafety::Mutating, request)
            .await
    }

    /// Cancels a replicant's current travel. The contract documents no
    /// response schema for this endpoint's success case, so the raw body is
    /// returned as-is.
    pub async fn cancel_travel(
        &self,
        replicant_code: &str,
    ) -> Result<RawResponse<serde_json::Value>, Error> {
        let path = format!(
            "v1/replicants/{}/travel",
            encode_path_segment(replicant_code)
        );
        self.client
            .execute(Method::DELETE, &path, true, RequestSafety::Mutating)
            .await
    }

    /// Begins replicant travel to a destination.
    pub async fn travel(
        &self,
        replicant_code: &str,
        request: &TravelRequest,
    ) -> Result<RawResponse<TravelResponse>, Error> {
        let path = format!(
            "v1/replicants/{}/travel",
            encode_path_segment(replicant_code)
        );
        self.client
            .execute_json(Method::POST, &path, true, RequestSafety::Mutating, request)
            .await
    }
}

#[cfg(test)]
mod print_request_tests {
    use super::PrintRequest;

    #[test]
    fn vessel_print_preserves_explicit_false_flatpack() {
        let request = PrintRequest {
            command: Some("print".to_owned()),
            device_type: Some("galactic_observatory".to_owned()),
            flatpack: Some(false),
            notify: None,
        };

        let payload = serde_json::to_value(request).expect("serialize vessel print request");
        assert_eq!(payload["command"], "print");
        assert_eq!(payload["device_type"], "galactic_observatory");
        assert_eq!(payload["flatpack"], false);
    }
}
