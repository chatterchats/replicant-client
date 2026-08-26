//! Account registration, profile, and account-scoped resource operations
//! (`/v1/accounts*`).
//!
//! This client owns every `/v1/accounts/*` endpoint, including the ones that
//! also have a thematic sibling client elsewhere (`achievements`,
//! `reputation`, `simulations`) — matching the server's own route grouping
//! rather than re-deriving family boundaries from response schema names.

use reqwest::Method;
use serde::{Deserialize, Serialize};

use crate::error::Error;
use crate::raw::common::UpdateField;
use crate::raw::events::LocationEventListResponse;
use crate::raw::{Client, JsonObject, RawResponse, RequestSafety};

/// Request body for `POST /v1/accounts`.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct RegisterRequest {
    /// Email address for the account.
    pub email: String,
    /// Display name.
    pub name: String,
    /// IANA zone name or a fixed UTC±N offset.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,
}

/// A newly registered account, as returned inline (before email
/// verification).
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct AccountInfo {
    /// Email address on file.
    pub email: Option<String>,
    /// Whether the email address has been verified yet.
    pub email_verified: Option<bool>,
    /// Display name.
    pub name: Option<String>,
    /// Account status.
    pub status: Option<String>,
    /// Any other fields the server returns.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Response body for `POST /v1/accounts`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct RegisterResponse {
    /// The newly created account.
    pub account: Option<AccountInfo>,
    /// Human-readable confirmation message.
    pub message: Option<String>,
    /// A verification URL, if the server issued one inline.
    pub verify_url: Option<String>,
}

/// Request body for `POST /v1/accounts/recover`.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct RecoverRequest {
    /// Email address to recover.
    pub email: String,
}

/// Response body for `POST /v1/accounts/recover`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct RecoverResponse {
    /// Human-readable confirmation message.
    pub message: Option<String>,
}

/// A brief replicant reference, as returned by email verification.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct ReplicantBrief {
    /// Replicant code.
    pub replicant_code: Option<String>,
    /// Replicant display name.
    pub name: Option<String>,
}

/// Response body for `GET /v1/accounts/verify/{token}`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct VerificationResponse {
    /// The account's bearer API token. Only present on first verification;
    /// treat as a secret.
    pub api_token: Option<String>,
    /// Human-readable confirmation message.
    pub message: Option<String>,
    /// The account's first replicant, on initial registration.
    pub replicant: Option<ReplicantBrief>,
}

/// Per-account event notification settings.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct EventSettings {
    /// Minimum interval, in seconds, between AMI digest notifications.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ami_digest_interval: Option<i64>,
    /// Event names muted from notification.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub muted: Option<Vec<String>>,
}

/// Per-account message notification settings.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct MessageSettings {
    /// Whether to email a digest of new messages.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<bool>,
    /// BobNet channels subscribed to for message notifications.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subscribed: Option<Vec<String>>,
}

/// A brief summary of one of the account's replicants.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct AccountReplicantSummary {
    /// Replicant code.
    pub replicant_code: Option<String>,
    /// Replicant display name.
    pub name: Option<String>,
    /// Current location designation.
    pub current_location: Option<String>,
    /// Human-readable current location name.
    pub current_location_name: Option<String>,
    /// Current star designation.
    pub current_star: Option<String>,
    /// Human-readable current star name.
    pub current_star_name: Option<String>,
    /// Number of devices owned.
    pub device_count: Option<i64>,
    /// Total experience points earned.
    pub experience_points: Option<i64>,
    /// The device this replicant is currently hosted on, if any.
    pub hosted_device_code: Option<String>,
    /// Creation timestamp, RFC3339.
    pub created_at: Option<String>,
}

/// Response body for `GET /v1/accounts/me`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct AccountMeResponse {
    /// Email address on file.
    pub email: Option<String>,
    /// Whether the email address has been verified.
    pub email_verified: Option<bool>,
    /// Display name.
    pub name: Option<String>,
    /// Account status.
    pub status: Option<String>,
    /// IANA zone name or fixed UTC±N offset.
    pub timezone: Option<String>,
    /// Total experience points across all replicants.
    pub experience_points_total: Option<i64>,
    /// Number of unread messages.
    pub unread_message_count: Option<i64>,
    /// Cross-account replicant cooperation preference.
    pub replicant_cooperation: Option<String>,
    /// Subscribed BobNet channels.
    #[serde(default)]
    pub bobnet_channels: Vec<String>,
    /// Event notification settings.
    pub events: Option<EventSettings>,
    /// Message notification settings.
    pub messages: Option<MessageSettings>,
    /// Legacy open-shaped message notification settings.
    #[serde(default)]
    pub message_notify: JsonObject,
    /// This account's replicants.
    #[serde(default)]
    pub replicants: Vec<AccountReplicantSummary>,
    /// Account creation timestamp, RFC3339.
    pub created_at: Option<String>,
}

/// Request body for `PATCH /v1/accounts/me`. Every field is a tri-state
/// [`UpdateField`]: omit to leave unchanged, or set explicitly (including to
/// `null`, via [`UpdateField::Null`]) to change it.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct AccountUpdateRequest {
    /// New display name.
    #[serde(default, skip_serializing_if = "UpdateField::is_omitted")]
    pub name: UpdateField<String>,
    /// New email address (triggers re-verification).
    #[serde(default, skip_serializing_if = "UpdateField::is_omitted")]
    pub email: UpdateField<String>,
    /// New IANA zone name or fixed UTC±N offset.
    #[serde(default, skip_serializing_if = "UpdateField::is_omitted")]
    pub timezone: UpdateField<String>,
    /// New cross-account replicant cooperation preference.
    #[serde(default, skip_serializing_if = "UpdateField::is_omitted")]
    pub replicant_cooperation: UpdateField<String>,
    /// New subscribed BobNet channel list.
    #[serde(default, skip_serializing_if = "UpdateField::is_omitted")]
    pub bobnet_channels: UpdateField<Vec<String>>,
    /// New event notification settings.
    #[serde(default, skip_serializing_if = "UpdateField::is_omitted")]
    pub events: UpdateField<EventSettings>,
    /// New message notification settings.
    #[serde(default, skip_serializing_if = "UpdateField::is_omitted")]
    pub messages: UpdateField<MessageSettings>,
    /// Legacy open-shaped message notification settings.
    #[serde(default, skip_serializing_if = "UpdateField::is_omitted")]
    pub message_notify: UpdateField<JsonObject>,
}

/// Response body for `PATCH /v1/accounts/me`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct AccountUpdateResponse {
    /// Account status.
    pub status: Option<String>,
    /// Display name after the update.
    pub name: Option<String>,
    /// IANA zone name or fixed UTC±N offset after the update.
    pub timezone: Option<String>,
    /// Cross-account replicant cooperation preference after the update.
    pub replicant_cooperation: Option<String>,
    /// Subscribed BobNet channels after the update.
    #[serde(default)]
    pub bobnet_channels: Vec<String>,
    /// Event notification settings after the update.
    pub events: Option<EventSettings>,
    /// Message notification settings after the update.
    pub messages: Option<MessageSettings>,
    /// Legacy open-shaped message notification settings after the update.
    #[serde(default)]
    pub message_notify: JsonObject,
}

/// Response body for `DELETE /v1/accounts/me`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct AccountWipeRequestedResponse {
    /// Server confirmation that the destructive wipe request was accepted.
    pub message: Option<String>,
}

/// One achievement earned by the authenticated account.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct AccountAchievement {
    /// Achievement key.
    pub achievement_key: Option<String>,
    /// Display title.
    pub title: Option<String>,
    /// Display description.
    pub description: Option<String>,
    /// Achievement category.
    pub category: Option<String>,
    /// XP reward for earning this achievement.
    pub xp_reward: Option<i64>,
    /// When this account earned it, RFC3339.
    pub achieved_at: Option<String>,
}

/// Response body for `GET /v1/accounts/achievements`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct AccountAchievementListResponse {
    /// Achievements this account has earned.
    #[serde(default)]
    pub achievements: Vec<AccountAchievement>,
}

/// Query parameters for `GET /v1/accounts/events`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AccountEventsQuery {
    /// Filter by event status, e.g. `"active"` or `"completed"`.
    pub status: Option<String>,
    /// Opaque integer cursor from a previous page's `next_cursor`.
    pub cursor: Option<i64>,
    /// Maximum number of events to return.
    pub limit: Option<i64>,
}

/// Typed client for `/v1/accounts*`.
#[derive(Clone, Debug)]
pub struct AccountsClient {
    client: Client,
}

impl AccountsClient {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    /// Registers a new account. Unauthenticated: there is no token yet.
    pub async fn register(
        &self,
        request: &RegisterRequest,
    ) -> Result<RawResponse<RegisterResponse>, Error> {
        self.client
            .execute_json(
                Method::POST,
                "v1/accounts",
                false,
                RequestSafety::Mutating,
                request,
            )
            .await
    }

    /// Requests account recovery (token rotation) by email. Unauthenticated.
    pub async fn recover(
        &self,
        request: &RecoverRequest,
    ) -> Result<RawResponse<RecoverResponse>, Error> {
        self.client
            .execute_json(
                Method::POST,
                "v1/accounts/recover",
                false,
                RequestSafety::Mutating,
                request,
            )
            .await
    }

    /// Verifies an email address (or rotates a token) using the token from
    /// the verification email or link. Unauthenticated: the path token is
    /// itself the credential.
    pub async fn verify(&self, token: &str) -> Result<RawResponse<VerificationResponse>, Error> {
        let path = format!(
            "v1/accounts/verify/{}",
            crate::raw::common::encode_path_segment(token)
        );
        self.client
            .execute(Method::GET, &path, false, RequestSafety::SafeRead)
            .await
    }

    /// Fetches the authenticated account's profile.
    pub async fn me(&self) -> Result<RawResponse<AccountMeResponse>, Error> {
        self.client
            .execute(Method::GET, "v1/accounts/me", true, RequestSafety::SafeRead)
            .await
    }

    /// Updates the authenticated account's profile. Every field is a
    /// tri-state [`UpdateField`]; only fields explicitly set are sent.
    pub async fn update(
        &self,
        request: &AccountUpdateRequest,
    ) -> Result<RawResponse<AccountUpdateResponse>, Error> {
        self.client
            .execute_json(
                Method::PATCH,
                "v1/accounts/me",
                true,
                RequestSafety::Mutating,
                request,
            )
            .await
    }

    /// Requests destructive, irreversible deletion of the authenticated
    /// account.
    pub async fn request_destructive_wipe(
        &self,
    ) -> Result<RawResponse<AccountWipeRequestedResponse>, Error> {
        self.client
            .execute(
                Method::DELETE,
                "v1/accounts/me",
                true,
                RequestSafety::Mutating,
            )
            .await
    }

    /// Lists achievements earned by the authenticated account.
    pub async fn achievements(&self) -> Result<RawResponse<AccountAchievementListResponse>, Error> {
        self.client
            .execute(
                Method::GET,
                "v1/accounts/achievements",
                true,
                RequestSafety::SafeRead,
            )
            .await
    }

    /// Lists location events discovered or completed by the authenticated
    /// account's replicants.
    pub async fn events(
        &self,
        query: &AccountEventsQuery,
    ) -> Result<RawResponse<LocationEventListResponse>, Error> {
        let path = crate::raw::common::with_query(
            "v1/accounts/events",
            &[
                ("status", query.status.clone()),
                ("cursor", query.cursor.map(|value| value.to_string())),
                ("limit", query.limit.map(|value| value.to_string())),
            ],
        );
        self.client
            .execute(Method::GET, &path, true, RequestSafety::SafeRead)
            .await
    }

    /// Fetches aggregated species reputation for the authenticated account.
    pub async fn reputation(
        &self,
    ) -> Result<RawResponse<crate::raw::reputation::AccountReputationResponse>, Error> {
        self.client
            .execute(
                Method::GET,
                "v1/accounts/reputation",
                true,
                RequestSafety::SafeRead,
            )
            .await
    }

    /// Lists the authenticated account's simulation run history.
    pub async fn simulations(
        &self,
    ) -> Result<RawResponse<crate::raw::simulations::SimulationHistoryResponse>, Error> {
        self.client
            .execute(
                Method::GET,
                "v1/accounts/simulations",
                true,
                RequestSafety::SafeRead,
            )
            .await
    }
}
