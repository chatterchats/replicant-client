//! Relay-capable device BobNet visibility (`GET /v1/devices/{device_code}/channels`,
//! `GET /v1/devices/{device_code}/messages`).
//!
//! Sending a message, and the account-wide inbox, live elsewhere
//! ([`crate::raw::replicants::ReplicantsClient::message`],
//! [`crate::raw::messages::MessagesClient`]); this client only covers what a
//! specific device can currently see on the network.

use reqwest::Method;
use serde::Deserialize;

use crate::error::Error;
use crate::raw::common::{encode_path_segment, with_query};
use crate::raw::{Client, RawResponse, RequestSafety};

/// One BobNet channel a relay-capable device has observed.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct ChannelItem {
    /// When this channel was last active, RFC3339.
    pub last_active: Option<String>,
    /// Channel name.
    pub name: Option<String>,
}

/// Response body for `GET /v1/devices/{device_code}/channels`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct DeviceChannelsResponse {
    /// Distinct channels seen by this device.
    #[serde(default)]
    pub channels: Vec<ChannelItem>,
}

/// One BobNet message visible from a relay-capable device.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct BobnetMessageItem {
    /// Channel the message was sent on.
    pub channel: Option<String>,
    /// The sending replicant's current star, if known.
    pub current_star: Option<String>,
    /// Message ID.
    pub id: Option<i64>,
    /// Message body.
    pub message: Option<String>,
    /// Sending replicant code when supplied. NPC senders may also have codes.
    pub replicant_code: Option<String>,
    /// Sending replicant display name when supplied. NPC senders may also have names.
    pub replicant_name: Option<String>,
    /// When the message was sent, RFC3339.
    pub time: Option<String>,
}

/// Query parameters for `GET /v1/devices/{device_code}/messages`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DeviceMessagesQuery {
    /// Opaque integer cursor from a previous page's `next_cursor`.
    pub cursor: Option<i64>,
    /// Maximum number of messages to return.
    pub limit: Option<i64>,
    /// Return only the single latest message.
    pub latest: Option<bool>,
    /// Include messages sent by non-player characters.
    pub include_npcs: Option<bool>,
}

/// Response body for `GET /v1/devices/{device_code}/messages`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct DeviceMessagesResponse {
    /// Messages on this page.
    #[serde(default)]
    pub messages: Vec<BobnetMessageItem>,
    /// Cursor for the next page, or `None` if this is the last page.
    pub next_cursor: Option<i64>,
    /// Messages returned on this page.
    pub total: Option<i64>,
    /// Total messages visible from this device.
    pub total_messages: Option<i64>,
}

/// Typed client for relay-capable device BobNet visibility.
#[derive(Clone, Debug)]
pub struct BobnetClient {
    client: Client,
}

impl BobnetClient {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    /// Lists distinct BobNet channels seen by a relay-capable device.
    pub async fn channels(
        &self,
        device_code: &str,
    ) -> Result<RawResponse<DeviceChannelsResponse>, Error> {
        let path = format!("v1/devices/{}/channels", encode_path_segment(device_code));
        self.client
            .execute(Method::GET, &path, true, RequestSafety::SafeRead)
            .await
    }

    /// Lists recent BobNet messages visible from a relay-capable device.
    pub async fn messages(
        &self,
        device_code: &str,
        query: &DeviceMessagesQuery,
    ) -> Result<RawResponse<DeviceMessagesResponse>, Error> {
        let base = format!("v1/devices/{}/messages", encode_path_segment(device_code));
        let path = with_query(
            &base,
            &[
                ("cursor", query.cursor.map(|value| value.to_string())),
                ("limit", query.limit.map(|value| value.to_string())),
                ("latest", query.latest.map(|value| value.to_string())),
                (
                    "include_npcs",
                    query.include_npcs.map(|value| value.to_string()),
                ),
            ],
        );
        self.client
            .execute(Method::GET, &path, true, RequestSafety::SafeRead)
            .await
    }
}
