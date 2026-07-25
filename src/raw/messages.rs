//! Account-wide BobNet message inbox (`GET /v1/messages`,
//! `POST /v1/messages/read`).
//!
//! Sending a message as a replicant, and reading a device's BobNet message
//! log, are distinct operations that live on
//! [`crate::raw::replicants::ReplicantsClient`] and
//! [`crate::raw::bobnet::BobnetClient`] respectively — this client only
//! covers the account's own inbox.

use reqwest::Method;
use serde::{Deserialize, Serialize};

use crate::error::Error;
use crate::raw::common::with_query;
use crate::raw::{Client, RawResponse, RequestSafety};

/// One inbox message.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct Message {
    /// Message body.
    pub body: Option<String>,
    /// Message category, e.g. `"system"`, `"social"`.
    pub category: Option<String>,
    /// Creation timestamp, RFC3339.
    pub created_at: Option<String>,
    /// Message ID.
    pub id: Option<i64>,
    /// Whether this message has been read.
    pub is_read: Option<bool>,
    /// Message type.
    pub message_type: Option<String>,
    /// Message subcategory.
    pub subcategory: Option<String>,
    /// Display title.
    pub title: Option<String>,
}

/// Query parameters for `GET /v1/messages`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct MessageListQuery {
    /// Opaque integer cursor from a previous page's `next_cursor`.
    pub cursor: Option<i64>,
    /// Maximum number of messages to return.
    pub limit: Option<i64>,
    /// Return only the single latest message.
    pub latest: Option<bool>,
    /// Return only unread messages.
    pub unread_only: Option<bool>,
}

/// Response body for `GET /v1/messages`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct MessageListResponse {
    /// Messages on this page.
    #[serde(default)]
    pub messages: Vec<Message>,
    /// Cursor for the next page, or `None` if this is the last page.
    pub next_cursor: Option<i64>,
    /// Total unread message count across the whole inbox.
    pub unread_message_count: Option<i64>,
}

/// Request body for `POST /v1/messages/read`.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct MessagesReadRequest {
    /// Specific message IDs to mark read.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ids: Option<Vec<i64>>,
    /// Mark every message in the inbox read.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mark_all: Option<bool>,
}

/// Response body for `POST /v1/messages/read`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct MessagesReadResponse {
    /// Update status.
    pub status: Option<String>,
}

/// Typed client for `/v1/messages*`.
#[derive(Clone, Debug)]
pub struct MessagesClient {
    client: Client,
}

impl MessagesClient {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    /// Lists the authenticated account's inbox messages.
    pub async fn list(
        &self,
        query: &MessageListQuery,
    ) -> Result<RawResponse<MessageListResponse>, Error> {
        let path = with_query(
            "v1/messages",
            &[
                ("cursor", query.cursor.map(|value| value.to_string())),
                ("limit", query.limit.map(|value| value.to_string())),
                ("latest", query.latest.map(|value| value.to_string())),
                (
                    "unread_only",
                    query.unread_only.map(|value| value.to_string()),
                ),
            ],
        );
        self.client
            .execute(Method::GET, &path, true, RequestSafety::SafeRead)
            .await
    }

    /// Marks one or more inbox messages read.
    pub async fn mark_read(
        &self,
        request: &MessagesReadRequest,
    ) -> Result<RawResponse<MessagesReadResponse>, Error> {
        self.client
            .execute_json(
                Method::POST,
                "v1/messages/read",
                true,
                RequestSafety::Mutating,
                request,
            )
            .await
    }
}
