//! BobNet: the galactic channel-based comms layer.
//!
//! Per `reference/replicant-space/concepts/bobnet/index.md`, modern BobNet
//! delivery is the account event stream (`bobnet.new`) plus relay-device
//! history; webhook delivery is deprecated and deliberately unsupported
//! here. Channel/network discovery and relay history are volatile
//! (`policy/authority-matrix.json` classifies them `state_neutral`), so
//! this gateway reads them directly rather than durably caching them.

use crate::domain::EventName;
use crate::raw;
use crate::{Client, Event, Result};

use super::events::EventWatch;
use super::operation::{self, Operation};

/// Gateway returned by [`crate::Client::bobnet`].
#[derive(Clone, Debug)]
pub struct BobnetGateway {
    client: Client,
}

impl BobnetGateway {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    /// Lists distinct BobNet channels a relay-capable device has observed.
    pub async fn channels(&self, relay_code: &str) -> Result<raw::bobnet::DeviceChannelsResponse> {
        self.client.ensure_open()?;
        Ok(self
            .client
            .managed_raw()
            .bobnet()
            .channels(relay_code)
            .await?
            .value)
    }

    /// Starts building a relay history query for the given relay-capable
    /// device.
    #[must_use]
    pub fn history(&self, relay_code: impl Into<String>) -> RelayHistoryQuery {
        RelayHistoryQuery::new(self.client.clone(), relay_code.into())
    }

    /// Broadcasts a message from a replicant on a channel. Sending to a
    /// channel the replicant is not subscribed to auto-subscribes it.
    pub async fn send(
        &self,
        replicant_code: &str,
        channel: impl Into<String>,
        text: impl Into<String>,
    ) -> Result<Operation> {
        operation::replicant_message(
            &self.client,
            replicant_code,
            raw::replicants::ReplicantMessageRequest {
                channel: channel.into(),
                text: text.into(),
            },
        )
        .await
    }

    /// Subscribes to `bobnet.new` account events. Local-only: it never
    /// itself issues a network request.
    pub async fn watch(&self) -> Result<BobnetWatch> {
        Ok(BobnetWatch {
            events: self.client.events().watch().await?,
        })
    }
}

/// A local, deduplicated `bobnet.new` event stream.
pub struct BobnetWatch {
    events: EventWatch,
}

impl BobnetWatch {
    /// Returns every `bobnet.new` event published since the last call, if
    /// any are available now.
    pub fn try_next(&mut self) -> Result<Vec<Event>> {
        Ok(self
            .events
            .try_next()?
            .into_iter()
            .filter(|event| event.name == EventName::BobnetNew)
            .collect())
    }
}

/// Builds a bounded relay-history read for one relay-capable device.
#[derive(Clone, Debug)]
pub struct RelayHistoryQuery {
    client: Client,
    relay_code: String,
    query: raw::bobnet::DeviceMessagesQuery,
}

impl RelayHistoryQuery {
    fn new(client: Client, relay_code: String) -> Self {
        Self {
            client,
            relay_code,
            query: raw::bobnet::DeviceMessagesQuery::default(),
        }
    }

    /// Pages from this opaque cursor.
    #[must_use]
    pub fn cursor(mut self, cursor: i64) -> Self {
        self.query.cursor = Some(cursor);
        self
    }

    /// Includes NPC chatter. The server default is `true`.
    #[must_use]
    pub fn include_npcs(mut self, include: bool) -> Self {
        self.query.include_npcs = Some(include);
        self
    }

    /// Fetches the most recent `limit` messages.
    pub async fn latest(mut self, limit: i64) -> Result<raw::bobnet::DeviceMessagesResponse> {
        self.query.latest = Some(true);
        self.query.limit = Some(limit);
        self.run().await
    }

    /// Fetches one page using the configured cursor.
    pub async fn list(self) -> Result<raw::bobnet::DeviceMessagesResponse> {
        self.run().await
    }

    async fn run(self) -> Result<raw::bobnet::DeviceMessagesResponse> {
        self.client.ensure_open()?;
        Ok(self
            .client
            .managed_raw()
            .bobnet()
            .messages(&self.relay_code, &self.query)
            .await?
            .value)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path, query_param},
    };

    use super::*;
    use crate::domain::{Event, EventCategory, EventId};
    use crate::managed::client::StartupPolicy;
    use crate::raw::{SecretString, Url};

    async fn client_at(base_url: &str) -> Client {
        Client::builder()
            .authentication_token(SecretString::from("token".to_string()))
            .base_url(Url::parse(base_url).expect("mock URL"))
            .in_memory()
            .startup_policy(StartupPolicy::RestoreOnly)
            .start()
            .await
            .expect("restore-only client")
    }

    #[tokio::test]
    async fn history_latest_hits_the_relay_message_log_not_a_webhook_endpoint() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/devices/RELAY1/messages"))
            .and(query_param("latest", "true"))
            .and(query_param("limit", "20"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "messages": [], "next_cursor": null
            })))
            .expect(1)
            .mount(&server)
            .await;
        let client = client_at(&server.uri()).await;

        client
            .bobnet()
            .history("RELAY1")
            .latest(20)
            .await
            .expect("relay history");

        server.verify().await;
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn watch_surfaces_only_bobnet_new_events() {
        let client = client_at(&MockServer::start().await.uri()).await;
        let mut watch = client.bobnet().watch().await.expect("watch");

        let bobnet_event = Event {
            id: EventId::new("1-0"),
            realm: None,
            name: EventName::BobnetNew,
            category: EventCategory::from("bobnet"),
            device: None,
            replicant: None,
            location: None,
            star: None,
            occurred_at: "2026-07-25T00:00:00Z".into(),
            payload: BTreeMap::new(),
        };
        let device_event = Event {
            id: EventId::new("2-0"),
            realm: None,
            name: EventName::from("device.attached"),
            category: EventCategory::from("device"),
            device: None,
            replicant: None,
            location: None,
            star: None,
            occurred_at: "2026-07-25T00:00:00Z".into(),
            payload: BTreeMap::new(),
        };
        client
            .managed_state()
            .apply_event(&bobnet_event, "1-0")
            .expect("apply bobnet event");
        client.managed_events().notify(bobnet_event);
        client
            .managed_state()
            .apply_event(&device_event, "2-0")
            .expect("apply device event");
        client.managed_events().notify(device_event);

        let observed = watch.try_next().expect("watch");
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].id.as_str(), "1-0");

        client.close().await.expect("close");
    }
}
