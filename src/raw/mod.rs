//! The raw, unmanaged Replicant Space HTTP client.
//!
//! `raw::Client` exposes the current non-deprecated, non-administrative
//! operations recorded by the OpenAPI baseline and the explicit rendered-doc
//! deltas under `policy/`. It returns transport DTOs and
//! [`crate::raw::ResponseMetadata`] only; it never hydrates, persists, publishes,
//! journals operations, or reconciles state against a local store. Those are
//! managed-client concerns built on top of this transport in a later phase.
//!
//! # Constructing a client
//!
//! ```no_run
//! use replicant_client::raw::{Client, SecretString};
//!
//! # async fn run() -> Result<(), replicant_client::Error> {
//! let client = Client::builder()
//!     .authentication_token(SecretString::from("token".to_string()))
//!     .build()?;
//! let response = client.accounts().me().await?;
//! println!("{:?}", response.value);
//! # Ok(())
//! # }
//! ```
//!
//! # Mutating calls are never retried automatically
//!
//! A failure on a state-changing (mutating) call — a device command, a trade,
//! a location action — may mean the request never reached the server, or
//! that it reached the server and only the response was lost in transit. The
//! raw client does not guess: it returns the failure immediately rather than
//! risk sending the same mutation twice. [`crate::Error::Transport`] on a
//! mutating call is definitionally ambiguous; see
//! [`crate::Error::is_ambiguous_transport_failure`]. Reconciling that
//! ambiguity into a definite outcome is a durable-operation concern handled
//! by the managed client in a later phase, not by this transport.
//!
//! Safe reads (`GET`/`HEAD`) may be retried with bounded backoff, honoring
//! any server `Retry-After` or rate-limit reset.

mod client;
pub mod common;
pub mod pagination;
pub mod rate_limit;
pub mod vocab;

pub mod accounts;
pub mod achievements;
pub mod blueprints;
pub mod bobnet;
pub mod devices;
pub mod events;
pub mod feedback;
pub mod galaxy;
pub mod inventory;
pub mod leaderboards;
pub mod location_events;
pub mod locations;
pub mod messages;
pub mod replicants;
pub mod reputation;
pub mod simulations;
pub mod species;
pub mod trading;
pub mod tutorials;

pub use client::{
    Client, ClientBuilder, ClientConfig, MutableTokenProvider, RawResponse, RequestId,
    RequestSafety, ResponseMetadata, RetryPolicy, SecretString, StatusCode, TlsBackend,
    TokenProvider, Url,
};
pub use common::{JsonObject, Position};

impl Client {
    /// Account registration, profile, and account-scoped resource operations.
    #[must_use]
    pub fn accounts(&self) -> accounts::AccountsClient {
        accounts::AccountsClient::new(self.clone())
    }

    /// Public and authenticated-account achievement operations.
    #[must_use]
    pub fn achievements(&self) -> achievements::AchievementsClient {
        achievements::AchievementsClient::new(self.clone())
    }

    /// Unlocked account blueprint catalogue operations.
    #[must_use]
    pub fn blueprints(&self) -> blueprints::BlueprintsClient {
        blueprints::BlueprintsClient::new(self.clone())
    }

    /// BobNet channel discovery and relay history operations.
    #[must_use]
    pub fn bobnet(&self) -> bobnet::BobnetClient {
        bobnet::BobnetClient::new(self.clone())
    }

    /// Device directory, status, configuration, and command operations.
    #[must_use]
    pub fn devices(&self) -> devices::DevicesClient {
        devices::DevicesClient::new(self.clone())
    }

    /// Feedback submission.
    #[must_use]
    pub fn feedback(&self) -> feedback::FeedbackClient {
        feedback::FeedbackClient::new(self.clone())
    }

    /// The public complete stellar catalogue and star detail lookups.
    #[must_use]
    pub fn galaxy(&self) -> galaxy::GalaxyClient {
        galaxy::GalaxyClient::new(self.clone())
    }

    /// Account inventory across all systems where the account has devices.
    #[must_use]
    pub fn inventory(&self) -> inventory::InventoryClient {
        inventory::InventoryClient::new(self.clone())
    }

    /// Public leaderboard index and board lookups.
    #[must_use]
    pub fn leaderboards(&self) -> leaderboards::LeaderboardsClient {
        leaderboards::LeaderboardsClient::new(self.clone())
    }

    /// Location detail, contribution, and location-event operations.
    #[must_use]
    pub fn location_events(&self) -> location_events::LocationEventsClient {
        location_events::LocationEventsClient::new(self.clone())
    }

    /// Location map and detail operations.
    #[must_use]
    pub fn locations(&self) -> locations::LocationsClient {
        locations::LocationsClient::new(self.clone())
    }

    /// Device message inbox and relay operations.
    #[must_use]
    pub fn messages(&self) -> messages::MessagesClient {
        messages::MessagesClient::new(self.clone())
    }

    /// Replicant directory, status, and convenience-command operations.
    #[must_use]
    pub fn replicants(&self) -> replicants::ReplicantsClient {
        replicants::ReplicantsClient::new(self.clone())
    }

    /// Per-replicant species reputation lookups. Account-level reputation
    /// lives on [`Self::accounts`].
    #[must_use]
    pub fn reputation(&self) -> reputation::ReputationClient {
        reputation::ReputationClient::new(self.clone())
    }

    /// Device-hosted simulation scenario, entry, and active-run operations.
    #[must_use]
    pub fn simulations(&self) -> simulations::SimulationsClient {
        simulations::SimulationsClient::new(self.clone())
    }

    /// Public civilisation species directory.
    #[must_use]
    pub fn species(&self) -> species::SpeciesClient {
        species::SpeciesClient::new(self.clone())
    }

    /// Tutorial progress for the authenticated account.
    #[must_use]
    pub fn tutorials(&self) -> tutorials::TutorialsClient {
        tutorials::TutorialsClient::new(self.clone())
    }

    /// Device-hosted and replicant-visible trade operations.
    #[must_use]
    pub fn trading(&self) -> trading::TradingClient {
        trading::TradingClient::new(self.clone())
    }
}
