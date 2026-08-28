//! `replicant-client` is a durable, stateful Rust client for building
//! Replicant Space applications.
//!
//! The crate targets the verified Replicant Space `2.5.1` rendered and
//! OpenAPI contract. Contract provenance, operation classifications, and
//! release-specific schema decisions are recorded under `policy/`; the pinned
//! snapshot and its digests live in `Cargo.toml` under
//! `[package.metadata.replicant-space]`.
//!
//! [`Client`] is the managed entry point: its gateways return normalized domain
//! values and commit successful observations before returning. [`raw`] is the
//! explicit escape hatch for transport DTOs and metadata; it never hydrates,
//! persists, publishes, journals operations, or reconciles state.
//!
//! # Observability
//!
//! The client emits structured [`tracing`](https://docs.rs/tracing) events but
//! never installs a global subscriber. Applications choose their own
//! subscriber and filtering policy. Important targets include
//! `replicant_client::raw::http`, `replicant_client::sync`,
//! `replicant_client::events`, `replicant_client::locations`,
//! `replicant_client::galaxy`, `replicant_client::store`, and
//! `replicant_client::state`. Duration fields use milliseconds and secret
//! values, authorization headers, and request bodies are never recorded.

#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(feature = "raw")]
mod error;

#[cfg(feature = "raw")]
pub use error::{Error, ErrorDetails, Result};

/// Typed raw HTTP transport for the current (non-deprecated, non-admin)
/// Replicant Space contract.
///
/// Returns transport DTOs and response metadata only. The supported surface
/// combines the checked-in OpenAPI inventory with documented post-OpenAPI
/// operation deltas. Deprecated and administrative operations remain excluded.
#[cfg(feature = "raw")]
pub mod raw;

/// Raw SSE parsing and account-wide event log access.
#[cfg(feature = "events")]
pub mod events;

/// Normalized domain types and pure authority rules used by the managed
/// client.
#[cfg(feature = "managed")]
pub mod domain;

#[cfg(feature = "managed")]
/// Managed-client implementation details. Only normalized domain types are
/// public until the managed client is introduced.
pub mod managed {
    pub use crate::domain;

    mod ami;
    mod bobnet;
    mod client;
    mod events;
    mod galaxy;
    #[allow(missing_docs)] // Gateway implementation details are re-exported below.
    mod gateways;
    mod operation;
    mod refresh;
    mod simulations;
    mod state;
    mod store;
    mod sync;
    mod trading;
    mod travel;

    #[cfg(test)]
    async fn test_client_at(base_url: &str) -> client::Client {
        client::Client::builder()
            .authentication_token(crate::raw::SecretString::from("token".to_string()))
            .base_url(crate::raw::Url::parse(base_url).expect("mock URL"))
            .in_memory()
            .startup_policy(client::StartupPolicy::RestoreOnly)
            .start()
            .await
            .expect("restore-only client")
    }

    pub use ami::{
        FleetController, MiningController, MiningDirective, SurveyController, SurveyDirective,
        TransportController, TransportDirective,
    };
    pub use bobnet::{BobnetGateway, BobnetWatch, RelayHistoryQuery};
    pub use client::{
        Client, ClientBuilder, ClientDegradation, ClientStatus, EventStreamOptions, Readiness,
        ReadinessComponent, ReconciliationPolicy, StartupPolicy, default_data_directory,
        default_database_path, default_history_database_path,
    };
    pub use events::{
        EventCatchUpReport, EventHistoryQuery, EventTelemetrySample, EventTelemetrySink,
        EventWatch, EventsGateway,
    };
    pub use galaxy::{CatalogueReport, GalaxyGateway, ReplicantStarSyncReport};
    pub use gateways::{
        AccountGateway, AutofactoryPrintOptions, BlueprintsGateway, DeviceHandle, DeviceQuery,
        DeviceQueryChange, DeviceQuerySubscription, DeviceRefreshQuery, DeviceWatch,
        DevicesGateway, DirectoryGateway, InventoryGateway, LocationDiagnostic,
        LocationPredicateDiagnostic, LocationPredicateOutcome, LocationQuery,
        LocationQueryDiagnostics, ReplicantHandle, ReplicantQuery, ReplicantsGateway,
        TutorialsGateway,
    };
    pub use operation::{
        ConfirmAccountWipe, DynamicCommand, LocationEventsGateway, LocationHydration,
        LocationHydrationFailure, LocationHydrationReport, LocationsGateway, MessageInbox,
        MessagesGateway, Operation, OperationOutcome, OperationStatus, OperationWatch,
        OperationsGateway,
    };
    pub use refresh::{
        RefreshClient, RefreshDelta, RefreshMode, RefreshPhase, RefreshPhaseState,
        RefreshPhaseStatus, RefreshReadiness, RefreshRequest, RefreshRunId, RefreshRunState,
        RefreshRunStatus,
    };
    pub use simulations::{SimulationQuery, SimulationsGateway};
    pub use state::{GalaxyRevisionWatch, ManagedStateSnapshot, StateGateway, StateRevisionWatch};
    pub use sync::{
        SyncCancellation, SyncClient, SyncDiagnostic, SyncDomain, SyncFailure, SyncFailureKind,
        SyncPlan, SyncPlanError, SyncProgress, SyncReadiness, SyncReport,
    };
    pub use trading::{TradeControllerHandle, TradingGateway};
    pub use travel::{TravelBuilder, TravelPreview, TravelVia};
}

#[cfg(feature = "managed")]
pub use managed::{
    AccountGateway, AutofactoryPrintOptions, BlueprintsGateway, BobnetGateway, BobnetWatch,
    CatalogueReport, Client, ClientBuilder, ClientDegradation, ClientStatus, ConfirmAccountWipe,
    DeviceHandle, DeviceQuery, DeviceQueryChange, DeviceQuerySubscription, DeviceRefreshQuery,
    DeviceWatch, DevicesGateway, DirectoryGateway, DynamicCommand, EventCatchUpReport,
    EventHistoryQuery, EventStreamOptions, EventTelemetrySample, EventTelemetrySink, EventWatch,
    EventsGateway, FleetController, GalaxyGateway, GalaxyRevisionWatch, InventoryGateway,
    LocationDiagnostic, LocationEventsGateway, LocationHydration, LocationHydrationFailure,
    LocationHydrationReport, LocationPredicateDiagnostic, LocationPredicateOutcome, LocationQuery,
    LocationQueryDiagnostics, LocationsGateway, ManagedStateSnapshot, MessagesGateway,
    MiningController, MiningDirective, Operation, OperationOutcome, OperationStatus,
    OperationWatch, OperationsGateway, Readiness, ReadinessComponent, ReconciliationPolicy,
    RefreshClient, RefreshDelta, RefreshMode, RefreshPhase, RefreshPhaseState, RefreshPhaseStatus,
    RefreshReadiness, RefreshRequest, RefreshRunId, RefreshRunState, RefreshRunStatus,
    RelayHistoryQuery, ReplicantHandle, ReplicantQuery, ReplicantStarSyncReport, ReplicantsGateway,
    SimulationQuery, SimulationsGateway, StartupPolicy, StateGateway, StateRevisionWatch,
    SurveyController, SurveyDirective, SyncCancellation, SyncClient, SyncDiagnostic, SyncDomain,
    SyncFailure, SyncFailureKind, SyncPlan, SyncPlanError, SyncProgress, SyncReadiness, SyncReport,
    TradeControllerHandle, TradingGateway, TransportController, TransportDirective, TravelBuilder,
    TravelPreview, TravelVia, TutorialsGateway, default_data_directory, default_database_path,
    default_history_database_path,
};

#[cfg(feature = "raw")]
pub use raw::SecretString;

#[cfg(feature = "managed")]
pub use domain::{
    Account, AccountId, ActiveDeviceDirective, Atmosphere, Device, DeviceCommand, DeviceId,
    DeviceKey, DeviceRelationships, DeviceStatus, DeviceType, Event, EventId, Knowledge, LifeStage,
    Location, LocationId, LocationKey, LocationSurveyProgress, OperationId, Realm, Replicant,
    ReplicantId, ReplicantKey, SimulationId, Star, StarId, StarKnowledge, TradeId, TravelState,
    WorldKey,
};
