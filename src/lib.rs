//! `replicant-client` is a durable, stateful Rust client for building
//! Replicant Space applications.
//!
//! The crate targets the Replicant Space `2.3.1` contract. The corrected
//! contract corpus is checked in under `reference/replicant-space/`, and the
//! machine-readable operation inventory it was derived from lives under
//! `policy/`.
//!
//! [`Client`] is the managed entry point: its gateways return normalized domain
//! values and commit successful observations before returning. [`raw`] is the
//! explicit escape hatch for transport DTOs and metadata; it never hydrates,
//! persists, publishes, journals operations, or reconciles state.

#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(feature = "raw")]
mod error;

#[cfg(feature = "raw")]
pub use error::{Error, ErrorDetails, Result};

/// Typed raw HTTP transport for the current (non-deprecated, non-admin)
/// Replicant Space contract.
///
/// Returns transport DTOs and response metadata only. Exposes only the 77
/// current, supported operations; the 5 deprecated and 2 administrative
/// operations excluded by `policy/operations.json` have no corresponding
/// method here and are not callable.
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

    mod client;
    mod events;
    mod gateways;
    mod operation;
    mod state;
    mod store;
    mod sync;

    pub use client::{
        Client, ClientBuilder, ClientDegradation, ClientStatus, EventStreamOptions,
        ReconciliationPolicy, StartupPolicy,
    };
    pub use events::{EventWatch, EventsGateway};
    pub use gateways::{
        AccountGateway, DeviceHandle, DeviceQuery, DeviceWatch, DevicesGateway, DirectoryGateway,
        ReplicantHandle, ReplicantsGateway,
    };
    pub use operation::{
        ConfirmAccountWipe, DynamicCommand, LocationEventsGateway, LocationsGateway,
        MessagesGateway, Operation, OperationOutcome, OperationStatus, OperationWatch,
        OperationsGateway,
    };
    pub use sync::{
        SyncCancellation, SyncClient, SyncDiagnostic, SyncDomain, SyncPlan, SyncPlanError,
        SyncProgress, SyncReadiness, SyncReport,
    };
}

#[cfg(feature = "managed")]
pub use managed::{
    AccountGateway, Client, ClientBuilder, ClientDegradation, ClientStatus, ConfirmAccountWipe,
    DeviceHandle, DeviceQuery, DeviceWatch, DevicesGateway, DirectoryGateway, DynamicCommand,
    EventStreamOptions, EventWatch, EventsGateway, LocationEventsGateway, LocationsGateway,
    MessagesGateway, Operation, OperationOutcome, OperationStatus, OperationWatch,
    OperationsGateway, ReconciliationPolicy, ReplicantHandle, ReplicantsGateway, StartupPolicy,
    SyncCancellation, SyncClient, SyncDiagnostic, SyncDomain, SyncPlan, SyncPlanError,
    SyncProgress, SyncReadiness, SyncReport,
};

#[cfg(feature = "raw")]
pub use raw::SecretString;

#[cfg(feature = "managed")]
pub use domain::{
    Account, AccountId, Device, DeviceCommand, DeviceId, DeviceKey, DeviceStatus, DeviceType,
    Event, EventId, Location, LocationId, LocationKey, OperationId, Realm, Replicant, ReplicantId,
    ReplicantKey, SimulationId, StarId, TradeId, WorldKey,
};
