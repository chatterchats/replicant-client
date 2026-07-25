//! `replicant-client` is a durable, stateful Rust client for building
//! Replicant Space applications.
//!
//! The crate targets the Replicant Space `2.3.1` contract. The corrected
//! contract corpus is checked in under `reference/replicant-space/`, and the
//! machine-readable operation inventory it was derived from lives under
//! `policy/`.
//!
//! Phase 2 implements [`raw`]: an unmanaged transport layer over the current,
//! non-deprecated, non-admin contract. It returns transport DTOs and response
//! metadata only — it never hydrates, persists, publishes, journals
//! operations, or reconciles state. Those are managed-client concerns built
//! on top of this transport in a later phase.

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

/// Durable local state, synchronization, durable operations, and the
/// managed `Client` entry point.
///
/// Not yet implemented; this module is a compilation placeholder for the
/// `managed` feature.
#[cfg(feature = "managed")]
pub mod managed {}
