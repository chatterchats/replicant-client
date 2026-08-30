//! Normalized game snapshots plus the evidence rules that govern them.
//!
//! This module deliberately contains no persistence, networking, or raw DTOs
//! in its snapshot types. Endpoint DTOs are accepted only by internal adapters.

#[allow(missing_docs)] // Internal normalization helpers are re-exported through this facade.
mod adapters;
#[allow(missing_docs)] // Identifier implementation details are re-exported through this facade.
mod ids;
#[allow(missing_docs)] // Pure merge implementation details are re-exported through this facade.
mod merge;
#[allow(missing_docs)] // Snapshot implementation details are re-exported through this facade.
mod model;
#[allow(missing_docs)] // Observation implementation details are re-exported through this facade.
mod observation;
#[allow(missing_docs)] // Query implementation details are re-exported through this facade.
mod query;
#[allow(missing_docs)] // Pure travel planning details are re-exported through this facade.
mod travel;
#[allow(missing_docs)] // Vocabulary implementation details are re-exported through this facade.
mod vocab;

pub use adapters::*;
pub use ids::*;
pub use merge::*;
pub use model::*;
pub use observation::*;
pub use query::*;
pub use travel::*;
pub use vocab::*;
