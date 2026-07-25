//! Normalized game snapshots plus the evidence rules that govern them.
//!
//! This module deliberately contains no persistence, networking, or raw DTOs
//! in its snapshot types. Endpoint DTOs are accepted only by [`adapters`].

#![allow(missing_docs)]

mod adapters;
mod ids;
mod merge;
mod model;
mod observation;
mod query;
mod vocab;

pub use adapters::*;
pub use ids::*;
pub use merge::*;
pub use model::*;
pub use observation::*;
pub use query::*;
pub use vocab::*;
