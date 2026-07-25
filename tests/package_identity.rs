//! Asserts the locked package identity and feature graph from
//! `docs/implementation/rewrite-guide.md` section 4.

#[test]
fn package_identity_is_locked() {
    assert_eq!(env!("CARGO_PKG_NAME"), "replicant-client");
    assert_eq!(env!("CARGO_PKG_VERSION"), "1.0.0");
    assert_eq!(env!("CARGO_PKG_VERSION_MAJOR"), "1");
    assert_eq!(env!("CARGO_PKG_VERSION_MINOR"), "0");
    assert_eq!(env!("CARGO_PKG_VERSION_PATCH"), "0");
}

// The imports below only compile if the crate's `[lib] name =
// "replicant_client"` and the feature implications in Cargo.toml
// (`events -> raw`, `managed -> events`) hold; a rename or a dropped
// implication fails the build rather than an assertion. They intentionally
// import nothing but the module path, so `#[allow(unused_imports)]` covers
// the otherwise-unused bindings.

#[test]
#[cfg(feature = "raw")]
#[allow(unused_imports)]
fn raw_module_is_reachable_as_replicant_client_raw() {
    use replicant_client::raw;
}

#[test]
#[cfg(feature = "events")]
#[allow(unused_imports)]
fn events_implies_raw() {
    #[cfg(not(feature = "raw"))]
    compile_error!("events must imply raw");

    use replicant_client::{events, raw};
}

#[test]
#[cfg(feature = "managed")]
#[allow(unused_imports)]
fn managed_implies_events_and_raw() {
    #[cfg(not(feature = "events"))]
    compile_error!("managed must imply events");
    #[cfg(not(feature = "raw"))]
    compile_error!("managed must imply raw (via events)");

    use replicant_client::{events, managed, raw};
}
