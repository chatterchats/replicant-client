//! Application and profile configuration.
//!
//! SDK credentials and managed game state do not belong in these types.

/// Non-secret configuration for one runtime instance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeConfig {
    profile: String,
}

impl RuntimeConfig {
    /// Creates configuration for a named application profile.
    #[must_use]
    pub fn new(profile: impl Into<String>) -> Self {
        Self {
            profile: profile.into(),
        }
    }

    /// Returns the application profile name.
    #[must_use]
    pub fn profile(&self) -> &str {
        &self.profile
    }
}
