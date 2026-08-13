//! Application and managed-client startup configuration.

use std::{
    env,
    error::Error as StdError,
    fmt,
    path::{Path, PathBuf},
};

use replicant_client::{SecretString, StartupPolicy};

/// Environment variable containing the Replicant Space API token.
pub const API_TOKEN_ENV: &str = "RS_API_TOKEN";

/// Managed-client startup configuration for one local application instance.
pub struct ManagedClientConfig {
    authentication_token: SecretString,
    database: PathBuf,
    startup_policy: StartupPolicy,
}

impl ManagedClientConfig {
    /// Resolves the API token from the process environment and uses essential startup.
    pub fn from_env(database: impl Into<PathBuf>) -> Result<Self, MissingApiToken> {
        Self::from_lookup(database, |name| env::var(name))
    }

    fn from_lookup(
        database: impl Into<PathBuf>,
        lookup: impl FnOnce(&str) -> Result<String, env::VarError>,
    ) -> Result<Self, MissingApiToken> {
        let authentication_token = lookup(API_TOKEN_ENV).map_err(|_| MissingApiToken)?;
        Ok(Self {
            authentication_token: SecretString::from(authentication_token),
            database: database.into(),
            startup_policy: StartupPolicy::Essential,
        })
    }

    /// Overrides the managed client's startup synchronization policy.
    #[must_use]
    pub fn with_startup_policy(mut self, startup_policy: StartupPolicy) -> Self {
        self.startup_policy = startup_policy;
        self
    }

    /// Returns the managed SQLite database path.
    #[must_use]
    pub fn database(&self) -> &Path {
        &self.database
    }

    /// Returns the managed startup synchronization policy.
    #[must_use]
    pub fn startup_policy(&self) -> StartupPolicy {
        self.startup_policy
    }

    pub(crate) fn into_parts(self) -> (SecretString, PathBuf, StartupPolicy) {
        (
            self.authentication_token,
            self.database,
            self.startup_policy,
        )
    }
}

impl fmt::Debug for ManagedClientConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ManagedClientConfig")
            .field("authentication_token", &"<redacted>")
            .field("database", &self.database)
            .field("startup_policy", &self.startup_policy)
            .finish()
    }
}

/// Error returned when the Replicant Space API token cannot be resolved.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MissingApiToken;

impl fmt::Display for MissingApiToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{API_TOKEN_ENV} is not set")
    }
}

impl StdError for MissingApiToken {}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn managed_client_defaults_resolve_without_live_api() {
        let config = ManagedClientConfig::from_lookup("state.sqlite", |name| {
            assert_eq!(name, API_TOKEN_ENV);
            Ok("test-secret".to_owned())
        })
        .expect("resolve config");

        assert_eq!(config.database(), Path::new("state.sqlite"));
        assert_eq!(config.startup_policy(), StartupPolicy::Essential);
        assert!(!format!("{config:?}").contains("test-secret"));
    }

    #[test]
    fn managed_client_startup_policy_is_explicitly_overridable() {
        let config = ManagedClientConfig::from_lookup("state.sqlite", |_| Ok("secret".to_owned()))
            .expect("resolve config")
            .with_startup_policy(StartupPolicy::RestoreOnly);

        assert_eq!(config.startup_policy(), StartupPolicy::RestoreOnly);
    }

    #[test]
    fn missing_api_token_has_a_non_secret_error() {
        let error =
            ManagedClientConfig::from_lookup("state.sqlite", |_| Err(env::VarError::NotPresent))
                .expect_err("missing token should fail");

        assert_eq!(error.to_string(), "RS_API_TOKEN is not set");
    }
}
