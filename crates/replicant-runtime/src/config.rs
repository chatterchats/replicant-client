//! Application and managed-client startup configuration.

use std::{
    env,
    error::Error as StdError,
    fmt, fs, io,
    path::{Path, PathBuf},
    sync::Arc,
};

use replicant_client::{
    SecretString, StartupPolicy, managed::EventTelemetrySink, raw::ApiTelemetrySink,
};
use replicant_protocol::ApiTokenSource;

/// Environment variable containing the Replicant Space API token.
pub const API_TOKEN_ENV: &str = "RS_API_TOKEN";

/// Environment variable naming a file containing the Replicant Space API token.
pub const API_TOKEN_FILE_ENV: &str = "RS_API_TOKEN_FILE";

/// Environment variable holding the `tracing` log filter directive.
pub const LOG_FILTER_ENV: &str = "RUST_LOG";

/// Default log filter directive used when `RUST_LOG` is unset.
pub const DEFAULT_LOG_FILTER: &str = "info";

/// Returns the default workflow/runtime SQLite database path.
#[must_use]
pub fn default_runtime_database_path() -> PathBuf {
    replicant_client::default_data_directory().join("replicant-runtime.sqlite")
}

/// Reports how the API token is currently configured, without exposing its value.
#[must_use]
pub fn api_token_source() -> ApiTokenSource {
    api_token_source_from(|name| env::var(name), |path| Path::new(path).is_file())
}

fn api_token_source_from(
    lookup: impl Fn(&str) -> Result<String, env::VarError>,
    file_exists: impl Fn(&str) -> bool,
) -> ApiTokenSource {
    if lookup(API_TOKEN_ENV).is_ok_and(|token| !token.is_empty()) {
        ApiTokenSource::Environment
    } else if lookup(API_TOKEN_FILE_ENV).is_ok_and(|path| file_exists(&path)) {
        ApiTokenSource::SecretFile
    } else {
        ApiTokenSource::Unset
    }
}

/// Returns the effective `tracing` log filter directive for this process.
#[must_use]
pub fn log_filter_directive() -> String {
    log_filter_directive_from(|name| env::var(name))
}

fn log_filter_directive_from(lookup: impl Fn(&str) -> Result<String, env::VarError>) -> String {
    lookup(LOG_FILTER_ENV)
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_LOG_FILTER.to_owned())
}

/// Reports whether the current process appears to be running inside Docker.
#[must_use]
pub fn docker_environment_detected() -> bool {
    Path::new("/.dockerenv").is_file()
}

/// Managed-client startup configuration for one local application instance.
pub struct ManagedClientConfig {
    authentication_token: SecretString,
    database: PathBuf,
    startup_policy: StartupPolicy,
    api_telemetry_sink: Option<Arc<dyn ApiTelemetrySink>>,
    event_telemetry_sink: Option<Arc<dyn EventTelemetrySink>>,
}

pub(crate) struct ManagedClientParts {
    pub(crate) authentication_token: SecretString,
    pub(crate) database: PathBuf,
    pub(crate) startup_policy: StartupPolicy,
    pub(crate) api_telemetry_sink: Option<Arc<dyn ApiTelemetrySink>>,
    pub(crate) event_telemetry_sink: Option<Arc<dyn EventTelemetrySink>>,
}

impl ManagedClientConfig {
    /// Resolves the API token from the process environment and uses essential startup.
    pub fn from_env(database: impl Into<PathBuf>) -> Result<Self, MissingApiToken> {
        Self::from_sources(
            database,
            |name| env::var(name),
            |path| fs::read_to_string(path),
        )
    }

    #[cfg(test)]
    fn from_lookup(
        database: impl Into<PathBuf>,
        lookup: impl Fn(&str) -> Result<String, env::VarError>,
    ) -> Result<Self, MissingApiToken> {
        Self::from_sources(database, lookup, |_| Err(io::ErrorKind::NotFound.into()))
    }

    fn from_sources(
        database: impl Into<PathBuf>,
        lookup: impl Fn(&str) -> Result<String, env::VarError>,
        read: impl Fn(&Path) -> io::Result<String>,
    ) -> Result<Self, MissingApiToken> {
        let authentication_token = lookup(API_TOKEN_ENV)
            .ok()
            .filter(|token| !token.is_empty())
            .or_else(|| {
                let path = lookup(API_TOKEN_FILE_ENV).ok()?;
                read(Path::new(&path))
                    .ok()
                    .map(|token| token.trim().to_owned())
            })
            .filter(|token| !token.is_empty())
            .ok_or(MissingApiToken)?;
        Ok(Self {
            authentication_token: SecretString::from(authentication_token),
            database: database.into(),
            startup_policy: StartupPolicy::Essential,
            api_telemetry_sink: None,
            event_telemetry_sink: None,
        })
    }

    /// Overrides the managed client's startup synchronization policy.
    #[must_use]
    pub fn with_startup_policy(mut self, startup_policy: StartupPolicy) -> Self {
        self.startup_policy = startup_policy;
        self
    }

    /// Installs a non-blocking destination for per-attempt API telemetry.
    #[must_use]
    pub fn with_api_telemetry_sink(mut self, sink: Arc<dyn ApiTelemetrySink>) -> Self {
        self.api_telemetry_sink = Some(sink);
        self
    }

    /// Installs a non-blocking destination for managed event/SSE telemetry.
    #[must_use]
    pub fn with_event_telemetry_sink(mut self, sink: Arc<dyn EventTelemetrySink>) -> Self {
        self.event_telemetry_sink = Some(sink);
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

    pub(crate) fn into_parts(self) -> ManagedClientParts {
        ManagedClientParts {
            authentication_token: self.authentication_token,
            database: self.database,
            startup_policy: self.startup_policy,
            api_telemetry_sink: self.api_telemetry_sink,
            event_telemetry_sink: self.event_telemetry_sink,
        }
    }
}

impl fmt::Debug for ManagedClientConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ManagedClientConfig")
            .field("authentication_token", &"<redacted>")
            .field("database", &self.database)
            .field("startup_policy", &self.startup_policy)
            .field("api_telemetry", &self.api_telemetry_sink.is_some())
            .field("event_telemetry", &self.event_telemetry_sink.is_some())
            .finish()
    }
}

/// Error returned when the Replicant Space API token cannot be resolved.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MissingApiToken;

impl fmt::Display for MissingApiToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "neither {API_TOKEN_ENV} nor a readable {API_TOKEN_FILE_ENV} is set"
        )
    }
}

impl StdError for MissingApiToken {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_database_shares_the_application_data_directory() {
        assert_eq!(
            default_runtime_database_path().parent(),
            Some(replicant_client::default_data_directory().as_path()),
        );
    }

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

        assert_eq!(
            error.to_string(),
            "neither RS_API_TOKEN nor a readable RS_API_TOKEN_FILE is set"
        );
    }

    #[test]
    fn api_token_source_prefers_environment_over_file() {
        assert_eq!(
            api_token_source_from(|_| Ok("secret".to_owned()), |_| panic!("unused")),
            ApiTokenSource::Environment,
        );
    }

    #[test]
    fn api_token_source_falls_back_to_an_existing_secret_file() {
        assert_eq!(
            api_token_source_from(
                |name| match name {
                    API_TOKEN_FILE_ENV => Ok("/run/secrets/token".to_owned()),
                    _ => Err(env::VarError::NotPresent),
                },
                |_| true,
            ),
            ApiTokenSource::SecretFile,
        );
    }

    #[test]
    fn api_token_source_is_unset_without_a_resolvable_source() {
        assert_eq!(
            api_token_source_from(|_| Err(env::VarError::NotPresent), |_| false),
            ApiTokenSource::Unset,
        );
    }

    #[test]
    fn log_filter_directive_defaults_when_unset() {
        assert_eq!(
            log_filter_directive_from(|_| Err(env::VarError::NotPresent)),
            DEFAULT_LOG_FILTER,
        );
    }

    #[test]
    fn log_filter_directive_uses_the_environment_value() {
        assert_eq!(
            log_filter_directive_from(|_| Ok("debug,replicant_runtime=trace".to_owned())),
            "debug,replicant_runtime=trace",
        );
    }

    #[test]
    fn token_file_is_trimmed_and_environment_takes_precedence() {
        let file_config = ManagedClientConfig::from_sources(
            "state.sqlite",
            |name| match name {
                API_TOKEN_FILE_ENV => Ok("/run/secrets/rs_api_token".to_owned()),
                _ => Err(env::VarError::NotPresent),
            },
            |path| {
                assert_eq!(path, Path::new("/run/secrets/rs_api_token"));
                Ok("file-secret\n".to_owned())
            },
        )
        .expect("resolve token file");
        assert!(!format!("{file_config:?}").contains("file-secret"));

        let env_config = ManagedClientConfig::from_sources(
            "state.sqlite",
            |name| match name {
                API_TOKEN_ENV => Ok("environment-secret".to_owned()),
                API_TOKEN_FILE_ENV => Ok("unused".to_owned()),
                _ => Err(env::VarError::NotPresent),
            },
            |_| panic!("token file must not be read when RS_API_TOKEN is set"),
        )
        .expect("resolve environment token");
        assert!(!format!("{env_config:?}").contains("environment-secret"));
    }
}
