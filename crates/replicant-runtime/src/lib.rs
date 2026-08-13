//! Application services above [`replicant_client`].
//!
//! The managed SDK client remains the authority for Replicant Space API access,
//! durable game state, operations, and SSE-backed events. This crate owns
//! application configuration and orchestration services that coordinate that
//! client. Frontends call the runtime and remain responsible only for
//! presentation and user interaction.
//!
//! Workflow execution is intentionally outside this initial boundary.

use replicant_client::Client;

pub mod config;

/// Read-only application queries and reports.
pub mod reports {}

/// Finite application mutations and bounded operations.
pub mod actions {}

/// Application-level events and user-facing notifications.
pub mod notifications {}

/// Shared state owned by the application layer.
///
/// [`Client`] is already a cheaply cloneable shared handle, so cloning this
/// context keeps one managed client lifecycle without an additional `Arc`.
#[derive(Clone, Debug)]
pub struct ApplicationContext {
    client: Client,
    config: config::RuntimeConfig,
}

impl ApplicationContext {
    /// Creates an application context around one managed client.
    #[must_use]
    pub fn new(client: Client, config: config::RuntimeConfig) -> Self {
        Self { client, config }
    }

    /// Returns the managed Replicant Space authority.
    #[must_use]
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Returns application configuration that is not SDK state.
    #[must_use]
    pub fn config(&self) -> &config::RuntimeConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use replicant_client::managed::{ClientStatus, StartupPolicy};

    use super::*;

    #[tokio::test]
    async fn context_owns_a_ready_managed_client_and_runtime_config() {
        let client = Client::builder()
            .in_memory()
            .startup_policy(StartupPolicy::RestoreOnly)
            .start()
            .await
            .expect("start managed client");
        let context = ApplicationContext::new(client, config::RuntimeConfig::new("test"));

        assert_eq!(context.client().status(), ClientStatus::Ready);
        assert_eq!(context.config().profile(), "test");

        context.client().close().await.expect("close client");
    }
}
