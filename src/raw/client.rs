//! The raw, unmanaged HTTP client and its shared request execution pipeline.
//!
//! [`Client`] returns transport DTOs and [`crate::raw::ResponseMetadata`] only. It never
//! hydrates, persists, publishes, journals operations, or reconciles state —
//! those are managed-client concerns built on top of this transport in a
//! later phase.

use std::{
    fmt,
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};

use reqwest::{
    Method,
    header::{AUTHORIZATION, HeaderMap, HeaderValue, USER_AGENT},
};
use secrecy::ExposeSecret;
use serde::{Serialize, de::DeserializeOwned};
use tracing::{debug, warn};
use uuid::Uuid;

pub use reqwest::{StatusCode, Url};
pub use secrecy::SecretString;

use crate::error::{Error, ErrorDetails};
use crate::raw::rate_limit::{
    RateLimitBucket, RateLimitCoordinator, RateLimitReset, RateLimitSnapshot, RequestPriority,
    RetryAfter, bucket_for,
};
use crate::raw::telemetry::{
    ApiAttemptOutcome, ApiAttemptTelemetry, ApiAttemptTimings, ApiRateLimitTelemetry,
    ApiTelemetrySink, duration_millis, normalize_route_key, now_unix_millis,
};

/// Opaque local request correlation identifier.
///
/// Distinct from any server-supplied request ID; always available even when
/// the server never responds.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RequestId(String);

impl RequestId {
    fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    /// Returns the opaque local correlation identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Response metadata retained independently of the decoded payload.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ResponseMetadata {
    /// HTTP status code of the response.
    pub status: StatusCode,
    /// Server-supplied request correlation ID, if present.
    pub request_id: Option<String>,
    /// Locally generated request correlation ID.
    pub local_request_id: RequestId,
    /// Rate-limit information observed on this response, if any.
    pub rate_limit: Option<RateLimitSnapshot>,
}

/// A decoded response paired with HTTP metadata.
#[derive(Clone, Debug)]
pub struct RawResponse<T> {
    /// The decoded response payload.
    pub value: T,
    /// HTTP response metadata.
    pub metadata: ResponseMetadata,
}

/// Whether a request may be safely retried after a transient failure.
///
/// Only `SafeRead` requests are ever retried automatically. A `Mutating`
/// request that fails with a transport-level error (timeout, connection
/// reset, or similar) has an ambiguous outcome — it may or may not have
/// reached the server — so the raw client returns the failure immediately
/// rather than risk duplicating the effect.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestSafety {
    /// A read-only request (GET/HEAD). Safe to retry on transient failure.
    SafeRead,
    /// A state-changing request. Never retried automatically once sent.
    Mutating,
}

/// One fully described HTTP request attempt. Grouping these fields keeps the
/// executor API cohesive and avoids an error-prone positional argument list.
struct RequestAttempt<'a> {
    method: &'a Method,
    path: &'a str,
    authenticated: bool,
    request_id: &'a RequestId,
    bucket: RateLimitBucket,
    attempt: u32,
    body: Option<Vec<u8>>,
    response_body_limit: usize,
    rate_limit_wait: Duration,
    logical_started: Instant,
    retry_backoff: Duration,
    priority: RequestPriority,
    outbound_in_flight: u64,
}

/// Bounded-backoff retry policy applied to safe reads.
#[derive(Clone, Debug)]
pub struct RetryPolicy {
    /// Maximum number of retry attempts.
    pub max_retries: u32,
    /// Delay before the first retry.
    pub initial_backoff: Duration,
    /// Upper bound on backoff delay.
    pub max_backoff: Duration,
    /// Maximum random jitter added to each backoff delay.
    pub jitter: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_backoff: Duration::from_millis(200),
            max_backoff: Duration::from_secs(5),
            jitter: Duration::from_millis(100),
        }
    }
}

/// TLS implementation selected for clients built internally.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum TlsBackend {
    /// Let the enabled reqwest feature select its default backend.
    #[default]
    Automatic,
    /// Select rustls. Requires the crate's `rustls-tls` feature.
    Rustls,
    /// Select the platform-native TLS backend. Requires `native-tls`.
    Native,
}

/// Client configuration, validated by [`ClientBuilder`].
#[derive(Clone)]
pub struct ClientConfig {
    /// Base URL all requests are resolved against.
    pub base_url: Url,
    /// Timeout for establishing a connection.
    pub connect_timeout: Duration,
    /// Timeout for a complete ordinary request/response cycle. Long-lived SSE
    /// streams retain only [`Self::connect_timeout`].
    pub request_timeout: Duration,
    /// User-Agent header sent with every request.
    pub user_agent: String,
    /// Retry policy applied to safe-read requests.
    pub retry: RetryPolicy,
    /// TLS backend for internally built HTTP clients.
    pub tls_backend: TlsBackend,
    /// Maximum response body size accepted for ordinary endpoints, in bytes.
    pub max_response_body_bytes: usize,
    /// Maximum response body size accepted from the unpaginated global star
    /// catalogue (`GET /v1/stars`), in bytes.
    pub max_star_catalogue_response_body_bytes: usize,
    /// Send a generated `X-Request-ID` header with each request.
    pub send_request_id: bool,
    /// Emit sanitized `tracing` events for requests and retries.
    pub emit_tracing: bool,
}

impl fmt::Debug for ClientConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientConfig")
            .field("base_url", &self.base_url)
            .field("connect_timeout", &self.connect_timeout)
            .field("request_timeout", &self.request_timeout)
            .field("user_agent", &self.user_agent)
            .field("retry", &self.retry)
            .field("tls_backend", &self.tls_backend)
            .field("max_response_body_bytes", &self.max_response_body_bytes)
            .field(
                "max_star_catalogue_response_body_bytes",
                &self.max_star_catalogue_response_body_bytes,
            )
            .field("send_request_id", &self.send_request_id)
            .field("emit_tracing", &self.emit_tracing)
            .finish()
    }
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            base_url: Url::parse("https://api.replicant.space/").unwrap_or_else(|_| unreachable!()),
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            user_agent: format!("replicant-client/{}", env!("CARGO_PKG_VERSION")),
            retry: RetryPolicy::default(),
            tls_backend: TlsBackend::Automatic,
            max_response_body_bytes: 1024 * 1024,
            max_star_catalogue_response_body_bytes: 32 * 1024 * 1024,
            send_request_id: true,
            emit_tracing: true,
        }
    }
}

/// Source of bearer tokens. Implementations must never expose token text via
/// diagnostics (`Debug`, logs, or panics).
pub trait TokenProvider: Send + Sync {
    /// Returns the current bearer token, if one is available.
    fn token(&self) -> Option<SecretString>;
}

/// Mutable in-memory bearer-token provider.
#[derive(Default)]
pub struct MutableTokenProvider {
    token: RwLock<Option<SecretString>>,
}

impl MutableTokenProvider {
    /// Replaces the current token.
    pub fn replace(&self, token: Option<SecretString>) {
        if let Ok(mut current) = self.token.write() {
            *current = token;
        }
    }
}

impl TokenProvider for MutableTokenProvider {
    fn token(&self) -> Option<SecretString> {
        self.token.read().ok().and_then(|token| token.clone())
    }
}

impl fmt::Debug for MutableTokenProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("MutableTokenProvider(<redacted>)")
    }
}

/// Builder for [`Client`].
#[derive(Clone)]
pub struct ClientBuilder {
    config: ClientConfig,
    tokens: Arc<dyn TokenProvider>,
    http_client: Option<reqwest::Client>,
    telemetry: Option<Arc<dyn ApiTelemetrySink>>,
}

impl fmt::Debug for ClientBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientBuilder")
            .field("config", &self.config)
            .field("tokens", &"<redacted provider>")
            .finish()
    }
}

impl ClientBuilder {
    /// Creates a builder with safe production defaults and no configured token.
    #[must_use]
    pub fn new() -> Self {
        Self {
            config: ClientConfig::default(),
            tokens: Arc::new(MutableTokenProvider::default()),
            http_client: None,
            telemetry: None,
        }
    }

    /// Sets the API base URL. Defaults to `https://api.replicant.space/`.
    #[must_use]
    pub fn base_url(mut self, url: Url) -> Self {
        self.config.base_url = url;
        self
    }

    /// Sets the connection timeout.
    #[must_use]
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.config.connect_timeout = timeout;
        self
    }

    /// Sets the complete timeout for ordinary requests. This does not impose a
    /// total lifetime on the long-lived SSE response body.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.config.request_timeout = timeout;
        self
    }

    /// Sets the `User-Agent` header sent with every request.
    #[must_use]
    pub fn user_agent(mut self, user_agent: impl Into<String>) -> Self {
        self.config.user_agent = user_agent.into();
        self
    }

    /// Sets the retry policy applied to safe-read requests.
    #[must_use]
    pub fn retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.config.retry = policy;
        self
    }

    /// Selects the TLS implementation used by an internally built HTTP client.
    #[must_use]
    pub fn tls_backend(mut self, backend: TlsBackend) -> Self {
        self.config.tls_backend = backend;
        self
    }

    /// Sets the bearer token used for authenticated requests.
    #[must_use]
    pub fn authentication_token(self, token: SecretString) -> Self {
        let provider = Arc::new(MutableTokenProvider::default());
        provider.replace(Some(token));
        self.token_provider(provider)
    }

    /// Sets a custom token provider (for example, one backed by refreshable
    /// credentials).
    #[must_use]
    pub fn token_provider(mut self, provider: Arc<dyn TokenProvider>) -> Self {
        self.tokens = provider;
        self
    }

    /// Sets the maximum accepted response body size, in bytes.
    #[must_use]
    pub fn max_response_body_bytes(mut self, bytes: usize) -> Self {
        self.config.max_response_body_bytes = bytes;
        self
    }

    /// Sets the maximum accepted response body size for the unpaginated global
    /// star catalogue (`GET /v1/stars`). This is separate from the ordinary
    /// endpoint limit because the complete catalogue is intentionally much
    /// larger than typical API responses.
    #[must_use]
    pub fn max_star_catalogue_response_body_bytes(mut self, bytes: usize) -> Self {
        self.config.max_star_catalogue_response_body_bytes = bytes;
        self
    }

    /// Disables the locally generated `X-Request-ID` header.
    #[must_use]
    pub fn send_request_id(mut self, enabled: bool) -> Self {
        self.config.send_request_id = enabled;
        self
    }

    /// Disables sanitized `tracing` events for requests and retries.
    #[must_use]
    pub fn emit_tracing(mut self, enabled: bool) -> Self {
        self.config.emit_tracing = enabled;
        self
    }

    /// Uses an already-configured HTTP client rather than building one from
    /// this builder's timeouts.
    #[must_use]
    pub fn http_client(mut self, client: reqwest::Client) -> Self {
        self.http_client = Some(client);
        self
    }

    /// Installs a best-effort sink for per-attempt HTTP telemetry.
    ///
    /// The sink is called inline after an attempt has enough information to
    /// classify its outcome. Implementations should enqueue without blocking;
    /// transport success and failure never depend on telemetry persistence.
    #[must_use]
    pub fn api_telemetry_sink(mut self, sink: Arc<dyn ApiTelemetrySink>) -> Self {
        self.telemetry = Some(sink);
        self
    }

    /// Builds the client.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Configuration`] when the base URL is not a bare,
    /// credential-free HTTP(S) origin, or when a timeout is zero.
    pub fn build(mut self) -> Result<Client, Error> {
        validate_base_url(&mut self.config.base_url)?;
        validate_client_config(&self.config)?;
        let client = if let Some(client) = self.http_client {
            client
        } else {
            let builder =
                reqwest::Client::builder().connect_timeout(self.config.connect_timeout);
            let builder = match self.config.tls_backend {
                TlsBackend::Automatic => builder,
                TlsBackend::Rustls => {
                    #[cfg(feature = "rustls-tls")]
                    {
                        builder.use_rustls_tls()
                    }
                    #[cfg(not(feature = "rustls-tls"))]
                    return Err(invalid("rustls TLS support is not enabled"));
                }
                TlsBackend::Native => {
                    #[cfg(feature = "native-tls")]
                    {
                        builder.use_native_tls()
                    }
                    #[cfg(not(feature = "native-tls"))]
                    return Err(invalid("native TLS support is not enabled"));
                }
            };
            builder.build().map_err(|error| Error::Configuration {
                message: error.to_string(),
            })?
        };
        Ok(Client {
            inner: Arc::new(ClientInner {
                http: client,
                base_url: self.config.base_url.clone(),
                tokens: self.tokens,
                rate_limits: RateLimitCoordinator::new(),
                config: self.config,
                telemetry: self.telemetry,
                outbound_in_flight: AtomicU64::new(0),
            }),
            priority: RequestPriority::default(),
            refresh_budget: None,
        })
    }
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// The raw, unmanaged Replicant Space HTTP client.
///
/// Cheaply cloneable; every clone shares the same connection pool, token
/// provider, and rate-limit coordinator. Returns transport DTOs and response
/// metadata only — it never hydrates, persists, publishes, journals
/// operations, or reconciles state.
///
/// **Mutating (unsafe) calls are never retried automatically.** A failure on
/// a mutating request may mean the request never reached the server, or that
/// it reached the server and the response was lost. Callers that need
/// exactly-once semantics for mutations must reconcile that ambiguity
/// themselves (the managed client's durable operation model does this in a
/// later phase).
#[derive(Clone)]
pub struct Client {
    pub(crate) inner: Arc<ClientInner>,
    priority: RequestPriority,
    refresh_budget: Option<crate::raw::rate_limit::RefreshBudgetContext>,
}

pub(crate) struct ClientInner {
    pub(crate) http: reqwest::Client,
    pub(crate) base_url: Url,
    pub(crate) tokens: Arc<dyn TokenProvider>,
    pub(crate) rate_limits: RateLimitCoordinator,
    pub(crate) config: ClientConfig,
    pub(crate) telemetry: Option<Arc<dyn ApiTelemetrySink>>,
    pub(crate) outbound_in_flight: AtomicU64,
}

struct OutboundAttemptGuard<'a> {
    counter: &'a AtomicU64,
}

impl<'a> OutboundAttemptGuard<'a> {
    fn enter(counter: &'a AtomicU64) -> (Self, u64) {
        let in_flight = counter.fetch_add(1, Ordering::Relaxed).saturating_add(1);
        (Self { counter }, in_flight)
    }
}

impl Drop for OutboundAttemptGuard<'_> {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct AttemptProgress {
    request_prepare_ms: Option<u64>,
    time_to_headers_ms: Option<u64>,
    metadata_ms: Option<u64>,
    body_read_ms: Option<u64>,
    decode_ms: Option<u64>,
}

impl AttemptProgress {
    fn with_elapsed(self, rate_limit_wait: Duration, elapsed: Duration) -> ApiAttemptTimings {
        ApiAttemptTimings {
            rate_limit_wait_ms: duration_millis(rate_limit_wait),
            request_prepare_ms: self.request_prepare_ms,
            time_to_headers_ms: self.time_to_headers_ms,
            metadata_ms: self.metadata_ms,
            body_read_ms: self.body_read_ms,
            decode_ms: self.decode_ms,
            elapsed_ms: duration_millis(rate_limit_wait).saturating_add(duration_millis(elapsed)),
        }
    }
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("base_url", &self.inner.base_url)
            .field("priority", &self.priority)
            .finish_non_exhaustive()
    }
}

impl Client {
    /// Starts configuring a raw client.
    #[must_use]
    pub fn builder() -> ClientBuilder {
        ClientBuilder::new()
    }

    /// Returns a cheap clone whose requests use `priority` while waiting for
    /// rate-limit permits.
    #[must_use]
    pub fn with_priority(&self, priority: RequestPriority) -> Self {
        Self {
            inner: self.inner.clone(),
            priority,
            refresh_budget: self.refresh_budget.clone(),
        }
    }

    pub(crate) fn with_refresh_budget(&self, run_id: &str, capacity: u32) -> Self {
        Self {
            inner: self.inner.clone(),
            priority: RequestPriority::Background,
            refresh_budget: Some(crate::raw::rate_limit::RefreshBudgetContext {
                run_id: run_id.to_owned(),
                capacity: capacity.clamp(1, 60),
            }),
        }
    }

    pub(crate) fn max_star_catalogue_response_body_bytes(&self) -> usize {
        self.inner.config.max_star_catalogue_response_body_bytes
    }

    /// Returns the shared rate-limit coordinator, for callers (including the
    /// future managed scheduler) that want to observe or share budgets.
    #[must_use]
    pub fn rate_limits(&self) -> &RateLimitCoordinator {
        &self.inner.rate_limits
    }

    /// Minimal, unauthenticated connectivity check.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] on transport failure or a non-2xx response.
    pub async fn health(&self) -> Result<RawResponse<serde_json::Value>, Error> {
        self.execute(Method::GET, "v1/health", false, RequestSafety::SafeRead)
            .await
    }

    /// Executes a bodiless typed request through the shared pipeline.
    pub(crate) async fn execute<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        authenticated: bool,
        safety: RequestSafety,
    ) -> Result<RawResponse<T>, Error> {
        self.execute_bytes(
            method,
            path,
            authenticated,
            safety,
            None,
            self.inner.config.max_response_body_bytes,
        )
        .await
    }

    /// Executes a bodiless typed request with an explicit bounded response
    /// body limit. Used by exceptional unpaginated endpoints such as the full
    /// global star catalogue while ordinary endpoints retain the default cap.
    pub(crate) async fn execute_with_response_limit<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        authenticated: bool,
        safety: RequestSafety,
        response_body_limit: usize,
    ) -> Result<RawResponse<T>, Error> {
        self.execute_bytes(
            method,
            path,
            authenticated,
            safety,
            None,
            response_body_limit,
        )
        .await
    }

    /// Executes a typed JSON-body request through the shared pipeline.
    pub(crate) async fn execute_json<T: DeserializeOwned, B: Serialize>(
        &self,
        method: Method,
        path: &str,
        authenticated: bool,
        safety: RequestSafety,
        body: &B,
    ) -> Result<RawResponse<T>, Error> {
        let bytes = serde_json::to_vec(body).map_err(|error| Error::Configuration {
            message: format!("request serialization failed: {error}"),
        })?;
        self.execute_bytes(
            method,
            path,
            authenticated,
            safety,
            Some(bytes),
            self.inner.config.max_response_body_bytes,
        )
        .await
    }

    #[cfg(feature = "events")]
    pub(crate) async fn event_stream_response(
        &self,
        cursor: Option<&str>,
    ) -> Result<reqwest::Response, Error> {
        let path = crate::raw::common::with_query(
            "v1/events/stream",
            &[("cursor", cursor.map(ToOwned::to_owned))],
        );
        let method = Method::GET;
        let request_id = RequestId::new();
        let overall_started = Instant::now();
        let permit_started = Instant::now();
        self.inner.rate_limits.acquire(RateLimitBucket::Sse).await;
        let permit_wait = permit_started.elapsed();
        let (_outbound_guard, outbound_in_flight) =
            OutboundAttemptGuard::enter(&self.inner.outbound_in_flight);
        let attempt_started = Instant::now();
        let mut timings = AttemptProgress::default();
        let request = RequestAttempt {
            method: &method,
            path: &path,
            authenticated: true,
            request_id: &request_id,
            bucket: RateLimitBucket::Sse,
            attempt: 1,
            body: None,
            response_body_limit: self.inner.config.max_response_body_bytes,
            rate_limit_wait: permit_wait,
            logical_started: overall_started,
            retry_backoff: Duration::ZERO,
            priority: self.priority,
            outbound_in_flight,
        };

        let prepare_started = Instant::now();
        let prepared = match self.prepare_request(method.clone(), &path, true, &request_id) {
            Ok(prepared) => prepared.header(reqwest::header::ACCEPT, "text/event-stream"),
            Err(error) => {
                timings.request_prepare_ms = Some(duration_millis(prepare_started.elapsed()));
                self.record_api_attempt(
                    &request,
                    None,
                    ApiAttemptOutcome::PrepareError,
                    Some("configuration"),
                    None,
                    timings.with_elapsed(permit_wait, attempt_started.elapsed()),
                );
                return Err(error);
            }
        };
        timings.request_prepare_ms = Some(duration_millis(prepare_started.elapsed()));

        let connect_started = Instant::now();
        let response = match prepared.send().await {
            Ok(response) => response,
            Err(error) => {
                timings.time_to_headers_ms = Some(duration_millis(connect_started.elapsed()));
                self.record_api_attempt(
                    &request,
                    None,
                    ApiAttemptOutcome::TransportError,
                    Some(reqwest_error_kind(&error)),
                    None,
                    timings.with_elapsed(permit_wait, attempt_started.elapsed()),
                );
                return Err(map_reqwest_error(error));
            }
        };
        let time_to_headers = connect_started.elapsed();
        timings.time_to_headers_ms = Some(duration_millis(time_to_headers));
        let metadata_started = Instant::now();
        let metadata = self
            .observe_response(RateLimitBucket::Sse, &response, request_id.clone())
            .await;
        timings.metadata_ms = Some(duration_millis(metadata_started.elapsed()));
        if self.inner.config.emit_tracing {
            debug!(
                target: "replicant_client::raw::http",
                event = "http.sse_connected",
                method = "GET",
                path = %path,
                local_request_id = %request_id,
                status = metadata.status.as_u16(),
                rate_limit_wait_ms = permit_wait.as_millis() as u64,
                time_to_headers_ms = time_to_headers.as_millis() as u64,
                elapsed_ms = overall_started.elapsed().as_millis() as u64,
                "SSE request received response headers"
            );
        }
        if response.status().is_success() {
            self.record_api_attempt(
                &request,
                Some(&metadata),
                ApiAttemptOutcome::Success,
                None,
                None,
                timings.with_elapsed(permit_wait, attempt_started.elapsed()),
            );
            return Ok(response);
        }

        let body_started = Instant::now();
        let bytes = match read_bounded(response, self.inner.config.max_response_body_bytes).await {
            Ok(bytes) => bytes,
            Err(error) => {
                timings.body_read_ms = Some(duration_millis(body_started.elapsed()));
                self.record_api_attempt(
                    &request,
                    Some(&metadata),
                    ApiAttemptOutcome::BodyError,
                    Some(api_error_kind(&error)),
                    None,
                    timings.with_elapsed(permit_wait, attempt_started.elapsed()),
                );
                return Err(error);
            }
        };
        let body_elapsed = body_started.elapsed();
        timings.body_read_ms = Some(duration_millis(body_elapsed));
        if self.inner.config.emit_tracing {
            warn!(
                target: "replicant_client::raw::http",
                event = "http.sse_rejected",
                method = "GET",
                path = %path,
                local_request_id = %request_id,
                status = metadata.status.as_u16(),
                response_bytes = bytes.len(),
                body_read_ms = body_elapsed.as_millis() as u64,
                elapsed_ms = overall_started.elapsed().as_millis() as u64,
                "SSE request was rejected"
            );
        }
        self.record_api_attempt(
            &request,
            Some(&metadata),
            ApiAttemptOutcome::HttpError,
            Some(if metadata.status == StatusCode::TOO_MANY_REQUESTS {
                "rate_limited"
            } else {
                "http_status"
            }),
            Some(bytes.len()),
            timings.with_elapsed(permit_wait, attempt_started.elapsed()),
        );
        Err(map_status(metadata.status, &bytes, &metadata))
    }

    async fn execute_bytes<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        authenticated: bool,
        safety: RequestSafety,
        body: Option<Vec<u8>>,
        response_body_limit: usize,
    ) -> Result<RawResponse<T>, Error> {
        let request_id = RequestId::new();
        let bucket = bucket_for(&method, path);
        let overall_started = Instant::now();
        let mut attempts: u32 = 0;
        let mut retry_backoff = Duration::ZERO;
        loop {
            let permit_started = Instant::now();
            self.inner
                .rate_limits
                .acquire_with_refresh(bucket, self.priority, self.refresh_budget.as_ref())
                .await;
            let permit_wait = permit_started.elapsed();
            let (outbound_guard, outbound_in_flight) =
                OutboundAttemptGuard::enter(&self.inner.outbound_in_flight);
            attempts += 1;
            let attempt_started = Instant::now();
            if self.inner.config.emit_tracing {
                debug!(
                    target: "replicant_client::raw::http",
                    event = "http.request_started",
                    method = %method,
                    path,
                    local_request_id = %request_id,
                    attempt = attempts,
                    ?bucket,
                    priority = ?self.priority,
                    ?safety,
                    authenticated,
                    response_body_limit_bytes = response_body_limit,
                    rate_limit_wait_ms = permit_wait.as_millis() as u64,
                    "sending raw HTTP request"
                );
            }
            let result = self
                .send_once::<T>(RequestAttempt {
                    method: &method,
                    path,
                    authenticated,
                    request_id: &request_id,
                    bucket,
                    attempt: attempts,
                    body: body.clone(),
                    response_body_limit,
                    rate_limit_wait: permit_wait,
                    logical_started: overall_started,
                    retry_backoff,
                    priority: self.priority,
                    outbound_in_flight,
                })
                .await;
            let attempt_elapsed = attempt_started.elapsed();
            drop(outbound_guard);
            match result {
                Ok(response) => {
                    if self.inner.config.emit_tracing {
                        debug!(
                            target: "replicant_client::raw::http",
                            event = "http.request_completed",
                            method = %method,
                            path,
                            local_request_id = %request_id,
                            server_request_id = response.metadata.request_id.as_deref().unwrap_or(""),
                            attempt = attempts,
                            status = response.metadata.status.as_u16(),
                            attempt_elapsed_ms = attempt_elapsed.as_millis() as u64,
                            permit_wait_ms = duration_millis(permit_wait),
                            logical_elapsed_ms = duration_millis(overall_started.elapsed()),
                            retry_backoff_ms = duration_millis(retry_backoff),
                            elapsed_ms = overall_started.elapsed().as_millis() as u64,
                            "raw HTTP request completed"
                        );
                    }
                    return Ok(response);
                }
                Err(error) if self.should_retry(&error, safety, attempts) => {
                    let delay =
                        retry_delay(&self.inner.config.retry, attempts, error.retry_after());
                    retry_backoff = retry_backoff.saturating_add(delay);
                    if self.inner.config.emit_tracing {
                        warn!(
                            target: "replicant_client::raw::http",
                            event = "http.request_retry",
                            method = %method,
                            path,
                            local_request_id = %request_id,
                            attempt = attempts,
                            attempt_elapsed_ms = attempt_elapsed.as_millis() as u64,
                            permit_wait_ms = duration_millis(permit_wait),
                            logical_elapsed_ms = duration_millis(overall_started.elapsed()),
                            retry_backoff_ms = duration_millis(retry_backoff),
                            elapsed_ms = overall_started.elapsed().as_millis() as u64,
                            delay_ms = delay.as_millis() as u64,
                            error = %error,
                            "retrying safe raw HTTP request"
                        );
                    }
                    tokio::time::sleep(delay).await;
                }
                Err(error) => {
                    if self.inner.config.emit_tracing {
                        warn!(
                            target: "replicant_client::raw::http",
                            event = "http.request_failed",
                            method = %method,
                            path,
                            local_request_id = %request_id,
                            attempt = attempts,
                            attempt_elapsed_ms = attempt_elapsed.as_millis() as u64,
                            permit_wait_ms = duration_millis(permit_wait),
                            logical_elapsed_ms = duration_millis(overall_started.elapsed()),
                            retry_backoff_ms = duration_millis(retry_backoff),
                            error_kind = api_error_kind(&error),
                            elapsed_ms = overall_started.elapsed().as_millis() as u64,
                            error = %error,
                            ambiguous = error.is_ambiguous_transport_failure(),
                            "raw HTTP request failed"
                        );
                    }
                    return Err(error);
                }
            }
        }
    }

    fn should_retry(&self, error: &Error, safety: RequestSafety, attempts: u32) -> bool {
        if safety != RequestSafety::SafeRead || attempts > self.inner.config.retry.max_retries {
            return false;
        }
        matches!(error, Error::Transport { .. } | Error::RateLimited { .. })
            || matches!(error, Error::Contract { status, .. } if matches!(*status, 502..=504))
    }

    async fn send_once<T: DeserializeOwned>(
        &self,
        request: RequestAttempt<'_>,
    ) -> Result<RawResponse<T>, Error> {
        let total_started = Instant::now();
        let mut timings = AttemptProgress::default();

        let prepare_started = Instant::now();
        let mut prepared = match self
            .prepare_request(
                request.method.clone(),
                request.path,
                request.authenticated,
                request.request_id,
            )
            .map(|request| request.timeout(self.inner.config.request_timeout))
        {
            Ok(prepared) => prepared,
            Err(error) => {
                timings.request_prepare_ms = Some(duration_millis(prepare_started.elapsed()));
                self.record_api_attempt(
                    &request,
                    None,
                    ApiAttemptOutcome::PrepareError,
                    Some("configuration"),
                    None,
                    timings.with_elapsed(request.rate_limit_wait, total_started.elapsed()),
                );
                return Err(error);
            }
        };
        if let Some(body) = request.body.as_ref() {
            prepared = prepared
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(body.clone());
        }
        let prepare_elapsed = prepare_started.elapsed();
        timings.request_prepare_ms = Some(duration_millis(prepare_elapsed));

        let send_started = Instant::now();
        let response = match prepared.send().await {
            Ok(response) => response,
            Err(error) => {
                timings.time_to_headers_ms = Some(duration_millis(send_started.elapsed()));
                self.record_api_attempt(
                    &request,
                    None,
                    ApiAttemptOutcome::TransportError,
                    Some(reqwest_error_kind(&error)),
                    None,
                    timings.with_elapsed(request.rate_limit_wait, total_started.elapsed()),
                );
                return Err(map_reqwest_error(error));
            }
        };
        let time_to_headers = send_started.elapsed();
        timings.time_to_headers_ms = Some(duration_millis(time_to_headers));
        let status = response.status();
        let metadata_started = Instant::now();
        let metadata = self
            .observe_response(request.bucket, &response, request.request_id.clone())
            .await;
        let metadata_elapsed = metadata_started.elapsed();
        timings.metadata_ms = Some(duration_millis(metadata_elapsed));

        let body_started = Instant::now();
        let bytes = match read_bounded(response, request.response_body_limit).await {
            Ok(bytes) => bytes,
            Err(error) => {
                timings.body_read_ms = Some(duration_millis(body_started.elapsed()));
                self.record_api_attempt(
                    &request,
                    Some(&metadata),
                    ApiAttemptOutcome::BodyError,
                    Some(api_error_kind(&error)),
                    None,
                    timings.with_elapsed(request.rate_limit_wait, total_started.elapsed()),
                );
                return Err(error);
            }
        };
        let body_elapsed = body_started.elapsed();
        timings.body_read_ms = Some(duration_millis(body_elapsed));
        if !metadata.status.is_success() {
            if self.inner.config.emit_tracing {
                debug!(
                    target: "replicant_client::raw::http",
                    event = "http.response_received",
                    method = %request.method,
                    path = request.path,
                    local_request_id = %request.request_id,
                    attempt = request.attempt,
                    status = status.as_u16(),
                    response_bytes = bytes.len(),
                    response_body_limit_bytes = request.response_body_limit,
                    request_prepare_ms = prepare_elapsed.as_millis() as u64,
                    time_to_headers_ms = time_to_headers.as_millis() as u64,
                    metadata_ms = metadata_elapsed.as_millis() as u64,
                    body_read_ms = body_elapsed.as_millis() as u64,
                    elapsed_ms = total_started.elapsed().as_millis() as u64,
                    "received non-success HTTP response"
                );
            }
            self.record_api_attempt(
                &request,
                Some(&metadata),
                ApiAttemptOutcome::HttpError,
                Some(if metadata.status == StatusCode::TOO_MANY_REQUESTS {
                    "rate_limited"
                } else {
                    "http_status"
                }),
                Some(bytes.len()),
                timings.with_elapsed(request.rate_limit_wait, total_started.elapsed()),
            );
            return Err(map_status(metadata.status, &bytes, &metadata));
        }

        let decode_started = Instant::now();
        let value = match serde_json::from_slice(if bytes.is_empty() { b"null" } else { &bytes }) {
            Ok(value) => value,
            Err(error) => {
                timings.decode_ms = Some(duration_millis(decode_started.elapsed()));
                self.record_api_attempt(
                    &request,
                    Some(&metadata),
                    ApiAttemptOutcome::DecodeError,
                    Some("decode"),
                    Some(bytes.len()),
                    timings.with_elapsed(request.rate_limit_wait, total_started.elapsed()),
                );
                return Err(Error::Decode {
                    message: error.to_string(),
                    status: Some(metadata.status.as_u16()),
                    source: Some(Box::new(error)),
                });
            }
        };
        let decode_elapsed = decode_started.elapsed();
        timings.decode_ms = Some(duration_millis(decode_elapsed));
        if self.inner.config.emit_tracing {
            debug!(
                target: "replicant_client::raw::http",
                event = "http.response_decoded",
                method = %request.method,
                path = request.path,
                local_request_id = %request.request_id,
                attempt = request.attempt,
                status = status.as_u16(),
                response_bytes = bytes.len(),
                response_body_limit_bytes = request.response_body_limit,
                request_prepare_ms = prepare_elapsed.as_millis() as u64,
                time_to_headers_ms = time_to_headers.as_millis() as u64,
                metadata_ms = metadata_elapsed.as_millis() as u64,
                body_read_ms = body_elapsed.as_millis() as u64,
                decode_ms = decode_elapsed.as_millis() as u64,
                elapsed_ms = total_started.elapsed().as_millis() as u64,
                "decoded successful HTTP response"
            );
        }
        self.record_api_attempt(
            &request,
            Some(&metadata),
            ApiAttemptOutcome::Success,
            None,
            Some(bytes.len()),
            timings.with_elapsed(request.rate_limit_wait, total_started.elapsed()),
        );
        Ok(RawResponse { value, metadata })
    }

    fn record_api_attempt(
        &self,
        request: &RequestAttempt<'_>,
        metadata: Option<&ResponseMetadata>,
        outcome: ApiAttemptOutcome,
        error_kind: Option<&'static str>,
        response_bytes: Option<usize>,
        timings: ApiAttemptTimings,
    ) {
        let route_key = normalize_route_key(request.path);
        let attempt_elapsed_ms = timings
            .elapsed_ms
            .saturating_sub(timings.rate_limit_wait_ms);
        let logical_elapsed_ms = duration_millis(request.logical_started.elapsed());
        let retry_backoff_ms = duration_millis(request.retry_backoff);
        let priority = match request.priority {
            RequestPriority::Foreground => "foreground",
            RequestPriority::Background => "background",
        };
        if self.inner.config.emit_tracing {
            debug!(
                target: "replicant_client::raw::http",
                event = "http.outbound.completed",
                method = %request.method,
                endpoint = %route_key,
                local_request_id = %request.request_id,
                server_request_id = metadata
                    .and_then(|metadata| metadata.request_id.as_deref())
                    .unwrap_or(""),
                attempt = request.attempt,
                status = ?metadata.map(|metadata| metadata.status.as_u16()),
                outcome = outcome.as_str(),
                error_kind = error_kind.unwrap_or(""),
                ?request.bucket,
                priority = ?request.priority,
                queue_wait_ms = timings.rate_limit_wait_ms,
                permit_wait_ms = timings.rate_limit_wait_ms,
                request_prepare_ms = ?timings.request_prepare_ms,
                time_to_headers_ms = ?timings.time_to_headers_ms,
                body_read_ms = ?timings.body_read_ms,
                decode_ms = ?timings.decode_ms,
                attempt_ms = attempt_elapsed_ms,
                logical_elapsed_ms,
                retry_backoff_ms,
                outbound_in_flight = request.outbound_in_flight,
                response_bytes = ?response_bytes,
                "outbound HTTP attempt completed"
            );
        }
        let Some(sink) = self.inner.telemetry.as_ref() else {
            return;
        };
        let concrete_path = request.path.split('?').next().unwrap_or(request.path);
        let stored_path = if route_key.contains("{token}") {
            route_key.as_str()
        } else {
            concrete_path
        };
        let rate_limit = metadata
            .and_then(|metadata| metadata.rate_limit.as_ref())
            .map_or_else(ApiRateLimitTelemetry::default, rate_limit_telemetry);
        sink.record(ApiAttemptTelemetry {
            observed_at_ms: now_unix_millis(),
            local_request_id: request.request_id.to_string(),
            server_request_id: metadata.and_then(|metadata| metadata.request_id.clone()),
            method: request.method.as_str().to_owned(),
            path: stored_path.to_owned(),
            route_key,
            rate_limit_bucket: rate_limit_bucket_label(request.bucket).to_owned(),
            priority: priority.to_owned(),
            attempt: request.attempt,
            status_code: metadata.map(|metadata| metadata.status.as_u16()),
            outcome,
            error_kind: error_kind.map(ToOwned::to_owned),
            response_bytes: response_bytes.map(|bytes| bytes.try_into().unwrap_or(u64::MAX)),
            logical_elapsed_ms,
            retry_backoff_ms,
            outbound_in_flight: request.outbound_in_flight,
            timings,
            rate_limit,
        });
    }

    fn prepare_request(
        &self,
        method: Method,
        path: &str,
        authenticated: bool,
        request_id: &RequestId,
    ) -> Result<reqwest::RequestBuilder, Error> {
        let url = request_url(&self.inner.base_url, path)?;
        let mut request = self
            .inner
            .http
            .request(method, url)
            .header(USER_AGENT, &self.inner.config.user_agent);
        if self.inner.config.send_request_id {
            request = request.header("x-request-id", request_id.to_string());
        }
        if authenticated {
            let token = self
                .inner
                .tokens
                .token()
                .ok_or_else(|| Error::Configuration {
                    message: "authentication token is required for this endpoint".into(),
                })?;
            let value = HeaderValue::from_str(&format!("Bearer {}", token.expose_secret()))
                .map_err(|_| Error::Configuration {
                    message: "bearer token contains invalid header bytes".into(),
                })?;
            request = request.header(AUTHORIZATION, value);
        }
        Ok(request)
    }

    async fn observe_response(
        &self,
        bucket: RateLimitBucket,
        response: &reqwest::Response,
        local_request_id: RequestId,
    ) -> ResponseMetadata {
        let headers = response.headers();
        let rate_limit = parse_rate_limit(headers, SystemTime::now());
        if let Some(snapshot) = rate_limit.clone() {
            if self.inner.config.emit_tracing {
                debug!(
                    target: "replicant_client::raw::rate_limit",
                    event = "rate_limit.observed",
                    ?bucket,
                    status = response.status().as_u16(),
                    local_request_id = %local_request_id,
                    limit = ?snapshot.limit,
                    remaining = ?snapshot.remaining,
                    retry_after_ms = ?snapshot.retry_after.map(|value| value.delay().as_millis() as u64),
                    reset_delay_ms = ?snapshot.reset.map(|value| value.delay().as_millis() as u64),
                    delay_enforced = response.status() == StatusCode::TOO_MANY_REQUESTS || snapshot.remaining == Some(0),
                    "observed server rate-limit metadata"
                );
            }
            self.inner
                .rate_limits
                .observe_response(bucket, response.status(), snapshot)
                .await;
        }
        ResponseMetadata {
            status: response.status(),
            request_id: header_string(headers, "x-request-id")
                .or_else(|| header_string(headers, "request-id")),
            local_request_id,
            rate_limit,
        }
    }
}

fn rate_limit_bucket_label(bucket: RateLimitBucket) -> &'static str {
    match bucket {
        RateLimitBucket::Read => "read",
        RateLimitBucket::Action => "action",
        RateLimitBucket::Registration => "registration",
        RateLimitBucket::Verification => "verification",
        RateLimitBucket::Feedback => "feedback",
        RateLimitBucket::StarCatalogue => "star_catalogue",
        RateLimitBucket::Sse => "sse",
    }
}

fn rate_limit_telemetry(snapshot: &RateLimitSnapshot) -> ApiRateLimitTelemetry {
    ApiRateLimitTelemetry {
        limit: snapshot.limit,
        remaining: snapshot.remaining,
        reset_epoch_seconds: snapshot.reset.map(RateLimitReset::epoch_seconds),
        retry_after_ms: snapshot
            .retry_after
            .map(RetryAfter::delay)
            .map(duration_millis),
    }
}

fn validate_base_url(url: &mut Url) -> Result<(), Error> {
    if !matches!(url.scheme(), "https" | "http")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || url.query().is_some()
        || url.host_str().is_none()
    {
        return Err(Error::Configuration {
            message: "base URL must be HTTP(S), host-bearing, credential-free, query-free, and fragment-free".into(),
        });
    }
    if url.scheme() == "http" && !matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "::1"))
    {
        return Err(Error::Configuration {
            message: "non-local base URL must use HTTPS".into(),
        });
    }
    if !url.path().ends_with('/') {
        let path = format!("{}/", url.path());
        url.set_path(&path);
    }
    Ok(())
}

fn request_url(base_url: &Url, path: &str) -> Result<Url, Error> {
    let (path_part, query) = path
        .split_once('?')
        .map_or((path, None), |(path, query)| (path, Some(query)));
    if path_part.is_empty()
        || path_part.starts_with('/')
        || path_part.contains('#')
        || path_part
            .split('/')
            .any(|segment| matches!(segment, "." | ".."))
        || Url::parse(path_part).is_ok()
    {
        return Err(Error::Configuration {
            message: "request path must be a relative path within the configured base URL".into(),
        });
    }
    let mut url = base_url.clone();
    url.set_path(&format!("{}{}", base_url.path(), path_part));
    url.set_query(query);
    Ok(url)
}

fn header_string(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
}

fn reqwest_error_kind(error: &reqwest::Error) -> &'static str {
    if error.is_timeout() {
        if error.is_connect() {
            "connect_timeout"
        } else if error.is_body() {
            "body_timeout"
        } else {
            "request_timeout"
        }
    } else if error.is_connect() {
        "connect"
    } else if error.is_body() {
        "body"
    } else if error.is_request() {
        "request"
    } else {
        "transport"
    }
}

fn api_error_kind(error: &Error) -> &'static str {
    match error {
        Error::Transport {
            source: Some(source),
            ..
        } => source
            .downcast_ref::<reqwest::Error>()
            .map_or("transport", reqwest_error_kind),
        Error::Transport { source: None, .. } => "transport",
        Error::Configuration { .. } => "configuration",
        Error::Authentication { .. } => "authentication",
        Error::RateLimited { .. } => "rate_limited",
        Error::Decode { .. } => "decode",
        Error::Contract { .. } => "http_status",
        Error::Persistence { .. } => "persistence",
        Error::AccountStoreMismatch { .. } => "account_store_mismatch",
        Error::Operation { .. } => "operation",
        Error::Closed => "closed",
    }
}

fn map_reqwest_error(error: reqwest::Error) -> Error {
    if error.is_timeout() {
        Error::Transport {
            message: "request timed out".into(),
            source: Some(Box::new(error)),
        }
    } else {
        Error::Transport {
            message: "network request failed".into(),
            source: Some(Box::new(error)),
        }
    }
}

async fn read_bounded(response: reqwest::Response, limit: usize) -> Result<Vec<u8>, Error> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(Error::Decode {
            message: format!("response body exceeds {limit} bytes"),
            status: Some(response.status().as_u16()),
            source: None,
        });
    }
    let status = response.status();
    let mut response = response;
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(map_reqwest_error)? {
        let length = bytes
            .len()
            .checked_add(chunk.len())
            .ok_or_else(|| Error::Decode {
                message: format!("response body exceeds {limit} bytes"),
                status: Some(status.as_u16()),
                source: None,
            })?;
        if length > limit {
            return Err(Error::Decode {
                message: format!("response body exceeds {limit} bytes"),
                status: Some(status.as_u16()),
                source: None,
            });
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn parse_rate_limit(headers: &HeaderMap, observed_at: SystemTime) -> Option<RateLimitSnapshot> {
    let retry_after = header_string(headers, "retry-after")
        .and_then(|value| parse_retry_after(&value).map(RetryAfter::new));
    let limit = header_string(headers, "x-ratelimit-limit").and_then(|value| value.parse().ok());
    let remaining =
        header_string(headers, "x-ratelimit-remaining").and_then(|value| value.parse().ok());
    let reset = header_string(headers, "x-ratelimit-reset").and_then(|value| {
        value
            .parse::<u64>()
            .ok()
            .and_then(|epoch| RateLimitReset::from_epoch_seconds(epoch, observed_at))
    });
    if retry_after.is_none() && limit.is_none() && remaining.is_none() && reset.is_none() {
        None
    } else {
        Some(RateLimitSnapshot {
            limit,
            remaining,
            reset,
            retry_after,
        })
    }
}

/// Parses a `Retry-After` header value: either delay-seconds or an HTTP-date.
#[must_use]
pub fn parse_retry_after(value: &str) -> Option<Duration> {
    if let Ok(seconds) = value.trim().parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    let date = httpdate::parse_http_date(value).ok()?;
    date.duration_since(SystemTime::now()).ok()
}

fn error_details(bytes: &[u8], metadata: &ResponseMetadata) -> ErrorDetails {
    let mut parsed: serde_json::Value =
        serde_json::from_slice(bytes).unwrap_or(serde_json::Value::Null);
    redact_json(&mut parsed);
    let excerpt = (!parsed.is_null()).then(|| parsed.to_string());
    ErrorDetails {
        remote_request_id: metadata.request_id.clone(),
        local_request_id: Some(metadata.local_request_id.to_string()),
        // The `default` envelope shape carries a numeric `code`; the simple
        // `{"error": "..."}` shape used by explicit status responses does not.
        code: parsed.get("code").and_then(serde_json::Value::as_i64),
        status: parsed
            .get("status")
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned),
        message: parsed
            .get("message")
            .or_else(|| parsed.get("error"))
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned),
        field_errors: parsed.get("errors").map(ToString::to_string),
        body_excerpt: excerpt,
    }
}

/// Redacts secret-shaped fields and bearer tokens from a parsed JSON body
/// before it is retained in an error or emitted through tracing.
fn redact_json(value: &mut serde_json::Value) {
    const SENSITIVE_KEYS: [&str; 6] = [
        "token",
        "secret",
        "authorization",
        "password",
        "credential",
        "webhook",
    ];
    match value {
        serde_json::Value::Object(fields) => {
            for (key, value) in fields {
                let key = key.to_ascii_lowercase();
                if SENSITIVE_KEYS
                    .iter()
                    .any(|sensitive| key.contains(sensitive))
                {
                    *value = serde_json::Value::String("<redacted>".into());
                } else {
                    redact_json(value);
                }
            }
        }
        serde_json::Value::Array(values) => values.iter_mut().for_each(redact_json),
        serde_json::Value::String(value) if value.contains("Bearer ") => {
            *value = "<redacted>".into();
        }
        _ => {}
    }
}

fn map_status(status: StatusCode, bytes: &[u8], metadata: &ResponseMetadata) -> Error {
    let details = error_details(bytes, metadata);
    if status == StatusCode::UNAUTHORIZED {
        return Error::Authentication {
            details: Box::new(details),
        };
    }
    if status == StatusCode::TOO_MANY_REQUESTS {
        let retry_after = metadata
            .rate_limit
            .as_ref()
            .and_then(RateLimitSnapshot::delay);
        return Error::RateLimited {
            retry_after,
            details: Box::new(details),
        };
    }
    Error::Contract {
        status: status.as_u16(),
        details: Box::new(details),
    }
}

fn validate_client_config(config: &ClientConfig) -> Result<(), Error> {
    if config.user_agent.trim().is_empty() {
        return Err(invalid("user agent must not be empty"));
    }
    if config.connect_timeout.is_zero() || config.request_timeout.is_zero() {
        return Err(invalid("timeouts must be greater than zero"));
    }
    if config.max_response_body_bytes == 0 || config.max_response_body_bytes > 64 * 1024 * 1024 {
        return Err(invalid(
            "response body limit must be between 1 byte and 64 MiB",
        ));
    }
    if config.max_star_catalogue_response_body_bytes == 0
        || config.max_star_catalogue_response_body_bytes > 64 * 1024 * 1024
    {
        return Err(invalid(
            "star catalogue response body limit must be between 1 byte and 64 MiB",
        ));
    }
    if config.retry.initial_backoff > config.retry.max_backoff
        || config.retry.jitter > config.retry.max_backoff
    {
        return Err(invalid(
            "retry initial backoff and jitter must not exceed the maximum",
        ));
    }
    Ok(())
}

fn invalid(message: &str) -> Error {
    Error::Configuration {
        message: message.to_owned(),
    }
}

fn retry_delay(policy: &RetryPolicy, attempt: u32, server: Option<Duration>) -> Duration {
    if let Some(delay) = server {
        return delay.min(policy.max_backoff);
    }
    let exponent = attempt.saturating_sub(1).min(20);
    let base = policy
        .initial_backoff
        .saturating_mul(2_u32.saturating_pow(exponent))
        .min(policy.max_backoff);
    let jitter = Duration::from_millis(
        (Uuid::new_v4().as_u128() as u64) % (policy.jitter.as_millis().max(1) as u64),
    );
    base.saturating_add(jitter).min(policy.max_backoff)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    #[derive(Default)]
    struct Capture(StdMutex<Vec<ApiAttemptTelemetry>>);

    impl ApiTelemetrySink for Capture {
        fn record(&self, sample: ApiAttemptTelemetry) {
            self.0.lock().expect("capture lock").push(sample);
        }
    }

    #[test]
    fn base_url_rejects_credentials_and_query() {
        let mut url = Url::parse("https://user:pass@api.replicant.space/").unwrap();
        assert!(validate_base_url(&mut url).is_err());

        let mut url = Url::parse("https://api.replicant.space/?x=1").unwrap();
        assert!(validate_base_url(&mut url).is_err());
    }

    #[test]
    fn base_url_rejects_non_local_http() {
        let mut url = Url::parse("http://api.replicant.space/").unwrap();
        assert!(validate_base_url(&mut url).is_err());

        let mut url = Url::parse("http://localhost:8080/").unwrap();
        assert!(validate_base_url(&mut url).is_ok());
    }

    #[test]
    fn base_url_gets_a_trailing_slash() {
        let mut url = Url::parse("https://api.replicant.space").unwrap();
        validate_base_url(&mut url).unwrap();
        assert_eq!(url.path(), "/");
    }

    #[test]
    fn request_url_rejects_absolute_and_traversal_paths() {
        let base = Url::parse("https://api.replicant.space/").unwrap();
        assert!(request_url(&base, "/v1/health").is_err());
        assert!(request_url(&base, "../v1/health").is_err());
        assert!(request_url(&base, "https://evil.example/").is_err());
        assert!(request_url(&base, "v1/health").is_ok());
    }

    #[test]
    fn client_debug_never_prints_the_token() {
        let client = Client::builder()
            .authentication_token(SecretString::from("super-secret-token".to_string()))
            .build()
            .unwrap();
        let rendered = format!("{client:?}");
        assert!(!rendered.contains("super-secret-token"));
    }

    #[test]
    fn request_priority_is_scoped_and_foreground_by_default() {
        let foreground = Client::builder().build().expect("client");
        let background = foreground.with_priority(RequestPriority::Background);

        assert_eq!(foreground.priority, RequestPriority::Foreground);
        assert_eq!(background.priority, RequestPriority::Background);
    }

    #[test]
    fn builder_debug_never_prints_the_token() {
        let builder = Client::builder()
            .authentication_token(SecretString::from("super-secret-token".to_string()));
        let rendered = format!("{builder:?}");
        assert!(!rendered.contains("super-secret-token"));
    }

    #[test]
    fn redact_json_masks_sensitive_fields_and_bearer_strings() {
        let mut value = serde_json::json!({
            "message": "failed",
            "webhook_secret": "abc123",
            "note": "Authorization: Bearer sekrit",
        });
        redact_json(&mut value);
        assert_eq!(value["webhook_secret"], "<redacted>");
        assert_eq!(value["note"], "<redacted>");
        assert_eq!(value["message"], "failed");
    }

    #[test]
    fn reset_epoch_is_converted_relative_to_observation_time() {
        let mut headers = HeaderMap::new();
        headers.insert("x-ratelimit-reset", "1779087998".parse().unwrap());
        let observed_at = SystemTime::UNIX_EPOCH + Duration::from_secs(1_779_087_900);

        let snapshot = parse_rate_limit(&headers, observed_at).unwrap();
        assert_eq!(snapshot.reset.unwrap().delay(), Duration::from_secs(98));
    }

    #[test]
    fn past_or_malformed_reset_never_blocks_or_panics() {
        let observed_at = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        let mut past = HeaderMap::new();
        past.insert("x-ratelimit-reset", "99".parse().unwrap());
        assert_eq!(
            parse_rate_limit(&past, observed_at)
                .unwrap()
                .reset
                .unwrap()
                .delay(),
            Duration::ZERO
        );

        let mut malformed = HeaderMap::new();
        malformed.insert("x-ratelimit-reset", "not-an-epoch".parse().unwrap());
        assert!(parse_rate_limit(&malformed, observed_at).is_none());
    }

    #[tokio::test]
    async fn telemetry_sink_receives_physical_http_attempts() {
        use wiremock::{
            Mock, MockServer, ResponseTemplate,
            matchers::{method, path},
        };

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/health"))
            .respond_with(ResponseTemplate::new(503).set_body_json(serde_json::json!({
                "error": "maintenance"
            })))
            .mount(&server)
            .await;
        let capture = Arc::new(Capture::default());
        let client = Client::builder()
            .base_url(Url::parse(&server.uri()).expect("mock URL"))
            .retry_policy(RetryPolicy {
                max_retries: 0,
                ..RetryPolicy::default()
            })
            .api_telemetry_sink(capture.clone())
            .build()
            .expect("client");

        let error = client.health().await.expect_err("503 should fail");
        assert_eq!(error.status(), Some(503));
        let samples = capture.0.lock().expect("capture lock");
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].status_code, Some(503));
        assert_eq!(samples[0].route_key, "v1/health");
        assert_eq!(samples[0].outcome, ApiAttemptOutcome::HttpError);
        assert_eq!(samples[0].attempt, 1);
    }

    #[tokio::test]
    async fn rate_limit_response_has_a_distinct_error_class() {
        use wiremock::{
            Mock, MockServer, ResponseTemplate,
            matchers::{method, path},
        };

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/health"))
            .respond_with(
                ResponseTemplate::new(429)
                    .insert_header("retry-after", "1")
                    .set_body_json(serde_json::json!({"error": "slow down"})),
            )
            .mount(&server)
            .await;
        let capture = Arc::new(Capture::default());
        let client = Client::builder()
            .base_url(Url::parse(&server.uri()).expect("mock URL"))
            .retry_policy(RetryPolicy {
                max_retries: 0,
                ..RetryPolicy::default()
            })
            .api_telemetry_sink(capture.clone())
            .build()
            .expect("client");

        let error = client.health().await.expect_err("429 should fail");
        assert_eq!(error.status(), Some(429));
        let samples = capture.0.lock().expect("capture lock");
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].status_code, Some(429));
        assert_eq!(samples[0].error_kind.as_deref(), Some("rate_limited"));
        assert_eq!(samples[0].rate_limit.retry_after_ms, Some(1_000));
    }

    #[tokio::test]
    async fn retry_telemetry_separates_attempts_backoff_and_logical_elapsed() {
        use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

        use wiremock::{
            Mock, MockServer, Request, ResponseTemplate,
            matchers::{method, path},
        };

        let calls = Arc::new(AtomicUsize::new(0));
        let responder_calls = calls.clone();
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/health"))
            .respond_with(move |_request: &Request| {
                if responder_calls.fetch_add(1, AtomicOrdering::SeqCst) == 0 {
                    ResponseTemplate::new(503)
                        .set_body_json(serde_json::json!({"error": "maintenance"}))
                } else {
                    ResponseTemplate::new(200).set_body_json(serde_json::json!({"status": "ok"}))
                }
            })
            .mount(&server)
            .await;
        let capture = Arc::new(Capture::default());
        let client = Client::builder()
            .base_url(Url::parse(&server.uri()).expect("mock URL"))
            .retry_policy(RetryPolicy {
                max_retries: 1,
                initial_backoff: Duration::from_millis(30),
                max_backoff: Duration::from_millis(30),
                jitter: Duration::ZERO,
            })
            .api_telemetry_sink(capture.clone())
            .build()
            .expect("client")
            .with_priority(RequestPriority::Background);

        client.health().await.expect("retry succeeds");
        let samples = capture.0.lock().expect("capture lock");
        assert_eq!(calls.load(AtomicOrdering::SeqCst), 2);
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0].local_request_id, samples[1].local_request_id);
        assert_eq!(samples[0].attempt, 1);
        assert_eq!(samples[0].retry_backoff_ms, 0);
        assert_eq!(samples[0].error_kind.as_deref(), Some("http_status"));
        assert_eq!(samples[1].attempt, 2);
        assert_eq!(samples[1].error_kind, None);
        assert!(samples[1].retry_backoff_ms >= 30);
        assert!(samples[1].logical_elapsed_ms >= samples[1].retry_backoff_ms);
        assert_eq!(samples[1].priority, "background");
        assert_eq!(samples[1].outbound_in_flight, 1);
    }

    #[tokio::test]
    async fn timeout_telemetry_attributes_delay_to_the_physical_attempt() {
        use wiremock::{
            Mock, MockServer, ResponseTemplate,
            matchers::{method, path},
        };

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/health"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_millis(100))
                    .set_body_json(serde_json::json!({"status": "ok"})),
            )
            .mount(&server)
            .await;
        let capture = Arc::new(Capture::default());
        let client = Client::builder()
            .base_url(Url::parse(&server.uri()).expect("mock URL"))
            .request_timeout(Duration::from_millis(25))
            .retry_policy(RetryPolicy {
                max_retries: 0,
                ..RetryPolicy::default()
            })
            .api_telemetry_sink(capture.clone())
            .build()
            .expect("client");

        client.health().await.expect_err("request should time out");
        assert_eq!(client.inner.outbound_in_flight.load(Ordering::Relaxed), 0);
        let samples = capture.0.lock().expect("capture lock");
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].outcome, ApiAttemptOutcome::TransportError);
        assert_eq!(samples[0].error_kind.as_deref(), Some("request_timeout"));
        assert_eq!(samples[0].status_code, None);
        assert!(samples[0].timings.time_to_headers_ms.unwrap_or_default() >= 20);
        assert!(samples[0].logical_elapsed_ms >= 20);
        assert_eq!(samples[0].retry_backoff_ms, 0);
    }
    #[tokio::test]
    async fn connection_refusal_is_classified_and_releases_the_gauge() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind unused port");
        let address = listener.local_addr().expect("local address");
        drop(listener);
        let capture = Arc::new(Capture::default());
        let client = Client::builder()
            .base_url(Url::parse(&format!("http://{address}/")).expect("local URL"))
            .connect_timeout(Duration::from_millis(100))
            .request_timeout(Duration::from_millis(200))
            .retry_policy(RetryPolicy {
                max_retries: 0,
                ..RetryPolicy::default()
            })
            .api_telemetry_sink(capture.clone())
            .build()
            .expect("client");

        client
            .health()
            .await
            .expect_err("closed local port should refuse the connection");
        assert_eq!(client.inner.outbound_in_flight.load(Ordering::Relaxed), 0);
        let samples = capture.0.lock().expect("capture lock");
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].outcome, ApiAttemptOutcome::TransportError);
        assert_eq!(samples[0].error_kind.as_deref(), Some("connect"));
    }

    #[tokio::test]
    async fn healthy_burst_reports_concurrency_and_releases_the_gauge() {
        use wiremock::{
            Mock, MockServer, ResponseTemplate,
            matchers::{method, path},
        };

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/health"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_millis(40))
                    .set_body_json(serde_json::json!({"status": "ok"})),
            )
            .mount(&server)
            .await;
        let capture = Arc::new(Capture::default());
        let client = Client::builder()
            .base_url(Url::parse(&server.uri()).expect("mock URL"))
            .retry_policy(RetryPolicy {
                max_retries: 0,
                ..RetryPolicy::default()
            })
            .api_telemetry_sink(capture.clone())
            .build()
            .expect("client");
        client
            .rate_limits()
            .set_policy(
                RateLimitBucket::Read,
                crate::raw::rate_limit::RateLimitPolicy {
                    capacity: 8,
                    refill_every: Duration::from_secs(1),
                },
            )
            .await;

        let (a, b, c, d) = tokio::join!(
            client.health(),
            client.health(),
            client.health(),
            client.health()
        );
        a.expect("request a");
        b.expect("request b");
        c.expect("request c");
        d.expect("request d");
        assert_eq!(client.inner.outbound_in_flight.load(Ordering::Relaxed), 0);
        let samples = capture.0.lock().expect("capture lock");
        assert_eq!(samples.len(), 4);
        assert!(
            samples
                .iter()
                .all(|sample| sample.outcome == ApiAttemptOutcome::Success)
        );
        assert!(
            samples
                .iter()
                .map(|sample| sample.outbound_in_flight)
                .max()
                .unwrap_or_default()
                >= 2
        );
    }

    #[test]
    fn retry_after_takes_precedence_over_reset() {
        let mut headers = HeaderMap::new();
        headers.insert("retry-after", "47".parse().unwrap());
        headers.insert("x-ratelimit-reset", "1779087998".parse().unwrap());
        let observed_at = SystemTime::UNIX_EPOCH + Duration::from_secs(1_779_087_900);

        assert_eq!(
            parse_rate_limit(&headers, observed_at).unwrap().delay(),
            Some(Duration::from_secs(47))
        );
    }
}
