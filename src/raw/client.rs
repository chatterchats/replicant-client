//! The raw, unmanaged HTTP client and its shared request execution pipeline.
//!
//! [`Client`] returns transport DTOs and [`ResponseMetadata`] only. It never
//! hydrates, persists, publishes, journals operations, or reconciles state —
//! those are managed-client concerns built on top of this transport in a
//! later phase.

use std::{
    fmt,
    sync::{Arc, RwLock},
    time::{Duration, SystemTime},
};

use reqwest::{
    Method,
    header::{AUTHORIZATION, HeaderMap, HeaderValue, USER_AGENT},
};
use secrecy::ExposeSecret;
use serde::{Serialize, de::DeserializeOwned};
use uuid::Uuid;

pub use reqwest::{StatusCode, Url};
pub use secrecy::SecretString;

use crate::error::{Error, ErrorDetails};
use crate::raw::rate_limit::{
    RateLimitBucket, RateLimitCoordinator, RateLimitSnapshot, bucket_for,
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
    /// Timeout for a complete request/response cycle.
    pub request_timeout: Duration,
    /// User-Agent header sent with every request.
    pub user_agent: String,
    /// Retry policy applied to safe-read requests.
    pub retry: RetryPolicy,
    /// TLS backend for internally built HTTP clients.
    pub tls_backend: TlsBackend,
    /// Maximum response body size accepted, in bytes.
    pub max_response_body_bytes: usize,
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

    /// Sets the complete request timeout.
    #[must_use]
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
            let builder = reqwest::Client::builder()
                .connect_timeout(self.config.connect_timeout)
                .timeout(self.config.request_timeout);
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
            }),
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
}

pub(crate) struct ClientInner {
    pub(crate) http: reqwest::Client,
    pub(crate) base_url: Url,
    pub(crate) tokens: Arc<dyn TokenProvider>,
    pub(crate) rate_limits: RateLimitCoordinator,
    pub(crate) config: ClientConfig,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("base_url", &self.inner.base_url)
            .finish_non_exhaustive()
    }
}

impl Client {
    /// Starts configuring a raw client.
    #[must_use]
    pub fn builder() -> ClientBuilder {
        ClientBuilder::new()
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
        self.execute_bytes(method, path, authenticated, safety, None)
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
        self.execute_bytes(method, path, authenticated, safety, Some(bytes))
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
        let url = request_url(&self.inner.base_url, &path)?;
        let token = self
            .inner
            .tokens
            .token()
            .ok_or_else(|| Error::Configuration {
                message: "authentication token is required for this endpoint".into(),
            })?;
        let authorization = HeaderValue::from_str(&format!("Bearer {}", token.expose_secret()))
            .map_err(|_| Error::Configuration {
                message: "bearer token contains invalid header bytes".into(),
            })?;
        self.inner.rate_limits.acquire(RateLimitBucket::Sse).await;
        let response = self
            .inner
            .http
            .get(url)
            .header(USER_AGENT, &self.inner.config.user_agent)
            .header(AUTHORIZATION, authorization)
            .header(reqwest::header::ACCEPT, "text/event-stream")
            .send()
            .await
            .map_err(map_reqwest_error)?;
        if response.status().is_success() {
            return Ok(response);
        }
        let status = response.status();
        let headers = response.headers().clone();
        let request_id = RequestId::new();
        let metadata = ResponseMetadata {
            status,
            request_id: header_string(&headers, "x-request-id")
                .or_else(|| header_string(&headers, "request-id")),
            local_request_id: request_id,
            rate_limit: parse_rate_limit(&headers),
        };
        let bytes = read_bounded(response, self.inner.config.max_response_body_bytes).await?;
        Err(map_status(status, &bytes, &metadata))
    }

    async fn execute_bytes<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        authenticated: bool,
        safety: RequestSafety,
        body: Option<Vec<u8>>,
    ) -> Result<RawResponse<T>, Error> {
        let request_id = RequestId::new();
        let bucket = bucket_for(&method, path);
        let mut attempts: u32 = 0;
        loop {
            self.inner.rate_limits.acquire(bucket).await;
            attempts += 1;
            let result = self
                .send_once::<T>(
                    &method,
                    path,
                    authenticated,
                    &request_id,
                    bucket,
                    body.clone(),
                )
                .await;
            match result {
                Ok(response) => return Ok(response),
                Err(error) if self.should_retry(&error, safety, attempts) => {
                    let delay =
                        retry_delay(&self.inner.config.retry, attempts, error.retry_after());
                    if self.inner.config.emit_tracing {
                        tracing::debug!(
                            path,
                            local_request_id = %request_id,
                            attempts,
                            delay_ms = delay.as_millis(),
                            "retrying raw request"
                        );
                    }
                    tokio::time::sleep(delay).await;
                }
                Err(error) => {
                    if self.inner.config.emit_tracing {
                        tracing::debug!(path, local_request_id = %request_id, error = %error, "raw request failed");
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
        method: &Method,
        path: &str,
        authenticated: bool,
        request_id: &RequestId,
        bucket: RateLimitBucket,
        body: Option<Vec<u8>>,
    ) -> Result<RawResponse<T>, Error> {
        let url = request_url(&self.inner.base_url, path)?;
        let mut request = self
            .inner
            .http
            .request(method.clone(), url)
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
        if let Some(body) = body {
            request = request
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(body);
        }
        let response = request.send().await.map_err(map_reqwest_error)?;
        let status = response.status();
        let headers = response.headers().clone();
        let rate_limit = parse_rate_limit(&headers);
        if let Some(snapshot) = rate_limit.clone() {
            self.inner.rate_limits.observe(bucket, snapshot).await;
        }
        let bytes = read_bounded(response, self.inner.config.max_response_body_bytes).await?;
        let metadata = ResponseMetadata {
            status,
            request_id: header_string(&headers, "x-request-id")
                .or_else(|| header_string(&headers, "request-id")),
            local_request_id: request_id.clone(),
            rate_limit,
        };
        if !status.is_success() {
            return Err(map_status(status, &bytes, &metadata));
        }
        let value = serde_json::from_slice(if bytes.is_empty() { b"null" } else { &bytes })
            .map_err(|error| Error::Decode {
                message: error.to_string(),
                status: Some(status.as_u16()),
            })?;
        Ok(RawResponse { value, metadata })
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

fn map_reqwest_error(error: reqwest::Error) -> Error {
    if error.is_timeout() {
        Error::Transport {
            message: "request timed out".into(),
        }
    } else {
        Error::Transport {
            message: "network request failed".into(),
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
        });
    }
    let status = response.status();
    let bytes = response.bytes().await.map_err(map_reqwest_error)?;
    if bytes.len() > limit {
        return Err(Error::Decode {
            message: format!("response body exceeds {limit} bytes"),
            status: Some(status.as_u16()),
        });
    }
    Ok(bytes.to_vec())
}

fn parse_rate_limit(headers: &HeaderMap) -> Option<RateLimitSnapshot> {
    let retry_after =
        header_string(headers, "retry-after").and_then(|value| parse_retry_after(&value));
    let limit = header_string(headers, "x-ratelimit-limit").and_then(|value| value.parse().ok());
    let remaining =
        header_string(headers, "x-ratelimit-remaining").and_then(|value| value.parse().ok());
    let reset_after = header_string(headers, "x-ratelimit-reset")
        .and_then(|value| value.parse::<u64>().ok().map(Duration::from_secs));
    if retry_after.is_none() && limit.is_none() && remaining.is_none() && reset_after.is_none() {
        None
    } else {
        Some(RateLimitSnapshot {
            limit,
            remaining,
            reset_after,
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
            .and_then(|value| value.retry_after);
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
}
