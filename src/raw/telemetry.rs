//! Typed, best-effort observability emitted by the raw HTTP transport.
//!
//! The transport owns measurement but deliberately does not own persistence.
//! Applications may install an [`ApiTelemetrySink`] to forward samples to
//! SQLite, OpenTelemetry, or another collector. Sinks are synchronous by
//! interface so callers can use a non-blocking queue; implementations must not
//! perform slow I/O from [`ApiTelemetrySink::record`].

use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Terminal outcome observed for one physical HTTP request attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApiAttemptOutcome {
    /// The server returned a successful response and its payload decoded.
    Success,
    /// The server returned a non-success HTTP response.
    HttpError,
    /// No complete HTTP response was received.
    TransportError,
    /// A successful response could not be decoded into the expected contract.
    DecodeError,
    /// The request could not be prepared locally and was not sent.
    PrepareError,
    /// A response body could not be read completely within transport limits.
    BodyError,
}

impl ApiAttemptOutcome {
    /// Stable storage label used by observability backends.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::HttpError => "http_error",
            Self::TransportError => "transport_error",
            Self::DecodeError => "decode_error",
            Self::PrepareError => "prepare_error",
            Self::BodyError => "body_error",
        }
    }
}

/// Timings captured for one physical HTTP request attempt.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ApiAttemptTimings {
    /// Time spent waiting for the local/shared rate-limit permit.
    pub rate_limit_wait_ms: u64,
    /// Time spent constructing the request before network I/O.
    pub request_prepare_ms: Option<u64>,
    /// Time from sending the request until response headers arrived.
    pub time_to_headers_ms: Option<u64>,
    /// Time spent observing/updating response metadata and rate limits.
    pub metadata_ms: Option<u64>,
    /// Time spent reading the response body.
    pub body_read_ms: Option<u64>,
    /// Time spent decoding a successful response body.
    pub decode_ms: Option<u64>,
    /// Total duration of this physical attempt, including rate-limit wait.
    pub elapsed_ms: u64,
}

/// Server rate-limit metadata captured with an HTTP response.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ApiRateLimitTelemetry {
    /// Server-advertised request limit for the current window.
    pub limit: Option<u32>,
    /// Server-advertised requests remaining in the current window.
    pub remaining: Option<u32>,
    /// Server-advertised absolute reset time as Unix epoch seconds.
    pub reset_epoch_seconds: Option<u64>,
    /// Server-requested retry delay.
    pub retry_after_ms: Option<u64>,
}

/// One physical Replicant Space HTTP request attempt.
///
/// `path` is retained for short-lived drill-down data. `route_key` is the
/// bounded-cardinality form intended for long-lived aggregation, for example
/// `v1/devices/{device}/logs`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApiAttemptTelemetry {
    /// Observation timestamp in Unix epoch milliseconds.
    pub observed_at_ms: i64,
    /// Locally generated request ID shared by all retries of one logical call.
    pub local_request_id: String,
    /// Server-generated request ID, when a response supplied one.
    pub server_request_id: Option<String>,
    /// HTTP method.
    pub method: String,
    /// Concrete request path with query parameters removed.
    pub path: String,
    /// Normalized route key suitable for aggregation.
    pub route_key: String,
    /// Stable rate-limit bucket label.
    pub rate_limit_bucket: String,
    /// Scheduling priority used while waiting for a local permit.
    pub priority: String,
    /// One-based attempt number within the logical request.
    pub attempt: u32,
    /// HTTP status when response headers were received.
    pub status_code: Option<u16>,
    /// Terminal outcome for this physical attempt.
    pub outcome: ApiAttemptOutcome,
    /// Stable local classification for a terminal error, when applicable.
    pub error_kind: Option<String>,
    /// Response body size when a body was read.
    pub response_bytes: Option<u64>,
    /// Time since the logical request began, including earlier retries and backoff.
    pub logical_elapsed_ms: u64,
    /// Cumulative retry sleep completed before this attempt.
    pub retry_backoff_ms: u64,
    /// Number of physical attempts executing when this attempt started.
    pub outbound_in_flight: u64,
    /// Captured request timings.
    pub timings: ApiAttemptTimings,
    /// Captured server rate-limit metadata.
    pub rate_limit: ApiRateLimitTelemetry,
}

/// Destination for best-effort raw HTTP telemetry.
///
/// Implementations should enqueue quickly and return. The raw transport calls
/// this method inline with request completion and deliberately ignores sink
/// failures so observability can never become an API availability dependency.
pub trait ApiTelemetrySink: Send + Sync + 'static {
    /// Records one request-attempt sample.
    fn record(&self, sample: ApiAttemptTelemetry);
}

/// Returns a bounded-cardinality route key for a concrete API path.
///
/// Query parameters are always removed. Known entity identifiers are replaced
/// by stable placeholders while operation names remain intact.
#[must_use]
pub fn normalize_route_key(path: &str) -> String {
    let path = path.split('?').next().unwrap_or(path).trim_matches('/');
    let mut segments = path.split('/').map(str::to_owned).collect::<Vec<_>>();
    if segments.is_empty() {
        return String::new();
    }

    replace_after(&mut segments, "devices", "{device}", &["tags"]);
    replace_after(&mut segments, "locations", "{location}", &[]);
    replace_after(&mut segments, "replicants", "{replicant}", &[]);
    replace_after(&mut segments, "achievements", "{achievement}", &[]);
    replace_after(&mut segments, "tutorials", "{tutorial}", &[]);

    for index in 0..segments.len() {
        if segments[index] == "verify" && index > 0 && segments[index - 1] == "accounts" {
            replace_next(&mut segments, index, "{token}", &[]);
        } else if segments[index] == "events" && index >= 2 && segments[index - 2] == "locations" {
            replace_next(&mut segments, index, "{event}", &[]);
        } else if segments[index] == "simulate" && index >= 2 && segments[index - 2] == "devices" {
            replace_next(&mut segments, index, "{simulation}", &["active"]);
        } else if segments[index] == "trades" && index >= 2 && segments[index - 2] == "devices" {
            replace_next(&mut segments, index, "{trade}", &[]);
        } else if segments[index] == "stars" && index >= 2 && segments[index - 2] == "replicants" {
            replace_next(&mut segments, index, "{star}", &[]);
        } else if segments[index] == "simulations"
            && index > 0
            && segments[index - 1] == "leaderboards"
        {
            replace_next(&mut segments, index, "{simulation}", &[]);
        }
    }

    segments.join("/")
}

fn replace_after(
    segments: &mut [String],
    collection: &str,
    placeholder: &str,
    exceptions: &[&str],
) {
    for index in 0..segments.len() {
        if segments[index] == collection {
            replace_next(segments, index, placeholder, exceptions);
        }
    }
}

fn replace_next(segments: &mut [String], index: usize, placeholder: &str, exceptions: &[&str]) {
    let Some(next) = segments.get_mut(index + 1) else {
        return;
    };
    if next.starts_with('{') || exceptions.contains(&next.as_str()) {
        return;
    }
    *next = placeholder.to_owned();
}

pub(crate) fn now_unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

pub(crate) fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::{ApiAttemptOutcome, normalize_route_key};

    #[test]
    fn normalizes_dynamic_api_identifiers_without_collapsing_operations() {
        assert_eq!(
            normalize_route_key("v1/devices/RX-123/logs?limit=10"),
            "v1/devices/{device}/logs"
        );
        assert_eq!(
            normalize_route_key("v1/replicants/Chats-1/stars/SOL"),
            "v1/replicants/{replicant}/stars/{star}"
        );
        assert_eq!(
            normalize_route_key("v1/locations/SOL-3-L4/events/evt-123"),
            "v1/locations/{location}/events/{event}"
        );
        assert_eq!(
            normalize_route_key("v1/devices/tags/mining"),
            "v1/devices/tags/mining"
        );
        assert_eq!(normalize_route_key("v1/health"), "v1/health");
        assert_eq!(
            normalize_route_key("v1/accounts/verify/private-token"),
            "v1/accounts/verify/{token}"
        );
    }

    #[test]
    fn outcome_labels_are_stable() {
        assert_eq!(ApiAttemptOutcome::Success.as_str(), "success");
        assert_eq!(ApiAttemptOutcome::HttpError.as_str(), "http_error");
    }
}
