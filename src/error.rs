//! Crate-wide error type.
//!
//! One [`Error`] is shared by every feature tier so callers can classify
//! failures consistently across raw transport and managed durability work.

use std::time::Duration;

/// A server-supplied error body, with only safe diagnostic fields retained.
///
/// The live contract uses two different JSON error envelope shapes: a simple
/// `{"error": "..."}` form on explicit 4xx/5xx responses, and a
/// `{"code", "errors", "message", "status"}` form on the `default` response
/// case present on almost every operation. Both are folded into this shape.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ErrorDetails {
    /// Correlation ID supplied by the server, when one was present.
    pub remote_request_id: Option<String>,
    /// Correlation ID generated locally for this request.
    pub local_request_id: Option<String>,
    /// Server error code, when the `default` envelope shape was used.
    pub code: Option<i64>,
    /// Server status label, when the `default` envelope shape was used.
    pub status: Option<String>,
    /// Human-readable server message, from either envelope shape.
    pub message: Option<String>,
    /// Per-field validation errors, encoded as compact JSON text.
    pub field_errors: Option<String>,
    /// A bounded, secret-redacted body excerpt.
    pub body_excerpt: Option<String>,
}

/// Failures produced by raw client construction or request execution.
#[non_exhaustive]
pub enum Error {
    /// Client configuration was invalid.
    Configuration {
        /// Description of the invalid configuration.
        message: String,
    },
    /// Authentication is required or failed (HTTP 401).
    Authentication {
        /// Sanitized server error details.
        details: Box<ErrorDetails>,
    },
    /// The server rejected the request due to local or remote rate limiting.
    RateLimited {
        /// Server-supplied delay before the next request should be attempted, when present.
        retry_after: Option<Duration>,
        /// Sanitized server error details.
        details: Box<ErrorDetails>,
    },
    /// A network-level failure occurred before a complete response was read.
    ///
    /// For a mutating (unsafe) request, this failure is definitionally
    /// ambiguous: the request may or may not have reached the server. The
    /// raw client never retries such a request automatically.
    Transport {
        /// Generic, non-sensitive description of the transport failure.
        message: String,
        /// The underlying transport failure, when retaining it is safe.
        ///
        /// This cause is available through [`std::error::Error::source`], but
        /// is deliberately omitted from [`std::fmt::Display`] and [`Debug`]
        /// because transport libraries can include request details.
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
    /// A success response could not be decoded into the expected shape.
    Decode {
        /// Description of the decoding failure.
        message: String,
        /// HTTP status of the response that failed to decode, when available.
        status: Option<u16>,
        /// The underlying decoder failure, when one is available.
        ///
        /// It is exposed only through [`std::error::Error::source`] to keep
        /// display output free of response content.
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
    /// The server returned an HTTP error status not covered by a more
    /// specific category (403, 404, 409, 410, 422, 5xx, or any other
    /// non-2xx status).
    Contract {
        /// HTTP status returned by the server.
        status: u16,
        /// Sanitized server error details.
        details: Box<ErrorDetails>,
    },
    /// The durable store could not be opened or committed.
    Persistence {
        /// Sanitized description of the storage failure.
        message: String,
    },
    /// The authenticated account does not match the account bound to this store.
    AccountStoreMismatch {
        /// Account identifier already bound to the database.
        stored_account_id: String,
        /// Account identifier presented by the current authentication.
        supplied_account_id: String,
    },
    /// A durable operation could not be created, located, or resolved as
    /// requested (for example: an unknown operation ID, a destructive
    /// confirmation mismatch, or a request that would persist material this
    /// client never accepts into the operation journal).
    Operation {
        /// Description of the operation failure.
        message: String,
    },
    /// The managed client has completed shutdown.
    Closed,
}

impl std::fmt::Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Configuration { message } => f
                .debug_struct("Error::Configuration")
                .field("message", message)
                .finish(),
            Self::Authentication { details } => f
                .debug_struct("Error::Authentication")
                .field("details", details)
                .finish(),
            Self::RateLimited {
                retry_after,
                details,
            } => f
                .debug_struct("Error::RateLimited")
                .field("retry_after", retry_after)
                .field("details", details)
                .finish(),
            Self::Transport { message, .. } => f
                .debug_struct("Error::Transport")
                .field("message", message)
                .finish(),
            Self::Decode {
                message, status, ..
            } => f
                .debug_struct("Error::Decode")
                .field("message", message)
                .field("status", status)
                .finish(),
            Self::Contract { status, details } => f
                .debug_struct("Error::Contract")
                .field("status", status)
                .field("details", details)
                .finish(),
            Self::Persistence { message } => f
                .debug_struct("Error::Persistence")
                .field("message", message)
                .finish(),
            Self::AccountStoreMismatch { .. } => {
                f.write_str("Error::AccountStoreMismatch(<redacted>)")
            }
            Self::Operation { message } => f
                .debug_struct("Error::Operation")
                .field("message", message)
                .finish(),
            Self::Closed => f.write_str("Error::Closed"),
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Configuration { message } => write!(f, "invalid client configuration: {message}"),
            Self::Authentication { .. } => write!(f, "authentication failed"),
            Self::RateLimited { .. } => write!(f, "rate limited"),
            Self::Transport { message, .. } => write!(f, "transport failure: {message}"),
            Self::Decode {
                message, status, ..
            } => {
                write!(f, "response decoding failed ({status:?}): {message}")
            }
            Self::Contract { status, details } => {
                write!(f, "unexpected HTTP status {status}")?;
                if let Some(message) = &details.message {
                    write!(f, ": {message}")?;
                }
                Ok(())
            }
            Self::Persistence { message } => write!(f, "durable store failure: {message}"),
            Self::AccountStoreMismatch { .. } => {
                write!(f, "authenticated account does not match the durable store")
            }
            Self::Operation { message } => write!(f, "durable operation failure: {message}"),
            Self::Closed => write!(f, "client is closed"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Transport {
                source: Some(source),
                ..
            }
            | Self::Decode {
                source: Some(source),
                ..
            } => Some(source.as_ref()),
            _ => None,
        }
    }
}

impl Error {
    /// Returns the HTTP status when the failure originated from a response.
    #[must_use]
    pub fn status(&self) -> Option<u16> {
        match self {
            Self::Authentication { .. } => Some(401),
            Self::Decode { status, .. } => *status,
            Self::Contract { status, .. } => Some(*status),
            Self::RateLimited { details, .. } => details
                .code
                .and_then(|code| u16::try_from(code).ok())
                .or(Some(429)),
            Self::Configuration { .. }
            | Self::Transport { .. }
            | Self::Persistence { .. }
            | Self::AccountStoreMismatch { .. }
            | Self::Operation { .. }
            | Self::Closed => None,
        }
    }

    /// Returns a server-provided retry delay when one is available.
    #[must_use]
    pub const fn retry_after(&self) -> Option<Duration> {
        match self {
            Self::RateLimited { retry_after, .. } => *retry_after,
            _ => None,
        }
    }

    /// Returns sanitized server error details when present.
    #[must_use]
    pub fn details(&self) -> Option<&ErrorDetails> {
        match self {
            Self::Authentication { details }
            | Self::RateLimited { details, .. }
            | Self::Contract { details, .. } => Some(details.as_ref()),
            Self::Configuration { .. }
            | Self::Transport { .. }
            | Self::Decode { .. }
            | Self::Persistence { .. }
            | Self::AccountStoreMismatch { .. }
            | Self::Operation { .. }
            | Self::Closed => None,
        }
    }

    /// Reports whether this failure came from a network-level transport
    /// error, meaning a mutating request's outcome is ambiguous: it may or
    /// may not have reached the server.
    #[must_use]
    pub const fn is_ambiguous_transport_failure(&self) -> bool {
        matches!(self, Self::Transport { .. })
    }

    /// Reports whether a mutating request may have executed despite the
    /// client being unable to return a typed success value.
    ///
    /// Besides network-level transport failures, a decoding failure on a
    /// successful `2xx` response is ambiguous: the server accepted and
    /// processed the request, but the client could not understand the success
    /// body. Such mutations must be reconciled rather than classified as
    /// rejected or retried blindly.
    #[must_use]
    pub const fn is_ambiguous_mutation_outcome(&self) -> bool {
        match self {
            Self::Transport { .. } => true,
            Self::Decode {
                status: Some(status),
                ..
            } => *status >= 200 && *status < 300,
            _ => false,
        }
    }
}

/// Convenience alias for results produced by the raw transport.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use super::Error;

    #[test]
    fn source_is_available_without_leaking_through_display() {
        let error = Error::Transport {
            message: "network request failed".into(),
            source: Some(Box::new(std::io::Error::other("Bearer secret-token"))),
        };

        assert!(error.source().is_some());
        assert!(error.source().unwrap().to_string().contains("secret-token"));
        assert!(!error.to_string().contains("secret-token"));
        assert!(!format!("{error:?}").contains("secret-token"));
    }

    #[test]
    fn successful_decode_failure_is_an_ambiguous_mutation_outcome() {
        let successful_decode_failure = Error::Decode {
            message: "live success body evolved".into(),
            status: Some(200),
            source: None,
        };
        let rejected_decode_failure = Error::Decode {
            message: "error body mismatch".into(),
            status: Some(422),
            source: None,
        };

        assert!(successful_decode_failure.is_ambiguous_mutation_outcome());
        assert!(!rejected_decode_failure.is_ambiguous_mutation_outcome());
    }
}
