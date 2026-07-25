//! Shared, token-scoped request rate limiting.
//!
//! One coordinator is shared by every clone of a [`crate::raw::Client`] (and,
//! later, by the managed scheduler built on top of it). It enforces the
//! contract's documented budgets locally and folds in server-observed
//! `X-RateLimit-*`/`Retry-After` state, which always wins over the local
//! estimate.

use std::{collections::HashMap, sync::Arc, time::Duration};

use tokio::sync::Mutex;
use tokio::time::Instant;

/// Logical request budgets shared across every raw operation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum RateLimitBucket {
    /// Authenticated read (GET/HEAD) requests: 120 per minute.
    Read,
    /// Authenticated state-changing requests: 60 per minute.
    Action,
    /// `POST /v1/accounts`: 10 per hour.
    Registration,
    /// `GET /v1/accounts/verify/{token}`: 30 per hour.
    Verification,
    /// `POST /v1/feedback`: 10 per hour.
    Feedback,
    /// `GET /v1/stars`: 1 per minute.
    StarCatalogue,
    /// The SSE event stream connection.
    Sse,
}

/// Local policy for one rate-limit bucket.
#[derive(Clone, Copy, Debug)]
pub struct RateLimitPolicy {
    /// Number of permits available per refill window.
    pub capacity: u32,
    /// Duration of one refill window.
    pub refill_every: Duration,
}

impl RateLimitPolicy {
    const fn new(capacity: u32, refill_every: Duration) -> Self {
        Self {
            capacity,
            refill_every,
        }
    }

    fn default_for(bucket: RateLimitBucket) -> Self {
        match bucket {
            RateLimitBucket::Read => Self::new(120, Duration::from_secs(60)),
            RateLimitBucket::Action | RateLimitBucket::Sse => {
                Self::new(60, Duration::from_secs(60))
            }
            RateLimitBucket::Registration => Self::new(10, Duration::from_secs(3600)),
            RateLimitBucket::Verification => Self::new(30, Duration::from_secs(3600)),
            RateLimitBucket::Feedback => Self::new(10, Duration::from_secs(3600)),
            RateLimitBucket::StarCatalogue => Self::new(1, Duration::from_secs(60)),
        }
    }
}

/// Most recent rate-limit information observed from the server for a bucket.
#[derive(Clone, Debug)]
pub struct RateLimitSnapshot {
    /// Total permits allowed per window, as reported by the server.
    pub limit: Option<u32>,
    /// Permits remaining in the current window, as reported by the server.
    pub remaining: Option<u32>,
    /// Time until the server's window resets.
    pub reset_after: Option<Duration>,
    /// Server-mandated delay before the next request, if given.
    pub retry_after: Option<Duration>,
}

#[derive(Debug)]
struct Bucket {
    next: Instant,
    policy: RateLimitPolicy,
    server: Option<RateLimitSnapshot>,
}

/// In-process rate-limit coordinator shared by all clones of a raw client.
#[derive(Clone, Debug)]
pub struct RateLimitCoordinator {
    buckets: Arc<Mutex<HashMap<RateLimitBucket, Bucket>>>,
}

const ALL_BUCKETS: [RateLimitBucket; 7] = [
    RateLimitBucket::Read,
    RateLimitBucket::Action,
    RateLimitBucket::Registration,
    RateLimitBucket::Verification,
    RateLimitBucket::Feedback,
    RateLimitBucket::StarCatalogue,
    RateLimitBucket::Sse,
];

impl RateLimitCoordinator {
    /// Creates a coordinator using the documented contract defaults for
    /// every bucket.
    #[must_use]
    pub fn new() -> Self {
        let now = Instant::now();
        let buckets = ALL_BUCKETS
            .into_iter()
            .map(|kind| {
                (
                    kind,
                    Bucket {
                        next: now,
                        policy: RateLimitPolicy::default_for(kind),
                        server: None,
                    },
                )
            })
            .collect();
        Self {
            buckets: Arc::new(Mutex::new(buckets)),
        }
    }

    /// Overrides a bucket's local policy.
    pub async fn set_policy(&self, kind: RateLimitBucket, policy: RateLimitPolicy) {
        if let Some(bucket) = self.buckets.lock().await.get_mut(&kind) {
            bucket.policy = policy;
        }
    }

    /// Waits for a permit. Dropping the future cancels waiting without
    /// consuming a future permit.
    pub async fn acquire(&self, kind: RateLimitBucket) {
        loop {
            let wait = {
                let mut all = self.buckets.lock().await;
                let Some(bucket) = all.get_mut(&kind) else {
                    return;
                };
                let now = Instant::now();
                if bucket.next <= now {
                    let spacing = bucket.policy.refill_every / bucket.policy.capacity.max(1);
                    bucket.next = now + spacing;
                    return;
                }
                bucket.next.saturating_duration_since(now)
            };
            tracing::debug!(
                ?kind,
                queue_wait_ms = wait.as_millis(),
                "waiting for rate-limit permit"
            );
            tokio::time::sleep(wait).await;
        }
    }

    /// Records authoritative server limits; a server retry/reset delay wins
    /// over the local estimate, including when it shortens it.
    pub async fn observe(&self, kind: RateLimitBucket, snapshot: RateLimitSnapshot) {
        let mut all = self.buckets.lock().await;
        if let Some(bucket) = all.get_mut(&kind) {
            if let Some(delay) = snapshot.retry_after.or(snapshot.reset_after) {
                bucket.next = Instant::now() + delay;
            }
            bucket.server = Some(snapshot);
        }
    }

    /// Returns the most recently observed server information for a bucket.
    pub async fn snapshot(&self, kind: RateLimitBucket) -> Option<RateLimitSnapshot> {
        self.buckets
            .lock()
            .await
            .get(&kind)
            .and_then(|bucket| bucket.server.clone())
    }
}

impl Default for RateLimitCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

/// Selects the rate-limit bucket for a request.
#[must_use]
pub fn bucket_for(method: &reqwest::Method, path: &str) -> RateLimitBucket {
    if path.starts_with("v1/stars") && *method == reqwest::Method::GET {
        RateLimitBucket::StarCatalogue
    } else if path == "v1/accounts" && *method == reqwest::Method::POST {
        RateLimitBucket::Registration
    } else if path.starts_with("v1/accounts/verify/") && *method == reqwest::Method::GET {
        RateLimitBucket::Verification
    } else if path == "v1/feedback" && *method == reqwest::Method::POST {
        RateLimitBucket::Feedback
    } else if matches!(*method, reqwest::Method::GET | reqwest::Method::HEAD) {
        RateLimitBucket::Read
    } else {
        RateLimitBucket::Action
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::task::yield_now;

    use super::{RateLimitBucket, RateLimitCoordinator, RateLimitPolicy, RateLimitSnapshot};

    #[tokio::test(start_paused = true)]
    async fn cancelled_wait_does_not_consume_a_future_permit() {
        let coordinator = RateLimitCoordinator::new();
        coordinator
            .set_policy(
                RateLimitBucket::Read,
                RateLimitPolicy {
                    capacity: 1,
                    refill_every: Duration::from_secs(10),
                },
            )
            .await;
        coordinator.acquire(RateLimitBucket::Read).await;
        let waiting = tokio::spawn({
            let coordinator = coordinator.clone();
            async move { coordinator.acquire(RateLimitBucket::Read).await }
        });
        yield_now().await;
        waiting.abort();
        let _ = waiting.await;

        tokio::time::advance(Duration::from_secs(10)).await;
        coordinator.acquire(RateLimitBucket::Read).await;
    }

    #[tokio::test(start_paused = true)]
    async fn server_reset_overrides_a_longer_local_estimate() {
        let coordinator = RateLimitCoordinator::new();
        coordinator
            .set_policy(
                RateLimitBucket::Read,
                RateLimitPolicy {
                    capacity: 1,
                    refill_every: Duration::from_secs(60),
                },
            )
            .await;
        coordinator.acquire(RateLimitBucket::Read).await;
        coordinator
            .observe(
                RateLimitBucket::Read,
                RateLimitSnapshot {
                    limit: Some(1),
                    remaining: Some(0),
                    reset_after: Some(Duration::from_secs(5)),
                    retry_after: None,
                },
            )
            .await;

        let permit = tokio::spawn({
            let coordinator = coordinator.clone();
            async move { coordinator.acquire(RateLimitBucket::Read).await }
        });
        yield_now().await;
        assert!(!permit.is_finished());
        tokio::time::advance(Duration::from_secs(5)).await;
        yield_now().await;
        assert!(permit.is_finished());
    }

    #[test]
    fn special_endpoint_windows_are_bucketed_by_route() {
        use reqwest::Method;

        assert_eq!(
            super::bucket_for(&Method::POST, "v1/accounts"),
            RateLimitBucket::Registration
        );
        assert_eq!(
            super::bucket_for(&Method::GET, "v1/accounts/verify/abc123"),
            RateLimitBucket::Verification
        );
        assert_eq!(
            super::bucket_for(&Method::POST, "v1/feedback"),
            RateLimitBucket::Feedback
        );
        assert_eq!(
            super::bucket_for(&Method::GET, "v1/stars"),
            RateLimitBucket::StarCatalogue
        );
        assert_eq!(
            super::bucket_for(&Method::GET, "v1/devices"),
            RateLimitBucket::Read
        );
        assert_eq!(
            super::bucket_for(&Method::POST, "v1/devices/RX-1"),
            RateLimitBucket::Action
        );
    }
}
