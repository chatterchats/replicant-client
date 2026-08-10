//! Shared, token-scoped request rate limiting.
//!
//! One coordinator is shared by every clone of a [`crate::raw::Client`].
//! Managed file-backed clients additionally coordinate peer processes through
//! a small SQLite schedule. It enforces the contract's documented budgets and
//! retains server-observed
//! `X-RateLimit-*`/`Retry-After` state. Server countdowns become mandatory
//! only after an actual 429 response or when the reported window is exhausted;
//! successful responses with remaining quota keep the normal local schedule.

use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[cfg(feature = "shared-rate-limit")]
use std::{
    path::{Path, PathBuf},
    sync::{Mutex as StdMutex, RwLock as StdRwLock},
};

#[cfg(feature = "shared-rate-limit")]
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
#[cfg(feature = "shared-rate-limit")]
use tracing::warn;

use tokio::sync::Mutex;
use tokio::time::Instant;
use tracing::debug;

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

/// Scheduling class for work sharing a rate-limit bucket.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RequestPriority {
    /// User-initiated work.
    #[default]
    Foreground,
    /// Synchronization and reconciliation work that yields to user work.
    Background,
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

/// A server-mandated relative delay from `Retry-After`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetryAfter(Duration);

impl RetryAfter {
    /// Creates a relative delay.
    #[must_use]
    pub const fn new(delay: Duration) -> Self {
        Self(delay)
    }

    /// Returns the delay.
    #[must_use]
    pub const fn delay(self) -> Duration {
        self.0
    }
}

/// An absolute Unix-epoch rate-limit reset observed from the server.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RateLimitReset {
    epoch_seconds: u64,
    delay: Duration,
}

impl RateLimitReset {
    /// Converts epoch seconds to a delay from the captured observation time.
    /// Past values are immediately eligible.
    #[must_use]
    pub fn from_epoch_seconds(epoch_seconds: u64, observed_at: SystemTime) -> Option<Self> {
        let reset_at = UNIX_EPOCH.checked_add(Duration::from_secs(epoch_seconds))?;
        Some(Self {
            epoch_seconds,
            delay: reset_at.duration_since(observed_at).unwrap_or_default(),
        })
    }

    /// Returns the server-provided Unix epoch seconds.
    #[must_use]
    pub const fn epoch_seconds(self) -> u64 {
        self.epoch_seconds
    }

    /// Returns the nonnegative delay calculated at observation time.
    #[must_use]
    pub const fn delay(self) -> Duration {
        self.delay
    }
}

/// Most recent rate-limit information observed from the server for a bucket.
#[derive(Clone, Debug)]
pub struct RateLimitSnapshot {
    /// Total permits allowed per window, as reported by the server.
    pub limit: Option<u32>,
    /// Permits remaining in the current window, as reported by the server.
    pub remaining: Option<u32>,
    /// Absolute server reset time and its delay from the captured observation.
    pub reset: Option<RateLimitReset>,
    /// Server-mandated delay before the next request, if given.
    pub retry_after: Option<RetryAfter>,
}

impl RateLimitSnapshot {
    /// Returns the candidate delay to enforce when the response is actually
    /// rate limited or the reported window has no remaining permits. HTTP's
    /// explicit `Retry-After` wins over the rate-window reset estimate.
    #[must_use]
    pub fn delay(&self) -> Option<Duration> {
        self.retry_after
            .map(RetryAfter::delay)
            .or_else(|| self.reset.map(RateLimitReset::delay))
    }
}

#[derive(Debug)]
struct Bucket {
    next: Instant,
    policy: RateLimitPolicy,
    server: Option<RateLimitSnapshot>,
}

#[derive(Debug, Default)]
struct RequestQueue {
    foreground: VecDeque<Arc<AtomicBool>>,
    background: VecDeque<Arc<AtomicBool>>,
    foreground_streak: u8,
}

struct QueueTicket(Arc<AtomicBool>);

impl Drop for QueueTicket {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// Rate-limit coordinator shared by all clones of a raw client, with optional
/// cross-process SQLite scheduling.
#[derive(Clone, Debug)]
pub struct RateLimitCoordinator {
    buckets: Arc<Mutex<HashMap<RateLimitBucket, Bucket>>>,
    queues: Arc<Mutex<HashMap<RateLimitBucket, RequestQueue>>>,
    #[cfg(feature = "shared-rate-limit")]
    shared: Arc<StdRwLock<Option<SharedRateLimit>>>,
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
            queues: Arc::new(Mutex::new(HashMap::new())),
            #[cfg(feature = "shared-rate-limit")]
            shared: Arc::new(StdRwLock::new(None)),
        }
    }

    /// Overrides a bucket's local policy.
    pub async fn set_policy(&self, kind: RateLimitBucket, policy: RateLimitPolicy) {
        if let Some(bucket) = self.buckets.lock().await.get_mut(&kind) {
            bucket.policy = policy;
        }
    }

    /// Enables cross-process coordination through a small SQLite database.
    ///
    /// The database contains only anonymous permit timestamps and server block
    /// deadlines. It is intentionally separate from the managed state database
    /// so request scheduling cannot contend with projection/journal writes.
    #[cfg(feature = "shared-rate-limit")]
    pub async fn enable_shared_sqlite(&self, path: impl AsRef<Path>) -> Result<(), String> {
        let shared = SharedRateLimit::open(path.as_ref()).await?;
        *self.shared.write().expect("shared rate-limit lock poisoned") = Some(shared);
        Ok(())
    }

    /// Changes the anonymous scope used by the shared permit ledger. Managed
    /// clients set this to the authenticated account identity after login.
    #[cfg(feature = "shared-rate-limit")]
    pub fn set_shared_scope(&self, scope: impl Into<String>) {
        let scope = scope.into();
        let mut hash = 0xcbf29ce484222325_u64;
        for byte in scope.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        if let Some(shared) = self
            .shared
            .read()
            .expect("shared rate-limit lock poisoned")
            .as_ref()
        {
            shared.set_scope(format!("account:{hash:016x}"));
        }
    }

    /// Waits for a permit. Dropping the future cancels waiting without
    /// consuming a future in-process permit. A cross-process permit may already
    /// have been recorded if cancellation happens during its final sleep.
    pub async fn acquire(&self, kind: RateLimitBucket) {
        self.acquire_with_priority(kind, RequestPriority::Foreground)
            .await;
    }

    /// Waits for a permit. Queued foreground work runs first, except that a
    /// background request gets one turn after eight foreground permits.
    pub async fn acquire_with_priority(&self, kind: RateLimitBucket, priority: RequestPriority) {
        let ticket = QueueTicket(Arc::new(AtomicBool::new(true)));
        {
            let mut queues = self.queues.lock().await;
            let queue = queues.entry(kind).or_default();
            match priority {
                RequestPriority::Foreground => queue.foreground.push_back(ticket.0.clone()),
                RequestPriority::Background => queue.background.push_back(ticket.0.clone()),
            }
        }
        loop {
            let permitted = {
                let mut queues = self.queues.lock().await;
                let queue = queues.entry(kind).or_default();
                queue
                    .foreground
                    .retain(|entry| entry.load(Ordering::Acquire));
                queue
                    .background
                    .retain(|entry| entry.load(Ordering::Acquire));
                let foreground_turn = !queue.foreground.is_empty()
                    && (queue.background.is_empty() || queue.foreground_streak < 8);
                let next = if foreground_turn {
                    queue.foreground.front()
                } else {
                    queue.background.front()
                };
                next.is_some_and(|entry| Arc::ptr_eq(entry, &ticket.0))
            };
            if !permitted {
                tokio::task::yield_now().await;
                continue;
            }
            let wait = {
                let mut all = self.buckets.lock().await;
                let Some(bucket) = all.get_mut(&kind) else {
                    return;
                };
                let now = Instant::now();
                if bucket.next <= now {
                    let spacing = bucket.policy.refill_every / bucket.policy.capacity.max(1);
                    bucket.next = now + spacing;
                    drop(all);
                    let mut queues = self.queues.lock().await;
                    let queue = queues.entry(kind).or_default();
                    let foreground_turn = !queue.foreground.is_empty()
                        && (queue.background.is_empty() || queue.foreground_streak < 8);
                    if foreground_turn {
                        queue.foreground.pop_front();
                        queue.foreground_streak = queue.foreground_streak.saturating_add(1);
                    } else {
                        queue.background.pop_front();
                        queue.foreground_streak = 0;
                    }
                    break;
                }
                bucket.next.saturating_duration_since(now)
            };
            debug!(
                target: "replicant_client::raw::rate_limit",
                event = "rate_limit.wait",
                ?kind,
                delay_ms = wait.as_millis() as u64,
                "waiting for rate-limit permit"
            );
            tokio::time::sleep(wait).await;
        }

        #[cfg(feature = "shared-rate-limit")]
        if let Some(shared) = self.shared.read().expect("shared rate-limit lock poisoned").clone() {
            let policy = self
                .buckets
                .lock()
                .await
                .get(&kind)
                .map_or_else(|| RateLimitPolicy::default_for(kind), |bucket| bucket.policy);
            if let Err(error) = shared.acquire(kind, policy).await {
                warn!(
                    target: "replicant_client::raw::rate_limit",
                    event = "rate_limit.shared_failed",
                    ?kind,
                    error = %error,
                    "shared SQLite rate limiter failed; continuing with in-process limiting"
                );
            }
        }
    }

    /// Records server rate-limit metadata from a successful response.
    ///
    /// Some Replicant Space success responses include `Retry-After` and reset
    /// headers as informational countdowns for the current window. Those
    /// values do not pause a bucket while permits remain. A successful
    /// response delays future work only when the server reports zero permits.
    pub async fn observe(&self, kind: RateLimitBucket, snapshot: RateLimitSnapshot) {
        self.observe_response(kind, reqwest::StatusCode::OK, snapshot)
            .await;
    }

    pub(crate) async fn observe_response(
        &self,
        kind: RateLimitBucket,
        status: reqwest::StatusCode,
        snapshot: RateLimitSnapshot,
    ) {
        let exhausted = snapshot.remaining == Some(0);
        let enforce_delay = status == reqwest::StatusCode::TOO_MANY_REQUESTS || exhausted;
        let enforced_delay = enforce_delay.then(|| snapshot.delay()).flatten();
        {
            let mut all = self.buckets.lock().await;
            if let Some(bucket) = all.get_mut(&kind) {
                if let Some(delay) = enforced_delay {
                    debug!(
                        target: "replicant_client::raw::rate_limit",
                        event = "rate_limit.schedule_updated",
                        ?kind,
                        status = status.as_u16(),
                        remaining = ?snapshot.remaining,
                        delay_ms = delay.as_millis() as u64,
                        "enforced server rate-limit delay"
                    );
                    bucket.next = Instant::now() + delay;
                } else if snapshot.delay().is_some() {
                    debug!(
                        target: "replicant_client::raw::rate_limit",
                        event = "rate_limit.delay_informational",
                        ?kind,
                        status = status.as_u16(),
                        remaining = ?snapshot.remaining,
                        retry_after_ms = ?snapshot
                            .retry_after
                            .map(|value| value.delay().as_millis() as u64),
                        reset_delay_ms = ?snapshot
                            .reset
                            .map(|value| value.delay().as_millis() as u64),
                        "retained informational rate-limit countdown without delaying requests"
                    );
                }
                bucket.server = Some(snapshot);
            }
        }

        #[cfg(feature = "shared-rate-limit")]
        {
            let shared = self
                .shared
                .read()
                .expect("shared rate-limit lock poisoned")
                .clone();
            if let (Some(delay), Some(shared)) = (enforced_delay, shared)
                && let Err(error) = shared.block_for(kind, delay).await
            {
                warn!(
                    target: "replicant_client::raw::rate_limit",
                    event = "rate_limit.shared_observe_failed",
                    ?kind,
                    error = %error,
                    "could not publish server rate-limit delay to peer processes"
                );
            }
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


#[cfg(feature = "shared-rate-limit")]
#[derive(Clone)]
struct SharedRateLimit {
    path: PathBuf,
    connection: Arc<StdMutex<Connection>>,
    scope: Arc<StdRwLock<String>>,
}

#[cfg(feature = "shared-rate-limit")]
impl std::fmt::Debug for SharedRateLimit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SharedRateLimit")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "shared-rate-limit")]
impl SharedRateLimit {
    async fn open(path: &Path) -> Result<Self, String> {
        let path = path.to_path_buf();
        let opened_path = path.clone();
        let connection = tokio::task::spawn_blocking(move || -> Result<Connection, String> {
            if let Some(parent) = opened_path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
            {
                std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
            }
            let connection = Connection::open(&opened_path).map_err(|error| error.to_string())?;
            connection
                .busy_timeout(Duration::from_secs(5))
                .map_err(|error| error.to_string())?;
            connection
                .pragma_update(None, "journal_mode", "WAL")
                .map_err(|error| error.to_string())?;
            connection
                .pragma_update(None, "synchronous", "NORMAL")
                .map_err(|error| error.to_string())?;
            connection
                .execute_batch(
                    "CREATE TABLE IF NOT EXISTS rate_limit_schedule (\
                       scope TEXT NOT NULL,\
                       bucket TEXT NOT NULL,\
                       next_permit_ms INTEGER NOT NULL DEFAULT 0,\
                       blocked_until_ms INTEGER NOT NULL DEFAULT 0,\
                       PRIMARY KEY(scope, bucket)\
                     );",
                )
                .map_err(|error| error.to_string())?;
            Ok(connection)
        })
        .await
        .map_err(|error| error.to_string())??;
        Ok(Self {
            path,
            connection: Arc::new(StdMutex::new(connection)),
            scope: Arc::new(StdRwLock::new("bootstrap".into())),
        })
    }

    fn set_scope(&self, scope: String) {
        *self
            .scope
            .write()
            .expect("shared rate-limit scope lock poisoned") = scope;
    }

    async fn acquire(&self, kind: RateLimitBucket, policy: RateLimitPolicy) -> Result<(), String> {
        let connection = Arc::clone(&self.connection);
        let scope = self
            .scope
            .read()
            .expect("shared rate-limit scope lock poisoned")
            .clone();
        let bucket = bucket_name(kind).to_owned();
        let spacing_ms = duration_millis_i64(policy.refill_every)
            .checked_div(i64::from(policy.capacity.max(1)))
            .unwrap_or(0)
            .max(1);
        let wait_ms = tokio::task::spawn_blocking(move || -> Result<i64, String> {
            let now_ms = unix_millis();
            let mut connection = connection
                .lock()
                .map_err(|_| "shared rate-limit SQLite lock poisoned".to_owned())?;
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|error| error.to_string())?;
            let current = transaction
                .query_row(
                    "SELECT next_permit_ms, blocked_until_ms FROM rate_limit_schedule \
                     WHERE scope = ?1 AND bucket = ?2",
                    params![&scope, &bucket],
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
                )
                .optional()
                .map_err(|error| error.to_string())?;
            let (next_permit_ms, blocked_until_ms) = current.unwrap_or((0, 0));
            let grant_at_ms = now_ms.max(next_permit_ms).max(blocked_until_ms);
            let following_permit_ms = grant_at_ms.saturating_add(spacing_ms);
            transaction
                .execute(
                    "INSERT INTO rate_limit_schedule(\
                       scope, bucket, next_permit_ms, blocked_until_ms\
                     ) VALUES (?1, ?2, ?3, ?4) \
                     ON CONFLICT(scope, bucket) DO UPDATE SET \
                       next_permit_ms = excluded.next_permit_ms",
                    params![&scope, &bucket, following_permit_ms, blocked_until_ms],
                )
                .map_err(|error| error.to_string())?;
            transaction.commit().map_err(|error| error.to_string())?;
            Ok(grant_at_ms.saturating_sub(now_ms))
        })
        .await
        .map_err(|error| error.to_string())??;

        if wait_ms > 0 {
            debug!(
                target: "replicant_client::raw::rate_limit",
                event = "rate_limit.shared_wait",
                ?kind,
                path = %self.path.display(),
                delay_ms = wait_ms as u64,
                "waiting for cross-process rate-limit permit"
            );
            tokio::time::sleep(Duration::from_millis(wait_ms as u64)).await;
        }
        Ok(())
    }

    async fn block_for(&self, kind: RateLimitBucket, delay: Duration) -> Result<(), String> {
        let connection = Arc::clone(&self.connection);
        let scope = self
            .scope
            .read()
            .expect("shared rate-limit scope lock poisoned")
            .clone();
        let bucket = bucket_name(kind).to_owned();
        let blocked_until_ms = unix_millis().saturating_add(duration_millis_i64(delay));
        tokio::task::spawn_blocking(move || -> Result<(), String> {
            let connection = connection
                .lock()
                .map_err(|_| "shared rate-limit SQLite lock poisoned".to_owned())?;
            connection
                .execute(
                    "INSERT INTO rate_limit_schedule(\
                       scope, bucket, next_permit_ms, blocked_until_ms\
                     ) VALUES (?1, ?2, ?3, ?3) \
                     ON CONFLICT(scope, bucket) DO UPDATE SET \
                       blocked_until_ms = MAX(blocked_until_ms, excluded.blocked_until_ms), \
                       next_permit_ms = MAX(next_permit_ms, excluded.blocked_until_ms)",
                    params![&scope, &bucket, blocked_until_ms],
                )
                .map_err(|error| error.to_string())?;
            Ok(())
        })
        .await
        .map_err(|error| error.to_string())??;
        Ok(())
    }
}

#[cfg(feature = "shared-rate-limit")]
fn unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

#[cfg(feature = "shared-rate-limit")]
fn duration_millis_i64(duration: Duration) -> i64 {
    duration.as_millis().try_into().unwrap_or(i64::MAX)
}

#[cfg(feature = "shared-rate-limit")]
const fn bucket_name(kind: RateLimitBucket) -> &'static str {
    match kind {
        RateLimitBucket::Read => "read",
        RateLimitBucket::Action => "action",
        RateLimitBucket::Registration => "registration",
        RateLimitBucket::Verification => "verification",
        RateLimitBucket::Feedback => "feedback",
        RateLimitBucket::StarCatalogue => "star_catalogue",
        RateLimitBucket::Sse => "sse",
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

    use tokio::{sync::mpsc, task::yield_now};

    use super::{
        RateLimitBucket, RateLimitCoordinator, RateLimitPolicy, RateLimitReset, RateLimitSnapshot,
    };

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
    async fn queued_foreground_request_precedes_background_work() {
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

        let (sent, mut received) = mpsc::unbounded_channel();
        for (priority, name) in [
            (super::RequestPriority::Background, "background"),
            (super::RequestPriority::Foreground, "foreground"),
        ] {
            let coordinator = coordinator.clone();
            let sent = sent.clone();
            tokio::spawn(async move {
                coordinator
                    .acquire_with_priority(RateLimitBucket::Read, priority)
                    .await;
                sent.send(name).expect("receiver remains open");
            });
            yield_now().await;
        }

        tokio::time::advance(Duration::from_secs(10)).await;
        assert_eq!(received.recv().await, Some("foreground"));
        tokio::time::advance(Duration::from_secs(10)).await;
        assert_eq!(received.recv().await, Some("background"));
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
                    reset: Some(RateLimitReset {
                        epoch_seconds: 0,
                        delay: Duration::from_secs(5),
                    }),
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

    #[tokio::test(start_paused = true)]
    async fn successful_response_with_remaining_quota_does_not_wait_until_reset() {
        let coordinator = RateLimitCoordinator::new();
        coordinator
            .set_policy(
                RateLimitBucket::Read,
                RateLimitPolicy {
                    capacity: 120,
                    refill_every: Duration::from_secs(60),
                },
            )
            .await;
        coordinator.acquire(RateLimitBucket::Read).await;

        coordinator
            .observe(
                RateLimitBucket::Read,
                RateLimitSnapshot {
                    limit: Some(120),
                    remaining: Some(117),
                    reset: Some(RateLimitReset {
                        epoch_seconds: 0,
                        delay: Duration::from_secs(45),
                    }),
                    retry_after: Some(super::RetryAfter::new(Duration::from_secs(45))),
                },
            )
            .await;

        let permit = tokio::spawn({
            let coordinator = coordinator.clone();
            async move { coordinator.acquire(RateLimitBucket::Read).await }
        });
        yield_now().await;
        assert!(!permit.is_finished());

        tokio::time::advance(Duration::from_millis(500)).await;
        yield_now().await;
        assert!(permit.is_finished());
    }

    #[tokio::test(start_paused = true)]
    async fn successful_response_with_no_remaining_quota_waits_until_reset() {
        let coordinator = RateLimitCoordinator::new();
        coordinator
            .observe(
                RateLimitBucket::Read,
                RateLimitSnapshot {
                    limit: Some(120),
                    remaining: Some(0),
                    reset: Some(RateLimitReset {
                        epoch_seconds: 0,
                        delay: Duration::from_secs(45),
                    }),
                    retry_after: Some(super::RetryAfter::new(Duration::from_secs(45))),
                },
            )
            .await;

        let permit = tokio::spawn({
            let coordinator = coordinator.clone();
            async move { coordinator.acquire(RateLimitBucket::Read).await }
        });
        yield_now().await;
        assert!(!permit.is_finished());

        tokio::time::advance(Duration::from_secs(44)).await;
        yield_now().await;
        assert!(!permit.is_finished());
        tokio::time::advance(Duration::from_secs(1)).await;
        yield_now().await;
        assert!(permit.is_finished());
    }

    #[tokio::test(start_paused = true)]
    async fn rate_limited_response_enforces_retry_after_even_without_remaining_header() {
        let coordinator = RateLimitCoordinator::new();
        coordinator
            .observe_response(
                RateLimitBucket::Read,
                reqwest::StatusCode::TOO_MANY_REQUESTS,
                RateLimitSnapshot {
                    limit: Some(120),
                    remaining: None,
                    reset: None,
                    retry_after: Some(super::RetryAfter::new(Duration::from_secs(30))),
                },
            )
            .await;

        let permit = tokio::spawn({
            let coordinator = coordinator.clone();
            async move { coordinator.acquire(RateLimitBucket::Read).await }
        });
        yield_now().await;
        assert!(!permit.is_finished());

        tokio::time::advance(Duration::from_secs(30)).await;
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
