//! Dependency-aware managed synchronization and safe device reconciliation.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use crate::{
    Error, Result,
    domain::{self, DeviceKey, Realm},
    raw,
};
use tracing::{debug, info, warn};

use super::{Client, ReadinessComponent};

/// A managed synchronization domain.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum SyncDomain {
    /// The authenticated account singleton.
    Account,
    /// The authenticated account's device fleet.
    Devices,
    /// Owned replicant detail observations.
    Replicants,
    /// Locations, where an implemented managed reader exists.
    Locations,
    /// Account-wide resource inventory, keyed by location.
    Inventory,
    /// The owned simulation-history collection.
    Simulations,
}

/// One domain's final synchronization state.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SyncProgress {
    /// The domain committed successfully.
    Complete,
    /// A prerequisite did not commit, so this domain was not requested.
    Blocked,
    /// The domain did not complete; any prior committed pages remain usable.
    Failed,
    /// Synchronization was cancelled before a request was made.
    Cancelled,
}

/// A sanitized partial-success diagnostic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncDiagnostic {
    /// The affected domain.
    pub domain: SyncDomain,
    /// Its final progress state.
    pub progress: SyncProgress,
    /// Pages that committed before this domain finished.
    pub pages: usize,
    /// Items that committed before this domain finished.
    pub items: usize,
    /// State revisions committed before this domain finished.
    pub revisions: usize,
    /// Whether this traversal had complete collection authority.
    pub complete: bool,
    /// Whether the domain queued follow-up reconciliation work.
    pub reconciliation_queued: bool,
    /// A safe, structured description of a failed domain.
    pub failure: Option<SyncFailure>,
}

/// A sanitized sync failure retained in a partial-success report.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncFailure {
    /// HTTP status, when the server supplied one.
    pub status: Option<u16>,
    /// Whether a later synchronization may retry the domain.
    pub retryable: bool,
    /// Secret-safe failure category.
    pub kind: SyncFailureKind,
}

/// Stable categories for [`SyncFailure`].
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncFailureKind {
    /// Authentication must be repaired before retrying.
    Authentication,
    /// The remote or local rate limit deferred the work.
    RateLimited,
    /// The request did not complete at the transport layer.
    Transport,
    /// The response was invalid or rejected by the contract.
    Response,
    /// Durable state could not be updated.
    Persistence,
    /// The requested synchronization was invalid locally.
    Configuration,
    /// A future error category not represented above.
    Other,
}

/// Readiness established by a completed synchronization report.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SyncReadiness {
    /// No essential baseline completed.
    #[default]
    Unavailable,
    /// The REST essential baseline completed; event continuity is still pending.
    RestBaseline,
    /// Every domain in the requested plan completed.
    Complete,
}

/// Result of a synchronization attempt, including successful domains and failures.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SyncReport {
    /// Domains that committed successfully.
    pub completed: Vec<SyncDomain>,
    /// Domain-level partial-success diagnostics.
    pub diagnostics: Vec<SyncDiagnostic>,
    /// Readiness obtained by this REST-only synchronization attempt.
    pub readiness: SyncReadiness,
}

impl SyncReport {
    fn record(&mut self, domain: SyncDomain, progress: SyncProgress, outcome: SyncOutcome) {
        if progress == SyncProgress::Complete {
            self.completed.push(domain);
        }
        self.diagnostics.push(SyncDiagnostic {
            domain,
            progress,
            pages: outcome.pages,
            items: outcome.items,
            revisions: outcome.revisions,
            complete: outcome.complete,
            reconciliation_queued: outcome.reconciliation_queued,
            failure: None,
        });
    }

    fn failed(&mut self, domain: SyncDomain, outcome: SyncOutcome, error: &Error) {
        self.diagnostics.push(SyncDiagnostic {
            domain,
            progress: SyncProgress::Failed,
            pages: outcome.pages,
            items: outcome.items,
            revisions: outcome.revisions,
            complete: outcome.complete,
            reconciliation_queued: outcome.reconciliation_queued,
            failure: Some(SyncFailure::from(error)),
        });
    }
}

impl From<&Error> for SyncFailure {
    fn from(error: &Error) -> Self {
        let kind = match error {
            Error::Authentication { .. } => SyncFailureKind::Authentication,
            Error::RateLimited { .. } => SyncFailureKind::RateLimited,
            Error::Transport { .. } => SyncFailureKind::Transport,
            Error::Decode { .. } | Error::Contract { .. } => SyncFailureKind::Response,
            Error::Persistence { .. } | Error::AccountStoreMismatch { .. } => {
                SyncFailureKind::Persistence
            }
            Error::Configuration { .. } => SyncFailureKind::Configuration,
            _ => SyncFailureKind::Other,
        };
        let retryable = matches!(
            kind,
            SyncFailureKind::RateLimited | SyncFailureKind::Transport
        ) || error.status().is_some_and(|status| status >= 500);
        Self {
            status: error.status(),
            retryable,
            kind,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct SyncOutcome {
    pages: usize,
    items: usize,
    revisions: usize,
    complete: bool,
    reconciliation_queued: bool,
}

#[derive(Debug)]
struct SyncDomainError {
    outcome: SyncOutcome,
    error: Error,
}

impl From<Error> for SyncDomainError {
    fn from(error: Error) -> Self {
        Self {
            outcome: SyncOutcome::default(),
            error,
        }
    }
}

/// A cancellation handle checked between paginated requests and dependencies.
#[derive(Clone, Debug, Default)]
pub struct SyncCancellation(Arc<AtomicBool>);

impl SyncCancellation {
    /// Requests cancellation of the associated synchronization call.
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }

    fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

/// A validated dependency plan for a synchronization sweep.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SyncPlan {
    domains: BTreeSet<SyncDomain>,
    dependencies: BTreeMap<SyncDomain, BTreeSet<SyncDomain>>,
    essential: BTreeSet<SyncDomain>,
}

/// A plan cannot be safely scheduled.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncPlanError {
    /// A dependency refers to a domain omitted from the plan.
    MissingDependency,
    /// The dependency graph has a cycle.
    Cycle,
}

impl SyncPlan {
    /// Creates an empty plan.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a domain to this plan.
    pub fn include(&mut self, domain: SyncDomain) -> &mut Self {
        self.domains.insert(domain);
        self
    }

    /// Requires `dependency` to commit before `domain` starts.
    pub fn depends_on(&mut self, domain: SyncDomain, dependency: SyncDomain) -> &mut Self {
        self.dependencies
            .entry(domain)
            .or_default()
            .insert(dependency);
        self
    }

    /// Marks a domain as required for the REST baseline.
    pub fn require(&mut self, domain: SyncDomain) -> &mut Self {
        self.essential.insert(domain);
        self.include(domain)
    }

    /// Validates dependency references and cycles before networking begins.
    pub fn validate(&self) -> std::result::Result<(), SyncPlanError> {
        for (domain, dependencies) in &self.dependencies {
            if !self.domains.contains(domain) || !dependencies.is_subset(&self.domains) {
                return Err(SyncPlanError::MissingDependency);
            }
        }
        self.ordered().map(|_| ())
    }

    fn ordered(&self) -> std::result::Result<Vec<SyncDomain>, SyncPlanError> {
        let mut left = self.domains.clone();
        let mut ordered = Vec::with_capacity(left.len());
        while !left.is_empty() {
            let next = left.iter().copied().find(|domain| {
                self.dependencies
                    .get(domain)
                    .is_none_or(|dependencies| dependencies.is_disjoint(&left))
            });
            let Some(next) = next else {
                return Err(SyncPlanError::Cycle);
            };
            left.remove(&next);
            ordered.push(next);
        }
        Ok(ordered)
    }

    /// The bounded REST baseline used by `essential`.
    #[must_use]
    pub fn essential() -> Self {
        let mut plan = Self::new();
        plan.require(SyncDomain::Account);
        plan.require(SyncDomain::Devices);
        plan.depends_on(SyncDomain::Devices, SyncDomain::Account);
        plan
    }

    /// The current bounded managed-read baseline. Global catalogue and volatile
    /// event surfaces are intentionally excluded.
    #[must_use]
    pub fn full() -> Self {
        let mut plan = Self::essential();
        plan.include(SyncDomain::Replicants)
            .include(SyncDomain::Locations)
            .include(SyncDomain::Inventory)
            .include(SyncDomain::Simulations)
            .depends_on(SyncDomain::Replicants, SyncDomain::Devices)
            .depends_on(SyncDomain::Locations, SyncDomain::Devices)
            .depends_on(SyncDomain::Locations, SyncDomain::Replicants)
            .depends_on(SyncDomain::Inventory, SyncDomain::Locations)
            .depends_on(SyncDomain::Simulations, SyncDomain::Account);
        plan
    }
}

/// Managed synchronization entry point returned by [`Client::sync`].
#[derive(Clone, Debug)]
pub struct SyncClient {
    client: Client,
    cancellation: SyncCancellation,
    max_pages: usize,
}

impl SyncClient {
    pub(crate) fn new(client: Client) -> Self {
        Self {
            client,
            cancellation: SyncCancellation::default(),
            max_pages: 100,
        }
    }

    /// Uses this cancellation handle for the next synchronization call.
    #[must_use]
    pub fn cancellation(mut self, cancellation: SyncCancellation) -> Self {
        self.cancellation = cancellation;
        self
    }

    /// Bounds the number of pages accepted from one cursor traversal.
    #[must_use]
    pub fn max_pages(mut self, max_pages: usize) -> Self {
        self.max_pages = max_pages.max(1);
        self
    }

    /// Runs the essential REST baseline.
    pub async fn essential(self) -> Result<SyncReport> {
        self.run(SyncPlan::essential()).await
    }

    /// Runs the full bounded REST baseline, excluding event continuity and the
    /// separately cached star catalogue.
    pub async fn full(self) -> Result<SyncReport> {
        self.run(SyncPlan::full()).await
    }

    /// Runs one supported managed domain.
    pub async fn domain(self, domain: SyncDomain) -> Result<SyncReport> {
        let mut plan = SyncPlan::new();
        plan.include(domain);
        self.run(plan).await
    }

    /// Reconciles a targeted device through its authoritative detail endpoint.
    pub async fn device(self, code: &str) -> Result<SyncReport> {
        self.client.devices().refresh(code).await?;
        Ok(completed_report(SyncDomain::Devices))
    }

    /// Reconciles a targeted owned replicant through its authoritative detail endpoint.
    pub async fn replicant(self, code: &str) -> Result<SyncReport> {
        self.client.replicants().get_owned(code).await?;
        Ok(completed_report(SyncDomain::Replicants))
    }

    /// Reconciles one location through its unscoped authoritative detail endpoint.
    pub async fn location(self, designation: &str) -> Result<SyncReport> {
        let response = self
            .client
            .managed_raw()
            .locations()
            .get(designation, None)
            .await?;
        let observation = domain::location_detail(&response.value, Realm::Live, observed_at())
            .map_err(|_| Error::Decode {
                message: "location synchronization response is invalid".into(),
                status: None,
                source: None,
            })?;
        self.client
            .managed_state()
            .persist_location(observation)
            .map_err(super::client::store_error)?;
        Ok(completed_report(SyncDomain::Locations))
    }

    /// Runs a validated dependency plan and returns partial-success diagnostics.
    pub async fn run(self, plan: SyncPlan) -> Result<SyncReport> {
        plan.validate().map_err(|error| Error::Configuration {
            message: format!("invalid synchronization plan: {error:?}"),
        })?;
        self.client.ensure_open()?;
        let ordered = plan.ordered().expect("validated plan");
        let domain_count = ordered.len();
        let sync_started = Instant::now();
        info!(
            target: "replicant_client::sync",
            event = "sync.started",
            domains = domain_count,
            max_pages = self.max_pages,
            "starting managed synchronization"
        );
        let is_essential = plan.domains == SyncPlan::essential().domains;
        let is_full = plan.domains == SyncPlan::full().domains;
        let mut report = SyncReport::default();
        let mut completed = BTreeSet::new();
        for (index, domain) in ordered.into_iter().enumerate() {
            let domain_started = Instant::now();
            info!(
                target: "replicant_client::sync",
                event = "sync.domain_started",
                ?domain,
                index = index + 1,
                total = domain_count,
                "synchronizing managed domain"
            );
            if self.cancellation.is_cancelled() {
                warn!(target: "replicant_client::sync", "synchronization cancelled before domain={domain:?}");
                report.record(domain, SyncProgress::Cancelled, SyncOutcome::default());
                continue;
            }
            if plan
                .dependencies
                .get(&domain)
                .is_some_and(|required| !required.is_subset(&completed))
            {
                warn!(target: "replicant_client::sync", "synchronization blocked domain={domain:?}");
                report.record(domain, SyncProgress::Blocked, SyncOutcome::default());
                continue;
            }
            match self.sync_domain(domain).await {
                Ok(outcome) => {
                    info!(
                        target: "replicant_client::sync",
                        event = "sync.domain_completed",
                        ?domain,
                        elapsed_ms = domain_started.elapsed().as_millis() as u64,
                        pages = outcome.pages,
                        items = outcome.items,
                        revisions = outcome.revisions,
                        complete = outcome.complete,
                        reconciliation_queued = outcome.reconciliation_queued,
                        "managed domain synchronization completed"
                    );
                    completed.insert(domain);
                    report.record(domain, SyncProgress::Complete, outcome);
                }
                Err(_) if self.cancellation.is_cancelled() => {
                    warn!(
                        target: "replicant_client::sync",
                        event = "sync.domain_cancelled",
                        ?domain,
                        elapsed_ms = domain_started.elapsed().as_millis() as u64,
                        "synchronization cancelled during domain"
                    );
                    report.record(domain, SyncProgress::Cancelled, SyncOutcome::default());
                }
                Err(error) => {
                    warn!(
                        target: "replicant_client::sync",
                        event = "sync.domain_failed",
                        ?domain,
                        elapsed_ms = domain_started.elapsed().as_millis() as u64,
                        pages = error.outcome.pages,
                        items = error.outcome.items,
                        revisions = error.outcome.revisions,
                        error = %error.error,
                        "managed domain synchronization failed"
                    );
                    report.failed(domain, error.outcome, &error.error);
                }
            }
        }
        let essentials_complete = plan.essential.is_subset(&completed);
        report.readiness = if completed.len() == plan.domains.len() {
            SyncReadiness::Complete
        } else if essentials_complete {
            SyncReadiness::RestBaseline
        } else {
            SyncReadiness::Unavailable
        };
        if is_essential || is_full {
            self.client.set_readiness(|readiness| {
                readiness.essential_rest = if essentials_complete {
                    ReadinessComponent::Ready
                } else {
                    ReadinessComponent::Degraded
                };
                if is_full {
                    readiness.full_rest = if matches!(report.readiness, SyncReadiness::Complete) {
                        ReadinessComponent::Ready
                    } else {
                        ReadinessComponent::Degraded
                    };
                }
            });
        }
        info!(
            target: "replicant_client::sync",
            event = "sync.completed",
            elapsed_ms = sync_started.elapsed().as_millis() as u64,
            readiness = ?report.readiness,
            completed_domains = report.completed.len(),
            total_domains = domain_count,
            "managed synchronization completed"
        );
        Ok(report)
    }

    async fn sync_domain(
        &self,
        domain: SyncDomain,
    ) -> std::result::Result<SyncOutcome, SyncDomainError> {
        debug!(target: "replicant_client::sync", "synchronizing domain={domain:?}");
        match domain {
            SyncDomain::Account => {
                let started = Instant::now();
                debug!(
                    target: "replicant_client::sync",
                    event = "sync.account_started",
                    "refreshing authenticated account"
                );
                self.client.account().refresh().await?;
                debug!(
                    target: "replicant_client::sync",
                    event = "sync.account_completed",
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "refreshed authenticated account"
                );
                Ok(SyncOutcome {
                    pages: 1,
                    items: 1,
                    revisions: 1,
                    complete: true,
                    reconciliation_queued: false,
                })
            }
            SyncDomain::Devices => self.sync_devices().await,
            SyncDomain::Replicants => self.sync_replicants().await,
            SyncDomain::Locations => self.sync_locations().await,
            SyncDomain::Inventory => self.sync_inventory().await,
            SyncDomain::Simulations => self.sync_simulations().await.map_err(Into::into),
        }
    }

    async fn sync_devices(&self) -> std::result::Result<SyncOutcome, SyncDomainError> {
        let mut query = raw::devices::DeviceListQuery {
            limit: Some(50),
            ..Default::default()
        };
        let mut present = BTreeSet::<DeviceKey>::new();
        for (page, _) in (0..self.max_pages).enumerate() {
            if self.cancellation.is_cancelled() {
                return Err(SyncDomainError {
                    outcome: SyncOutcome {
                        pages: page,
                        items: present.len(),
                        revisions: page,
                        complete: false,
                        reconciliation_queued: false,
                    },
                    error: Error::Configuration {
                        message: "synchronization cancelled".into(),
                    },
                });
            }
            let request_started = Instant::now();
            let response = self
                .client
                .managed_raw()
                .devices()
                .list(&query)
                .await
                .map_err(|error| SyncDomainError {
                    outcome: SyncOutcome {
                        pages: page,
                        items: present.len(),
                        revisions: page,
                        complete: false,
                        reconciliation_queued: false,
                    },
                    error,
                })?;
            let request_elapsed = request_started.elapsed();
            let normalize_started = Instant::now();
            let next_cursor = response.value.next_cursor;
            let collection = domain::device_collection(
                &response.value,
                Realm::Live,
                false,
                next_cursor.is_none(),
                observed_at(),
            )
            .map_err(|_| SyncDomainError {
                outcome: SyncOutcome {
                    pages: page,
                    items: present.len(),
                    revisions: page,
                    complete: false,
                    reconciliation_queued: false,
                },
                error: Error::Decode {
                    message: "device synchronization response is invalid".into(),
                    status: None,
                    source: None,
                },
            })?;
            let normalize_elapsed = normalize_started.elapsed();
            for device in &collection.members {
                present.insert(device.value.key.clone());
            }
            let persist_started = Instant::now();
            self.client
                .managed_state()
                .persist_devices(&collection.members)
                .map_err(|error| SyncDomainError {
                    outcome: SyncOutcome {
                        pages: page,
                        items: present.len(),
                        revisions: page,
                        complete: false,
                        reconciliation_queued: false,
                    },
                    error: super::client::store_error(error),
                })?;
            info!(
                target: "replicant_client::sync",
                event = "sync.devices_page_completed",
                page = page + 1,
                records = collection.members.len(),
                has_next = next_cursor.is_some(),
                request_ms = request_elapsed.as_millis() as u64,
                normalize_ms = normalize_elapsed.as_millis() as u64,
                persist_ms = persist_started.elapsed().as_millis() as u64,
                total_ms = request_started.elapsed().as_millis() as u64,
                "synchronized device page"
            );
            match next_cursor {
                Some(cursor) => query.cursor = Some(cursor),
                None => {
                    // Reconciliation is deliberately delayed until every unfiltered page
                    // committed. Filtered and visibility-scoped lists never enter here.
                    let reconcile_started = Instant::now();
                    self.client
                        .managed_state()
                        .reconcile_owned_devices(&present)
                        .map_err(|error| SyncDomainError {
                            outcome: SyncOutcome {
                                pages: page + 1,
                                items: present.len(),
                                revisions: page + 1,
                                complete: false,
                                reconciliation_queued: false,
                            },
                            error: super::client::store_error(error),
                        })?;
                    info!(
                        target: "replicant_client::sync",
                        event = "sync.devices_reconciled",
                        present = present.len(),
                        elapsed_ms = reconcile_started.elapsed().as_millis() as u64,
                        "reconciled owned device membership"
                    );
                    return Ok(SyncOutcome {
                        pages: page + 1,
                        items: present.len(),
                        revisions: page + 2,
                        complete: true,
                        reconciliation_queued: false,
                    });
                }
            }
        }
        Err(SyncDomainError {
            outcome: SyncOutcome {
                pages: self.max_pages,
                items: present.len(),
                revisions: self.max_pages,
                complete: false,
                reconciliation_queued: false,
            },
            error: Error::Configuration {
                message: "device synchronization exceeded configured page bound".into(),
            },
        })
    }

    async fn sync_replicants(&self) -> std::result::Result<SyncOutcome, SyncDomainError> {
        let mut codes = BTreeSet::new();
        for replicant in self.client.managed_state().replicants() {
            if replicant.value.private.is_some() {
                codes.insert(replicant.value.key.id.as_str().to_owned());
            }
        }
        for device in self.client.managed_state().devices() {
            if let Some(replicant) = device.value.relationships.hosting_replicant {
                codes.insert(replicant.id.as_str().to_owned());
            }
        }
        let mut completed = 0;
        for (index, code) in codes.iter().enumerate() {
            if self.cancellation.is_cancelled() {
                return Err(SyncDomainError {
                    outcome: SyncOutcome {
                        pages: usize::from(completed > 0),
                        items: completed,
                        revisions: completed,
                        complete: false,
                        reconciliation_queued: false,
                    },
                    error: Error::Configuration {
                        message: "synchronization cancelled".into(),
                    },
                });
            }
            let item_started = Instant::now();
            debug!(
                target: "replicant_client::sync",
                event = "sync.replicant_started",
                replicant = %code,
                index = index + 1,
                total = codes.len(),
                "synchronizing owned replicant"
            );
            self.client
                .replicants()
                .get_owned(code)
                .await
                .map_err(|error| SyncDomainError {
                    outcome: SyncOutcome {
                        pages: usize::from(completed > 0),
                        items: completed,
                        revisions: completed,
                        complete: false,
                        reconciliation_queued: false,
                    },
                    error,
                })?;
            completed += 1;
            info!(
                target: "replicant_client::sync",
                event = "sync.replicant_completed",
                replicant = %code,
                index = completed,
                total = codes.len(),
                elapsed_ms = item_started.elapsed().as_millis() as u64,
                "synchronized owned replicant"
            );
        }
        Ok(SyncOutcome {
            pages: usize::from(!codes.is_empty()),
            items: completed,
            revisions: completed,
            complete: true,
            reconciliation_queued: false,
        })
    }

    async fn sync_locations(&self) -> std::result::Result<SyncOutcome, SyncDomainError> {
        let mut designations = BTreeSet::new();
        for device in self.client.managed_state().devices() {
            if let Some(location) = device.value.location {
                designations.insert(location.id.as_str().to_owned());
            }
        }
        for replicant in self.client.managed_state().replicants() {
            if let Some(location) = replicant.value.location {
                designations.insert(location.id.as_str().to_owned());
            }
        }
        let mut completed = 0;
        for (index, designation) in designations.iter().enumerate() {
            if self.cancellation.is_cancelled() {
                return Err(SyncDomainError {
                    outcome: SyncOutcome {
                        pages: usize::from(completed > 0),
                        items: completed,
                        revisions: completed,
                        complete: false,
                        reconciliation_queued: false,
                    },
                    error: Error::Configuration {
                        message: "synchronization cancelled".into(),
                    },
                });
            }
            let item_started = Instant::now();
            debug!(
                target: "replicant_client::sync",
                event = "sync.location_started",
                designation = %designation,
                index = index + 1,
                total = designations.len(),
                "synchronizing location"
            );
            let request_started = Instant::now();
            let response = match self
                .client
                .managed_raw()
                .locations()
                .get(designation, None)
                .await
            {
                Ok(response) => response,
                Err(error) if location_is_temporarily_unobservable(&error) => {
                    debug!(
                        target: "replicant_client::sync",
                        event = "sync.location_unobservable",
                        designation = %designation,
                        index = index + 1,
                        total = designations.len(),
                        "skipping location detail that currently requires a replicant in-system"
                    );
                    continue;
                }
                Err(error) => {
                    return Err(SyncDomainError {
                        outcome: SyncOutcome {
                            pages: usize::from(completed > 0),
                            items: completed,
                            revisions: completed,
                            complete: false,
                            reconciliation_queued: false,
                        },
                        error,
                    });
                }
            };
            let request_elapsed = request_started.elapsed();
            let normalize_started = Instant::now();
            let observation = domain::location_detail(&response.value, Realm::Live, observed_at())
                .map_err(|_| SyncDomainError {
                    outcome: SyncOutcome {
                        pages: usize::from(completed > 0),
                        items: completed,
                        revisions: completed,
                        complete: false,
                        reconciliation_queued: false,
                    },
                    error: Error::Decode {
                        message: "location synchronization response is invalid".into(),
                        status: None,
                        source: None,
                    },
                })?;
            let normalize_elapsed = normalize_started.elapsed();
            let persist_started = Instant::now();
            self.client
                .managed_state()
                .persist_location(observation)
                .map_err(|error| SyncDomainError {
                    outcome: SyncOutcome {
                        pages: usize::from(completed > 0),
                        items: completed,
                        revisions: completed,
                        complete: false,
                        reconciliation_queued: false,
                    },
                    error: super::client::store_error(error),
                })?;
            completed += 1;
            info!(
                target: "replicant_client::sync",
                event = "sync.location_completed",
                designation = %designation,
                index = completed,
                total = designations.len(),
                request_ms = request_elapsed.as_millis() as u64,
                normalize_ms = normalize_elapsed.as_millis() as u64,
                persist_ms = persist_started.elapsed().as_millis() as u64,
                elapsed_ms = item_started.elapsed().as_millis() as u64,
                "synchronized location"
            );
        }
        Ok(SyncOutcome {
            pages: usize::from(!designations.is_empty()),
            items: completed,
            revisions: completed,
            complete: true,
            reconciliation_queued: false,
        })
    }

    async fn sync_inventory(&self) -> std::result::Result<SyncOutcome, SyncDomainError> {
        let mut query = raw::inventory::AccountInventoryQuery {
            limit: Some(100),
            ..Default::default()
        };
        let mut pages = 0;
        let mut items = 0;
        for _ in 0..self.max_pages {
            if self.cancellation.is_cancelled() {
                return Err(SyncDomainError {
                    outcome: SyncOutcome {
                        pages,
                        items,
                        revisions: items,
                        complete: false,
                        reconciliation_queued: false,
                    },
                    error: Error::Configuration {
                        message: "synchronization cancelled".into(),
                    },
                });
            }
            let page_started = Instant::now();
            let (inventories, next_cursor) =
                self.client
                    .inventory()
                    .list(&query)
                    .await
                    .map_err(|error| SyncDomainError {
                        outcome: SyncOutcome {
                            pages,
                            items,
                            revisions: items,
                            complete: false,
                            reconciliation_queued: false,
                        },
                        error,
                    })?;
            pages += 1;
            items += inventories.len();
            info!(
                target: "replicant_client::sync",
                event = "sync.inventory_page_completed",
                page = pages,
                records = inventories.len(),
                has_next = next_cursor.is_some(),
                elapsed_ms = page_started.elapsed().as_millis() as u64,
                "synchronized inventory page"
            );
            match next_cursor {
                Some(cursor) => query.cursor = Some(cursor),
                None => {
                    return Ok(SyncOutcome {
                        pages,
                        items,
                        revisions: items,
                        complete: true,
                        reconciliation_queued: false,
                    });
                }
            }
        }
        Err(SyncDomainError {
            outcome: SyncOutcome {
                pages,
                items,
                revisions: items,
                complete: false,
                reconciliation_queued: false,
            },
            error: Error::Configuration {
                message: "inventory synchronization exceeded configured page bound".into(),
            },
        })
    }

    async fn sync_simulations(&self) -> Result<SyncOutcome> {
        let started = Instant::now();
        debug!(
            target: "replicant_client::sync",
            event = "sync.simulations_started",
            "synchronizing simulation history"
        );
        let simulations = self.client.simulations().history().await?;
        info!(
            target: "replicant_client::sync",
            event = "sync.simulations_completed",
            records = simulations.len(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            "synchronized simulation history"
        );
        Ok(SyncOutcome {
            pages: 1,
            items: simulations.len(),
            revisions: simulations.len(),
            // Account history is authoritative for entries returned, but the
            // contract does not authorize local-run deletion by absence.
            complete: true,
            reconciliation_queued: false,
        })
    }
}

fn completed_report(domain: SyncDomain) -> SyncReport {
    SyncReport {
        completed: vec![domain],
        diagnostics: vec![SyncDiagnostic {
            domain,
            progress: SyncProgress::Complete,
            pages: 1,
            items: 1,
            revisions: 1,
            complete: true,
            reconciliation_queued: false,
            failure: None,
        }],
        readiness: SyncReadiness::Complete,
    }
}

fn observed_at() -> crate::domain::ObservationTime {
    crate::domain::ObservationTime::now()
}

fn location_is_temporarily_unobservable(error: &Error) -> bool {
    error.status() == Some(403)
        && error
            .details()
            .and_then(|details| details.message.as_deref())
            .is_some_and(|message| {
                message
                    .to_ascii_lowercase()
                    .contains("no replicant in system")
            })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::managed::ClientStatus;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use crate::managed::client::StartupPolicy;
    use crate::raw::{SecretString, Url};

    use crate::managed::test_client_at as client_at;

    #[test]
    fn location_without_in_system_replicant_is_skipped_during_sync() {
        let error = Error::Contract {
            status: 403,
            details: Box::new(crate::ErrorDetails {
                message: Some("No replicant in system".to_owned()),
                ..Default::default()
            }),
        };
        assert!(location_is_temporarily_unobservable(&error));

        let other_forbidden = Error::Contract {
            status: 403,
            details: Box::new(crate::ErrorDetails {
                message: Some("Not your location".to_owned()),
                ..Default::default()
            }),
        };
        assert!(!location_is_temporarily_unobservable(&other_forbidden));
    }

    #[test]
    fn dependency_ordering_is_stable() {
        let mut plan = SyncPlan::new();
        plan.include(SyncDomain::Devices)
            .include(SyncDomain::Account)
            .depends_on(SyncDomain::Devices, SyncDomain::Account);
        assert_eq!(
            plan.ordered().expect("valid plan"),
            vec![SyncDomain::Account, SyncDomain::Devices]
        );
    }

    #[test]
    fn full_plan_contains_every_bounded_managed_domain() {
        assert_eq!(
            SyncPlan::full().ordered().expect("valid full plan"),
            vec![
                SyncDomain::Account,
                SyncDomain::Devices,
                SyncDomain::Replicants,
                SyncDomain::Locations,
                SyncDomain::Inventory,
                SyncDomain::Simulations,
            ]
        );
    }

    #[test]
    fn invalid_plans_fail_before_networking() {
        let mut missing = SyncPlan::new();
        missing
            .include(SyncDomain::Devices)
            .depends_on(SyncDomain::Devices, SyncDomain::Account);
        assert_eq!(missing.validate(), Err(SyncPlanError::MissingDependency));

        let mut cycle = SyncPlan::new();
        cycle
            .include(SyncDomain::Account)
            .include(SyncDomain::Devices)
            .depends_on(SyncDomain::Account, SyncDomain::Devices)
            .depends_on(SyncDomain::Devices, SyncDomain::Account);
        assert_eq!(cycle.validate(), Err(SyncPlanError::Cycle));
    }

    #[test]
    fn cancellation_is_shared_between_clones() {
        let cancellation = SyncCancellation::default();
        cancellation.clone().cancel();
        assert!(cancellation.is_cancelled());
    }

    #[tokio::test]
    async fn full_sync_restores_every_durable_managed_domain_after_restart() {
        let server = MockServer::start().await;
        for (route, body) in [
            (
                "/v1/accounts/me",
                serde_json::json!({"email": "me@example.test"}),
            ),
            (
                "/v1/devices",
                serde_json::json!({"devices": [{"device_code": "D1", "replicant_code": "OWNER", "hosting_replicant": "MATRIX", "location": "SOL-4"}]}),
            ),
            (
                "/v1/replicants/MATRIX",
                serde_json::json!({"replicant_code": "MATRIX", "location": "SOL-4"}),
            ),
            (
                "/v1/locations/SOL-4",
                serde_json::json!({"location": "SOL-4", "location_type": "planet"}),
            ),
            (
                "/v1/inventory",
                serde_json::json!({"locations": [{"location": "SOL-4", "items": [{"resource_type": "structural", "quantity": 3}]}]}),
            ),
            (
                "/v1/accounts/simulations",
                serde_json::json!({"simulations": [{"id": 7, "scenario_code": "mine", "started_at": "2026-01-01T00:00:00Z", "completed_at": "2026-01-01T01:00:00Z"}]}),
            ),
        ] {
            Mock::given(method("GET"))
                .and(path(route))
                .respond_with(ResponseTemplate::new(200).set_body_json(body))
                .expect(1)
                .mount(&server)
                .await;
        }
        let database = std::env::temp_dir().join(format!(
            "replicant-client-full-sync-restart-{}.sqlite",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos()
        ));
        let client = Client::builder()
            .authentication_token(SecretString::from("token".to_string()))
            .base_url(Url::parse(&server.uri()).expect("mock URL"))
            .sqlite(&database)
            .startup_policy(StartupPolicy::RestoreOnly)
            .start()
            .await
            .expect("restore-only client");

        let report = client.sync().full().await.expect("full sync");

        // The Riker candidate query is evaluated three times after the one
        // explicit remote synchronization. Every mock expectation above is
        // consumed by `full`; any hidden query request fails this test.
        for _ in 0..3 {
            client
                .locations()
                .find()
                .in_realm(Realm::Live)
                .planetary_bodies()
                .surveyed()
                .breathable_atmosphere()
                .without_advanced_civilisation()
                .life_stage_below(domain::LifeStage::Intelligent)
                .gravity_g_between(0.8..=1.3)
                .surface_temp_c_between(10.0..=25.0)
                .collect()
                .await
                .expect("local Riker candidate query");
        }

        assert_eq!(report.readiness, SyncReadiness::Complete, "{report:?}");
        assert_eq!(
            report.completed,
            SyncPlan::full().ordered().expect("full plan")
        );
        assert!(
            client
                .managed_state()
                .device(&DeviceKey::live("D1".into()))
                .is_some()
        );
        assert!(
            client
                .managed_state()
                .replicants()
                .iter()
                .any(|entry| entry.value.key.id.as_str() == "MATRIX")
        );
        assert!(
            !client
                .managed_state()
                .replicants()
                .iter()
                .any(|entry| entry.value.key.id.as_str() == "OWNER")
        );
        assert!(
            client
                .managed_state()
                .location(&domain::LocationKey::live("SOL-4".into()))
                .is_some()
        );
        assert!(
            client
                .managed_state()
                .inventory(&domain::InventoryOwner::Location(
                    domain::LocationKey::live("SOL-4".into())
                ))
                .is_some()
        );
        assert!(
            client
                .managed_state()
                .simulation(crate::domain::SimulationId::new(7))
                .is_some()
        );
        let before_restart = client
            .locations()
            .find()
            .at("SOL-4")
            .collect()
            .await
            .expect("local location query");
        client.close().await.expect("close synced client");

        let restored = Client::builder()
            .authentication_token(SecretString::from("token".to_string()))
            .base_url(Url::parse("http://127.0.0.1:9").expect("offline URL"))
            .sqlite(&database)
            .startup_policy(StartupPolicy::RestoreOnly)
            .start()
            .await
            .expect("offline restore without network I/O");
        assert_eq!(
            restored
                .managed_state()
                .account()
                .expect("restored account")
                .value
                .email,
            Some("me@example.test".into())
        );
        assert!(
            restored
                .managed_state()
                .device(&DeviceKey::live("D1".into()))
                .is_some()
        );
        assert!(
            restored
                .managed_state()
                .replicants()
                .iter()
                .any(|entry| entry.value.key.id.as_str() == "MATRIX")
        );
        assert!(
            restored
                .managed_state()
                .location(&domain::LocationKey::live("SOL-4".into()))
                .is_some()
        );
        assert!(
            restored
                .managed_state()
                .inventory(&domain::InventoryOwner::Location(
                    domain::LocationKey::live("SOL-4".into())
                ))
                .is_some()
        );
        assert!(
            restored
                .managed_state()
                .simulation(crate::domain::SimulationId::new(7))
                .is_some()
        );
        assert_eq!(
            restored
                .locations()
                .find()
                .at("SOL-4")
                .collect()
                .await
                .expect("restored local location query"),
            before_restart
        );
        restored.close().await.expect("close restored client");
        std::fs::remove_file(database).expect("remove test database");
    }

    #[tokio::test]
    async fn failed_domain_keeps_prior_commits_and_error_cause() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/accounts/me"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"email": "me@example.test"})),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/devices"))
            .respond_with(ResponseTemplate::new(200).set_body_json(
                serde_json::json!({"devices": [{"device_code": "D1", "replicant_code": "OWNER", "hosting_replicant": "R1"}]}),
            ))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/replicants/R1"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/accounts/simulations"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"simulations": []})),
            )
            .mount(&server)
            .await;
        let client = client_at(&server.uri()).await;

        let report = client.sync().full().await.expect("partial report");

        assert!(report.completed.contains(&SyncDomain::Devices));
        assert!(
            client
                .managed_state()
                .device(&DeviceKey::live("D1".into()))
                .is_some()
        );
        let failure = report
            .diagnostics
            .iter()
            .find(|entry| entry.domain == SyncDomain::Replicants)
            .expect("replicant diagnostic");
        assert_eq!(failure.progress, SyncProgress::Failed);
        assert_eq!(
            failure.failure.as_ref().and_then(|error| error.status),
            Some(503)
        );
        assert!(
            failure
                .failure
                .as_ref()
                .is_some_and(|error| error.retryable)
        );
    }

    #[tokio::test]
    async fn cancellation_is_reported_without_closing_the_client() {
        let server = MockServer::start().await;
        let client = client_at(&server.uri()).await;
        let cancellation = SyncCancellation::default();
        cancellation.cancel();

        let report = client
            .sync()
            .cancellation(cancellation)
            .full()
            .await
            .expect("cancelled sync is a report");

        assert!(
            report
                .diagnostics
                .iter()
                .all(|entry| entry.progress == SyncProgress::Cancelled)
        );
        assert_ne!(client.status(), ClientStatus::Closed);
    }
}
