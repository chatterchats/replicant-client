//! Dependency-aware managed synchronization and safe device reconciliation.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    Error, Result,
    domain::{self, DeviceKey, Realm},
    raw,
};

use super::{Client, ClientStatus};

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
    fn record(&mut self, domain: SyncDomain, progress: SyncProgress) {
        if progress == SyncProgress::Complete {
            self.completed.push(domain);
        }
        self.diagnostics.push(SyncDiagnostic { domain, progress });
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
        Self::essential()
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
            })?;
        self.client
            .managed_state()
            .persist_location(observation)
            .map_err(|_| Error::Persistence {
                message: "SQLite store operation failed".into(),
            })?;
        Ok(completed_report(SyncDomain::Locations))
    }

    /// Runs a validated dependency plan and returns partial-success diagnostics.
    pub async fn run(self, plan: SyncPlan) -> Result<SyncReport> {
        plan.validate().map_err(|error| Error::Configuration {
            message: format!("invalid synchronization plan: {error:?}"),
        })?;
        self.client.ensure_open()?;
        self.client.set_status(ClientStatus::Synchronizing);
        let mut report = SyncReport::default();
        let mut completed = BTreeSet::new();
        for domain in plan.ordered().expect("validated plan") {
            if self.cancellation.is_cancelled() {
                report.record(domain, SyncProgress::Cancelled);
                continue;
            }
            if plan
                .dependencies
                .get(&domain)
                .is_some_and(|required| !required.is_subset(&completed))
            {
                report.record(domain, SyncProgress::Blocked);
                continue;
            }
            match self.sync_domain(domain).await {
                Ok(()) => {
                    completed.insert(domain);
                    report.record(domain, SyncProgress::Complete);
                }
                Err(_) => report.record(domain, SyncProgress::Failed),
            }
        }
        let essentials_complete = plan.essential.is_subset(&completed);
        report.readiness = if essentials_complete {
            SyncReadiness::RestBaseline
        } else if completed.len() == plan.domains.len() {
            SyncReadiness::Complete
        } else {
            SyncReadiness::Unavailable
        };
        if essentials_complete {
            // Phase 8 owns catch-up and live-event connectivity; never claim it here.
            self.client.set_status(ClientStatus::CatchingUp);
        }
        Ok(report)
    }

    async fn sync_domain(&self, domain: SyncDomain) -> Result<()> {
        match domain {
            SyncDomain::Account => {
                self.client.account().refresh().await?;
                Ok(())
            }
            SyncDomain::Devices => self.sync_devices().await,
            SyncDomain::Replicants | SyncDomain::Locations => Err(Error::Configuration {
                message: "managed synchronization for this domain is not implemented".into(),
            }),
        }
    }

    async fn sync_devices(&self) -> Result<()> {
        let mut query = raw::devices::DeviceListQuery {
            limit: Some(100),
            ..Default::default()
        };
        let mut present = BTreeSet::<DeviceKey>::new();
        for _ in 0..self.max_pages {
            if self.cancellation.is_cancelled() {
                return Err(Error::Closed);
            }
            let response = self.client.managed_raw().devices().list(&query).await?;
            let next_cursor = response.value.next_cursor;
            let collection = domain::device_collection(
                &response.value,
                Realm::Live,
                false,
                next_cursor.is_none(),
                observed_at(),
            )
            .map_err(|_| Error::Decode {
                message: "device synchronization response is invalid".into(),
                status: None,
            })?;
            for device in &collection.members {
                present.insert(device.value.key.clone());
            }
            self.client
                .managed_state()
                .persist_devices(&collection.members)
                .map_err(|_| Error::Persistence {
                    message: "SQLite store operation failed".into(),
                })?;
            match next_cursor {
                Some(cursor) => query.cursor = Some(cursor),
                None => {
                    // Reconciliation is deliberately delayed until every unfiltered page
                    // committed. Filtered and visibility-scoped lists never enter here.
                    self.client
                        .managed_state()
                        .reconcile_owned_devices(&present)
                        .map_err(|_| Error::Persistence {
                            message: "SQLite store operation failed".into(),
                        })?;
                    return Ok(());
                }
            }
        }
        Err(Error::Configuration {
            message: "device synchronization exceeded configured page bound".into(),
        })
    }
}

fn completed_report(domain: SyncDomain) -> SyncReport {
    SyncReport {
        completed: vec![domain],
        diagnostics: vec![SyncDiagnostic {
            domain,
            progress: SyncProgress::Complete,
        }],
        readiness: SyncReadiness::Complete,
    }
}

fn observed_at() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|time| time.as_secs().to_string())
        .unwrap_or_else(|_| "0".into())
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
