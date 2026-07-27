//! Durable galaxy catalogue and per-replicant star-knowledge reads.

use std::{collections::BTreeSet, time::Instant};

use crate::domain::{self, Realm, ReplicantId, ReplicantKey, Star};
use crate::raw;
use crate::{Client, Error, Result};
use tracing::info;

fn persistence_error(_: super::store::StoreError) -> Error {
    Error::Persistence {
        message: "SQLite store operation failed".into(),
    }
}

/// Result of replacing the complete global catalogue.
#[derive(Clone, Debug, PartialEq)]
pub struct CatalogueReport {
    stars: usize,
    generated_at: Option<String>,
}
impl CatalogueReport {
    /// Number of catalogue stars committed.
    #[must_use]
    pub fn stars(&self) -> usize {
        self.stars
    }
    /// Server generation timestamp retained with the replacement.
    #[must_use]
    pub fn generated_at(&self) -> Option<&str> {
        self.generated_at.as_deref()
    }
}

/// Result of one complete replicant-scoped star traversal.
#[derive(Clone, Debug, PartialEq)]
pub struct ReplicantStarSyncReport {
    pages: usize,
    stars_seen: usize,
    explored_designations: BTreeSet<crate::domain::StarId>,
}
impl ReplicantStarSyncReport {
    /// Pages traversed.
    #[must_use]
    pub fn pages(&self) -> usize {
        self.pages
    }
    /// Star rows observed across all pages.
    #[must_use]
    pub fn stars_seen(&self) -> usize {
        self.stars_seen
    }
    /// Deduplicated explored star designations.
    #[must_use]
    pub fn explored_designations(&self) -> &BTreeSet<crate::domain::StarId> {
        &self.explored_designations
    }
}

/// Managed, restart-safe galaxy reads.
#[derive(Clone, Debug)]
pub struct GalaxyGateway {
    client: Client,
    max_pages: usize,
}
impl GalaxyGateway {
    pub(crate) fn new(client: Client) -> Self {
        Self {
            client,
            max_pages: 1024,
        }
    }
    /// Sets the defensive page cap for one subsequent star-knowledge traversal.
    #[must_use]
    pub fn max_star_pages(mut self, max_pages: usize) -> Self {
        self.max_pages = max_pages.max(1);
        self
    }
    /// Returns the locally committed catalogue; no network I/O occurs.
    #[must_use]
    pub fn catalogue(&self) -> Vec<Star> {
        self.client
            .managed_state()
            .catalogue()
            .into_iter()
            .map(|value| value.value)
            .collect()
    }
    /// Returns the persisted generation timestamp, if a catalogue has been fetched.
    #[must_use]
    pub fn catalogue_generated_at(&self) -> Option<String> {
        self.client.managed_state().catalogue_generated_at()
    }
    /// Replaces the catalogue atomically after its single safe-read response is normalized.
    pub async fn refresh_catalogue(&self) -> Result<CatalogueReport> {
        self.client.ensure_open()?;
        let total_started = Instant::now();
        info!(
            target: "replicant_client::galaxy",
            event = "galaxy.catalogue_refresh_started",
            "refreshing global star catalogue"
        );
        let request_started = Instant::now();
        let response = self.client.managed_raw().galaxy().catalogue().await?;
        let request_elapsed = request_started.elapsed();
        let normalize_started = Instant::now();
        let stars = response
            .value
            .stars
            .iter()
            .map(|star| domain::catalogue_star(star, Realm::Live, domain::ObservationTime::now()))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| Error::Decode {
                message: error.to_string(),
                status: None,
                source: None,
            })?;
        let normalize_elapsed = normalize_started.elapsed();
        let report = CatalogueReport {
            stars: stars.len(),
            generated_at: response.value.generated_at,
        };
        let persist_started = Instant::now();
        self.client
            .managed_state()
            .replace_catalogue(stars, report.generated_at.clone())
            .map_err(persistence_error)?;
        info!(
            target: "replicant_client::galaxy",
            event = "galaxy.catalogue_refresh_completed",
            stars = report.stars,
            generated_at = report.generated_at.as_deref().unwrap_or(""),
            request_ms = request_elapsed.as_millis() as u64,
            normalize_ms = normalize_elapsed.as_millis() as u64,
            persist_ms = persist_started.elapsed().as_millis() as u64,
            elapsed_ms = total_started.elapsed().as_millis() as u64,
            "global star catalogue committed"
        );
        Ok(report)
    }
    /// Traverses all pages of a replicant's visible star knowledge.  A page is
    /// committed before the next page is requested, so an interrupted run is safe to repeat.
    pub async fn sync_replicant_stars(
        &self,
        replicant_code: &str,
    ) -> Result<ReplicantStarSyncReport> {
        self.client.ensure_open()?;
        let total_started = Instant::now();
        info!(
            target: "replicant_client::galaxy",
            event = "galaxy.replicant_stars_started",
            replicant = replicant_code,
            max_pages = self.max_pages,
            "synchronizing replicant star knowledge"
        );
        let replicant = ReplicantKey::live(ReplicantId::from(replicant_code));
        let mut page = 1_i64;
        let mut pages = 0_usize;
        let mut stars_seen = 0_usize;
        let mut explored_designations = BTreeSet::new();
        loop {
            if pages == self.max_pages {
                return Err(Error::Decode {
                    message: "replicant star pagination exceeded configured page bound".into(),
                    status: None,
                    source: None,
                });
            }
            let page_started = Instant::now();
            let request_started = Instant::now();
            let response = self
                .client
                .managed_raw()
                .replicants()
                .stars(
                    replicant_code,
                    &raw::galaxy::StarListQuery {
                        page: Some(page),
                        per_page: Some(100),
                    },
                )
                .await?;
            let request_elapsed = request_started.elapsed();
            if response.value.page.is_some_and(|actual| actual != page)
                || response.value.total_pages.is_some_and(|total| total < page)
            {
                return Err(Error::Decode {
                    message: "replicant star pagination did not progress monotonically".into(),
                    status: None,
                    source: None,
                });
            }
            let page_rows = response.value.stars.len();
            let persist_started = Instant::now();
            for star in &response.value.stars {
                let observation = domain::replicant_star_knowledge(
                    star,
                    replicant.clone(),
                    Realm::Live,
                    domain::ObservationTime::now(),
                )
                .map_err(|error| Error::Decode {
                    message: error.to_string(),
                    status: None,
                    source: None,
                })?;
                if observation.value.explored == Some(true) {
                    explored_designations.insert(observation.value.star.id.clone());
                }
                self.client
                    .managed_state()
                    .persist_star_knowledge(observation)
                    .map_err(persistence_error)?;
                stars_seen += 1;
            }
            pages += 1;
            let total_pages = response.value.total_pages.unwrap_or(page);
            info!(
                target: "replicant_client::galaxy",
                event = "galaxy.replicant_stars_page_completed",
                replicant = replicant_code,
                page,
                total_pages,
                records = page_rows,
                explored_total = explored_designations.len(),
                request_ms = request_elapsed.as_millis() as u64,
                normalize_and_persist_ms = persist_started.elapsed().as_millis() as u64,
                elapsed_ms = page_started.elapsed().as_millis() as u64,
                "replicant star page committed"
            );
            if page >= total_pages {
                break;
            }
            page += 1;
        }
        let report = ReplicantStarSyncReport {
            pages,
            stars_seen,
            explored_designations,
        };
        info!(
            target: "replicant_client::galaxy",
            event = "galaxy.replicant_stars_completed",
            replicant = replicant_code,
            pages = report.pages,
            stars_seen = report.stars_seen,
            explored = report.explored_designations.len(),
            elapsed_ms = total_started.elapsed().as_millis() as u64,
            "replicant star knowledge synchronization completed"
        );
        Ok(report)
    }
}
