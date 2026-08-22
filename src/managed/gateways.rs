//! Managed read gateways. They normalize the one response they fetch, commit it,
//! publish the resulting revision, and only then return a domain value.

use std::{
    collections::{BTreeMap, BTreeSet},
    ops::RangeInclusive,
    sync::{Arc, Mutex},
    time::Instant,
};

use tokio::sync::watch;
use tracing::{debug, info};

use crate::domain::{
    self, AccessScope, Account, AccountId, Atmosphere, Blueprint, Device, DeviceCommand,
    DeviceFeature, DeviceId, DeviceKey, DeviceStatus, DeviceType, Knowledge, LifeStage, Location,
    LocationType, Realm, Replicant, ReplicantId, ReplicantKey, ReplicantStatus,
};
use crate::raw;
use crate::{Client, Error, Result};

use super::ami::{FleetController, MiningController, SurveyController, TransportController};
use super::operation::{self, ConfirmAccountWipe, DynamicCommand, Operation};
use super::travel::TravelBuilder;

const MAX_DEVICE_TAG_CHARACTERS: usize = 32;

/// A local device-update stream. It never polls or otherwise issues network requests.
pub struct DeviceWatch {
    receiver: watch::Receiver<std::sync::Arc<super::state::StateSnapshot>>,
    key: DeviceKey,
    last_seen: Option<Device>,
}

impl DeviceWatch {
    fn take_device_change(&mut self) -> (bool, Option<Device>) {
        let current = self
            .receiver
            .borrow_and_update()
            .devices()
            .get(&self.key)
            .map(|observation| observation.value.clone());
        if current == self.last_seen {
            return (false, None);
        }
        self.last_seen = current.clone();
        (true, current)
    }

    /// Returns the latest published snapshot for this device only when that
    /// device actually changed. Unrelated global state revisions are ignored.
    pub fn try_next(&mut self) -> Option<Device> {
        if !self.receiver.has_changed().ok()? {
            return None;
        }
        let (changed, current) = self.take_device_change();
        changed.then_some(current).flatten()
    }

    /// Waits for the newest committed value of this device. Intermediate
    /// revisions coalesce and unrelated global state revisions are ignored.
    /// `None` retains the existing meaning that the watched device disappeared
    /// or the underlying state stream closed.
    pub async fn next(&mut self) -> Option<Device> {
        loop {
            self.receiver.changed().await.ok()?;
            let (changed, current) = self.take_device_change();
            if changed {
                return current;
            }
        }
    }
}

fn normalization(error: domain::NormalizeError) -> Error {
    Error::Decode {
        message: error.to_string(),
        status: None,
        source: None,
    }
}

fn observed_at() -> crate::domain::ObservationTime {
    crate::domain::ObservationTime::now()
}

/// The result of one local location predicate evaluation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LocationPredicateOutcome {
    Matched,
    Rejected,
    Unknown,
}

/// Sanitized detail for one location predicate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocationPredicateDiagnostic {
    pub predicate: String,
    pub outcome: LocationPredicateOutcome,
    pub observed: Option<String>,
    pub reason: String,
}

/// The local predicate trace for one location.
#[derive(Clone, Debug, PartialEq)]
pub struct LocationDiagnostic {
    pub location: Location,
    pub predicates: Vec<LocationPredicateDiagnostic>,
}

/// Results and local predicate traces from [`LocationQuery::collect_with_diagnostics`].
#[derive(Clone, Debug, PartialEq)]
pub struct LocationQueryDiagnostics {
    pub matches: Vec<Location>,
    pub evaluations: Vec<LocationDiagnostic>,
}

#[derive(Clone, Debug)]
enum LocationPredicate {
    Realm(Realm),
    PlanetaryBody,
    Surveyed,
    HasAtmosphere,
    BreathableAtmosphere,
    Atmosphere(Atmosphere),
    MagneticField,
    HabitableZone,
    LifeStageBelow(LifeStage),
    GravityAbove(f64),
    GravityBelow(f64),
    GravityBetween(RangeInclusive<f64>),
    TemperatureAbove(f64),
    TemperatureBelow(f64),
    TemperatureBetween(RangeInclusive<f64>),
    System(String),
    Location(String),
}

/// Fluent, local-only query over committed location observations.
#[derive(Clone, Debug)]
pub struct LocationQuery {
    client: Client,
    predicates: Vec<LocationPredicate>,
}

impl LocationQuery {
    pub(crate) fn new(client: Client) -> Self {
        Self {
            client,
            predicates: Vec::new(),
        }
    }

    #[must_use]
    pub fn in_realm(mut self, realm: Realm) -> Self {
        self.predicates.push(LocationPredicate::Realm(realm));
        self
    }
    #[must_use]
    pub fn planetary_bodies(mut self) -> Self {
        self.predicates.push(LocationPredicate::PlanetaryBody);
        self
    }
    #[must_use]
    pub fn surveyed(mut self) -> Self {
        self.predicates.push(LocationPredicate::Surveyed);
        self
    }
    #[must_use]
    pub fn has_atmosphere(mut self) -> Self {
        self.predicates.push(LocationPredicate::HasAtmosphere);
        self
    }
    /// Matches atmospheres known to support unassisted human breathing.
    ///
    /// Both the semantic `breathable` value and the live API's `standard`
    /// classification are accepted.
    #[must_use]
    pub fn breathable_atmosphere(mut self) -> Self {
        self.predicates
            .push(LocationPredicate::BreathableAtmosphere);
        self
    }
    #[must_use]
    pub fn atmosphere_is(mut self, atmosphere: Atmosphere) -> Self {
        self.predicates
            .push(LocationPredicate::Atmosphere(atmosphere));
        self
    }
    #[must_use]
    pub fn has_magnetic_field(mut self) -> Self {
        self.predicates.push(LocationPredicate::MagneticField);
        self
    }
    #[must_use]
    pub fn in_habitable_zone(mut self) -> Self {
        self.predicates.push(LocationPredicate::HabitableZone);
        self
    }
    #[must_use]
    pub fn life_stage_below(mut self, stage: LifeStage) -> Self {
        self.predicates
            .push(LocationPredicate::LifeStageBelow(stage));
        self
    }
    /// Excludes only locations whose known life stage is intelligent or later.
    #[must_use]
    pub fn without_advanced_civilisation(self) -> Self {
        self.life_stage_below(LifeStage::Intelligent)
    }
    /// Strictly greater than `g` Earth gravities.
    #[must_use]
    pub fn gravity_g_above(mut self, g: f64) -> Self {
        self.predicates.push(LocationPredicate::GravityAbove(g));
        self
    }
    /// Strictly less than `g` Earth gravities.
    #[must_use]
    pub fn gravity_g_below(mut self, g: f64) -> Self {
        self.predicates.push(LocationPredicate::GravityBelow(g));
        self
    }
    /// Inclusive Earth-gravity range.
    #[must_use]
    pub fn gravity_g_between(mut self, range: RangeInclusive<f64>) -> Self {
        self.predicates
            .push(LocationPredicate::GravityBetween(range));
        self
    }
    /// Strictly greater than `c` degrees Celsius.
    #[must_use]
    pub fn surface_temp_c_above(mut self, c: f64) -> Self {
        self.predicates.push(LocationPredicate::TemperatureAbove(c));
        self
    }
    /// Strictly less than `c` degrees Celsius.
    #[must_use]
    pub fn surface_temp_c_below(mut self, c: f64) -> Self {
        self.predicates.push(LocationPredicate::TemperatureBelow(c));
        self
    }
    /// Inclusive Celsius range.
    #[must_use]
    pub fn surface_temp_c_between(mut self, range: RangeInclusive<f64>) -> Self {
        self.predicates
            .push(LocationPredicate::TemperatureBetween(range));
        self
    }
    #[must_use]
    pub fn in_system(mut self, system: impl Into<String>) -> Self {
        self.predicates
            .push(LocationPredicate::System(system.into()));
        self
    }
    #[must_use]
    pub fn at(mut self, location: impl Into<String>) -> Self {
        self.predicates
            .push(LocationPredicate::Location(location.into()));
        self
    }

    /// Returns a stable key-sorted local snapshot; it never performs network I/O.
    pub async fn collect(self) -> Result<Vec<Location>> {
        Ok(self.evaluate().matches)
    }

    /// Returns the same local evaluation as [`Self::collect`] plus predicate traces.
    pub async fn collect_with_diagnostics(self) -> Result<LocationQueryDiagnostics> {
        Ok(self.evaluate())
    }

    fn evaluate(&self) -> LocationQueryDiagnostics {
        let started_at = Instant::now();
        let observations = self.client.managed_state().locations();
        let input_locations = observations.len();
        let predicate_count = self.predicates.len();
        let mut matches = Vec::new();
        let mut matched_predicates = 0usize;
        let mut rejected_predicates = 0usize;
        let mut unknown_predicates = 0usize;

        let evaluations = observations
            .into_iter()
            .map(|observation| {
                let location = observation.value;
                let predicates = self
                    .predicates
                    .iter()
                    .map(|predicate| evaluate_location(predicate, &location))
                    .inspect(|diagnostic| match diagnostic.outcome {
                        LocationPredicateOutcome::Matched => matched_predicates += 1,
                        LocationPredicateOutcome::Rejected => rejected_predicates += 1,
                        LocationPredicateOutcome::Unknown => unknown_predicates += 1,
                    })
                    .collect::<Vec<_>>();
                if predicates
                    .iter()
                    .all(|diagnostic| diagnostic.outcome == LocationPredicateOutcome::Matched)
                {
                    matches.push(location.clone());
                }
                LocationDiagnostic {
                    location,
                    predicates,
                }
            })
            .collect::<Vec<_>>();

        debug!(
            target: "replicant_client::query::locations",
            event = "location_query.evaluated",
            input_locations,
            predicate_count,
            result_count = matches.len(),
            matched_predicates,
            rejected_predicates,
            unknown_predicates,
            elapsed_ms = started_at.elapsed().as_millis() as u64,
            "evaluated local location predicates"
        );

        LocationQueryDiagnostics {
            matches,
            evaluations,
        }
    }
}

fn result(
    predicate: impl Into<String>,
    outcome: LocationPredicateOutcome,
    observed: Option<String>,
    reason: impl Into<String>,
) -> LocationPredicateDiagnostic {
    LocationPredicateDiagnostic {
        predicate: predicate.into(),
        outcome,
        observed,
        reason: reason.into(),
    }
}

fn known_bool(
    predicate: &str,
    value: &Knowledge<bool>,
    expected: bool,
) -> LocationPredicateDiagnostic {
    match value {
        Knowledge::Unknown => result(
            predicate,
            LocationPredicateOutcome::Unknown,
            None,
            "unknown field",
        ),
        Knowledge::Absent => result(
            predicate,
            LocationPredicateOutcome::Rejected,
            Some("absent".into()),
            "known absent",
        ),
        Knowledge::Present(value) if *value == expected => result(
            predicate,
            LocationPredicateOutcome::Matched,
            Some(value.to_string()),
            "matched",
        ),
        Knowledge::Present(value) => result(
            predicate,
            LocationPredicateOutcome::Rejected,
            Some(value.to_string()),
            "value did not match",
        ),
    }
}

fn known_number(
    predicate: &str,
    value: &Knowledge<f64>,
    matches: impl FnOnce(f64) -> bool,
) -> LocationPredicateDiagnostic {
    match value {
        Knowledge::Unknown => result(
            predicate,
            LocationPredicateOutcome::Unknown,
            None,
            "unknown field",
        ),
        Knowledge::Absent => result(
            predicate,
            LocationPredicateOutcome::Rejected,
            Some("absent".into()),
            "known absent",
        ),
        Knowledge::Present(value) if matches(*value) => result(
            predicate,
            LocationPredicateOutcome::Matched,
            Some(value.to_string()),
            "matched",
        ),
        Knowledge::Present(value) => result(
            predicate,
            LocationPredicateOutcome::Rejected,
            Some(value.to_string()),
            "numeric boundary failure",
        ),
    }
}

fn evaluate_location(
    predicate: &LocationPredicate,
    location: &Location,
) -> LocationPredicateDiagnostic {
    match predicate {
        LocationPredicate::Realm(realm) => {
            let matched = location.key.realm == *realm;
            result(
                "realm",
                if matched {
                    LocationPredicateOutcome::Matched
                } else {
                    LocationPredicateOutcome::Rejected
                },
                Some(format!("{:?}", location.key.realm)),
                "realm filter",
            )
        }
        LocationPredicate::PlanetaryBody => {
            let matched = matches!(
                location.location_type,
                Some(LocationType::Planet | LocationType::Moon)
            );
            result(
                "planetary_bodies",
                if matched {
                    LocationPredicateOutcome::Matched
                } else {
                    LocationPredicateOutcome::Rejected
                },
                location
                    .location_type
                    .as_ref()
                    .map(|kind| kind.as_str().into()),
                "location type",
            )
        }
        LocationPredicate::Surveyed => match location.scanned {
            Some(true) => result(
                "surveyed",
                LocationPredicateOutcome::Matched,
                Some("true".into()),
                "explicit survey flag",
            ),
            Some(false) => result(
                "surveyed",
                LocationPredicateOutcome::Rejected,
                Some("false".into()),
                "explicitly not surveyed",
            ),
            None if location.has_survey_environment_evidence() => result(
                "surveyed",
                LocationPredicateOutcome::Matched,
                Some("inferred".into()),
                "survey-only environment fields are present",
            ),
            None => result(
                "surveyed",
                LocationPredicateOutcome::Unknown,
                None,
                "unknown survey state",
            ),
        },
        LocationPredicate::HasAtmosphere => match &location.environment.atmosphere {
            Knowledge::Unknown => result(
                "has_atmosphere",
                LocationPredicateOutcome::Unknown,
                None,
                "unknown field",
            ),
            Knowledge::Absent => result(
                "has_atmosphere",
                LocationPredicateOutcome::Rejected,
                Some("absent".into()),
                "known absent",
            ),
            Knowledge::Present(value) => result(
                "has_atmosphere",
                LocationPredicateOutcome::Matched,
                Some(value.as_str().into()),
                "matched",
            ),
        },
        LocationPredicate::BreathableAtmosphere => match &location.environment.atmosphere {
            Knowledge::Unknown => result(
                "breathable_atmosphere",
                LocationPredicateOutcome::Unknown,
                None,
                "unknown field",
            ),
            Knowledge::Absent => result(
                "breathable_atmosphere",
                LocationPredicateOutcome::Rejected,
                Some("absent".into()),
                "known absent",
            ),
            Knowledge::Present(value) if value.is_breathable() => result(
                "breathable_atmosphere",
                LocationPredicateOutcome::Matched,
                Some(value.as_str().into()),
                "classification supports unassisted breathing",
            ),
            Knowledge::Present(value) => result(
                "breathable_atmosphere",
                LocationPredicateOutcome::Rejected,
                Some(value.as_str().into()),
                "classification is not breathable",
            ),
        },
        LocationPredicate::Atmosphere(expected) => match &location.environment.atmosphere {
            Knowledge::Unknown => result(
                format!("atmosphere_is({})", expected.as_str()),
                LocationPredicateOutcome::Unknown,
                None,
                "unknown field",
            ),
            Knowledge::Absent => result(
                format!("atmosphere_is({})", expected.as_str()),
                LocationPredicateOutcome::Rejected,
                Some("absent".into()),
                "known absent",
            ),
            Knowledge::Present(value) if value == expected => result(
                format!("atmosphere_is({})", expected.as_str()),
                LocationPredicateOutcome::Matched,
                Some(value.as_str().into()),
                "matched",
            ),
            Knowledge::Present(value) => result(
                format!("atmosphere_is({})", expected.as_str()),
                LocationPredicateOutcome::Rejected,
                Some(value.as_str().into()),
                "value did not match",
            ),
        },
        LocationPredicate::MagneticField => known_bool(
            "has_magnetic_field",
            &location.environment.magnetic_field,
            true,
        ),
        LocationPredicate::HabitableZone => known_bool(
            "in_habitable_zone",
            &location.environment.in_habitable_zone,
            true,
        ),
        LocationPredicate::LifeStageBelow(expected) => match &location.environment.life_stage {
            Knowledge::Unknown => result(
                format!("life_stage_below({})", expected.as_str()),
                LocationPredicateOutcome::Unknown,
                None,
                "unknown life knowledge",
            ),
            Knowledge::Absent => result(
                format!("life_stage_below({})", expected.as_str()),
                LocationPredicateOutcome::Matched,
                Some("no_life".into()),
                "known no life",
            ),
            Knowledge::Present(value) => {
                match (value.canonical_rank(), expected.canonical_rank()) {
                    (Some(rank), Some(threshold)) if rank < threshold => result(
                        format!("life_stage_below({})", expected.as_str()),
                        LocationPredicateOutcome::Matched,
                        Some(value.as_str().into()),
                        "matched",
                    ),
                    (Some(_), Some(_)) => result(
                        format!("life_stage_below({})", expected.as_str()),
                        LocationPredicateOutcome::Rejected,
                        Some(value.as_str().into()),
                        "life stage boundary failure",
                    ),
                    _ => result(
                        format!("life_stage_below({})", expected.as_str()),
                        LocationPredicateOutcome::Unknown,
                        Some(value.as_str().into()),
                        "unknown future life stage",
                    ),
                }
            }
        },
        LocationPredicate::GravityAbove(value) => known_number(
            "gravity_g_above",
            &location.environment.gravity_g,
            |actual| actual > *value,
        ),
        LocationPredicate::GravityBelow(value) => known_number(
            "gravity_g_below",
            &location.environment.gravity_g,
            |actual| actual < *value,
        ),
        LocationPredicate::GravityBetween(range) => known_number(
            "gravity_g_between",
            &location.environment.gravity_g,
            |actual| range.contains(&actual),
        ),
        LocationPredicate::TemperatureAbove(value) => known_number(
            "surface_temp_c_above",
            &location.environment.surface_temp_c,
            |actual| actual > *value,
        ),
        LocationPredicate::TemperatureBelow(value) => known_number(
            "surface_temp_c_below",
            &location.environment.surface_temp_c,
            |actual| actual < *value,
        ),
        LocationPredicate::TemperatureBetween(range) => known_number(
            "surface_temp_c_between",
            &location.environment.surface_temp_c,
            |actual| range.contains(&actual),
        ),
        LocationPredicate::System(system) => {
            let designation = location.key.id.as_str();
            let matched = location.system.as_deref().map_or_else(
                || designation == system || designation.starts_with(&format!("{system}-")),
                |actual| actual == system,
            );
            result(
                "in_system",
                if matched {
                    LocationPredicateOutcome::Matched
                } else {
                    LocationPredicateOutcome::Rejected
                },
                location.system.clone(),
                "system filter",
            )
        }
        LocationPredicate::Location(expected) => {
            let matched = location.key.id.as_str() == expected;
            result(
                "at",
                if matched {
                    LocationPredicateOutcome::Matched
                } else {
                    LocationPredicateOutcome::Rejected
                },
                Some(location.key.id.as_str().into()),
                "location filter",
            )
        }
    }
}

#[cfg(test)]
mod location_predicate_tests {
    use super::*;
    use crate::domain::{LocationEnvironment, LocationId, WorldKey};

    fn location() -> Location {
        Location {
            key: WorldKey::in_realm(Realm::Live, LocationId::from("SOL-2")),
            location_type: Some(LocationType::Planet),
            scanned: None,
            system_scanned: Some(true),
            system_tags: Vec::new(),
            system: Some("SOL".into()),
            parent: None,
            survey_progress: Default::default(),
            environment: LocationEnvironment {
                atmosphere: Knowledge::Present(Atmosphere::Standard),
                magnetic_field: Knowledge::Present(true),
                gravity_g: Knowledge::Present(1.0),
                surface_temp_c: Knowledge::Present(18.0),
                in_habitable_zone: Knowledge::Present(true),
                life_stage: Knowledge::Present(LifeStage::Microbial),
                ..LocationEnvironment::default()
            },
            unknown: BTreeMap::new(),
        }
    }

    #[test]
    fn environment_predicates_use_documented_boundaries_and_unknown_is_not_a_match() {
        let value = location();
        assert_eq!(
            evaluate_location(&LocationPredicate::GravityAbove(1.0), &value).outcome,
            LocationPredicateOutcome::Rejected
        );
        assert_eq!(
            evaluate_location(&LocationPredicate::GravityBetween(0.8..=1.0), &value).outcome,
            LocationPredicateOutcome::Matched
        );
        assert_eq!(
            evaluate_location(&LocationPredicate::TemperatureBelow(18.0), &value).outcome,
            LocationPredicateOutcome::Rejected
        );
        assert_eq!(
            evaluate_location(
                &LocationPredicate::LifeStageBelow(LifeStage::Intelligent),
                &value
            )
            .outcome,
            LocationPredicateOutcome::Matched
        );
        assert_eq!(
            evaluate_location(&LocationPredicate::Surveyed, &value).outcome,
            LocationPredicateOutcome::Matched
        );
        assert_eq!(
            evaluate_location(&LocationPredicate::BreathableAtmosphere, &value).outcome,
            LocationPredicateOutcome::Matched
        );
        let mut unknown = value;
        unknown.environment.magnetic_field = Knowledge::Unknown;
        assert_eq!(
            evaluate_location(&LocationPredicate::MagneticField, &unknown).outcome,
            LocationPredicateOutcome::Unknown
        );
        unknown.environment.life_stage = Knowledge::Present(LifeStage::from("post-singularity"));
        assert_eq!(
            evaluate_location(
                &LocationPredicate::LifeStageBelow(LifeStage::Intelligent),
                &unknown
            )
            .outcome,
            LocationPredicateOutcome::Unknown
        );
    }
}

/// State-neutral tutorial progress gateway. Tutorial progression is
/// authoritative server-owned onboarding state and is intentionally not
/// projected into the managed SQLite store.
#[derive(Clone, Debug)]
pub struct TutorialsGateway {
    client: Client,
}

impl TutorialsGateway {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    /// Lists every tutorial and the authenticated account's current progress.
    pub async fn list(&self) -> Result<raw::tutorials::TutorialListResponse> {
        self.client.ensure_open()?;
        Ok(self.client.managed_raw().tutorials().list().await?.value)
    }

    /// Fetches one tutorial's detailed step progress by slug.
    pub async fn get(&self, slug: &str) -> Result<raw::tutorials::TutorialDetail> {
        self.client.ensure_open()?;
        Ok(self.client.managed_raw().tutorials().get(slug).await?.value)
    }
}

/// Managed blueprint catalogue gateway.
///
/// Blueprint knowledge is account-wide and server-authoritative. This gateway
/// keeps the raw endpoint behind the managed client boundary and returns only
/// normalized domain values, allowing Director/runtime callers to avoid raw
/// HTTP without inventing a durable cache before one is needed.
#[derive(Clone, Debug)]
pub struct BlueprintsGateway {
    client: Client,
}

impl BlueprintsGateway {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    /// Lists the account's currently unlocked blueprints as normalized values.
    pub async fn list(&self) -> Result<Vec<Blueprint>> {
        let started_at = Instant::now();
        self.client.ensure_open()?;
        let response = self.client.managed_raw().blueprints().list().await?;
        let mut blueprints = response
            .value
            .blueprints
            .iter()
            .map(domain::blueprint)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(normalization)?;
        blueprints.sort_by(|left, right| left.id.cmp(&right.id));
        info!(
            target: "replicant_client::gateway::blueprints",
            event = "blueprints.list_completed",
            blueprints = blueprints.len(),
            elapsed_ms = started_at.elapsed().as_millis() as u64,
            "completed managed blueprint catalogue read"
        );
        Ok(blueprints)
    }

    /// Returns the currently unlocked blueprint device types.
    pub async fn unlocked_device_types(&self) -> Result<BTreeSet<DeviceType>> {
        Ok(self
            .list()
            .await?
            .into_iter()
            .filter_map(|blueprint| blueprint.device_type)
            .collect())
    }
}

/// Gateway for the authenticated account. `get` is explicit remote I/O.
#[derive(Clone, Debug)]
pub struct AccountGateway {
    client: Client,
}
impl AccountGateway {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    pub async fn get(&self) -> Result<Account> {
        let started_at = Instant::now();
        self.client.ensure_open()?;

        let request_started_at = Instant::now();
        let response = self.client.managed_raw().accounts().me().await?;
        let request_ms = request_started_at.elapsed().as_millis() as u64;

        let normalize_started_at = Instant::now();
        let id = response
            .value
            .email
            .clone()
            .filter(|email| !email.is_empty())
            .map(AccountId::from)
            .ok_or_else(|| Error::Decode {
                message: "response omitted required identity `email`".into(),
                status: None,
                source: None,
            })?;
        let observation = domain::account_me(&response.value, id, observed_at());
        let value = observation.value.clone();
        let normalize_ms = normalize_started_at.elapsed().as_millis() as u64;

        let persist_started_at = Instant::now();
        self.client
            .managed_state()
            .persist_account(observation)
            .map_err(super::client::store_error)?;
        let persist_ms = persist_started_at.elapsed().as_millis() as u64;

        info!(
            target: "replicant_client::gateway::account",
            event = "account.get_completed",
            request_ms,
            normalize_ms,
            persist_ms,
            elapsed_ms = started_at.elapsed().as_millis() as u64,
            "completed managed account read"
        );
        Ok(value)
    }

    pub async fn refresh(&self) -> Result<Account> {
        self.get().await
    }

    /// Rebinds this store after a requested email change has been verified.
    /// The ordinary startup path still rejects a different account.
    pub async fn rebind_after_verified_email(&self, previous_email: &str) -> Result<Account> {
        self.client.ensure_open()?;
        let response = self.client.managed_raw().accounts().me().await?;
        if response.value.email_verified != Some(true) {
            return Err(Error::Configuration {
                message: "the replacement email is not verified".into(),
            });
        }
        let id = response
            .value
            .email
            .clone()
            .filter(|email| !email.is_empty())
            .map(AccountId::from)
            .ok_or_else(|| Error::Decode {
                message: "response omitted required identity `email`".into(),
                status: None,
                source: None,
            })?;
        let observation = domain::account_me(&response.value, id, observed_at());
        let value = observation.value.clone();
        self.client
            .managed_state()
            .rebind_account_and_persist(&AccountId::from(previous_email), observation)
            .map_err(super::client::store_error)?;
        Ok(value)
    }

    /// Updates the authenticated account's profile as a durable operation.
    pub async fn update(&self, request: raw::accounts::AccountUpdateRequest) -> Result<Operation> {
        operation::account_update(&self.client, request).await
    }

    /// Requests destructive, irreversible deletion of the authenticated
    /// account. Requires an explicit [`ConfirmAccountWipe`] naming the exact
    /// account being destroyed, checked against the account bound to this
    /// store before the request is even registered.
    ///
    /// A destructive wipe cannot be requested without naming the account:
    ///
    /// ```compile_fail
    /// # async fn example(client: replicant_client::Client) -> Result<(), replicant_client::Error> {
    /// // Missing the required destructive confirmation: this does not compile.
    /// client.account().wipe().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn wipe(&self, confirm: ConfirmAccountWipe) -> Result<Operation> {
        operation::account_wipe(&self.client, confirm).await
    }
}

/// Options for one Autofactory print command.
#[derive(Clone, Debug, PartialEq)]
pub struct AutofactoryPrintOptions {
    quantity: i64,
    controller: Option<String>,
    oncomplete: Option<raw::JsonObject>,
    tags: Vec<String>,
    flatpack: Option<bool>,
}

impl AutofactoryPrintOptions {
    /// Creates options for `quantity` copies in the normal assembled state.
    #[must_use]
    pub fn new(quantity: i64) -> Self {
        Self {
            quantity,
            controller: None,
            oncomplete: None,
            tags: Vec::new(),
            flatpack: None,
        }
    }

    /// Routes printed devices to an AMI controller.
    #[must_use]
    pub fn controller(mut self, controller: impl Into<String>) -> Self {
        self.controller = Some(controller.into());
        self
    }

    /// Sets the server-defined command run after each print completes.
    #[must_use]
    pub fn oncomplete(mut self, command: raw::JsonObject) -> Self {
        self.oncomplete = Some(command);
        self
    }

    /// Applies tags to every printed device.
    #[must_use]
    pub fn tags<I, S>(mut self, tags: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.tags = tags.into_iter().map(Into::into).collect();
        self
    }

    /// Requests compacted output for a device with the `modular` feature.
    #[must_use]
    pub fn flatpacked(mut self) -> Self {
        self.flatpack = Some(true);
        self
    }
}

/// A durable, local-state handle for a mutable device. It performs no unsafe actions.
#[derive(Clone, Debug)]
pub struct DeviceHandle {
    client: Client,
    key: DeviceKey,
}
impl DeviceHandle {
    fn new(client: Client, key: DeviceKey) -> Self {
        Self { client, key }
    }
    #[cfg(test)]
    pub(crate) fn for_test(client: Client, key: DeviceKey) -> Self {
        Self::new(client, key)
    }
    #[must_use]
    pub fn id(&self) -> &DeviceId {
        &self.key.id
    }
    #[must_use]
    pub fn realm(&self) -> &Realm {
        &self.key.realm
    }
    pub async fn snapshot(&self) -> Result<Device> {
        self.client.ensure_open()?;
        self.client
            .managed_state()
            .device(&self.key)
            .map(|observation| observation.value)
            .ok_or_else(|| Error::Configuration {
                message: "device is not cached".into(),
            })
    }
    pub async fn refresh(&self) -> Result<DeviceHandle> {
        if self.key.realm != Realm::Live {
            return Err(Error::Configuration {
                message:
                    "simulation refresh is not available until simulation reads are implemented"
                        .into(),
            });
        }
        self.client.devices().get(self.id().as_str()).await
    }
    pub async fn watch(&self) -> Result<DeviceWatch> {
        self.client.ensure_open()?;
        Ok(DeviceWatch {
            receiver: self.client.managed_state().subscribe(),
            key: self.key.clone(),
            last_seen: self
                .client
                .managed_state()
                .device(&self.key)
                .map(|observation| observation.value),
        })
    }

    /// Activates a dormant device.
    pub async fn activate(&self) -> Result<Operation> {
        self.command(raw::devices::DeviceCommand::Activate).await
    }
    /// Deactivates an active device.
    pub async fn deactivate(&self) -> Result<Operation> {
        self.command(raw::devices::DeviceCommand::Deactivate).await
    }
    /// Cancels this device's current interruptible operation.
    pub async fn cancel(&self) -> Result<Operation> {
        self.command(raw::devices::DeviceCommand::Cancel).await
    }
    /// Deploys a device into the field.
    pub async fn deploy(&self) -> Result<Operation> {
        self.command(raw::devices::DeviceCommand::Deploy).await
    }
    /// Recalls this device to its assigned replicant's vessel.
    pub async fn recall(&self) -> Result<Operation> {
        self.command(raw::devices::DeviceCommand::Recall).await
    }
    /// Stows this device inside another.
    pub async fn stow(&self, target: Option<String>) -> Result<Operation> {
        self.command(raw::devices::DeviceCommand::Stow { target })
            .await
    }
    /// Transfers this device to another account or replicant.
    pub async fn change_owner(&self, target: impl Into<String>) -> Result<Operation> {
        self.command(raw::devices::DeviceCommand::ChangeOwner {
            target: target.into(),
        })
        .await
    }
    /// Attaches one or more devices to this device.
    pub async fn attach(&self, targets: raw::devices::TargetsCommand) -> Result<Operation> {
        self.command(raw::devices::DeviceCommand::Attach(targets))
            .await
    }
    /// Repairs `target` using this maintenance-capable device.
    pub async fn repair(&self, target: impl Into<String>) -> Result<Operation> {
        self.command(raw::devices::DeviceCommand::Repair {
            device: None,
            target: Some(target.into()),
        })
        .await
    }
    /// Compacts this device's stowed contents.
    pub async fn compact(&self) -> Result<Operation> {
        self.command(raw::devices::DeviceCommand::Compact).await
    }
    /// Unfurls a deployed structure.
    pub async fn unfurl(&self) -> Result<Operation> {
        self.command(raw::devices::DeviceCommand::Unfurl).await
    }

    /// Begins a galactic-observatory prospect in an optional direction vector.
    ///
    /// Passing `None` uses the server's documented default: outward from Sol.
    /// Any non-zero vector is accepted by the wire contract; normalization is
    /// deliberately left to callers because vector magnitude has no semantic
    /// meaning for prospecting.
    pub async fn prospect(&self, direction: Option<[f64; 3]>) -> Result<Operation> {
        self.command(raw::devices::DeviceCommand::Prospect {
            direction: direction.map(Vec::from),
        })
        .await
    }

    /// Triangulates a spectral signature from a three-dimensional reference point.
    pub async fn triangulate(
        &self,
        signature: impl Into<String>,
        target: [f64; 3],
    ) -> Result<Operation> {
        self.command(raw::devices::DeviceCommand::Triangulate {
            signature: signature.into(),
            target: target.into(),
        })
        .await
    }

    /// Queues `quantity` copies of a device on this autofactory.
    pub async fn enqueue_print(
        &self,
        device_type: impl Into<String>,
        quantity: i64,
    ) -> Result<Operation> {
        self.enqueue_print_configured(device_type, AutofactoryPrintOptions::new(quantity))
            .await
    }

    /// Queues `quantity` tagged copies of a device on this autofactory.
    ///
    /// Tags are persisted with each printed device and are useful for
    /// restart-safe orchestration that must identify the physical outputs of
    /// individual print jobs.
    pub async fn enqueue_print_with_tags<I, S>(
        &self,
        device_type: impl Into<String>,
        quantity: i64,
        tags: I,
    ) -> Result<Operation>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.enqueue_print_configured(
            device_type,
            AutofactoryPrintOptions::new(quantity).tags(tags),
        )
        .await
    }

    /// Queues a print using quantity, tags, controller, follow-up, and
    /// flatpack options.
    pub async fn enqueue_print_configured(
        &self,
        device_type: impl Into<String>,
        options: AutofactoryPrintOptions,
    ) -> Result<Operation> {
        let AutofactoryPrintOptions {
            quantity,
            controller,
            oncomplete,
            tags,
            flatpack,
        } = options;
        if quantity < 1 {
            return Err(Error::Configuration {
                message: "autofactory print quantity must be at least one".into(),
            });
        }
        if let Some(tag) = tags
            .iter()
            .find(|tag| tag.chars().count() > MAX_DEVICE_TAG_CHARACTERS)
        {
            return Err(Error::Configuration {
                message: format!(
                    "device tag {tag:?} exceeds the {MAX_DEVICE_TAG_CHARACTERS}-character limit"
                ),
            });
        }
        self.command(raw::devices::DeviceCommand::EnqueuePrint {
            device_type: device_type.into(),
            quantity: Some(quantity),
            controller,
            oncomplete,
            tags: (!tags.is_empty()).then_some(tags),
            flatpack,
        })
        .await
    }

    /// Dispatches any known device command as a durable operation. The
    /// intended command is checked against this device's latest cached
    /// `available_commands` first, but the server remains authoritative.
    pub async fn command(&self, command: raw::devices::DeviceCommand) -> Result<Operation> {
        operation::device_command(&self.client, self.id().as_str(), command).await
    }

    /// Dispatches a forward-compatible command this client's typed
    /// [`raw::devices::DeviceCommand`] does not (yet) name.
    pub async fn dynamic_command(&self, command: DynamicCommand) -> Result<Operation> {
        operation::device_dynamic_command(&self.client, self.id().as_str(), command).await
    }

    /// Updates this device's configuration as a durable operation.
    pub async fn configure(
        &self,
        configuration: raw::devices::DeviceConfiguration,
    ) -> Result<Operation> {
        operation::device_configure(
            &self.client,
            self.id().as_str(),
            raw::devices::DeviceConfigurationRequest { configuration },
        )
        .await
    }

    /// Links this device to another device through the 2.5.0
    /// `linked_device` configuration field. FTL slingshots use this to bind
    /// their remote empty replicant matrix.
    pub async fn link_device(&self, device_code: impl Into<String>) -> Result<Operation> {
        self.configure(raw::devices::DeviceConfiguration::default().link_device(device_code))
            .await
    }

    /// Clears this device's configured `linked_device` relationship.
    pub async fn unlink_device(&self) -> Result<Operation> {
        self.configure(raw::devices::DeviceConfiguration::default().unlink_device())
            .await
    }

    /// Grants a permission on this device to another account.
    pub async fn grant_permission(&self, request: raw::JsonObject) -> Result<Operation> {
        operation::device_grant_permission(&self.client, self.id().as_str(), request).await
    }

    /// Revokes a permission on this device.
    pub async fn revoke_permission(&self) -> Result<Operation> {
        operation::device_revoke_permission(&self.client, self.id().as_str()).await
    }

    /// Enters a simulation scenario via this simulator device.
    pub async fn enter_simulation(
        &self,
        request: raw::simulations::SimulationEnterRequest,
    ) -> Result<Operation> {
        operation::device_enter_simulation(&self.client, self.id().as_str(), request).await
    }

    /// Cancels a running simulation on this device.
    pub async fn abandon_simulation(&self, simulation_id: i64) -> Result<Operation> {
        operation::device_abandon_simulation(&self.client, self.id().as_str(), simulation_id).await
    }

    /// Creates a new trade on this trade controller device.
    pub async fn create_trade(&self, request: raw::JsonObject) -> Result<Operation> {
        operation::device_create_trade(&self.client, self.id().as_str(), request).await
    }

    /// Deletes a trade on this device, returning its escrowed rewards.
    pub async fn delete_trade(&self, trade_code: &str) -> Result<Operation> {
        operation::device_delete_trade(&self.client, self.id().as_str(), trade_code).await
    }

    /// Fulfills one unit of a trade on this device as a buyer.
    pub async fn fulfill_trade(&self, trade_code: &str) -> Result<Operation> {
        operation::device_fulfill_trade(&self.client, self.id().as_str(), trade_code).await
    }

    pub(crate) fn client(&self) -> &Client {
        &self.client
    }

    pub(crate) fn key(&self) -> &DeviceKey {
        &self.key
    }

    /// Views this device as an AMI mining controller. Rejected only when the
    /// latest cached snapshot names a different known device type; the
    /// server remains authoritative for command dispatch either way.
    pub fn as_mining_controller(&self) -> Result<MiningController> {
        MiningController::new(self.clone())
    }

    /// Views this device as an AMI survey controller.
    pub fn as_survey_controller(&self) -> Result<SurveyController> {
        SurveyController::new(self.clone())
    }

    /// Views this device as an AMI transport controller.
    pub fn as_transport_controller(&self) -> Result<TransportController> {
        TransportController::new(self.clone())
    }

    /// Views this device as an AMI fleet controller.
    pub fn as_fleet_controller(&self) -> Result<FleetController> {
        FleetController::new(self.clone())
    }

    /// Lists this device's event log. Diagnostic/append-only history,
    /// distinct from account events (`client.events()`) and location events
    /// (`client.location_events()`).
    pub async fn logs(
        &self,
        query: &raw::devices::DeviceLogsQuery,
    ) -> Result<raw::devices::DeviceLogsResponse> {
        self.client.ensure_open()?;
        Ok(self
            .client
            .managed_raw()
            .devices()
            .logs(self.id().as_str(), query)
            .await?
            .value)
    }

    /// Lists ownership/configuration audit entries for this device. The
    /// contract documents no response schema for this endpoint.
    pub async fn audit(&self, query: &raw::devices::DeviceAuditQuery) -> Result<serde_json::Value> {
        self.client.ensure_open()?;
        Ok(self
            .client
            .managed_raw()
            .devices()
            .audit(self.id().as_str(), query)
            .await?
            .value)
    }

    /// Lists permissions granted on this device. Volatile/diagnostic, not a
    /// durably reconciled collection: the contract documents no response
    /// schema for this endpoint.
    pub async fn permissions(&self) -> Result<serde_json::Value> {
        self.client.ensure_open()?;
        Ok(self
            .client
            .managed_raw()
            .devices()
            .list_permissions(self.id().as_str())
            .await?
            .value)
    }

    /// Fetches this device's relay network topology. Volatile: it is never
    /// durably reconciled.
    pub async fn network(&self) -> Result<raw::devices::DeviceNetwork> {
        self.client.ensure_open()?;
        Ok(self
            .client
            .managed_raw()
            .devices()
            .network(self.id().as_str())
            .await?
            .value)
    }

    /// Lists distinct BobNet channels this relay-capable device has
    /// observed. See [`Client::bobnet`](crate::Client::bobnet) for sending
    /// and account-wide BobNet event observation.
    pub async fn channels(&self) -> Result<raw::bobnet::DeviceChannelsResponse> {
        self.client.ensure_open()?;
        Ok(self
            .client
            .managed_raw()
            .bobnet()
            .channels(self.id().as_str())
            .await?
            .value)
    }

    /// Lists recent BobNet messages visible from this relay-capable device.
    /// Bounded diagnostic/catch-up history, distinct from the account-wide
    /// inbox (`client.messages()`).
    pub async fn relay_history(
        &self,
        query: &raw::bobnet::DeviceMessagesQuery,
    ) -> Result<raw::bobnet::DeviceMessagesResponse> {
        self.client.ensure_open()?;
        Ok(self
            .client
            .managed_raw()
            .bobnet()
            .messages(self.id().as_str(), query)
            .await?
            .value)
    }
}

#[derive(Clone, Debug, Default)]
enum DeviceLinkFilter<T> {
    #[default]
    Any,
    Is(T),
    None,
}

/// Local-only device query. It reads one immutable committed snapshot and
/// cannot perform network I/O.
#[derive(Clone, Debug)]
pub struct DeviceQuery {
    client: Client,
    predicate: domain::DevicePredicate,
    tags: Vec<String>,
    system: Option<String>,
    attached_to: DeviceLinkFilter<DeviceKey>,
    stowed_in: DeviceLinkFilter<DeviceKey>,
    controller: DeviceLinkFilter<DeviceKey>,
    assigned_replicant: DeviceLinkFilter<ReplicantKey>,
    hosting_replicant: DeviceLinkFilter<ReplicantKey>,
    untagged: bool,
    without_adopted_devices: bool,
}

impl DeviceQuery {
    fn new(client: Client) -> Self {
        Self {
            client,
            predicate: domain::DevicePredicate::default(),
            tags: Vec::new(),
            system: None,
            attached_to: DeviceLinkFilter::Any,
            stowed_in: DeviceLinkFilter::Any,
            controller: DeviceLinkFilter::Any,
            assigned_replicant: DeviceLinkFilter::Any,
            hosting_replicant: DeviceLinkFilter::Any,
            untagged: false,
            without_adopted_devices: false,
        }
    }

    #[must_use]
    pub fn in_realm(mut self, realm: Realm) -> Self {
        self.predicate = self.predicate.in_realm(realm);
        self
    }

    #[must_use]
    pub fn of_type(mut self, value: DeviceType) -> Self {
        self.predicate = self.predicate.of_type(value);
        self
    }

    #[must_use]
    pub fn with_status(mut self, value: DeviceStatus) -> Self {
        self.predicate = self.predicate.with_status(value);
        self
    }

    #[must_use]
    pub fn with_access(mut self, value: AccessScope) -> Self {
        self.predicate = self.predicate.with_access(value);
        self
    }

    #[must_use]
    pub fn owned(self) -> Self {
        self.with_access(AccessScope::Owned)
    }

    #[must_use]
    pub fn with_feature(mut self, value: DeviceFeature) -> Self {
        self.predicate = self.predicate.with_feature(value);
        self
    }

    #[must_use]
    pub fn with_command(mut self, value: DeviceCommand) -> Self {
        self.predicate = self.predicate.with_command(value);
        self
    }

    #[must_use]
    pub fn miners(self) -> Self {
        self.of_type(DeviceType::MiningDrone)
    }

    #[must_use]
    pub fn idle(self) -> Self {
        self.with_status(DeviceStatus::from("idle"))
    }

    /// Matches this exact location ID in every realm. Combine with
    /// [`Self::in_realm`] when a location name is ambiguous.
    #[must_use]
    pub fn at(mut self, location: impl Into<String>) -> Self {
        self.predicate = self.predicate.at(location.into());
        self
    }

    /// Matches locations in a system by its canonical code prefix (for
    /// example, `SOL` matches `SOL-1`).
    #[must_use]
    pub fn in_system(mut self, system: impl Into<String>) -> Self {
        self.system = Some(system.into());
        self
    }

    /// Requires every supplied tag to be present on the device.
    #[must_use]
    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    /// Requires the device to have no user-assigned tags.
    #[must_use]
    pub fn untagged(mut self) -> Self {
        self.untagged = true;
        self
    }

    #[must_use]
    pub fn attached_to(mut self, device: DeviceKey) -> Self {
        self.attached_to = DeviceLinkFilter::Is(device);
        self
    }

    #[must_use]
    pub fn unattached(mut self) -> Self {
        self.attached_to = DeviceLinkFilter::None;
        self
    }

    /// Matches devices physically stowed inside `device`.
    #[must_use]
    pub fn stowed_in(mut self, device: DeviceKey) -> Self {
        self.stowed_in = DeviceLinkFilter::Is(device);
        self
    }

    /// Matches devices which are not currently stowed inside another device.
    #[must_use]
    pub fn not_stowed(mut self) -> Self {
        self.stowed_in = DeviceLinkFilter::None;
        self
    }

    #[must_use]
    pub fn controlled_by(mut self, controller: DeviceKey) -> Self {
        self.controller = DeviceLinkFilter::Is(controller);
        self
    }

    #[must_use]
    pub fn without_controller(mut self) -> Self {
        self.controller = DeviceLinkFilter::None;
        self
    }

    #[must_use]
    pub fn assigned_to(mut self, replicant: ReplicantKey) -> Self {
        self.assigned_replicant = DeviceLinkFilter::Is(replicant);
        self
    }

    /// Matches vessels physically hosting this replicant's matrix.
    #[must_use]
    pub fn hosting_replicant(mut self, replicant: ReplicantKey) -> Self {
        self.hosting_replicant = DeviceLinkFilter::Is(replicant);
        self
    }

    /// Keeps controller devices which currently control no other cached
    /// device. A device is adopted when its `controller` relationship points
    /// at that controller.
    #[must_use]
    pub fn without_adopted_devices(mut self) -> Self {
        self.without_adopted_devices = true;
        self
    }

    fn matching_entries(
        &self,
        devices: impl IntoIterator<Item = domain::Observation<Device>>,
    ) -> BTreeMap<DeviceKey, Device> {
        let started = Instant::now();
        let devices: Vec<_> = devices.into_iter().collect();
        let input = devices.len();
        let adopted = self.without_adopted_devices.then(|| {
            devices
                .iter()
                .filter_map(|entry| entry.value.relationships.controller.clone())
                .collect::<BTreeSet<_>>()
        });
        let adopted_relationships = adopted.as_ref().map_or(0, BTreeSet::len);

        let mut predicate_matches = 0usize;
        let mut tag_matches = 0usize;
        let mut system_matches = 0usize;
        let mut relationship_matches = 0usize;
        let mut adoption_matches = 0usize;
        let mut result = BTreeMap::new();

        for entry in &devices {
            if !self.predicate.matches(&entry.value) {
                continue;
            }
            predicate_matches += 1;

            if !self.tags.iter().all(|tag| entry.value.tags.contains(tag))
                || (self.untagged && !entry.value.tags.is_empty())
            {
                continue;
            }
            tag_matches += 1;

            let matches_system = self.system.as_ref().is_none_or(|system| {
                entry.value.location.as_ref().is_some_and(|location| {
                    let id = location.id.as_str();
                    id == system
                        || id
                            .strip_prefix(system)
                            .is_some_and(|suffix| suffix.starts_with('-'))
                })
            });
            if !matches_system {
                continue;
            }
            system_matches += 1;

            if !matches_link(
                &self.attached_to,
                entry.value.relationships.attached_to.as_ref(),
            ) || !matches_link(
                &self.stowed_in,
                entry.value.relationships.stowed_in.as_ref(),
            ) || !matches_link(
                &self.controller,
                entry.value.relationships.controller.as_ref(),
            ) || !matches_link(
                &self.assigned_replicant,
                entry.value.relationships.assigned_replicant.as_ref(),
            ) || !matches_link(
                &self.hosting_replicant,
                entry.value.relationships.hosting_replicant.as_ref(),
            ) {
                continue;
            }
            relationship_matches += 1;

            if adopted
                .as_ref()
                .is_some_and(|adopted| adopted.contains(&entry.value.key))
            {
                continue;
            }
            adoption_matches += 1;
            result.insert(entry.value.key.clone(), entry.value.clone());
        }

        debug!(
            target: "replicant_client::query::devices",
            event = "query.devices_evaluated",
            input,
            predicate_matches,
            tag_matches,
            system_matches,
            relationship_matches,
            adoption_matches,
            adopted_relationships,
            results = result.len(),
            tags = self.tags.len(),
            system = self.system.as_deref().unwrap_or(""),
            without_adopted_devices = self.without_adopted_devices,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "evaluated local device query"
        );
        result
    }

    fn handles(&self, entries: &BTreeMap<DeviceKey, Device>) -> Vec<DeviceHandle> {
        entries
            .keys()
            .cloned()
            .map(|key| DeviceHandle::new(self.client.clone(), key))
            .collect()
    }

    /// Collects a stable, key-sorted view from the current committed snapshot.
    pub async fn collect(self) -> Result<Vec<DeviceHandle>> {
        self.client.ensure_open()?;
        let started = Instant::now();
        let entries = self.matching_entries(self.client.managed_state().devices());
        let handles = self.handles(&entries);
        debug!(
            target: "replicant_client::query::devices",
            event = "query.devices_collected",
            results = handles.len(),
            untagged = self.untagged,
            without_adopted_devices = self.without_adopted_devices,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "collected local device query results"
        );
        Ok(handles)
    }

    /// Subscribes to meaningful changes to this local result set. The first
    /// [`DeviceQuerySubscription::try_next`] returns an initial result; later
    /// calls coalesce all pending revisions into their newest distinct result.
    pub async fn subscribe(self) -> Result<DeviceQuerySubscription> {
        self.client.ensure_open()?;
        let receiver = self.client.managed_state().subscribe();
        let initial = self.matching_entries(self.client.managed_state().devices());
        Ok(DeviceQuerySubscription {
            query: self,
            receiver,
            previous: Mutex::new(initial.clone()),
            initial: Mutex::new(Some(initial)),
        })
    }
}

fn matches_link<T: PartialEq>(filter: &DeviceLinkFilter<T>, value: Option<&T>) -> bool {
    match filter {
        DeviceLinkFilter::Any => true,
        DeviceLinkFilter::Is(expected) => value == Some(expected),
        DeviceLinkFilter::None => value.is_none(),
    }
}

/// A coalescing local result-set subscription. It never performs network I/O.
pub struct DeviceQuerySubscription {
    query: DeviceQuery,
    receiver: watch::Receiver<Arc<super::state::StateSnapshot>>,
    previous: Mutex<BTreeMap<DeviceKey, Device>>,
    initial: Mutex<Option<BTreeMap<DeviceKey, Device>>>,
}

/// A meaningful change to a device query result set.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum DeviceQueryChange {
    Initial {
        results: Vec<DeviceHandle>,
    },
    Updated {
        revision: u64,
        added: Vec<DeviceHandle>,
        removed: Vec<DeviceHandle>,
        changed: Vec<DeviceHandle>,
        results: Vec<DeviceHandle>,
    },
}

impl DeviceQuerySubscription {
    /// Returns the initial results once, then the newest distinct pending
    /// committed result-set change. This is intentionally non-blocking.
    pub fn try_next(&mut self) -> Option<DeviceQueryChange> {
        if let Some(initial) = self
            .initial
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            return Some(DeviceQueryChange::Initial {
                results: self.query.handles(&initial),
            });
        }
        if !self.receiver.has_changed().ok()? {
            return None;
        }
        let snapshot = Arc::clone(&self.receiver.borrow_and_update());
        self.change(snapshot)
    }

    /// Waits for the next distinct local result set. Intermediate revisions
    /// coalesce to the latest committed snapshot.
    pub async fn next(&mut self) -> Option<DeviceQueryChange> {
        if let Some(initial) = self
            .initial
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            return Some(DeviceQueryChange::Initial {
                results: self.query.handles(&initial),
            });
        }
        loop {
            self.receiver.changed().await.ok()?;
            let snapshot = Arc::clone(&self.receiver.borrow_and_update());
            if let Some(change) = self.change(snapshot) {
                return Some(change);
            }
        }
    }

    fn change(&self, snapshot: Arc<super::state::StateSnapshot>) -> Option<DeviceQueryChange> {
        let next = self
            .query
            .matching_entries(snapshot.devices().values().cloned());
        let mut previous = self
            .previous
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if *previous == next {
            return None;
        }
        let added = next
            .keys()
            .filter(|key| !previous.contains_key(*key))
            .cloned()
            .map(|key| DeviceHandle::new(self.query.client.clone(), key))
            .collect();
        let removed = previous
            .keys()
            .filter(|key| !next.contains_key(*key))
            .cloned()
            .map(|key| DeviceHandle::new(self.query.client.clone(), key))
            .collect();
        let changed = next
            .iter()
            .filter(|(key, value)| previous.get(*key).is_some_and(|old| old != *value))
            .map(|(key, _)| DeviceHandle::new(self.query.client.clone(), key.clone()))
            .collect();
        *previous = next.clone();
        Some(DeviceQueryChange::Updated {
            revision: snapshot.revision(),
            added,
            removed,
            changed,
            results: self.query.handles(&next),
        })
    }
}

/// Explicit remote collection refresh for owned devices.
///
/// Every fetched page is normalized, committed, and published before the
/// builder advances to the next page. Filtered traversals never infer device
/// removal from absence; an unfiltered traversal reconciles membership only
/// after reaching the terminal cursor.
#[derive(Clone, Debug)]
pub struct DeviceRefreshQuery {
    client: Client,
    replicant_code: Option<String>,
    device_type: Option<DeviceType>,
    tag: Option<String>,
    tags: Option<String>,
    exclude_tags: Option<String>,
    untagged: bool,
    location: Option<String>,
    page_size: i64,
    max_pages: usize,
}

impl DeviceRefreshQuery {
    fn new(client: Client) -> Self {
        Self {
            client,
            replicant_code: None,
            device_type: None,
            tag: None,
            tags: None,
            exclude_tags: None,
            untagged: false,
            location: None,
            page_size: 50,
            max_pages: 100,
        }
    }

    /// Restricts the remote traversal to devices assigned to this replicant.
    #[must_use]
    pub fn assigned_to(mut self, replicant_code: impl Into<String>) -> Self {
        self.replicant_code = Some(replicant_code.into());
        self
    }

    /// Restricts the remote traversal to one device type.
    #[must_use]
    pub fn of_type(mut self, device_type: DeviceType) -> Self {
        self.device_type = Some(device_type);
        self
    }

    /// Restricts the remote traversal to one exact location designation.
    #[must_use]
    pub fn at(mut self, location: impl Into<String>) -> Self {
        self.location = Some(location.into());
        self
    }

    /// Restricts the remote traversal to devices carrying this tag.
    #[must_use]
    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tag = Some(tag.into());
        self
    }

    /// Restricts the remote traversal to devices matching any comma-separated
    /// tag pattern. The upstream API supports `*` wildcards.
    #[must_use]
    pub fn with_tag_patterns(mut self, patterns: impl Into<String>) -> Self {
        self.tags = Some(patterns.into());
        self
    }

    /// Excludes devices matching any comma-separated tag pattern. The
    /// upstream API supports `*` wildcards.
    #[must_use]
    pub fn excluding_tag_patterns(mut self, patterns: impl Into<String>) -> Self {
        self.exclude_tags = Some(patterns.into());
        self
    }

    /// Restricts the remote traversal to devices with no tags.
    #[must_use]
    pub fn untagged(mut self) -> Self {
        self.untagged = true;
        self
    }

    /// Sets the requested page size, clamped to the documented `1..=50` range.
    #[must_use]
    pub fn page_size(mut self, page_size: i64) -> Self {
        self.page_size = page_size.clamp(1, 50);
        self
    }

    /// Bounds the number of cursor pages accepted from the server.
    #[must_use]
    pub fn max_pages(mut self, max_pages: usize) -> Self {
        self.max_pages = max_pages.max(1);
        self
    }

    fn is_filtered(&self) -> bool {
        self.replicant_code.is_some()
            || self.device_type.is_some()
            || self.tag.is_some()
            || self.tags.is_some()
            || self.exclude_tags.is_some()
            || self.untagged
            || self.location.is_some()
    }

    /// Fetches every page, committing each observation before returning a
    /// stable key-sorted set of handles.
    pub async fn collect(self) -> Result<Vec<DeviceHandle>> {
        self.client.ensure_open()?;
        if self.tag.is_some() && self.tags.is_some() {
            return Err(Error::Configuration {
                message: "device refresh cannot combine `tag` with `tags`".into(),
            });
        }
        if self.untagged
            && (self.tag.is_some() || self.tags.is_some() || self.exclude_tags.is_some())
        {
            return Err(Error::Configuration {
                message: "device refresh cannot combine `untagged` with tag filters".into(),
            });
        }

        let started_at = Instant::now();
        let filtered = self.is_filtered();
        let mut query = raw::devices::DeviceListQuery {
            replicant_code: self.replicant_code,
            device_type: self.device_type.map(|value| value.as_str().to_owned()),
            tag: self.tag,
            tags: self.tags,
            exclude_tags: self.exclude_tags,
            untagged: self.untagged.then_some(true),
            location: self.location,
            cursor: None,
            limit: Some(self.page_size),
        };
        let mut keys = BTreeSet::new();
        let mut pages = 0usize;

        loop {
            if pages >= self.max_pages {
                return Err(Error::Configuration {
                    message: format!(
                        "device refresh exceeded its {}-page bound before reaching the terminal cursor",
                        self.max_pages
                    ),
                });
            }

            let response = self.client.managed_raw().devices().list(&query).await?;
            let next_cursor = response.value.next_cursor;
            let collection = domain::device_collection(
                &response.value,
                Realm::Live,
                filtered,
                !filtered && next_cursor.is_none(),
                observed_at(),
            )
            .map_err(normalization)?;
            for observation in &collection.members {
                keys.insert(observation.value.key.clone());
            }
            self.client
                .managed_state()
                .persist_devices(&collection.members)
                .map_err(super::client::store_error)?;
            pages += 1;

            match next_cursor {
                Some(cursor) if query.cursor != Some(cursor) => query.cursor = Some(cursor),
                Some(_) => {
                    return Err(Error::Decode {
                        message: "device collection returned a non-advancing cursor".into(),
                        status: None,
                        source: None,
                    });
                }
                None => break,
            }
        }

        if !filtered {
            self.client
                .managed_state()
                .reconcile_owned_devices(&keys)
                .map_err(super::client::store_error)?;
        }

        info!(
            target: "replicant_client::gateway::devices",
            event = "devices.refresh_many_completed",
            pages,
            item_count = keys.len(),
            filtered,
            elapsed_ms = started_at.elapsed().as_millis() as u64,
            "completed managed paginated device refresh"
        );

        Ok(keys
            .into_iter()
            .map(|key| DeviceHandle::new(self.client.clone(), key))
            .collect())
    }
}

/// Gateway for owned devices. `cached` and `find` are local-only.
#[derive(Clone, Debug)]
pub struct DevicesGateway {
    client: Client,
}
impl DevicesGateway {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }
    #[must_use]
    pub fn cached(&self, code: &str) -> Option<DeviceHandle> {
        let key = DeviceKey::live(DeviceId::from(code));
        self.client
            .managed_state()
            .device(&key)
            .map(|_| DeviceHandle::new(self.client.clone(), key))
    }
    #[must_use]
    pub fn find(&self) -> DeviceQuery {
        DeviceQuery::new(self.client.clone())
    }
    #[must_use]
    pub fn miners(&self) -> DeviceQuery {
        self.find().miners()
    }
    /// Starts an explicit managed, paginated remote collection refresh.
    #[must_use]
    pub fn refresh_many(&self) -> DeviceRefreshQuery {
        DeviceRefreshQuery::new(self.client.clone())
    }

    /// Retrieves a one-time device from an equipment locker through the
    /// durable mutation journal. The locker code need not identify an owned
    /// device, so retrieval lives on the collection gateway rather than a
    /// [`DeviceHandle`].
    pub async fn retrieve(&self, device_code: &str) -> Result<Operation> {
        operation::device_retrieve(&self.client, device_code).await
    }
    /// Starts a local query for devices of a controller type.
    #[must_use]
    pub fn controllers(&self, controller_type: DeviceType) -> DeviceQuery {
        self.find().of_type(controller_type)
    }
    pub async fn get(&self, code: &str) -> Result<DeviceHandle> {
        let started_at = Instant::now();
        self.client.ensure_open()?;

        let request_started_at = Instant::now();
        let response = self.client.managed_raw().devices().get(code).await?;
        let request_ms = request_started_at.elapsed().as_millis() as u64;

        let normalize_started_at = Instant::now();
        let observation = domain::device_detail(
            &response.value,
            Realm::Live,
            AccessScope::Owned,
            observed_at(),
        )
        .map_err(normalization)?;
        let key = observation.value.key.clone();
        let normalize_ms = normalize_started_at.elapsed().as_millis() as u64;

        let persist_started_at = Instant::now();
        self.client
            .managed_state()
            .persist_devices(&[observation])
            .map_err(super::client::store_error)?;
        let persist_ms = persist_started_at.elapsed().as_millis() as u64;

        info!(
            target: "replicant_client::gateway::devices",
            event = "device.get_completed",
            device_code = code,
            request_ms,
            normalize_ms,
            persist_ms,
            elapsed_ms = started_at.elapsed().as_millis() as u64,
            "completed managed device detail read"
        );
        Ok(DeviceHandle::new(self.client.clone(), key))
    }
    pub async fn refresh(&self, code: &str) -> Result<DeviceHandle> {
        self.get(code).await
    }
    pub async fn list(&self, query: &raw::devices::DeviceListQuery) -> Result<Vec<DeviceHandle>> {
        let started_at = Instant::now();
        self.client.ensure_open()?;

        let request_started_at = Instant::now();
        let response = self.client.managed_raw().devices().list(query).await?;
        let request_ms = request_started_at.elapsed().as_millis() as u64;

        let normalize_started_at = Instant::now();
        let collection = domain::device_collection(
            &response.value,
            Realm::Live,
            !query_is_unfiltered(query),
            false,
            observed_at(),
        )
        .map_err(normalization)?;
        let keys = collection
            .members
            .iter()
            .map(|item| item.value.key.clone())
            .collect::<Vec<_>>();
        let item_count = keys.len();
        let normalize_ms = normalize_started_at.elapsed().as_millis() as u64;

        let persist_started_at = Instant::now();
        self.client
            .managed_state()
            .persist_devices(&collection.members)
            .map_err(super::client::store_error)?;
        let persist_ms = persist_started_at.elapsed().as_millis() as u64;

        info!(
            target: "replicant_client::gateway::devices",
            event = "devices.list_completed",
            item_count,
            filtered = !query_is_unfiltered(query),
            request_ms,
            normalize_ms,
            persist_ms,
            elapsed_ms = started_at.elapsed().as_millis() as u64,
            "completed managed device collection read"
        );

        Ok(keys
            .into_iter()
            .map(|key| DeviceHandle::new(self.client.clone(), key))
            .collect())
    }
}

fn query_is_unfiltered(query: &raw::devices::DeviceListQuery) -> bool {
    query.device_type.is_none()
        && query.location.is_none()
        && query.replicant_code.is_none()
        && query.tag.is_none()
        && query.tags.is_none()
        && query.exclude_tags.is_none()
        && query.untagged.is_none()
}

/// Owned replicant gateway. Owned detail is normalized with private authority.
#[derive(Clone, Debug)]
pub struct ReplicantsGateway {
    client: Client,
}

/// Local-only query over cached replicants.
#[derive(Clone, Debug)]
pub struct ReplicantQuery {
    client: Client,
    realm: Option<Realm>,
    access: Option<AccessScope>,
    status: Option<ReplicantStatus>,
    location: Option<String>,
}

impl ReplicantQuery {
    fn new(client: Client) -> Self {
        Self {
            client,
            realm: None,
            access: None,
            status: None,
            location: None,
        }
    }

    #[must_use]
    pub fn in_realm(mut self, realm: Realm) -> Self {
        self.realm = Some(realm);
        self
    }
    #[must_use]
    pub fn with_access(mut self, access: AccessScope) -> Self {
        self.access = Some(access);
        self
    }
    #[must_use]
    pub fn owned(self) -> Self {
        self.with_access(AccessScope::Owned)
    }
    #[must_use]
    pub fn with_status(mut self, status: ReplicantStatus) -> Self {
        self.status = Some(status);
        self
    }
    #[must_use]
    pub fn at(mut self, location: impl Into<String>) -> Self {
        self.location = Some(location.into());
        self
    }

    /// Collects a stable, key-sorted view from the current committed snapshot.
    pub async fn collect(self) -> Result<Vec<ReplicantHandle>> {
        self.client.ensure_open()?;
        Ok(self
            .client
            .managed_state()
            .replicants()
            .into_iter()
            .filter(|entry| {
                self.realm
                    .as_ref()
                    .is_none_or(|realm| realm == &entry.value.key.realm)
            })
            .filter(|entry| {
                self.access
                    .as_ref()
                    .is_none_or(|access| access == &entry.value.access)
            })
            .filter(|entry| {
                self.status
                    .as_ref()
                    .is_none_or(|status| entry.value.status.as_ref() == Some(status))
            })
            .filter(|entry| {
                self.location.as_ref().is_none_or(|location| {
                    entry
                        .value
                        .location
                        .as_ref()
                        .is_some_and(|key| key.id.as_str() == location)
                })
            })
            .map(|entry| ReplicantHandle {
                client: self.client.clone(),
                key: entry.value.key,
            })
            .collect())
    }
}

#[derive(Clone, Debug)]
pub struct ReplicantHandle {
    client: Client,
    key: ReplicantKey,
}
impl ReplicantsGateway {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }
    /// Returns a local-only handle when this owned replicant is already cached.
    /// No network request is performed.
    #[must_use]
    pub fn cached(&self, code: &str) -> Option<ReplicantHandle> {
        let key = ReplicantKey::live(ReplicantId::from(code));
        self.client
            .managed_state()
            .replicant(&key)
            .map(|_| ReplicantHandle {
                client: self.client.clone(),
                key,
            })
    }

    /// Starts a local query over committed replicant snapshots.
    #[must_use]
    pub fn find(&self) -> ReplicantQuery {
        ReplicantQuery::new(self.client.clone())
    }
    pub async fn get_owned(&self, code: &str) -> Result<ReplicantHandle> {
        let started_at = Instant::now();
        self.client.ensure_open()?;

        let request_started_at = Instant::now();
        let response = self.client.managed_raw().replicants().get(code).await?;
        let request_ms = request_started_at.elapsed().as_millis() as u64;

        let normalize_started_at = Instant::now();
        let observation =
            domain::owned_replicant_detail(&response.value, Realm::Live, observed_at())
                .map_err(normalization)?;
        let key = observation.value.key.clone();
        let normalize_ms = normalize_started_at.elapsed().as_millis() as u64;

        let persist_started_at = Instant::now();
        self.client
            .managed_state()
            .persist_replicant(observation)
            .map_err(super::client::store_error)?;
        let persist_ms = persist_started_at.elapsed().as_millis() as u64;

        info!(
            target: "replicant_client::gateway::replicants",
            event = "replicant.get_owned_completed",
            replicant_code = code,
            request_ms,
            normalize_ms,
            persist_ms,
            elapsed_ms = started_at.elapsed().as_millis() as u64,
            "completed managed owned replicant read"
        );
        Ok(ReplicantHandle {
            client: self.client.clone(),
            key,
        })
    }
}
impl ReplicantHandle {
    #[must_use]
    pub fn id(&self) -> &ReplicantId {
        &self.key.id
    }
    #[must_use]
    pub fn realm(&self) -> &Realm {
        &self.key.realm
    }
    pub async fn snapshot(&self) -> Result<Replicant> {
        self.client.ensure_open()?;
        self.client
            .managed_state()
            .replicant(&self.key)
            .map(|observation| observation.value)
            .ok_or_else(|| Error::Configuration {
                message: "replicant is not cached".into(),
            })
    }
    pub async fn refresh(&self) -> Result<ReplicantHandle> {
        self.client.replicants().get_owned(self.id().as_str()).await
    }
    pub async fn watch(&self) -> Result<DeviceWatch> {
        self.client.ensure_open()?;
        let key = DeviceKey::live(DeviceId::from(self.id().as_str()));
        Ok(DeviceWatch {
            receiver: self.client.managed_state().subscribe(),
            last_seen: self
                .client
                .managed_state()
                .device(&key)
                .map(|observation| observation.value),
            key,
        })
    }

    /// Updates this replicant's profile fields as a durable operation.
    pub async fn update(
        &self,
        request: raw::replicants::ReplicantUpdateRequest,
    ) -> Result<Operation> {
        operation::replicant_update(&self.client, self.id().as_str(), request).await
    }

    /// Broadcasts a BobNet message from this replicant.
    pub async fn message(
        &self,
        request: raw::replicants::ReplicantMessageRequest,
    ) -> Result<Operation> {
        operation::replicant_message(&self.client, self.id().as_str(), request).await
    }

    /// Begins a mining operation for this replicant.
    pub async fn mine(&self, request: raw::replicants::MineRequest) -> Result<Operation> {
        operation::replicant_mine(&self.client, self.id().as_str(), request).await
    }

    /// Stops this replicant's current mining operation.
    pub async fn stop_mining(&self) -> Result<Operation> {
        operation::replicant_stop_mining(&self.client, self.id().as_str()).await
    }

    /// Queues a device to be printed by this replicant.
    pub async fn print(&self, request: raw::replicants::PrintRequest) -> Result<Operation> {
        operation::replicant_print(&self.client, self.id().as_str(), request).await
    }

    /// Runs a full system scan from this replicant's current location.
    pub async fn scan(&self) -> Result<Operation> {
        operation::replicant_scan(&self.client, self.id().as_str()).await
    }

    /// Teleports this replicant to a new matrix, incurring offline time.
    pub async fn teleport(&self, request: raw::replicants::TeleportRequest) -> Result<Operation> {
        operation::replicant_teleport(&self.client, self.id().as_str(), request).await
    }

    /// Teleports through an FTL slingshot. The server resolves the
    /// slingshot's configured linked matrix; this is the same durable
    /// teleport operation as direct matrix teleportation.
    pub async fn teleport_via_slingshot(
        &self,
        slingshot_code: impl Into<String>,
    ) -> Result<Operation> {
        self.teleport(raw::replicants::TeleportRequest {
            target: slingshot_code.into(),
        })
        .await
    }

    /// Transfers this replicant to a new hosting device or account.
    pub async fn transfer(&self, request: raw::replicants::TransferRequest) -> Result<Operation> {
        operation::replicant_transfer(&self.client, self.id().as_str(), request).await
    }

    /// Starts building a travel request: `.to(destination).preview()` to
    /// compute the route without departing, or `.depart()` to register a
    /// durable departure operation.
    #[must_use]
    pub fn travel(&self) -> TravelBuilder {
        TravelBuilder::new(self.client.clone(), self.id().as_str().to_string())
    }

    /// Cancels this replicant's current travel.
    pub async fn cancel_travel(&self) -> Result<Operation> {
        operation::replicant_cancel_travel(&self.client, self.id().as_str()).await
    }
}

/// Public directory gateway. It never treats public data as owned authority.
#[derive(Clone, Debug)]
pub struct DirectoryGateway {
    client: Client,
}
impl DirectoryGateway {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }
    pub async fn replicant(&self, code: &str) -> Result<Replicant> {
        self.client.ensure_open()?;
        let response = self.client.managed_raw().replicants().get(code).await?;
        let observation =
            domain::public_replicant_detail(&response.value, Realm::Live, observed_at())
                .map_err(normalization)?;
        let value = observation.value.clone();
        self.client
            .managed_state()
            .persist_replicant(observation)
            .map_err(super::client::store_error)?;
        Ok(value)
    }
    pub async fn search(
        &self,
        query: &raw::replicants::ReplicantListQuery,
    ) -> Result<Vec<domain::DirectoryProfile>> {
        self.client.ensure_open()?;
        let response = self.client.managed_raw().replicants().list(query).await?;
        response
            .value
            .replicants
            .iter()
            .map(|profile| {
                domain::directory_profile(profile, observed_at())
                    .map(|observation| observation.value)
                    .map_err(normalization)
            })
            .collect()
    }
}

/// Gateway for resource inventory (`GET /v1/inventory`,
/// `GET /v1/replicants/{code}/inventory`). Reads commit before returning.
#[derive(Clone, Debug)]
pub struct InventoryGateway {
    client: Client,
}
impl InventoryGateway {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    /// Fetches one account-inventory page and commits every location before
    /// returning.  Callers that need complete collection authority must
    /// traverse `next_cursor` without adding a location filter.
    pub async fn list(
        &self,
        query: &raw::inventory::AccountInventoryQuery,
    ) -> Result<(Vec<domain::Inventory>, Option<String>)> {
        self.client.ensure_open()?;
        let response = self.client.managed_raw().inventory().list(query).await?;
        let mut inventories = Vec::with_capacity(response.value.locations.len());
        for raw_location in &response.value.locations {
            let Some(location) = raw_location.location.as_deref() else {
                continue;
            };
            let observation = domain::location_inventory(
                raw_location,
                domain::InventoryOwner::Location(domain::LocationKey::live(location.into())),
                Realm::Live,
                observed_at(),
            )
            .map_err(normalization)?;
            let value = observation.value.clone();
            self.client
                .managed_state()
                .persist_inventory(observation)
                .map_err(super::client::store_error)?;
            inventories.push(value);
        }
        Ok((inventories, response.value.next_cursor))
    }

    /// Fetches a replicant's current-system inventory: its current location
    /// plus every other location in that star system.
    pub async fn for_replicant(&self, replicant_code: &str) -> Result<Vec<domain::Inventory>> {
        self.client.ensure_open()?;
        let response = self
            .client
            .managed_raw()
            .inventory()
            .for_replicant(replicant_code, None)
            .await?;
        let owner = domain::InventoryOwner::Replicant(ReplicantKey::live(ReplicantId::from(
            replicant_code,
        )));
        let mut raw_locations = vec![raw::inventory::LocationInventory {
            items: response.value.items.clone(),
            location: response.value.location.clone(),
            location_name: response.value.location_name.clone(),
        }];
        raw_locations.extend(response.value.locations.iter().cloned());

        let mut inventories = Vec::with_capacity(raw_locations.len());
        for raw_location in &raw_locations {
            if raw_location.location.is_none() {
                continue;
            }
            let observation =
                domain::location_inventory(raw_location, owner.clone(), Realm::Live, observed_at())
                    .map_err(normalization)?;
            let value = observation.value.clone();
            self.client
                .managed_state()
                .persist_inventory(observation)
                .map_err(super::client::store_error)?;
            inventories.push(value);
        }
        Ok(inventories)
    }
}

#[cfg(test)]
mod tests {
    use wiremock::{
        Match, Mock, MockServer, Request, ResponseTemplate,
        matchers::{method, path, query_param},
    };

    use super::*;
    fn cached_device(
        id: &str,
        device_type: DeviceType,
        status: DeviceStatus,
    ) -> domain::Observation<Device> {
        domain::Observation {
            value: Device {
                key: DeviceKey::live(DeviceId::from(id)),
                device_type: Some(device_type),
                status: Some(status),
                location: None,
                features: Vec::new(),
                available_commands: Vec::new(),
                available_directives: Vec::new(),
                tags: Vec::new(),
                relationships: domain::DeviceRelationships::default(),
                attach_capacity: None,
                stow_capacity: None,
                stow_used: None,
                operational_capacity: None,
                grace_period_remaining: None,
                upkeep_requirements: Vec::new(),
                system_status: None,
                active_directive: None,
                travel: None,
                access: AccessScope::Owned,
            },
            metadata: domain::ObservationMetadata {
                source: domain::ObservationSource::RestDetail,
                authority: domain::ObservationAuthority::EntitySnapshot,
                observed_at: "2026-07-25T00:00:00Z".into(),
                access: AccessScope::Owned,
                reachability: domain::Reachability::Reachable,
                stale: false,
                source_document: domain::SourceDocument {
                    operation: "test".into(),
                    request_id: None,
                    document_id: None,
                },
            },
        }
    }

    #[derive(Debug)]
    struct MissingQueryParam(&'static str);

    impl Match for MissingQueryParam {
        fn matches(&self, request: &Request) -> bool {
            request
                .url
                .query_pairs()
                .all(|(name, _)| name.as_ref() != self.0)
        }
    }

    use crate::managed::test_client_at as client_at;

    #[tokio::test]
    async fn refresh_many_page_size_clamps_to_raw_device_bounds() {
        let server = MockServer::start().await;
        let client = client_at(&server.uri()).await;

        assert_eq!(
            DeviceRefreshQuery::new(client.clone())
                .page_size(0)
                .page_size,
            1
        );
        assert_eq!(
            DeviceRefreshQuery::new(client.clone())
                .page_size(500)
                .page_size,
            50
        );

        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn for_replicant_normalizes_current_and_system_locations_and_commits_before_returning() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/replicants/R1/inventory"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "location": "SOL-4",
                "location_name": "Earth",
                "items": [{"resource_type": "structural", "quantity": 50}],
                "locations": [
                    {"location": "SOL-BELT-1", "location_name": "Sol Belt", "items": [{"resource_type": "rares", "quantity": 5}]}
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;
        let client = client_at(&server.uri()).await;

        let inventories = client
            .inventory()
            .for_replicant("R1")
            .await
            .expect("inventory");
        assert_eq!(inventories.len(), 2);
        assert!(
            inventories
                .iter()
                .any(|inventory| inventory.location.as_ref().unwrap().id.as_str() == "SOL-4")
        );
        assert!(
            inventories
                .iter()
                .any(|inventory| inventory.location.as_ref().unwrap().id.as_str() == "SOL-BELT-1")
        );

        server.verify().await;
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn prospect_serializes_the_documented_direction_shape() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/devices/OBS1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "prospecting",
                "completes_at": "2026-08-11T18:00:00Z"
            })))
            .expect(1)
            .mount(&server)
            .await;
        let client = client_at(&server.uri()).await;
        let device = DeviceHandle::for_test(client.clone(), DeviceKey::live("OBS1".into()));

        device
            .prospect(Some([0.0, -1.0, 0.0]))
            .await
            .expect("durable prospect operation");

        let requests = server.received_requests().await.expect("requests");
        let body: serde_json::Value = requests[0].body_json().expect("JSON body");
        assert_eq!(
            body,
            serde_json::json!({
                "command": "prospect",
                "direction": [0.0, -1.0, 0.0]
            })
        );
        server.verify().await;
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn triangulate_serializes_the_documented_command_shape() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/devices/OBS1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "triangulating",
                "signature": "a3f7c2e8b1d94f06",
                "target": [5000, 14000, 100],
                "started_at": "2026-08-06T15:22:00Z",
                "completes_at": "2026-08-06T16:22:00Z"
            })))
            .expect(1)
            .mount(&server)
            .await;
        let client = client_at(&server.uri()).await;
        let device = DeviceHandle::for_test(client.clone(), DeviceKey::live("OBS1".into()));

        device
            .triangulate("a3f7c2e8b1d94f06", [5000.0, 14_000.0, 100.0])
            .await
            .expect("durable triangulation operation");

        let requests = server.received_requests().await.expect("requests");
        let body: serde_json::Value = requests[0].body_json().expect("JSON body");
        assert_eq!(
            body,
            serde_json::json!({
                "command": "triangulate",
                "signature": "a3f7c2e8b1d94f06",
                "target": [5000.0, 14000.0, 100.0]
            })
        );
        server.verify().await;
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn local_device_queries_filter_relationships_and_never_use_the_network() {
        let server = MockServer::start().await;
        let client = client_at(&server.uri()).await;
        let controller = cached_device("CTRL", DeviceType::MiningController, DeviceStatus::Idle);
        let mut drone = cached_device("DRONE", DeviceType::MiningDrone, DeviceStatus::Idle);
        drone.value.location = Some(domain::LocationKey::live("SOL-1".into()));
        drone.value.tags.push("ore".into());
        drone.value.features.push(DeviceFeature::Mining);
        drone.value.relationships.controller = Some(controller.value.key.clone());
        let mut public = cached_device("PUBLIC", DeviceType::MiningDrone, DeviceStatus::Idle);
        public.value.access = AccessScope::Public;
        public.metadata.access = AccessScope::Public;
        public.value.location = Some(domain::LocationKey::live("SOL-2".into()));
        client
            .managed_state()
            .persist_devices(&[controller, drone, public])
            .expect("persist");

        let miner_ids: Vec<_> = client
            .devices()
            .miners()
            .idle()
            .in_system("SOL")
            .with_tag("ore")
            .with_feature(DeviceFeature::Mining)
            .owned()
            .collect()
            .await
            .expect("query")
            .into_iter()
            .map(|device| device.id().as_str().to_owned())
            .collect();
        assert_eq!(miner_ids, ["DRONE"]);

        let untagged_ids: Vec<_> = client
            .devices()
            .find()
            .owned()
            .untagged()
            .collect()
            .await
            .expect("untagged query")
            .into_iter()
            .map(|device| device.id().as_str().to_owned())
            .collect();
        assert_eq!(untagged_ids, ["CTRL"]);

        let snapshot = client
            .devices()
            .miners()
            .idle()
            .owned()
            .collect()
            .await
            .expect("snapshot");
        client
            .managed_state()
            .persist_devices(&[cached_device(
                "LATER",
                DeviceType::MiningDrone,
                DeviceStatus::Idle,
            )])
            .expect("later revision");
        assert_eq!(snapshot.len(), 1, "collected views are immutable snapshots");

        let controllers = client
            .devices()
            .controllers(DeviceType::MiningController)
            .idle()
            .without_adopted_devices()
            .collect()
            .await
            .expect("query");
        assert!(controllers.is_empty());

        // No mock is mounted: every successful assertion above proves that
        // local query evaluation made no HTTP request.
        server.verify().await;
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn enqueue_print_validates_quantity_and_uses_one_durable_submission() {
        let server = MockServer::start().await;
        let client = client_at(&server.uri()).await;
        let device = DeviceHandle::for_test(client.clone(), DeviceKey::live("FACTORY".into()));
        let error = device
            .enqueue_print("survey_drone", 0)
            .await
            .expect_err("zero quantity must be rejected before submission");
        assert!(matches!(error, Error::Configuration { .. }));
        let error = device
            .enqueue_print_with_tags("ftl_relay", 1, ["x".repeat(33)])
            .await
            .expect_err("overlong tags must be rejected before submission");
        assert!(matches!(error, Error::Configuration { .. }));

        Mock::given(method("POST"))
            .and(path("/v1/devices/FACTORY"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(3)
            .mount(&server)
            .await;
        device
            .enqueue_print("survey_drone", 3)
            .await
            .expect("durable print operation");
        let requests = server.received_requests().await.expect("requests");
        let body: serde_json::Value = requests[0].body_json().expect("JSON body");
        assert_eq!(body["command"], "enqueue_print");
        assert_eq!(body["device_type"], "survey_drone");
        assert_eq!(body["quantity"], 3);

        device
            .enqueue_print_with_tags(
                "ftl_relay",
                1,
                ["relay-expansion:test", "relay-site:TARGET"],
            )
            .await
            .expect("durable tagged print operation");
        let requests = server.received_requests().await.expect("requests");
        let body: serde_json::Value = requests[1].body_json().expect("JSON body");
        assert_eq!(body["command"], "enqueue_print");
        assert_eq!(body["device_type"], "ftl_relay");
        assert_eq!(body["quantity"], 1);
        assert_eq!(
            body["tags"],
            serde_json::json!(["relay-expansion:test", "relay-site:TARGET"])
        );

        device
            .enqueue_print_configured(
                "autofactory",
                AutofactoryPrintOptions::new(2)
                    .tags(["factory-stock"])
                    .flatpacked(),
            )
            .await
            .expect("durable flatpack print operation");
        let requests = server.received_requests().await.expect("requests");
        let body: serde_json::Value = requests[2].body_json().expect("JSON body");
        assert_eq!(body["command"], "enqueue_print");
        assert_eq!(body["device_type"], "autofactory");
        assert_eq!(body["quantity"], 2);
        assert_eq!(body["flatpack"], true);
        assert_eq!(body["tags"], serde_json::json!(["factory-stock"]));
        server.verify().await;
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn tagged_remote_list_is_filtered_and_never_reconciles_missing_devices() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/devices"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "devices": [{"device_code": "TAGGED", "device_type": "mining_drone", "status": "idle"}],
                "next_cursor": null
            })))
            .expect(1)
            .mount(&server)
            .await;
        let client = client_at(&server.uri()).await;
        client
            .managed_state()
            .persist_devices(&[cached_device(
                "MISSING",
                DeviceType::MiningDrone,
                DeviceStatus::Idle,
            )])
            .expect("seed device");
        let query = raw::devices::DeviceListQuery {
            tag: Some("ore".into()),
            ..Default::default()
        };
        assert!(!query_is_unfiltered(&query));
        client.devices().list(&query).await.expect("tagged list");
        assert!(
            client
                .managed_state()
                .device(&DeviceKey::live("MISSING".into()))
                .is_some(),
            "filtered collection absence is never a tombstone"
        );
        server.verify().await;
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn without_adopted_devices_matches_the_reference_relationship_filter() {
        let server = MockServer::start().await;
        let client = client_at(&server.uri()).await;
        let mut devices = (0..100)
            .map(|index| {
                cached_device(
                    &format!("C{index}"),
                    DeviceType::MiningController,
                    DeviceStatus::Idle,
                )
            })
            .collect::<Vec<_>>();
        for index in (0..100).step_by(3) {
            let mut adopted = cached_device(
                &format!("D{index}"),
                DeviceType::MiningDrone,
                DeviceStatus::Idle,
            );
            adopted.value.relationships.controller = Some(devices[index].value.key.clone());
            devices.push(adopted);
        }

        let reference = devices
            .iter()
            .filter(|candidate| {
                !devices.iter().any(|device| {
                    device.value.relationships.controller.as_ref() == Some(&candidate.value.key)
                })
            })
            .map(|entry| (entry.value.key.clone(), entry.value.clone()))
            .collect::<BTreeMap<_, _>>();
        let optimized = DeviceQuery::new(client.clone())
            .without_adopted_devices()
            .matching_entries(devices);

        assert_eq!(optimized, reference);
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn query_subscription_is_initial_stable_and_coalesces_revisions() {
        let server = MockServer::start().await;
        let client = client_at(&server.uri()).await;
        let mut subscription = client
            .devices()
            .miners()
            .idle()
            .subscribe()
            .await
            .expect("subscription");
        assert!(matches!(
            subscription.try_next(),
            Some(DeviceQueryChange::Initial { results }) if results.is_empty()
        ));

        let first = cached_device("B", DeviceType::MiningDrone, DeviceStatus::Idle);
        let second = cached_device("A", DeviceType::MiningDrone, DeviceStatus::Idle);
        client
            .managed_state()
            .persist_devices(&[first])
            .expect("first revision");
        client
            .managed_state()
            .persist_devices(&[second])
            .expect("second revision");
        match subscription.try_next().expect("coalesced update") {
            DeviceQueryChange::Updated { added, results, .. } => {
                assert_eq!(added.len(), 2);
                let ids: Vec<_> = results.iter().map(|device| device.id().as_str()).collect();
                assert_eq!(ids, ["A", "B"]);
            }
            DeviceQueryChange::Initial { .. } => panic!("initial already consumed"),
        }
        assert!(subscription.try_next().is_none());

        let mut changed = cached_device("B", DeviceType::MiningDrone, DeviceStatus::Idle);
        changed.value.tags.push("changed".into());
        client
            .managed_state()
            .persist_devices(&[changed])
            .expect("changed revision");
        match subscription.try_next().expect("changed result") {
            DeviceQueryChange::Updated { changed, .. } => {
                assert_eq!(changed[0].id().as_str(), "B");
            }
            DeviceQueryChange::Initial { .. } => panic!("initial already consumed"),
        }

        let inactive = cached_device("A", DeviceType::MiningDrone, DeviceStatus::Active);
        client
            .managed_state()
            .persist_devices(&[inactive])
            .expect("third revision");
        match subscription.try_next().expect("removal") {
            DeviceQueryChange::Updated {
                removed, results, ..
            } => {
                assert_eq!(removed[0].id().as_str(), "A");
                assert_eq!(results[0].id().as_str(), "B");
            }
            DeviceQueryChange::Initial { .. } => panic!("initial already consumed"),
        }

        client.close().await.expect("close");
        assert!(subscription.try_next().is_none());
        server.verify().await;
    }

    #[tokio::test]
    async fn location_queries_are_local_and_diagnostics_share_the_evaluator() {
        let server = MockServer::start().await;
        let client = client_at(&server.uri()).await;
        let observation = domain::Observation {
            value: Location {
                key: domain::LocationKey::live("SOL-2".into()),
                location_type: Some(LocationType::Planet),
                scanned: None,
                system_scanned: Some(true),
                system_tags: Vec::new(),
                system: None,
                parent: None,
                survey_progress: Default::default(),
                environment: domain::LocationEnvironment {
                    atmosphere: Knowledge::Present(Atmosphere::Standard),
                    magnetic_field: Knowledge::Present(true),
                    gravity_g: Knowledge::Present(1.0),
                    surface_temp_c: Knowledge::Present(18.0),
                    in_habitable_zone: Knowledge::Present(true),
                    life_stage: Knowledge::Present(LifeStage::Microbial),
                    ..domain::LocationEnvironment::default()
                },
                unknown: BTreeMap::new(),
            },
            metadata: domain::ObservationMetadata {
                source: domain::ObservationSource::RestDetail,
                authority: domain::ObservationAuthority::EntitySnapshot,
                observed_at: "2026-07-25T00:00:00Z".into(),
                access: AccessScope::Owned,
                reachability: domain::Reachability::Reachable,
                stale: false,
                source_document: domain::SourceDocument {
                    operation: "test".into(),
                    request_id: None,
                    document_id: None,
                },
            },
        };
        client
            .managed_state()
            .persist_location(observation)
            .expect("persist");
        let query = client
            .locations()
            .find()
            .planetary_bodies()
            .surveyed()
            .breathable_atmosphere()
            .has_magnetic_field()
            .in_habitable_zone()
            .in_system("SOL")
            .life_stage_below(LifeStage::Intelligent)
            .gravity_g_between(0.8..=1.3)
            .surface_temp_c_between(10.0..=25.0);
        let results = query.clone().collect().await.expect("local query");
        let diagnostics = query.collect_with_diagnostics().await.expect("diagnostics");
        assert_eq!(results, diagnostics.matches);
        assert_eq!(results.len(), 1);
        server.verify().await;
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn device_assignment_and_hosting_queries_are_local_and_distinct() {
        let server = MockServer::start().await;
        let client = client_at(&server.uri()).await;
        let mut vessel = cached_device("VESSEL", DeviceType::from("vessel"), DeviceStatus::Idle);
        vessel.value.relationships.assigned_replicant = Some(ReplicantKey::live("OWNER".into()));
        vessel.value.relationships.hosting_replicant = Some(ReplicantKey::live("MATRIX".into()));
        let mut drone = cached_device("DRONE", DeviceType::MiningDrone, DeviceStatus::Idle);
        drone.value.relationships.assigned_replicant = Some(ReplicantKey::live("OWNER".into()));
        client
            .managed_state()
            .persist_devices(&[vessel, drone])
            .expect("persist cached devices");

        let assigned = client
            .devices()
            .find()
            .assigned_to(ReplicantKey::live("OWNER".into()))
            .collect()
            .await
            .expect("local assignment query");
        let hosted = client
            .devices()
            .find()
            .hosting_replicant(ReplicantKey::live("MATRIX".into()))
            .collect()
            .await
            .expect("local hosting query");
        assert_eq!(assigned.len(), 2);
        assert_eq!(hosted.len(), 1);
        assert_eq!(hosted[0].id().as_str(), "VESSEL");
        server.verify().await;
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn refresh_many_paginates_and_commits_operational_device_state() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/devices"))
            .and(query_param("replicant_code", "R1"))
            .and(query_param("limit", "2"))
            .and(MissingQueryParam("cursor"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "devices": [
                    {
                        "device_code": "VESSEL",
                        "device_type": "racing_vessel",
                        "status": "idle",
                        "replicant_code": "R1",
                        "location": "SOL-4-L4",
                        "stow_capacity": 5,
                        "stow_used": 1,
                        "stowed_devices": [{"device_code": "DRONE"}]
                    },
                    {
                        "device_code": "CTRL",
                        "device_type": "ami_survey_controller",
                        "status": "active",
                        "replicant_code": "R1",
                        "location": null,
                        "stowed_in_device_code": "VESSEL",
                        "ami_directive": {"directive": "survey_system"},
                        "ami_directive_status": "active"
                    }
                ],
                "next_cursor": 2
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/devices"))
            .and(query_param("replicant_code", "R1"))
            .and(query_param("limit", "2"))
            .and(query_param("cursor", "2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "devices": [{
                    "device_code": "DRONE",
                    "device_type": "survey_drone",
                    "status": "recalling",
                    "replicant_code": "R1",
                    "location": null,
                    "controller_device_code": "CTRL",
                    "travel": {
                        "origin": "SOL-1-L4",
                        "destination": "SOL-4-L4",
                        "eta_seconds": 10,
                        "stage": "recalling"
                    }
                }],
                "next_cursor": null
            })))
            .expect(1)
            .mount(&server)
            .await;
        let client = client_at(&server.uri()).await;

        let handles = client
            .devices()
            .refresh_many()
            .assigned_to("R1")
            .page_size(2)
            .collect()
            .await
            .expect("managed paginated refresh");
        assert_eq!(handles.len(), 3);

        let vessel = client
            .devices()
            .cached("VESSEL")
            .expect("cached vessel")
            .snapshot()
            .await
            .expect("vessel snapshot");
        assert_eq!(vessel.stow_capacity, Some(5));
        assert_eq!(vessel.stow_used, Some(1));
        assert_eq!(
            vessel
                .relationships
                .stowed_devices
                .iter()
                .map(|key| key.id.as_str())
                .collect::<Vec<_>>(),
            ["DRONE"]
        );

        let controller = client
            .devices()
            .cached("CTRL")
            .expect("cached controller")
            .snapshot()
            .await
            .expect("controller snapshot");
        assert_eq!(
            controller
                .relationships
                .stowed_in
                .as_ref()
                .map(|key| key.id.as_str()),
            Some("VESSEL")
        );
        assert_eq!(
            controller
                .active_directive
                .as_ref()
                .and_then(|directive| directive.directive.as_ref())
                .map(|directive| directive.as_str()),
            Some("survey_system")
        );

        let drone = client
            .devices()
            .cached("DRONE")
            .expect("cached drone")
            .snapshot()
            .await
            .expect("drone snapshot");
        assert_eq!(
            drone
                .relationships
                .controller
                .as_ref()
                .map(|key| key.id.as_str()),
            Some("CTRL")
        );
        assert_eq!(
            drone
                .travel
                .as_ref()
                .and_then(|travel| travel.destination.as_ref())
                .map(|key| key.id.as_str()),
            Some("SOL-4-L4")
        );

        server.verify().await;
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn stowed_relationship_query_is_distinct_from_attachment() {
        let server = MockServer::start().await;
        let client = client_at(&server.uri()).await;
        let vessel = DeviceKey::live("VESSEL".into());
        let mut attached = cached_device("ATTACHED", DeviceType::MiningDrone, DeviceStatus::Idle);
        attached.value.relationships.attached_to = Some(vessel.clone());
        let mut stowed = cached_device("STOWED", DeviceType::MiningDrone, DeviceStatus::Idle);
        stowed.value.relationships.stowed_in = Some(vessel.clone());
        client
            .managed_state()
            .persist_devices(&[attached, stowed])
            .expect("persist cached devices");

        let stowed_ids = client
            .devices()
            .find()
            .stowed_in(vessel)
            .collect()
            .await
            .expect("stowed query")
            .into_iter()
            .map(|device| device.id().as_str().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(stowed_ids, ["STOWED"]);

        let not_stowed_ids = client
            .devices()
            .find()
            .not_stowed()
            .collect()
            .await
            .expect("not-stowed query")
            .into_iter()
            .map(|device| device.id().as_str().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(not_stowed_ids, ["ATTACHED"]);

        server.verify().await;
        client.close().await.expect("close");
    }
}
