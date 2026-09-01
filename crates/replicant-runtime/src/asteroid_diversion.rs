//! Occurrence-aware asteroid-diversion authority and durable regional campaign.
//!
//! This module intentionally keeps the account event history as the source of
//! truth.  The managed incoming-object projection is used only for the current
//! location read needed immediately before an operation.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use replicant_client::{
    Client, DynamicCommand, OperationStatus,
    domain::{Event, Realm},
};
use replicant_printing::{
    PrintRequest,
    managed::{QueueOptions, printing_status_in_system, queue_prints_with_components},
};
use replicant_transport::{DeliveryOptions, DeliveryRequest, execute_delivery, plan_delivery};
use replicant_workflow::{
    BoxWorkflowFuture, NewWorkflow, RepositoryError, RequirementScope, ResourceKey,
    ResourceRequirement, WorkItem, WorkItemId, WorkItemSpec, WorkItemTransition, WorkflowContext,
    WorkflowExecutor, WorkflowFactory, WorkflowId, WorkflowInstance, WorkflowKind,
    WorkflowMigration, WorkflowPlacementIntent, WorkflowPlacementIntentCoverage,
    WorkflowPlacementIntentProjection, WorkflowPlacementIntentRelation,
    WorkflowPlacementIntentSubject, WorkflowRepository,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use thiserror::Error;

const WORKFLOW_NAME: &str = "asteroid.diversion";
const OBJECTIVE: &str = "Divert incoming asteroids threatening regional systems";
const PRIORITY: u64 = 800;
const EVENT_NAMES: [&str; 6] = [
    "system.object_detected",
    "diversion.activated",
    "diversion.deactivated",
    "diversion.partial",
    "diversion.diverted",
    "diversion.impacted",
];

/// Stable workflow kind for regional asteroid diversion campaigns.
#[must_use]
pub fn asteroid_diversion_workflow_kind() -> WorkflowKind {
    WorkflowKind::new(WORKFLOW_NAME).expect("static workflow kind is valid")
}
/// Stable occurrence identity representation used by Director ledgers.
pub type AsteroidOccurrenceId = String;

/// Director intent for one regional asteroid campaign.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AsteroidDiversionIntent {
    /// Canonical operating region.
    pub region: String,
    /// Maintenance and manufacturing home.
    pub home: String,
}

/// One immutable asteroid occurrence, identified independently of designation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AsteroidOccurrence {
    /// Lowercase SHA-256 identity of the occurrence tuple.
    pub occurrence_id: AsteroidOccurrenceId,
    /// Event realm; `None` is the live realm for compatibility with old events.
    pub realm: Option<String>,
    /// Server designation (case is retained for display).
    pub designation: String,
    /// Parent star/system designation.
    pub star_or_system: String,
    /// Intended impact target.
    pub impact_target: String,
    /// RFC3339 impact ETA as received from history.
    pub impact_eta: String,
    /// Detection timestamp when supplied by the event.
    #[serde(default)]
    pub discovered_at: Option<String>,
    /// First detection event ID retained for auditability.
    pub first_detection_event_id: String,
    /// Last duplicate detection event ID retained for auditability.
    pub last_detection_event_id: String,
    /// First detection occurrence time retained for auditability.
    pub first_detection_at: String,
    /// Last duplicate detection occurrence time retained for auditability.
    pub last_detection_at: String,
    /// Current object location from detection history, when supplied.
    #[serde(default)]
    pub location: Option<String>,
    #[serde(default)]
    pub raw: Value,
}

/// Typed current-location asteroid observation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AsteroidObservation {
    /// Occurrence being observed.
    pub occurrence_id: String,
    /// Current asteroid location.
    pub location: String,
    /// Current designation.
    pub designation: String,
    /// Current impact target.
    pub impact_target: String,
    /// Impact ETA in Unix milliseconds.
    pub impact_eta_ms: i64,
    /// Number of active propulsor plates.
    pub active_plates: u64,
    /// Current thrust per hour.
    pub current_thrust_per_hour: f64,
    /// Progress ratio in the inclusive range 0..=1.
    pub progress_pct: f64,
    /// Strength still required at zero progress.
    pub required_strength: f64,
    /// Current server-provided impact likelihood.
    pub impact_likelihood: f64,
    /// Current server-provided asteroid size class.
    pub size_class: String,
    /// Open server status string.
    pub status: String,
    /// Original typed location object.
    #[serde(default)]
    pub raw: Value,
}

/// Lifecycle folded from occurrence history.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AsteroidLifecycle {
    /// Object was detected but no diversion has started.
    Detected,
    /// At least one propulsor is active.
    DiversionActive,
    /// Diversion is active but not complete.
    Partial,
    /// The server supplied terminal diversion proof.
    Diverted,
    /// The server supplied terminal impact proof.
    Impacted,
    /// ETA passed without diversion proof.
    Expired,
    /// A later non-overlapping occurrence reused the designation.
    Superseded,
    /// Two future occurrences reuse a designation with conflicting identity.
    IdentityConflict,
    /// Current location could not be read or parsed.
    ObservationUnavailable,
}
pub const fn asteroid_lifecycle_terminal(state: AsteroidLifecycle) -> bool {
    matches!(
        state,
        AsteroidLifecycle::Diverted
            | AsteroidLifecycle::Impacted
            | AsteroidLifecycle::Expired
            | AsteroidLifecycle::Superseded
    )
}

/// Only explicit `diversion.diverted` proof is a successful terminal outcome.
#[must_use]
pub const fn asteroid_terminal_proof(state: AsteroidLifecycle) -> bool {
    matches!(state, AsteroidLifecycle::Diverted)
}

/// Complete event-history authority snapshot.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct AsteroidHistorySnapshot {
    /// Occurrences keyed by immutable occurrence identity.
    #[serde(default)]
    pub occurrences: BTreeMap<String, AsteroidOccurrence>,
    /// Folded lifecycle keyed by occurrence identity.
    #[serde(default)]
    pub lifecycle: BTreeMap<String, AsteroidLifecycle>,
    /// Events which could not be attached to an occurrence.
    #[serde(default)]
    pub unmatched_deactivation_evidence: Vec<Value>,
    /// Last managed event cursor represented by this snapshot.
    pub cursor: Option<String>,
}

/// Observation parse failures are explicit and retryable by callers.
#[derive(Debug, Error)]
pub enum AsteroidObservationError {
    /// Location fetch failed.
    #[error("location read failed: {0}")]
    Read(String),
    /// Required typed location fields were absent or invalid.
    #[error("invalid asteroid location observation: {0}")]
    Invalid(String),
    /// The fetched object did not match the expected occurrence.
    #[error("asteroid occurrence mismatch: {0}")]
    Mismatch(String),
}

/// Sizing formula validation failures.
#[derive(Debug, Error, Clone, PartialEq)]
pub enum AsteroidSizingError {
    /// Any numeric input was non-finite or outside its contract bounds.
    #[error("invalid asteroid sizing input: {0}")]
    InvalidInput(&'static str),
    /// ETA has passed or is too close to produce a finite result.
    #[error("asteroid impact ETA is not in the future")]
    EtaNotFuture,
    /// The checked result cannot fit in the requested integer type.
    #[error("asteroid sizing result exceeds u64")]
    Overflow,
}

fn realm_key(realm: Option<&str>) -> String {
    realm
        .filter(|v| !v.trim().is_empty())
        .unwrap_or("live")
        .trim()
        .to_ascii_lowercase()
}

pub fn asteroid_occurrence_id(
    realm: Option<&str>,
    designation: &str,
    star_or_system: &str,
    impact_target: &str,
    impact_eta: &str,
) -> String {
    occurrence_id(
        realm,
        designation,
        star_or_system,
        impact_target,
        impact_eta,
    )
}

/// Computes an occurrence identity using the frozen length-delimited tuple.
#[must_use]
pub fn occurrence_id(
    realm: Option<&str>,
    designation: &str,
    star_or_system: &str,
    impact_target: &str,
    impact_eta: &str,
) -> String {
    let fields = [
        realm_key(realm),
        designation.trim().to_ascii_uppercase(),
        star_or_system.trim().to_ascii_uppercase(),
        impact_target.trim().to_ascii_uppercase(),
        impact_eta.trim().to_owned(),
    ];
    let mut hasher = Sha256::new();
    for field in fields {
        hasher.update(field.len().to_be_bytes());
        hasher.update(field.as_bytes());
    }
    format!("{:x}", hasher.finalize())
}

fn payload_string(event: &Event, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        event
            .payload
            .get(*key)
            .and_then(Value::as_str)
            .map(str::to_owned)
    })
}
fn event_designation(event: &Event) -> Option<String> {
    payload_string(
        event,
        &["object_designation", "designation", "asteroid", "object"],
    )
}
fn event_realm(event: &Event) -> String {
    match event.realm.as_ref() {
        Some(Realm::Simulation(id)) => format!("simulation:{id}"),
        _ => "live".to_owned(),
    }
}
fn event_star(event: &Event) -> Option<String> {
    event
        .star
        .as_ref()
        .map(|v| v.id.to_string())
        .or_else(|| payload_string(event, &["star", "system", "star_or_system"]))
}
fn event_location(event: &Event) -> Option<String> {
    event
        .location
        .as_ref()
        .map(|v| v.id.to_string())
        .or_else(|| payload_string(event, &["location"]))
}

fn event_order(events: &mut [Event]) {
    events.sort_by(|a, b| a.id.cmp(&b.id));
}

/// Folds ordered asteroid events into lifecycle state.
#[must_use]
pub fn fold_asteroid_lifecycle(
    events: &[Event],
    now_ms: i64,
) -> (
    BTreeMap<String, AsteroidOccurrence>,
    BTreeMap<String, AsteroidLifecycle>,
    Vec<Value>,
) {
    let mut ordered = events.to_vec();
    event_order(&mut ordered);
    let mut occurrences: BTreeMap<String, AsteroidOccurrence> = BTreeMap::new();
    let mut latest: BTreeMap<(String, String), String> = BTreeMap::new();
    let mut latest_device: BTreeMap<(String, String), String> = BTreeMap::new();
    let mut lifecycle = BTreeMap::new();
    let mut unmatched = Vec::new();
    for event in &ordered {
        if event.name.as_str() == "system.object_detected" {
            let Some(designation) = event_designation(event) else {
                continue;
            };
            let Some(target) = payload_string(event, &["impact_target", "target"]) else {
                continue;
            };
            let Some(eta) = payload_string(event, &["impact_eta", "eta"]) else {
                continue;
            };
            let star = event_star(event).unwrap_or_default();
            let realm = event_realm(event);
            let id = asteroid_occurrence_id(Some(&realm), &designation, &star, &target, &eta);
            let event_id = event.id.to_string();
            let occurred_at = event.occurred_at.clone();
            if let Some(existing) = occurrences.get_mut(&id) {
                existing.last_detection_event_id = event_id;
                existing.last_detection_at = occurred_at.clone();
                existing.discovered_at = Some(occurred_at);
                existing.location = event_location(event).or_else(|| existing.location.clone());
                existing.raw = Value::Object(event.payload.clone().into_iter().collect());
            } else {
                occurrences.insert(
                    id.clone(),
                    AsteroidOccurrence {
                        occurrence_id: id.clone(),
                        realm: Some(realm.clone()),
                        location: event_location(event),
                        designation: designation.clone(),
                        star_or_system: star,
                        impact_target: target,
                        impact_eta: eta,
                        discovered_at: Some(occurred_at.clone()),
                        first_detection_event_id: event_id.clone(),
                        last_detection_event_id: event_id,
                        first_detection_at: occurred_at.clone(),
                        last_detection_at: occurred_at,
                        raw: Value::Object(event.payload.clone().into_iter().collect()),
                    },
                );
            }
            latest.insert(
                (realm.clone(), designation.to_ascii_uppercase()),
                id.clone(),
            );
            lifecycle
                .entry(id.clone())
                .or_insert(AsteroidLifecycle::Detected);
            if let Some(device) = event.device.as_ref() {
                latest_device.insert((realm, device.id.to_string().to_ascii_uppercase()), id);
            }
            continue;
        }
        if !event.name.as_str().starts_with("diversion.") {
            continue;
        }
        let designation = event_designation(event);
        let realm = event_realm(event);
        let device = event
            .payload
            .get("device_code")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .or_else(|| event.device.as_ref().map(|value| value.id.to_string()));
        let id = designation
            .as_ref()
            .and_then(|value| {
                latest
                    .get(&(realm.clone(), value.to_ascii_uppercase()))
                    .cloned()
            })
            .or_else(|| {
                device.as_ref().and_then(|value| {
                    latest_device
                        .get(&(realm.clone(), value.to_ascii_uppercase()))
                        .cloned()
                })
            });
        let Some(id) = id else {
            if event.name.as_str() == "diversion.deactivated" {
                let mut evidence = event.payload.clone();
                if let Some(device) = device {
                    evidence
                        .entry("device_code".to_owned())
                        .or_insert_with(|| Value::String(device));
                }
                unmatched.push(Value::Object(evidence.into_iter().collect()));
            }
            continue;
        };
        if let Some(device) = device {
            latest_device.insert((realm.clone(), device.to_ascii_uppercase()), id.clone());
        }
        let state = match event.name.as_str() {
            "diversion.activated" => AsteroidLifecycle::DiversionActive,
            "diversion.deactivated" => AsteroidLifecycle::Detected,
            "diversion.partial" => AsteroidLifecycle::Partial,
            "diversion.diverted" => AsteroidLifecycle::Diverted,
            "diversion.impacted" => AsteroidLifecycle::Impacted,
            _ => lifecycle
                .get(&id)
                .copied()
                .unwrap_or(AsteroidLifecycle::Detected),
        };
        lifecycle.insert(id, state);
    }
    // Distinguish designation reuse. Future overlaps are conflicts; otherwise the
    // latest occurrence wins and older occurrences are superseded.
    let mut by_designation: BTreeMap<(String, String), Vec<String>> = BTreeMap::new();
    for (id, occurrence) in &occurrences {
        by_designation
            .entry((
                realm_key(occurrence.realm.as_deref()),
                occurrence.designation.to_ascii_uppercase(),
            ))
            .or_default()
            .push(id.clone());
    }
    for ids in by_designation.values() {
        for (index, left_id) in ids.iter().enumerate() {
            for right_id in ids.iter().skip(index + 1) {
                let (Some(left), Some(right)) =
                    (occurrences.get(left_id), occurrences.get(right_id))
                else {
                    continue;
                };
                let left_eta = parse_rfc3339_ms(&left.impact_eta).unwrap_or(i64::MAX);
                let right_eta = parse_rfc3339_ms(&right.impact_eta).unwrap_or(i64::MAX);
                if left_eta > now_ms && right_eta > now_ms {
                    lifecycle.insert(left_id.clone(), AsteroidLifecycle::IdentityConflict);
                    lifecycle.insert(right_id.clone(), AsteroidLifecycle::IdentityConflict);
                } else if left_eta < right_eta
                    && !matches!(
                        lifecycle.get(left_id),
                        Some(AsteroidLifecycle::Diverted | AsteroidLifecycle::Impacted)
                    )
                {
                    lifecycle.insert(left_id.clone(), AsteroidLifecycle::Superseded);
                } else if right_eta < left_eta
                    && !matches!(
                        lifecycle.get(right_id),
                        Some(AsteroidLifecycle::Diverted | AsteroidLifecycle::Impacted)
                    )
                {
                    lifecycle.insert(right_id.clone(), AsteroidLifecycle::Superseded);
                }
            }
        }
    }
    for (id, occurrence) in &occurrences {
        let state = lifecycle
            .entry(id.clone())
            .or_insert(AsteroidLifecycle::Detected);
        if !matches!(
            *state,
            AsteroidLifecycle::Diverted
                | AsteroidLifecycle::Impacted
                | AsteroidLifecycle::Superseded
        ) && parse_rfc3339_ms(&occurrence.impact_eta).is_some_and(|eta| eta <= now_ms)
        {
            *state = AsteroidLifecycle::Expired;
        }
    }
    (occurrences, lifecycle, unmatched)
}

/// Reads all authoritative asteroid event histories and folds them by managed cursor order.
pub async fn asteroid_history_snapshot(
    client: &Client,
    now_ms: i64,
) -> Result<AsteroidHistorySnapshot, String> {
    let mut events = Vec::new();
    for name in EVENT_NAMES {
        events.extend(
            client
                .events()
                .full_history_named(name)
                .await
                .map_err(|e| e.to_string())?,
        );
    }
    event_order(&mut events);
    let cursor = events.last().map(|event| event.id.to_string());
    let (occurrences, lifecycle, unmatched) = fold_asteroid_lifecycle(&events, now_ms);
    Ok(AsteroidHistorySnapshot {
        occurrences,
        lifecycle,
        unmatched_deactivation_evidence: unmatched,
        cursor,
    })
}

/// Associates designation-less deactivations only through one durable device owner.
fn associate_checkpoint_deactivations(
    snapshot: &mut AsteroidHistorySnapshot,
    checkpoint: &AsteroidDiversionCheckpoint,
) {
    let mut device_occurrences: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (occurrence_id, item) in &checkpoint.items {
        for code in item.claimed_propulsors.iter().chain(item.activated.iter()) {
            device_occurrences
                .entry(code.to_ascii_uppercase())
                .or_default()
                .insert(occurrence_id.clone());
        }
    }
    let mut still_unmatched = Vec::new();
    for evidence in snapshot.unmatched_deactivation_evidence.drain(..) {
        let device = evidence
            .get("device_code")
            .and_then(Value::as_str)
            .map(str::to_ascii_uppercase);
        let matched = device
            .as_ref()
            .and_then(|code| device_occurrences.get(code))
            .filter(|occurrences| occurrences.len() == 1)
            .and_then(|occurrences| occurrences.first())
            .cloned();
        if let Some(occurrence_id) = matched {
            if matches!(
                snapshot.lifecycle.get(&occurrence_id),
                Some(AsteroidLifecycle::DiversionActive | AsteroidLifecycle::Partial)
            ) {
                snapshot
                    .lifecycle
                    .insert(occurrence_id, AsteroidLifecycle::Detected);
            }
        } else {
            still_unmatched.push(evidence);
        }
    }
    snapshot.unmatched_deactivation_evidence = still_unmatched;
}
/// Reads and parses one current asteroid location from the typed managed gateway.
pub async fn observe_asteroid(
    client: &Client,
    occurrence: &AsteroidOccurrence,
) -> Result<AsteroidObservation, AsteroidObservationError> {
    let location_name = occurrence
        .location
        .as_deref()
        .unwrap_or(&occurrence.designation);
    let location = client
        .locations()
        .get(location_name)
        .await
        .map_err(|e| AsteroidObservationError::Read(e.to_string()))?;
    let location_code = location.id().to_string();
    let object = location
        .unknown
        .get("object")
        .ok_or_else(|| AsteroidObservationError::Invalid("location object is absent".to_owned()))?;
    let object = object.as_object().ok_or_else(|| {
        AsteroidObservationError::Invalid("location object is not an object".to_owned())
    })?;
    let designation = object
        .get("designation")
        .or_else(|| object.get("object_designation"))
        .and_then(Value::as_str)
        .unwrap_or(&occurrence.designation)
        .to_owned();
    if !designation.eq_ignore_ascii_case(&occurrence.designation) {
        return Err(AsteroidObservationError::Mismatch(format!(
            "expected {}, got {designation}",
            occurrence.designation
        )));
    }
    let impact_target = object
        .get("impact_target")
        .and_then(Value::as_str)
        .unwrap_or(&occurrence.impact_target)
        .to_owned();
    if !impact_target.eq_ignore_ascii_case(&occurrence.impact_target) {
        return Err(AsteroidObservationError::Mismatch(format!(
            "expected impact target {}, got {impact_target}",
            occurrence.impact_target
        )));
    }
    let eta = object
        .get("impact_eta")
        .and_then(Value::as_str)
        .unwrap_or(&occurrence.impact_eta);
    let impact_eta_ms = parse_rfc3339_ms(eta).ok_or_else(|| {
        AsteroidObservationError::Invalid("impact_eta is absent, naive, or invalid".to_owned())
    })?;
    if let Some(event_eta_ms) = parse_rfc3339_ms(&occurrence.impact_eta)
        && event_eta_ms != impact_eta_ms
    {
        return Err(AsteroidObservationError::Mismatch(format!(
            "event ETA {} and current ETA {eta} identify different occurrences",
            occurrence.impact_eta
        )));
    }
    let number = |key: &str| object.get(key).and_then(Value::as_f64);
    let active_plates = object
        .get("active_plates")
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            AsteroidObservationError::Invalid("active_plates is absent or invalid".to_owned())
        })?;
    let current_thrust_per_hour = number("current_thrust_per_hour").ok_or_else(|| {
        AsteroidObservationError::Invalid("current_thrust_per_hour is absent or invalid".to_owned())
    })?;
    let progress_pct = number("progress_pct").ok_or_else(|| {
        AsteroidObservationError::Invalid("progress_pct is absent or invalid".to_owned())
    })?;
    let required_strength = number("required_strength").ok_or_else(|| {
        AsteroidObservationError::Invalid("required_strength is absent or invalid".to_owned())
    })?;
    let impact_likelihood = number("impact_likelihood").ok_or_else(|| {
        AsteroidObservationError::Invalid("impact_likelihood is absent or invalid".to_owned())
    })?;
    let size_class = object
        .get("size_class")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            AsteroidObservationError::Invalid("size_class is absent or invalid".to_owned())
        })?
        .to_owned();
    let status = object
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_owned();
    let raw = Value::Object(object.clone().into_iter().collect());
    Ok(AsteroidObservation {
        occurrence_id: occurrence.occurrence_id.clone(),
        location: location_code,
        designation,
        impact_target,
        impact_eta_ms,
        active_plates,
        current_thrust_per_hour,
        progress_pct,
        required_strength,
        impact_likelihood,
        size_class,
        status,
        raw,
    })
}

/// Computes required active plates under the explicit one-propulsor/one-plate policy.
pub fn required_active_plates(
    observation: &AsteroidObservation,
    now_ms: i64,
) -> Result<u64, AsteroidSizingError> {
    if !observation.required_strength.is_finite() || observation.required_strength < 0.0 {
        return Err(AsteroidSizingError::InvalidInput("required_strength"));
    }
    if !observation.progress_pct.is_finite() || !(0.0..=1.0).contains(&observation.progress_pct) {
        return Err(AsteroidSizingError::InvalidInput("progress_pct"));
    }
    if observation.impact_eta_ms <= now_ms {
        return Err(AsteroidSizingError::EtaNotFuture);
    }
    let hours_left = (observation.impact_eta_ms - now_ms) as f64 / 3_600_000.0;
    if !hours_left.is_finite() || hours_left <= 0.0 {
        return Err(AsteroidSizingError::EtaNotFuture);
    }
    let remaining = (observation.required_strength * (1.0 - observation.progress_pct)).max(0.0);
    let desired = (remaining / hours_left).ceil();
    if !desired.is_finite() || desired < 0.0 || desired > (u64::MAX - 2) as f64 {
        return Err(AsteroidSizingError::Overflow);
    }
    (desired as u64)
        .checked_add(2)
        .ok_or(AsteroidSizingError::Overflow)
}

fn parse_rfc3339_ms(value: &str) -> Option<i64> {
    let value = value.trim();
    let (date, zone) = value.split_once('T').or_else(|| value.split_once('t'))?;
    let (year, month, day) = {
        let mut p = date.split('-');
        (
            p.next()?.parse::<i64>().ok()?,
            p.next()?.parse::<i64>().ok()?,
            p.next()?.parse::<i64>().ok()?,
        )
    };
    let (time, offset) =
        if let Some(stripped) = zone.strip_suffix('Z').or_else(|| zone.strip_suffix('z')) {
            (stripped, 0_i64)
        } else {
            let split = zone.rfind(['+', '-'])?;
            let (time, tz) = zone.split_at(split);
            let sign = if tz.starts_with('-') { -1 } else { 1 };
            let tz = &tz[1..];
            let mut p = tz.split(':');
            let h = p.next()?.parse::<i64>().ok()?;
            let m = p.next().unwrap_or("0").parse::<i64>().ok()?;
            (time, sign * (h * 3600 + m * 60))
        };
    let mut p = time.split(':');
    let hour = p.next()?.parse::<i64>().ok()?;
    let minute = p.next()?.parse::<i64>().ok()?;
    let second_fraction = p.next()?;
    let (second, fraction) = second_fraction
        .split_once('.')
        .map_or((second_fraction, "0"), |v| v);
    let second = second.parse::<i64>().ok()?;
    let nanos = format!("{fraction:0<9}")[..9].parse::<i64>().ok()?;
    let days = civil_days(year, month, day)?;
    let seconds = days
        .checked_mul(86_400)?
        .checked_add(hour.checked_mul(3600)?)?
        .checked_add(minute.checked_mul(60)?)?
        .checked_add(second)?
        .checked_sub(offset)?;
    seconds.checked_mul(1000)?.checked_add(nanos / 1_000_000)
}
fn civil_days(year: i64, month: i64, day: i64) -> Option<i64> {
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let y = year - i64::from(month <= 2);
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = month + if month > 2 { -3 } else { 9 };
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    Some(era * 146097 + doe - 719468)
}

/// Builds immutable per-occurrence campaign work items.
pub(crate) fn asteroid_diversion_item_specs(
    workflow_id: WorkflowId,
    occurrences: &BTreeMap<String, AsteroidOccurrence>,
    region: &str,
    home: &str,
) -> Result<Vec<WorkItemSpec>, RepositoryError> {
    let kind = asteroid_diversion_workflow_kind();
    occurrences
        .values()
        .map(|occurrence| {
            let eta = parse_rfc3339_ms(&occurrence.impact_eta);
            Ok(WorkItemSpec {
                workflow_id,
                dedupe_key: format!("divert:{}", occurrence.occurrence_id),
                kind: kind.clone(),
                sort_key: format!(
                    "{:020}:{}",
                    eta.unwrap_or(i64::MAX),
                    occurrence.occurrence_id
                ),
                payload_json: json!({
                    "objective": OBJECTIVE,
                    "priority": PRIORITY,
                    "region": region,
                    "home": home,
                    "occurrence_id": occurrence.occurrence_id,
                    "realm": occurrence.realm,
                    "designation": occurrence.designation,
                    "star_or_system": occurrence.star_or_system,
                    "impact_target": occurrence.impact_target,
                    "impact_eta": occurrence.impact_eta,
                }),
                preconditions_json: json!([]),
                requirements_json: serde_json::to_value([
                    ResourceRequirement {
                        key: "worker".into(),
                        kind: "replicant".into(),
                        capabilities: Vec::new(),
                        scope: RequirementScope::Region(region.to_owned()),
                        count: 1,
                        quantity: 1,
                    },
                    ResourceRequirement {
                        key: "autofactory".into(),
                        kind: "autofactory".into(),
                        capabilities: Vec::new(),
                        scope: RequirementScope::Region(region.to_owned()),
                        count: 1,
                        quantity: 1,
                    },
                ])
                .unwrap_or_else(|_| json!([])),
                deadline_at_ms: eta,
            })
        })
        .collect()
}
fn occurrence_matches_region(
    occurrence: &AsteroidOccurrence,
    region: &str,
    system_regions: &BTreeMap<String, String>,
) -> bool {
    let target_system = occurrence
        .impact_target
        .rsplit_once('-')
        .map_or(occurrence.impact_target.as_str(), |(system, _)| system)
        .to_ascii_uppercase();
    system_regions
        .get(&target_system)
        .is_some_and(|mapped| crate::canonical_region(mapped) == crate::canonical_region(region))
}

fn lifecycle_is_actionable(lifecycle: AsteroidLifecycle) -> bool {
    matches!(
        lifecycle,
        AsteroidLifecycle::Detected
            | AsteroidLifecycle::DiversionActive
            | AsteroidLifecycle::Partial
    )
}

/// Reconciles immutable occurrence work items before any campaign checkpoint.
pub(crate) fn reconcile_asteroid_work_items(
    repository: &WorkflowRepository,
    workflow_id: WorkflowId,
    specs: &[WorkItemSpec],
    now_ms: i64,
) -> Result<(), RepositoryError> {
    repository
        .reconcile_work_items(workflow_id, specs, now_ms)
        .map(|_| ())
}

/// Returns whether a workflow is a compatible active regional asteroid campaign.
pub(crate) fn asteroid_diversion_workflow_matches(
    workflow: &WorkflowInstance,
    region: &str,
) -> Result<bool, RepositoryError> {
    if workflow.kind != asteroid_diversion_workflow_kind() || workflow.status.is_terminal() {
        return Ok(false);
    }
    let intent: AsteroidDiversionIntent = workflow.config()?;
    Ok(!intent.home.trim().is_empty()
        && crate::canonical_region(&intent.region) == crate::canonical_region(region))
}

/// Restart-safe stage for one occurrence work item.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AsteroidDiversionStage {
    /// No side effect has been attempted.
    #[default]
    Observing,
    /// Capacity is being manufactured.
    Printing,
    /// Claimed Propulsors are being delivered.
    Delivering,
    /// Delivered Propulsors are being deployed.
    Deploying,
    /// Deployed Propulsors are being activated.
    Activating,
    /// Active capacity is awaiting terminal proof.
    Monitoring,
    /// The occurrence reached a durable terminal result.
    Terminal,
}

/// Durable state for one occurrence item.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct AsteroidDiversionItemCheckpoint {
    /// Current restart stage.
    pub stage: AsteroidDiversionStage,
    /// Latest accepted authoritative observation.
    pub observation: Option<AsteroidObservation>,
    /// Exact Propulsor codes owned by this occurrence.
    #[serde(default)]
    pub claimed_propulsors: BTreeSet<String>,
    /// Codes whose transport completed.
    #[serde(default)]
    pub delivered: BTreeSet<String>,
    /// Codes whose deployment completed.
    #[serde(default)]
    pub deployed: BTreeSet<String>,
    /// Codes whose activation completed.
    #[serde(default)]
    pub activated: BTreeSet<String>,
    /// Deterministic manufacturing tag.
    pub print_tag: Option<String>,
    /// Structured terminal result, written before the item transition.
    pub terminal_result: Option<Value>,
}

/// Durable checkpoint for restart-safe occurrence reconciliation.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct AsteroidDiversionCheckpoint {
    /// Immutable occurrence records.
    #[serde(default)]
    pub occurrences: BTreeMap<String, AsteroidOccurrence>,
    /// Latest observations by occurrence.
    #[serde(default)]
    pub observations: BTreeMap<String, AsteroidObservation>,
    /// Legacy activated-device projection retained for checkpoint compatibility.
    #[serde(default)]
    pub device_sets: BTreeMap<String, BTreeSet<String>>,
    /// Legacy print-tag projection retained for checkpoint compatibility.
    #[serde(default)]
    pub print_tags: BTreeMap<String, String>,
    /// Terminal results by occurrence.
    #[serde(default)]
    pub results: BTreeMap<String, Value>,
    /// Last authoritative history cursor.
    pub history_cursor: Option<String>,
    /// Restart-safe per-occurrence item checkpoints.
    #[serde(default)]
    pub items: BTreeMap<String, AsteroidDiversionItemCheckpoint>,
    /// Workflow-level exact device ownership, one occurrence per code.
    #[serde(default)]
    pub device_owners: BTreeMap<String, String>,
}
/// Current schema payload for one immutable asteroid work item.
#[derive(Clone, Debug, Deserialize)]
struct AsteroidDiversionWorkItemPayload {
    occurrence_id: String,
}

async fn wait_asteroid_operation(operation: &replicant_client::Operation) -> Result<(), String> {
    let outcome = operation
        .wait_timeout(Duration::from_secs(21_600))
        .await
        .map_err(|error| error.to_string())?;
    if outcome.status == OperationStatus::Completed {
        Ok(())
    } else {
        Err(format!("asteroid operation ended {:?}", outcome.status))
    }
}

fn managed_device_snapshot(
    client: &Client,
    code: &str,
) -> Result<replicant_client::domain::Device, String> {
    client
        .state()
        .owned_devices()
        .map_err(|error| error.to_string())?
        .into_iter()
        .find(|device| device.key.id.as_str().eq_ignore_ascii_case(code))
        .ok_or_else(|| format!("managed device {code} is unavailable"))
}

struct AsteroidItemRun<'a> {
    client: &'a Client,
    repository: &'a Arc<WorkflowRepository>,
    occurrence: &'a AsteroidOccurrence,
    lifecycle: AsteroidLifecycle,
    region: &'a str,
    home: &'a str,
}

async fn execute_asteroid_item(
    context: &mut WorkflowContext,
    item: replicant_workflow::WorkItem,
    checkpoint: &mut AsteroidDiversionCheckpoint,
    run: AsteroidItemRun<'_>,
) -> Result<(), String> {
    let AsteroidItemRun {
        client,
        repository,
        occurrence,
        lifecycle,
        region,
        home,
    } = run;
    if asteroid_lifecycle_terminal(lifecycle) {
        let assignment = format!("asteroid-history:{}", occurrence.occurrence_id);
        let authority = ResourceKey::Namespaced {
            namespace: "asteroid_history".to_owned(),
            key: occurrence.occurrence_id.clone(),
        };
        repository
            .assign_work_item(
                item.id,
                item.state.revision,
                &assignment,
                &authority,
                unix_millis(),
            )
            .map_err(|error| error.to_string())?;
        let started = repository
            .start_work_item(
                item.id,
                item.state.revision,
                "asteroid-history",
                &assignment,
                unix_millis(),
            )
            .map_err(|error| error.to_string())?;
        let result = json!({
            "occurrence_id": occurrence.occurrence_id,
            "lifecycle": lifecycle,
            "proof": match lifecycle {
                AsteroidLifecycle::Diverted => Some("diversion.diverted"),
                AsteroidLifecycle::Impacted => Some("diversion.impacted"),
                _ => None,
            },
        });
        checkpoint
            .results
            .insert(occurrence.occurrence_id.clone(), result.clone());
        checkpoint
            .items
            .entry(occurrence.occurrence_id.clone())
            .or_default()
            .terminal_result = Some(result.clone());
        context
            .persist_checkpoint(checkpoint)
            .map_err(|error| error.to_string())?;
        let transition = match lifecycle {
            AsteroidLifecycle::Diverted => WorkItemTransition::Succeeded {
                checkpoint_json: Some(json!({"stage": "terminal"})),
                result_json: Some(result),
            },
            AsteroidLifecycle::Superseded => WorkItemTransition::Skipped {
                reason: "asteroid occurrence was superseded".to_owned(),
                result_json: Some(result),
            },
            _ => WorkItemTransition::Failed {
                error: format!("asteroid lifecycle is {lifecycle:?}"),
                result_json: Some(result),
            },
        };
        repository
            .transition_work_item(
                started.id,
                started.state.revision,
                transition,
                unix_millis(),
            )
            .map_err(|error| error.to_string())?;
        return Ok(());
    }
    let broker =
        crate::assignment::ResourceBroker::with_managed_client(repository.clone(), client.clone());
    let candidates = crate::workflows::regional_relay_candidates(
        repository.as_ref(),
        client,
        broker
            .discover_candidates()
            .map_err(|error| error.to_string())?,
        region,
    )?;
    let allocations = broker
        .allocate(item.id, item.state.revision, &candidates)
        .map_err(|error| error.to_string())?;
    let mut allocation_claims = Vec::new();
    for allocation in allocations.iter() {
        context
            .acquire_claim(allocation.resource.clone())
            .map_err(|error| error.to_string())?;
        allocation_claims.push(allocation.resource.clone());
    }
    let occurrence_claim = ResourceKey::Namespaced {
        namespace: "asteroid_occurrence".to_owned(),
        key: occurrence.occurrence_id.clone(),
    };
    context
        .acquire_claim(occurrence_claim.clone())
        .map_err(|error| error.to_string())?;

    let worker = allocations
        .by_requirement
        .get("worker")
        .and_then(|rows| rows.first())
        .and_then(|allocation| match &allocation.resource {
            ResourceKey::Replicant(code) => Some(code.clone()),
            _ => None,
        })
        .ok_or_else(|| "asteroid item omitted worker allocation".to_owned())?;
    let factory_codes = allocations
        .by_requirement
        .get("autofactory")
        .into_iter()
        .flatten()
        .filter_map(|allocation| match &allocation.resource {
            ResourceKey::Autofactory(code) => Some(code.clone()),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    let assignment = format!("diversion:{}:{worker}", occurrence.occurrence_id);
    repository
        .assign_work_item(
            item.id,
            item.state.revision,
            &assignment,
            &ResourceKey::Replicant(worker.clone()),
            unix_millis(),
        )
        .map_err(|error| error.to_string())?;
    let started = repository
        .start_work_item(
            item.id,
            item.state.revision,
            &worker,
            &assignment,
            unix_millis(),
        )
        .map_err(|error| error.to_string())?;

    let now_ms = unix_millis();
    let observation = observe_asteroid(client, occurrence)
        .await
        .map_err(|error| error.to_string())?;
    if observation.impact_eta_ms <= now_ms {
        let result = json!({
            "occurrence_id": occurrence.occurrence_id,
            "lifecycle": AsteroidLifecycle::Expired,
        });
        checkpoint
            .results
            .insert(occurrence.occurrence_id.clone(), result.clone());
        checkpoint
            .items
            .entry(occurrence.occurrence_id.clone())
            .or_default()
            .terminal_result = Some(result.clone());
        context
            .persist_checkpoint(checkpoint)
            .map_err(|error| error.to_string())?;
        repository
            .transition_work_item(
                started.id,
                started.state.revision,
                WorkItemTransition::Failed {
                    error: "asteroid impact ETA passed without diversion proof".to_owned(),
                    result_json: Some(result),
                },
                now_ms,
            )
            .map_err(|error| error.to_string())?;
        for resource in allocation_claims
            .iter()
            .chain(std::iter::once(&occurrence_claim))
        {
            context
                .release_claim(resource)
                .map_err(|error| error.to_string())?;
        }
        return Ok(());
    }

    checkpoint
        .observations
        .insert(occurrence.occurrence_id.clone(), observation.clone());
    let desired =
        required_active_plates(&observation, now_ms).map_err(|error| error.to_string())?;
    let item_checkpoint = checkpoint
        .items
        .entry(occurrence.occurrence_id.clone())
        .or_default();
    item_checkpoint.observation = Some(observation.clone());
    item_checkpoint.activated.extend(
        checkpoint
            .device_sets
            .get(&occurrence.occurrence_id)
            .cloned()
            .unwrap_or_default(),
    );
    let staged_needed = desired.saturating_sub(observation.active_plates);
    for candidate in &candidates {
        if item_checkpoint.claimed_propulsors.len()
            >= usize::try_from(staged_needed).unwrap_or(usize::MAX)
        {
            break;
        }
        let ResourceKey::Device(code) = &candidate.resource else {
            continue;
        };
        if !candidate
            .capabilities
            .iter()
            .any(|capability| capability.eq_ignore_ascii_case("propulsor"))
        {
            continue;
        }
        let in_system = candidate
            .location
            .as_ref()
            .and_then(|location| location.designation.as_deref())
            .is_some_and(|location| {
                location.eq_ignore_ascii_case(&occurrence.star_or_system)
                    || location.to_ascii_uppercase().starts_with(&format!(
                        "{}-",
                        occurrence.star_or_system.to_ascii_uppercase()
                    ))
            });
        if !in_system
            || item_checkpoint.claimed_propulsors.contains(code)
            || checkpoint
                .device_owners
                .get(code)
                .is_some_and(|owner| owner != &occurrence.occurrence_id)
        {
            continue;
        }
        if context
            .acquire_claim(ResourceKey::Device(code.clone()))
            .is_ok()
        {
            checkpoint
                .device_owners
                .insert(code.clone(), occurrence.occurrence_id.clone());
            item_checkpoint.claimed_propulsors.insert(code.clone());
        }
    }
    let workflow_owned_not_active = item_checkpoint
        .claimed_propulsors
        .difference(&item_checkpoint.activated)
        .count() as u64;
    let shortage = desired
        .saturating_sub(observation.active_plates)
        .saturating_sub(workflow_owned_not_active);
    if shortage > 0 {
        item_checkpoint.stage = AsteroidDiversionStage::Printing;
        let tag = item_checkpoint
            .print_tag
            .get_or_insert_with(|| format!("asteroid-diversion:{}", occurrence.occurrence_id))
            .clone();
        checkpoint
            .print_tags
            .insert(occurrence.occurrence_id.clone(), tag.clone());
        context
            .persist_checkpoint(checkpoint)
            .map_err(|error| error.to_string())?;
        let print_location = home;
        let request = PrintRequest::new(
            "propulsor",
            i64::try_from(shortage).map_err(|_| "propulsor shortage overflow".to_owned())?,
        );
        let status = printing_status_in_system(
            client,
            print_location,
            std::slice::from_ref(&request),
            std::slice::from_ref(&tag),
        )
        .await
        .map_err(|error| error.to_string())?;
        let missing = status
            .requested
            .iter()
            .find(|line| line.device_type.eq_ignore_ascii_case("propulsor"))
            .map(|line| line.missing)
            .unwrap_or(request.quantity)
            .max(0);
        if missing > 0 {
            let mut options = QueueOptions::at(print_location);
            options.tags = vec![tag];
            options.factory_codes = Some(factory_codes);
            queue_prints_with_components(
                client,
                &[PrintRequest::new("propulsor", missing)],
                &options,
            )
            .await
            .map_err(|error| error.to_string())?;
        }
        repository
            .transition_work_item(
                started.id,
                started.state.revision,
                WorkItemTransition::Waiting {
                    checkpoint_json: Some(
                        serde_json::to_value(
                            checkpoint
                                .items
                                .get(&occurrence.occurrence_id)
                                .cloned()
                                .unwrap_or_default(),
                        )
                        .map_err(|error| error.to_string())?,
                    ),
                    reason: "waiting for tagged Propulsor capacity".to_owned(),
                    retry_at_ms: Some(now_ms.saturating_add(60_000)),
                },
                now_ms,
            )
            .map_err(|error| error.to_string())?;
        return Ok(());
    }

    let refreshed = observe_asteroid(client, occurrence)
        .await
        .map_err(|error| error.to_string())?;
    let refreshed_desired =
        required_active_plates(&refreshed, unix_millis()).map_err(|error| error.to_string())?;
    let selected = checkpoint
        .items
        .get(&occurrence.occurrence_id)
        .map(|state| {
            state
                .claimed_propulsors
                .difference(&state.activated)
                .take(
                    usize::try_from(refreshed_desired.saturating_sub(refreshed.active_plates))
                        .unwrap_or(usize::MAX),
                )
                .cloned()
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if !selected.is_empty() {
        checkpoint
            .items
            .get_mut(&occurrence.occurrence_id)
            .expect("item checkpoint was inserted")
            .stage = AsteroidDiversionStage::Delivering;
        context
            .persist_checkpoint(checkpoint)
            .map_err(|error| error.to_string())?;
        let delivery = DeliveryRequest {
            origin: home.to_owned(),
            destination: refreshed.location.clone(),
            device_codes: selected.clone(),
            ..DeliveryRequest::default()
        };
        let plan = plan_delivery(client, &delivery)
            .await
            .map_err(|error| error.to_string())?;
        execute_delivery(client, &plan, DeliveryOptions::default())
            .await
            .map_err(|error| error.to_string())?;
        checkpoint
            .items
            .get_mut(&occurrence.occurrence_id)
            .expect("item checkpoint was inserted")
            .delivered
            .extend(selected.iter().cloned());
        context
            .persist_checkpoint(checkpoint)
            .map_err(|error| error.to_string())?;
    }

    for code in selected {
        let before = observe_asteroid(client, occurrence)
            .await
            .map_err(|error| error.to_string())?;
        let current_desired =
            required_active_plates(&before, unix_millis()).map_err(|error| error.to_string())?;
        if before.active_plates >= current_desired {
            break;
        }
        let already_deployed = checkpoint
            .items
            .get(&occurrence.occurrence_id)
            .is_some_and(|state| state.deployed.contains(&code));
        let device = client
            .devices()
            .get(&code)
            .await
            .map_err(|error| error.to_string())?;
        let device_snapshot = managed_device_snapshot(client, &code)?;
        let live_deployed = device_snapshot
            .location
            .as_ref()
            .is_some_and(|location| location.id.as_str().eq_ignore_ascii_case(&before.location));
        if live_deployed && !already_deployed {
            checkpoint
                .items
                .get_mut(&occurrence.occurrence_id)
                .expect("item checkpoint was inserted")
                .deployed
                .insert(code.clone());
            context
                .persist_checkpoint(checkpoint)
                .map_err(|error| error.to_string())?;
        }
        if !already_deployed && !live_deployed {
            checkpoint
                .items
                .get_mut(&occurrence.occurrence_id)
                .expect("item checkpoint was inserted")
                .stage = AsteroidDiversionStage::Deploying;
            context
                .persist_checkpoint(checkpoint)
                .map_err(|error| error.to_string())?;
            let deploy = device
                .dynamic_command(
                    DynamicCommand::new("deploy").argument("target", before.location.clone()),
                )
                .await
                .map_err(|error| error.to_string())?;
            wait_asteroid_operation(&deploy).await?;
            checkpoint
                .items
                .get_mut(&occurrence.occurrence_id)
                .expect("item checkpoint was inserted")
                .deployed
                .insert(code.clone());
            context
                .persist_checkpoint(checkpoint)
                .map_err(|error| error.to_string())?;
        }
        let before_activation = observe_asteroid(client, occurrence)
            .await
            .map_err(|error| error.to_string())?;
        if before_activation.active_plates
            >= required_active_plates(&before_activation, unix_millis())
                .map_err(|error| error.to_string())?
        {
            break;
        }
        let live_active = device_snapshot
            .status
            .as_ref()
            .is_some_and(|status| status.as_str().eq_ignore_ascii_case("active"));
        if live_active {
            checkpoint
                .items
                .get_mut(&occurrence.occurrence_id)
                .expect("item checkpoint was inserted")
                .activated
                .insert(code.clone());
            checkpoint
                .device_sets
                .entry(occurrence.occurrence_id.clone())
                .or_default()
                .insert(code);
            context
                .persist_checkpoint(checkpoint)
                .map_err(|error| error.to_string())?;
            continue;
        }
        checkpoint
            .items
            .get_mut(&occurrence.occurrence_id)
            .expect("item checkpoint was inserted")
            .stage = AsteroidDiversionStage::Activating;
        context
            .persist_checkpoint(checkpoint)
            .map_err(|error| error.to_string())?;
        let activate = device
            .dynamic_command(DynamicCommand::new("activate"))
            .await
            .map_err(|error| error.to_string())?;
        wait_asteroid_operation(&activate).await?;
        let state = checkpoint
            .items
            .get_mut(&occurrence.occurrence_id)
            .expect("item checkpoint was inserted");
        state.activated.insert(code.clone());
        checkpoint
            .device_sets
            .entry(occurrence.occurrence_id.clone())
            .or_default()
            .insert(code);
        context
            .persist_checkpoint(checkpoint)
            .map_err(|error| error.to_string())?;
        let after = observe_asteroid(client, occurrence)
            .await
            .map_err(|error| error.to_string())?;
        if after.active_plates <= before_activation.active_plates
            && after.current_thrust_per_hour <= before_activation.current_thrust_per_hour
        {
            repository
                .transition_work_item(
                    started.id,
                    started.state.revision,
                    WorkItemTransition::Waiting {
                        checkpoint_json: Some(json!({
                            "stage": "contract_mismatch",
                            "device": checkpoint
                                .device_sets
                                .get(&occurrence.occurrence_id)
                                .and_then(|devices| devices.last()),
                        })),
                        reason: "activated Propulsor did not increase authoritative plate or thrust capacity"
                            .to_owned(),
                        retry_at_ms: Some(unix_millis().saturating_add(60_000)),
                    },
                    unix_millis(),
                )
                .map_err(|error| error.to_string())?;
            return Ok(());
        }
    }

    checkpoint
        .items
        .get_mut(&occurrence.occurrence_id)
        .expect("item checkpoint was inserted")
        .stage = AsteroidDiversionStage::Monitoring;
    context
        .persist_checkpoint(checkpoint)
        .map_err(|error| error.to_string())?;
    let mut event_names = crate::automation::EVENT_CAMPAIGN_DEPENDENCY_EVENT_NAMES.to_vec();
    event_names.extend(EVENT_NAMES);
    event_names.sort_unstable();
    event_names.dedup();
    let deadline = crate::automation::campaign_retry_deadline(
        repository.as_ref(),
        context.id(),
        observation.impact_eta_ms,
    )
    .map_err(|error| error.to_string())?;
    crate::automation::wait_for_campaign_work(
        context,
        &format!(
            "asteroid diversion {} terminal proof",
            occurrence.occurrence_id
        ),
        &event_names,
        Some(deadline),
        crate::automation::EVENT_DEPENDENCY_RECONCILIATION_INTERVAL,
    )
    .await?;

    let mut snapshot = asteroid_history_snapshot(client, unix_millis()).await?;
    associate_checkpoint_deactivations(&mut snapshot, checkpoint);
    let lifecycle = snapshot
        .lifecycle
        .get(&occurrence.occurrence_id)
        .copied()
        .unwrap_or(AsteroidLifecycle::ObservationUnavailable);
    let active = checkpoint
        .items
        .get(&occurrence.occurrence_id)
        .map(|state| state.activated.clone())
        .unwrap_or_default();
    let transition = match lifecycle {
        AsteroidLifecycle::Diverted => {
            let result = json!({
                "occurrence_id": occurrence.occurrence_id,
                "proof": "diversion.diverted",
                "devices": active,
            });
            checkpoint
                .results
                .insert(occurrence.occurrence_id.clone(), result.clone());
            checkpoint
                .items
                .get_mut(&occurrence.occurrence_id)
                .expect("item checkpoint was inserted")
                .terminal_result = Some(result.clone());
            WorkItemTransition::Succeeded {
                checkpoint_json: Some(json!({"stage": "terminal", "devices": active})),
                result_json: Some(result),
            }
        }
        AsteroidLifecycle::Impacted | AsteroidLifecycle::Expired => {
            let result = json!({
                "occurrence_id": occurrence.occurrence_id,
                "lifecycle": lifecycle,
                "devices": active,
            });
            checkpoint
                .results
                .insert(occurrence.occurrence_id.clone(), result.clone());
            checkpoint
                .items
                .get_mut(&occurrence.occurrence_id)
                .expect("item checkpoint was inserted")
                .terminal_result = Some(result.clone());
            WorkItemTransition::Failed {
                error: format!("asteroid lifecycle is {lifecycle:?}"),
                result_json: Some(result),
            }
        }
        AsteroidLifecycle::Superseded => WorkItemTransition::Skipped {
            reason: "asteroid occurrence was superseded before work started".to_owned(),
            result_json: Some(json!({
                "occurrence_id": occurrence.occurrence_id,
                "lifecycle": lifecycle,
            })),
        },
        _ => WorkItemTransition::Waiting {
            checkpoint_json: Some(
                serde_json::to_value(
                    checkpoint
                        .items
                        .get(&occurrence.occurrence_id)
                        .cloned()
                        .unwrap_or_default(),
                )
                .map_err(|error| error.to_string())?,
            ),
            reason: "awaiting authoritative diversion evidence".to_owned(),
            retry_at_ms: Some(unix_millis().saturating_add(60_000)),
        },
    };
    let terminal = matches!(
        transition,
        WorkItemTransition::Succeeded { .. }
            | WorkItemTransition::Failed { .. }
            | WorkItemTransition::Skipped { .. }
    );
    if terminal {
        checkpoint
            .items
            .get_mut(&occurrence.occurrence_id)
            .expect("item checkpoint was inserted")
            .stage = AsteroidDiversionStage::Terminal;
    }
    context
        .persist_checkpoint(checkpoint)
        .map_err(|error| error.to_string())?;
    repository
        .transition_work_item(
            started.id,
            started.state.revision,
            transition,
            unix_millis(),
        )
        .map_err(|error| error.to_string())?;
    if terminal {
        for resource in allocation_claims
            .iter()
            .chain(std::iter::once(&occurrence_claim))
        {
            context
                .release_claim(resource)
                .map_err(|error| error.to_string())?;
        }
        let device_codes = checkpoint
            .items
            .get(&occurrence.occurrence_id)
            .map(|state| state.claimed_propulsors.iter().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        for code in device_codes {
            checkpoint.device_owners.remove(&code);
            context
                .release_claim(&ResourceKey::Device(code))
                .map_err(|error| error.to_string())?;
        }
    }
    Ok(())
}
struct AsteroidDiversionWorkflow;
impl WorkflowExecutor for AsteroidDiversionWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        Box::pin(async move {
            let intent: AsteroidDiversionIntent = context.config().map_err(|e| e.to_string())?;
            let client = context
                .managed_client()
                .cloned()
                .ok_or_else(|| "managed client unavailable".to_owned())?;
            let repository = context.repository_handle();
            let mut checkpoint: AsteroidDiversionCheckpoint =
                context.checkpoint().map_err(|e| e.to_string())?;
            let mut snapshot = asteroid_history_snapshot(&client, unix_millis()).await?;
            associate_checkpoint_deactivations(&mut snapshot, &checkpoint);
            let catalogue = client.galaxy().catalogue();
            let system_regions = crate::orchestration::expanded_system_region_map(&catalogue);
            let regional_occurrences = snapshot
                .occurrences
                .iter()
                .filter(|(id, occurrence)| {
                    occurrence_matches_region(occurrence, &intent.region, &system_regions)
                        && snapshot
                            .lifecycle
                            .get(*id)
                            .copied()
                            .is_some_and(lifecycle_is_actionable)
                })
                .map(|(id, occurrence)| (id.clone(), occurrence.clone()))
                .collect::<BTreeMap<_, _>>();
            checkpoint.occurrences.extend(regional_occurrences.clone());
            checkpoint.history_cursor = snapshot.cursor.clone();
            let mut desired = BTreeMap::new();
            for occurrence in regional_occurrences.values() {
                if let Ok(observation) = observe_asteroid(&client, occurrence).await
                    && let Ok(plates) = required_active_plates(&observation, unix_millis())
                {
                    checkpoint
                        .observations
                        .insert(occurrence.occurrence_id.clone(), observation);
                    desired.insert(occurrence.occurrence_id.clone(), plates);
                }
            }
            let runnable = regional_occurrences
                .into_iter()
                .filter(|(id, _)| desired.contains_key(id))
                .collect::<BTreeMap<_, _>>();
            let specs = asteroid_diversion_item_specs(
                context.id(),
                &runnable,
                &intent.region,
                &intent.home,
            )
            .map_err(|e| e.to_string())?;
            reconcile_asteroid_work_items(repository.as_ref(), context.id(), &specs, unix_millis())
                .map_err(|e| e.to_string())?;
            let now_ms = unix_millis();
            for item in repository
                .list_work_items(context.id())
                .map_err(|error| error.to_string())?
            {
                if item.state.status.is_terminal() {
                    continue;
                }
                let occurrence_id = item
                    .spec
                    .payload_json
                    .get("occurrence_id")
                    .and_then(Value::as_str);
                if occurrence_id
                    .and_then(|id| snapshot.lifecycle.get(id))
                    .copied()
                    .is_some_and(asteroid_lifecycle_terminal)
                {
                    repository
                        .transition_work_item(
                            item.id,
                            item.state.revision,
                            WorkItemTransition::Waiting {
                                checkpoint_json: item.state.checkpoint_json.clone(),
                                reason: "authoritative terminal asteroid evidence arrived"
                                    .to_owned(),
                                retry_at_ms: Some(now_ms),
                            },
                            now_ms,
                        )
                        .map_err(|error| error.to_string())?;
                }
            }
            context
                .persist_checkpoint(&checkpoint)
                .map_err(|e| e.to_string())?;
            while let Some(item) = repository
                .claim_next_work_item(context.id(), unix_millis())
                .map_err(|e| e.to_string())?
            {
                let occurrence_id = item
                    .spec
                    .payload_json
                    .get("occurrence_id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| "asteroid work item omitted occurrence identity".to_owned())?
                    .to_owned();
                let occurrence = checkpoint
                    .occurrences
                    .get(&occurrence_id)
                    .cloned()
                    .ok_or_else(|| "asteroid occurrence disappeared from checkpoint".to_owned())?;
                let lifecycle = snapshot
                    .lifecycle
                    .get(&occurrence_id)
                    .copied()
                    .unwrap_or(AsteroidLifecycle::ObservationUnavailable);
                execute_asteroid_item(
                    context,
                    item,
                    &mut checkpoint,
                    AsteroidItemRun {
                        client: &client,
                        repository: &repository,
                        occurrence: &occurrence,
                        lifecycle,
                        region: &intent.region,
                        home: &intent.home,
                    },
                )
                .await?;
                context
                    .persist_checkpoint(&checkpoint)
                    .map_err(|e| e.to_string())?;
            }
            let mut refreshed = asteroid_history_snapshot(&client, unix_millis()).await?;
            associate_checkpoint_deactivations(&mut refreshed, &checkpoint);
            let tracked_lifecycles = checkpoint
                .occurrences
                .keys()
                .filter_map(|id| refreshed.lifecycle.get(id).copied())
                .collect::<Vec<_>>();
            let terminal = !tracked_lifecycles.is_empty()
                && tracked_lifecycles
                    .iter()
                    .all(|state| asteroid_lifecycle_terminal(*state));
            if terminal {
                if tracked_lifecycles
                    .iter()
                    .all(|state| asteroid_terminal_proof(*state))
                {
                    context
                        .mark_succeeded(Some(json!({
                            "objective": OBJECTIVE,
                            "outcomes": checkpoint.results,
                        })))
                        .map_err(|e| e.to_string())?;
                } else {
                    context
                        .mark_failed_with_result(
                            "one or more asteroid occurrences were not diverted",
                            json!({
                                "objective": OBJECTIVE,
                                "outcomes": checkpoint.results,
                            }),
                            replicant_workflow::WorkflowFailureDisposition::Permanent,
                        )
                        .map_err(|e| e.to_string())?;
                }
            } else {
                context.mark_waiting().map_err(|e| e.to_string())?;
            }
            Ok(())
        })
    }
}

/// Registered factory for asteroid diversion campaigns.
pub struct AsteroidDiversionWorkflowFactory(WorkflowKind);
impl AsteroidDiversionWorkflowFactory {
    /// Creates the stable factory.
    #[must_use]
    pub fn new() -> Self {
        Self(asteroid_diversion_workflow_kind())
    }
}
impl Default for AsteroidDiversionWorkflowFactory {
    fn default() -> Self {
        Self::new()
    }
}
/// Merges the durable portions of two observations of one asteroid work item.
///
/// The campaign checkpoint is authoritative across restarts, while a work-item
/// checkpoint can contain the most recent state written immediately before a
/// transition.  Keeping the union of monotonic device sets prevents a failed
/// transition from losing custody evidence.
fn merge_asteroid_item_checkpoint(
    target: &mut AsteroidDiversionItemCheckpoint,
    source: AsteroidDiversionItemCheckpoint,
) {
    target.stage = source.stage;
    if source.observation.is_some() {
        target.observation = source.observation;
    }
    target.claimed_propulsors.extend(source.claimed_propulsors);
    target.delivered.extend(source.delivered);
    target.deployed.extend(source.deployed);
    target.activated.extend(source.activated);
    if source.print_tag.is_some() {
        target.print_tag = source.print_tag;
    }
    if source.terminal_result.is_some() {
        target.terminal_result = source.terminal_result;
    }
}

fn normalized_asteroid_device_code(code: &str) -> Result<String, String> {
    let code = code.trim();
    if code.is_empty() {
        return Err("asteroid placement projection contained an empty device code".to_owned());
    }
    Ok(code.to_ascii_uppercase())
}

fn asteroid_code_set_contains(set: &BTreeSet<String>, normalized_code: &str) -> bool {
    set.iter()
        .any(|candidate| candidate.trim().eq_ignore_ascii_case(normalized_code))
}

fn asteroid_placement_projection(
    instance: &WorkflowInstance,
    work_items: &[WorkItem],
) -> Result<WorkflowPlacementIntentProjection, String> {
    if instance.schema_version != 1 {
        return Err(format!(
            "unsupported asteroid diversion schema {}",
            instance.schema_version
        ));
    }

    // Decode both typed campaign payloads even though region/home do not
    // themselves identify a device. A malformed current-schema payload is
    // unknown, never evidence that the campaign selected nothing.
    let _intent: AsteroidDiversionIntent = instance.config().map_err(|error| error.to_string())?;
    let checkpoint: AsteroidDiversionCheckpoint =
        instance.checkpoint().map_err(|error| error.to_string())?;
    asteroid_placement_projection_for_state(instance.status, &instance.kind, checkpoint, work_items)
}

fn asteroid_placement_projection_for_state(
    status: replicant_workflow::WorkflowStatus,
    kind: &WorkflowKind,
    checkpoint: AsteroidDiversionCheckpoint,
    work_items: &[WorkItem],
) -> Result<WorkflowPlacementIntentProjection, String> {
    let mut states = checkpoint.items.clone();
    let mut item_ids = BTreeMap::<String, Option<WorkItemId>>::new();
    for occurrence_id in states.keys() {
        item_ids.insert(occurrence_id.clone(), None);
    }
    // `device_sets` is the schema-v1 compatibility projection populated by
    // older executions.  Its entries are exact activated device codes, not
    // inferred asteroid references.
    for (occurrence_id, codes) in &checkpoint.device_sets {
        states
            .entry(occurrence_id.clone())
            .or_default()
            .activated
            .extend(codes.iter().cloned());
    }

    for item in work_items {
        if &item.spec.kind != kind {
            return Err("asteroid work item had a different workflow kind".to_owned());
        }
        let payload: AsteroidDiversionWorkItemPayload =
            serde_json::from_value(item.spec.payload_json.clone())
                .map_err(|error| error.to_string())?;
        let occurrence_id = payload.occurrence_id;
        if occurrence_id.trim().is_empty() {
            return Err("asteroid work item omitted occurrence identity".to_owned());
        }
        if let Some(value) = &item.state.checkpoint_json {
            let item_checkpoint: AsteroidDiversionItemCheckpoint =
                serde_json::from_value(value.clone()).map_err(|error| error.to_string())?;
            merge_asteroid_item_checkpoint(
                states.entry(occurrence_id.clone()).or_default(),
                item_checkpoint,
            );
        }
        item_ids.insert(occurrence_id, Some(item.id));
    }

    let mut projection = WorkflowPlacementIntentProjection {
        coverage: WorkflowPlacementIntentCoverage::Complete,
        intents: Vec::new(),
        resolutions: Vec::new(),
    };

    // Workflow-level ownership is an exact selection record.  Never inspect
    // occurrence designations, targets, or history as a device reference.
    let mut selected = BTreeSet::<String>::new();
    for code in checkpoint.device_owners.keys() {
        selected.insert(normalized_asteroid_device_code(code)?);
    }
    for state in states.values() {
        for code in state
            .claimed_propulsors
            .iter()
            .chain(state.delivered.iter())
            .chain(state.deployed.iter())
            .chain(state.activated.iter())
        {
            selected.insert(normalized_asteroid_device_code(code)?);
        }
    }

    let evidence_location = |state: &AsteroidDiversionItemCheckpoint| {
        state
            .observation
            .as_ref()
            .map(|observation| observation.location.trim())
            .filter(|location| !location.is_empty())
            .map(str::to_owned)
    };
    let push_intent = |projection: &mut WorkflowPlacementIntentProjection,
                       subject,
                       relation,
                       work_item_id,
                       expected_location| {
        projection.intents.push(WorkflowPlacementIntent {
            subject,
            relation,
            work_item_id,
            expected_location,
        });
    };

    // Emit exact campaign tags only while the workflow is live.  A stale
    // terminal print tag cannot prove that a device was ever in custody.
    let workflow_is_live = matches!(
        status,
        replicant_workflow::WorkflowStatus::Queued
            | replicant_workflow::WorkflowStatus::Running
            | replicant_workflow::WorkflowStatus::Waiting
            | replicant_workflow::WorkflowStatus::Reconciling
            | replicant_workflow::WorkflowStatus::Paused
    );
    if workflow_is_live {
        for tag in checkpoint.print_tags.values() {
            if !tag.is_empty() {
                push_intent(
                    &mut projection,
                    WorkflowPlacementIntentSubject::DeviceTag(tag.clone()),
                    WorkflowPlacementIntentRelation::Staged,
                    None,
                    None,
                );
            }
        }
        for state in states.values() {
            if let Some(tag) = state.print_tag.as_deref().filter(|tag| !tag.is_empty()) {
                push_intent(
                    &mut projection,
                    WorkflowPlacementIntentSubject::DeviceTag(tag.to_owned()),
                    WorkflowPlacementIntentRelation::Staged,
                    None,
                    None,
                );
            }
        }
    }

    for (occurrence_id, state) in &states {
        let work_item_id = item_ids.get(occurrence_id).copied().flatten();
        let location = evidence_location(state);
        let mut codes = state
            .claimed_propulsors
            .iter()
            .chain(state.delivered.iter())
            .chain(state.deployed.iter())
            .chain(state.activated.iter())
            .map(String::as_str)
            .collect::<Vec<_>>();
        codes.sort_unstable();
        codes.dedup();
        for code in codes {
            let code = normalized_asteroid_device_code(code)?;
            let delivered = asteroid_code_set_contains(&state.delivered, code.as_str());
            let deployed = asteroid_code_set_contains(&state.deployed, code.as_str());
            let activated = asteroid_code_set_contains(&state.activated, code.as_str());
            let achieved_location = if deployed || activated {
                location.clone()
            } else {
                None
            };

            match status {
                replicant_workflow::WorkflowStatus::Succeeded => {
                    if let Some(expected_location) = achieved_location {
                        push_intent(
                            &mut projection,
                            WorkflowPlacementIntentSubject::Device(code),
                            WorkflowPlacementIntentRelation::Deployed,
                            work_item_id,
                            Some(expected_location),
                        );
                    } else if !deployed && !activated {
                        let relation = if delivered {
                            WorkflowPlacementIntentRelation::Transported
                        } else {
                            WorkflowPlacementIntentRelation::Claimed
                        };
                        push_intent(
                            &mut projection,
                            WorkflowPlacementIntentSubject::Device(code),
                            relation,
                            work_item_id,
                            None,
                        );
                    }
                }
                replicant_workflow::WorkflowStatus::Failed
                | replicant_workflow::WorkflowStatus::Cancelled => {
                    // A deployed/activated device is no longer unfinished
                    // custody.  Claimed or transported-only state is durable
                    // orphan evidence; config and asteroid history are not.
                    if deployed || activated {
                        continue;
                    }
                    let relation = if delivered {
                        WorkflowPlacementIntentRelation::Transported
                    } else {
                        WorkflowPlacementIntentRelation::Claimed
                    };
                    push_intent(
                        &mut projection,
                        WorkflowPlacementIntentSubject::Device(code),
                        relation,
                        work_item_id,
                        None,
                    );
                }
                _ => {
                    let (relation, expected_location) = if let Some(location) = achieved_location {
                        (WorkflowPlacementIntentRelation::Deployed, Some(location))
                    } else if delivered {
                        (WorkflowPlacementIntentRelation::Transported, None)
                    } else {
                        (WorkflowPlacementIntentRelation::Claimed, None)
                    };
                    push_intent(
                        &mut projection,
                        WorkflowPlacementIntentSubject::Device(code),
                        relation,
                        work_item_id,
                        expected_location,
                    );
                }
            }
        }
    }

    // Device-owner records can survive without a per-occurrence checkpoint.
    // They are useful live claim evidence, but are not enough on their own to
    // establish failed terminal custody.
    if workflow_is_live {
        for code in selected {
            if !projection.intents.iter().any(|intent| {
                intent.subject == WorkflowPlacementIntentSubject::Device(code.clone())
            }) {
                push_intent(
                    &mut projection,
                    WorkflowPlacementIntentSubject::Device(code),
                    WorkflowPlacementIntentRelation::Claimed,
                    None,
                    None,
                );
            }
        }
    }

    projection.intents.sort();
    projection.intents.dedup();
    Ok(projection)
}

impl WorkflowFactory for AsteroidDiversionWorkflowFactory {
    fn kind(&self) -> &WorkflowKind {
        &self.0
    }
    fn current_schema_version(&self) -> u32 {
        1
    }
    fn migrate(&self, _instance: &WorkflowInstance) -> Result<Option<WorkflowMigration>, String> {
        Ok(None)
    }
    fn create_executor(&self) -> Option<Box<dyn WorkflowExecutor>> {
        Some(Box::new(AsteroidDiversionWorkflow))
    }
    fn placement_intents(
        &self,
        instance: &WorkflowInstance,
        work_items: &[WorkItem],
    ) -> Result<WorkflowPlacementIntentProjection, String> {
        asteroid_placement_projection(instance, work_items)
    }
}

/// Creates a queued regional asteroid diversion campaign.
#[must_use]
pub fn new_asteroid_diversion_workflow(
    intent: AsteroidDiversionIntent,
) -> NewWorkflow<AsteroidDiversionIntent, AsteroidDiversionCheckpoint> {
    NewWorkflow {
        kind: asteroid_diversion_workflow_kind(),
        schema_version: 1,
        config: intent,
        checkpoint: AsteroidDiversionCheckpoint::default(),
        current_step: Some("queued".to_owned()),
        parent_id: None,
    }
}

fn unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_millis()).ok())
        .unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn observation(eta: i64, progress: f64, strength: f64) -> AsteroidObservation {
        AsteroidObservation {
            occurrence_id: "x".into(),
            location: "X".into(),
            designation: "X".into(),
            impact_target: "Y".into(),
            impact_eta_ms: eta,
            active_plates: 0,
            current_thrust_per_hour: 1.0,
            progress_pct: progress,
            required_strength: strength,
            impact_likelihood: 1.0,
            size_class: "medium".into(),
            status: "active".into(),
            raw: Value::Null,
        }
    }

    #[test]
    fn identity_is_case_normalized_and_eta_sensitive() {
        assert_eq!(
            occurrence_id(None, "a", "b", "c", " d "),
            occurrence_id(Some("live"), "A", "B", "C", "d")
        );
        assert_ne!(
            occurrence_id(None, "a", "b", "c", "d"),
            occurrence_id(None, "a", "b", "c", "e")
        );
    }
    fn detection(
        event_id: &str,
        occurred_at: &str,
        designation: &str,
        star: &str,
        target: &str,
        eta: &str,
    ) -> Event {
        Event {
            id: replicant_client::domain::EventId::from(event_id),
            realm: Some(Realm::Live),
            name: replicant_client::domain::EventName::from("system.object_detected"),
            category: replicant_client::domain::EventCategory::from("system"),
            device: None,
            replicant: None,
            location: None,
            star: None,
            occurred_at: occurred_at.to_owned(),
            payload: BTreeMap::from([
                ("object_designation".to_owned(), json!(designation)),
                ("star".to_owned(), json!(star)),
                ("impact_target".to_owned(), json!(target)),
                ("impact_eta".to_owned(), json!(eta)),
                ("discovery_source".to_owned(), json!("hub")),
            ]),
        }
    }

    #[test]
    fn repeated_designations_keep_distinct_occurrences() {
        let detections = vec![
            detection(
                "1000-0",
                "2026-07-30T12:00:00Z",
                "SCEPTURUM-OBJ-1",
                "SCEPTURUM",
                "SCEPTURUM-7",
                "2026-08-02T12:00:00Z",
            ),
            detection(
                "1001-0",
                "2026-08-08T12:00:00Z",
                "SCEPTURUM-OBJ-1",
                "SCEPTURUM",
                "SCEPTURUM-4",
                "2026-08-11T12:00:00Z",
            ),
            detection(
                "1002-0",
                "2026-08-18T12:00:00Z",
                "SCEPTURUM-OBJ-1",
                "SCEPTURUM",
                "SCEPTURUM-4",
                "2026-08-21T12:00:00Z",
            ),
            detection(
                "1003-0",
                "2026-08-24T12:00:00Z",
                "THYFFAWFF-OBJ-1",
                "THYFFAWFF",
                "THYFFAWFF-5",
                "2026-08-27T12:00:00Z",
            ),
        ];
        let (occurrences, _, _) = fold_asteroid_lifecycle(&detections, 0);
        assert_eq!(occurrences.len(), 4);
        assert_eq!(
            occurrences
                .values()
                .filter(|occurrence| occurrence.designation == "SCEPTURUM-OBJ-1")
                .count(),
            3
        );

        let mut replayed = detections;
        replayed.push(replayed[1].clone());
        let (replayed_occurrences, _, _) = fold_asteroid_lifecycle(&replayed, 0);
        assert_eq!(replayed_occurrences.len(), 4);
    }

    #[test]
    fn sizing_matches_strength_and_time_policy() {
        let now = 1_000_000;
        assert_eq!(
            required_active_plates(&observation(now + 12 * 3_600_000, 0.5, 48.0), now),
            Ok(4)
        );
        assert_eq!(
            required_active_plates(&observation(now + 12 * 3_600_000, 0.5, 72.0), now),
            Ok(5)
        );
        assert_eq!(
            required_active_plates(&observation(now + 6 * 3_600_000, 0.5, 48.0), now),
            Ok(6)
        );
    }

    #[test]
    fn sizing_rejects_eta_boundary_and_bad_progress() {
        let now = 10;
        assert_eq!(
            required_active_plates(&observation(now, 0.0, 1.0), now),
            Err(AsteroidSizingError::EtaNotFuture)
        );
        assert_eq!(
            required_active_plates(&observation(now + 1_000, 1.1, 1.0), now),
            Err(AsteroidSizingError::InvalidInput("progress_pct"))
        );
    }

    #[test]
    fn deactivation_requires_one_checkpoint_device_owner() {
        let mut snapshot = AsteroidHistorySnapshot {
            lifecycle: BTreeMap::from([(
                "occurrence-a".to_owned(),
                AsteroidLifecycle::DiversionActive,
            )]),
            unmatched_deactivation_evidence: vec![json!({"device_code": "PROP-1"})],
            ..AsteroidHistorySnapshot::default()
        };
        let checkpoint = AsteroidDiversionCheckpoint {
            items: BTreeMap::from([(
                "occurrence-a".to_owned(),
                AsteroidDiversionItemCheckpoint {
                    claimed_propulsors: BTreeSet::from(["PROP-1".to_owned()]),
                    ..AsteroidDiversionItemCheckpoint::default()
                },
            )]),
            ..AsteroidDiversionCheckpoint::default()
        };
        associate_checkpoint_deactivations(&mut snapshot, &checkpoint);
        assert_eq!(
            snapshot.lifecycle.get("occurrence-a"),
            Some(&AsteroidLifecycle::Detected)
        );
        assert!(snapshot.unmatched_deactivation_evidence.is_empty());
    }
    fn projection_instance(checkpoint: AsteroidDiversionCheckpoint) -> WorkflowInstance {
        let repository = WorkflowRepository::open_in_memory().expect("repository");
        let mut workflow = new_asteroid_diversion_workflow(AsteroidDiversionIntent {
            region: "REGION".to_owned(),
            home: "HOME".to_owned(),
        });
        workflow.checkpoint = checkpoint;
        repository.create(workflow).expect("workflow")
    }

    #[test]
    fn placement_projection_is_empty_without_exact_selected_devices() {
        let checkpoint = AsteroidDiversionCheckpoint {
            occurrences: BTreeMap::from([(
                "occurrence".to_owned(),
                AsteroidOccurrence {
                    occurrence_id: "occurrence".to_owned(),
                    realm: None,
                    designation: "ASTEROID-1".to_owned(),
                    star_or_system: "SYSTEM".to_owned(),
                    impact_target: "TARGET".to_owned(),
                    impact_eta: "2026-09-01T00:00:00Z".to_owned(),
                    discovered_at: None,
                    first_detection_event_id: "event".to_owned(),
                    last_detection_event_id: "event".to_owned(),
                    first_detection_at: "2026-08-30T00:00:00Z".to_owned(),
                    last_detection_at: "2026-08-30T00:00:00Z".to_owned(),
                    location: Some("SYSTEM".to_owned()),
                    raw: Value::Null,
                },
            )]),
            ..AsteroidDiversionCheckpoint::default()
        };
        let instance = projection_instance(checkpoint);
        let projection = AsteroidDiversionWorkflowFactory::new()
            .placement_intents(&instance, &[])
            .expect("typed current schema");
        assert_eq!(
            projection.coverage,
            WorkflowPlacementIntentCoverage::Complete
        );
        assert!(projection.intents.is_empty());
    }

    #[test]
    fn placement_projection_uses_only_exact_codes_and_whole_print_tags() {
        let checkpoint = AsteroidDiversionCheckpoint {
            device_owners: BTreeMap::from([(" prop-1 ".to_owned(), "occurrence".to_owned())]),
            print_tags: BTreeMap::from([(
                "occurrence".to_owned(),
                "asteroid-diversion:occurrence".to_owned(),
            )]),
            items: BTreeMap::from([(
                "occurrence".to_owned(),
                AsteroidDiversionItemCheckpoint {
                    claimed_propulsors: BTreeSet::from(["prop-1".to_owned()]),
                    delivered: BTreeSet::from(["prop-2".to_owned()]),
                    print_tag: Some("asteroid-diversion:other".to_owned()),
                    ..AsteroidDiversionItemCheckpoint::default()
                },
            )]),
            ..AsteroidDiversionCheckpoint::default()
        };
        let instance = projection_instance(checkpoint);
        let projection = AsteroidDiversionWorkflowFactory::new()
            .placement_intents(&instance, &[])
            .expect("typed current schema");
        assert_eq!(
            projection
                .intents
                .iter()
                .filter_map(|intent| match &intent.subject {
                    WorkflowPlacementIntentSubject::Device(code) =>
                        Some((code, intent.relation, intent.expected_location.as_deref(),)),
                    WorkflowPlacementIntentSubject::DeviceTag(_) => None,
                })
                .collect::<Vec<_>>(),
            vec![
                (
                    &"PROP-1".to_owned(),
                    WorkflowPlacementIntentRelation::Claimed,
                    None,
                ),
                (
                    &"PROP-2".to_owned(),
                    WorkflowPlacementIntentRelation::Transported,
                    None,
                ),
            ]
        );
        let tags = projection
            .intents
            .iter()
            .filter_map(|intent| match &intent.subject {
                WorkflowPlacementIntentSubject::DeviceTag(tag) => Some(tag.as_str()),
                WorkflowPlacementIntentSubject::Device(_) => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            tags,
            vec!["asteroid-diversion:occurrence", "asteroid-diversion:other"]
        );
    }
    #[test]
    fn placement_projection_rejects_unsupported_schema() {
        let repository = WorkflowRepository::open_in_memory().expect("repository");
        let mut workflow = new_asteroid_diversion_workflow(AsteroidDiversionIntent {
            region: "REGION".to_owned(),
            home: "HOME".to_owned(),
        });
        workflow.schema_version = 2;
        let instance = repository.create(workflow).expect("workflow");
        assert!(
            AsteroidDiversionWorkflowFactory::new()
                .placement_intents(&instance, &[])
                .is_err()
        );
    }
    fn core_projection(
        status: replicant_workflow::WorkflowStatus,
        state: AsteroidDiversionItemCheckpoint,
    ) -> WorkflowPlacementIntentProjection {
        let checkpoint = AsteroidDiversionCheckpoint {
            items: BTreeMap::from([("occurrence".to_owned(), state)]),
            ..AsteroidDiversionCheckpoint::default()
        };
        let kind = asteroid_diversion_workflow_kind();
        asteroid_placement_projection_for_state(status, &kind, checkpoint, &[])
            .expect("typed asteroid projection")
    }

    #[test]
    fn placement_projection_succeeded_requires_exact_achieved_location() {
        let mut state = AsteroidDiversionItemCheckpoint::default();
        state.deployed.insert("prop-1".to_owned());
        state.observation = Some(observation(10_000, 0.0, 1.0));
        let projection = core_projection(replicant_workflow::WorkflowStatus::Succeeded, state);
        assert_eq!(projection.intents.len(), 1);
        assert_eq!(
            projection.intents[0].relation,
            WorkflowPlacementIntentRelation::Deployed
        );
        assert_eq!(
            projection.intents[0].expected_location.as_deref(),
            Some("X")
        );

        let mut missing_location = AsteroidDiversionItemCheckpoint::default();
        missing_location.deployed.insert("prop-1".to_owned());
        let projection = core_projection(
            replicant_workflow::WorkflowStatus::Succeeded,
            missing_location,
        );
        assert!(projection.intents.is_empty());
    }

    #[test]
    fn placement_projection_failed_emits_only_unfinished_custody() {
        let state = AsteroidDiversionItemCheckpoint {
            claimed_propulsors: BTreeSet::from(["prop-1".to_owned(), "prop-2".to_owned()]),
            delivered: BTreeSet::from(["prop-2".to_owned()]),
            deployed: BTreeSet::from(["prop-3".to_owned()]),
            ..AsteroidDiversionItemCheckpoint::default()
        };
        let projection = core_projection(replicant_workflow::WorkflowStatus::Failed, state);
        assert_eq!(projection.intents.len(), 2);
        assert!(
            projection
                .intents
                .iter()
                .all(|intent| intent.relation != WorkflowPlacementIntentRelation::Deployed)
        );
        assert_eq!(
            projection.intents[0].relation,
            WorkflowPlacementIntentRelation::Claimed
        );
        assert_eq!(
            projection.intents[1].relation,
            WorkflowPlacementIntentRelation::Transported
        );
    }

    #[test]
    fn placement_projection_cancelled_retains_durable_residual_custody() {
        let state = AsteroidDiversionItemCheckpoint {
            claimed_propulsors: BTreeSet::from(["prop-1".to_owned()]),
            ..AsteroidDiversionItemCheckpoint::default()
        };
        let projection = core_projection(replicant_workflow::WorkflowStatus::Cancelled, state);
        assert_eq!(projection.intents.len(), 1);
        assert_eq!(
            projection.intents[0].relation,
            WorkflowPlacementIntentRelation::Claimed
        );
    }
}
