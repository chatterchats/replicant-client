//! Explicit account maintenance for deterministic Replicant display names.

use std::collections::{BTreeMap, BTreeSet};

use replicant_client::domain::{Device, DeviceType, Replicant};
use replicant_client::{Client, Operation, OperationStatus, SyncDomain, raw};
use replicant_workflow::WorkflowRepository;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::{ActionEvent, ActionEventKind, ActionReport};
use crate::{ActionResult, canonical_region};

const HUB_GROUP: &str = "Hub";
const UNASSIGNED_GROUP: &str = "U";

/// Inputs for the explicit region-oriented Replicant rename action.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct RenameReplicantsByRegionAction {
    /// When true, resolve and validate the complete plan without mutating names.
    pub dry_run: bool,
}

impl RenameReplicantsByRegionAction {
    /// Creates a mutating region-oriented rename action.
    #[must_use]
    pub const fn new() -> Self {
        Self { dry_run: false }
    }

    /// Creates a preview-only region-oriented rename action.
    #[must_use]
    pub const fn dry_run() -> Self {
        Self { dry_run: true }
    }
}

/// One fully resolved name change, or an already-correct name, in a batch plan.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RenameReplicantPlan {
    /// Stable Replicant code used for ordering and identity.
    pub replicant_id: String,
    /// Name observed before the action, if the API supplied one.
    pub old_name: Option<String>,
    /// Canonical managed region, `Hub`, or `U`.
    pub classification: String,
    /// Deterministic final display name.
    pub target_name: String,
    /// Collision-safe intermediate name required for a swap or cycle.
    pub temporary_name: Option<String>,
}

/// Complete, validated mapping computed before any rename request is issued.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RenameBatchPlan {
    /// Every owned Replicant in stable identity order.
    pub replicants: Vec<RenameReplicantPlan>,
}

impl RenameBatchPlan {
    /// Number of names that differ from their deterministic target.
    #[must_use]
    pub fn changes_required(&self) -> usize {
        self.replicants
            .iter()
            .filter(|entry| entry.old_name.as_deref() != Some(entry.target_name.as_str()))
            .count()
    }
}

/// Action-level detail for one mutation that could not be completed.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RenameFailure {
    /// Stable Replicant code.
    pub replicant_id: String,
    /// Name observed when the plan was built.
    pub old_name: Option<String>,
    /// Final name the action intended to apply.
    pub intended_name: String,
    /// Sanitized managed-operation or validation detail.
    pub error: String,
}

/// Result of planning and, unless dry-run, applying region-oriented names.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RenameReplicantsByRegionActionResult {
    /// Number of owned Replicants included in the complete plan.
    pub scanned_replicants: usize,
    /// Number whose observed name differed from the target.
    pub changes_required: usize,
    /// Number of final names successfully applied.
    pub renamed_successfully: usize,
    /// Number already carrying their final name.
    pub already_correct: usize,
    /// Explicit per-Replicant failures. A non-empty list is resumable by rerunning.
    pub failures: Vec<RenameFailure>,
    /// The validated mapping used by the action.
    pub plan: RenameBatchPlan,
    /// Frontend-renderable planning and mutation events.
    pub report: ActionReport,
}

/// A safe-planning failure that prevents any rename request from being issued.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum RenamePlanningError {
    /// A Replicant did not contain the stable identity needed for a safe plan.
    #[error("Replicant at index {index} is missing its stable identity")]
    MissingReplicantIdentity {
        /// Index in the supplied owned snapshot.
        index: usize,
    },
    /// The complete owned set contained an identity more than once.
    #[error("duplicate owned Replicant identity `{replicant_id}`")]
    DuplicateReplicantIdentity {
        /// Duplicate stable Replicant code.
        replicant_id: String,
    },
    /// A persisted assignment could not be safely interpreted.
    #[error("invalid region assignment for `{replicant_id}`: {reason}")]
    InvalidAssignment {
        /// Stable Replicant code.
        replicant_id: String,
        /// Reason the persisted value was unsafe to use.
        reason: String,
    },
    /// An unassigned Replicant did not have enough managed location information.
    #[error("cannot classify `{replicant_id}` safely: {reason}")]
    MissingLocation {
        /// Stable Replicant code.
        replicant_id: String,
        /// Missing managed location facts.
        reason: String,
    },
    /// A Replicant points to a device that was not present in the managed snapshot.
    #[error("cannot classify `{replicant_id}` safely: hosted device `{device_id}` is missing")]
    MissingHostedDevice {
        /// Stable Replicant code.
        replicant_id: String,
        /// Referenced managed device code.
        device_id: String,
    },
    /// Two planned Replicants would receive the same final name.
    #[error("duplicate target name `{name}` for Replicants {}", replicants.join(", "))]
    DuplicateTargetName {
        /// Conflicting final name.
        name: String,
        /// Stable Replicant codes that would receive it.
        replicants: Vec<String>,
    },
    /// A target name is already occupied outside the owned plan.
    #[error("target name `{name}` is occupied by Replicant `{replicant_id}` outside the plan")]
    OccupiedTargetName {
        /// Conflicting final name.
        name: String,
        /// External owner of the name.
        replicant_id: String,
    },
    /// A generated name violates the conservative local API-safe name grammar.
    #[error("invalid generated target name `{name}`: {reason}")]
    InvalidTargetName {
        /// Generated name that failed validation.
        name: String,
        /// Reason the generated name is unsafe.
        reason: String,
    },
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Classification {
    Region(String),
    Hub,
    Unassigned,
}

impl Classification {
    fn label(&self) -> String {
        match self {
            Self::Region(region) => region.clone(),
            Self::Hub => HUB_GROUP.to_owned(),
            Self::Unassigned => UNASSIGNED_GROUP.to_owned(),
        }
    }
}

#[derive(Clone, Debug)]
struct ClassifiedReplicant {
    replicant_id: String,
    old_name: Option<String>,
    classification: Classification,
}

/// Builds and validates a complete deterministic rename plan from managed data.
///
/// `external_occupied_names` is keyed by public directory identity and contains
/// every known name outside the owned set. Passing the directory projection in
/// makes name uniqueness a planning concern rather than a sequence-dependent
/// API error.
pub fn plan_renames(
    replicants: &[Replicant],
    assignments: &BTreeMap<String, Option<String>>,
    devices: &[Device],
    external_occupied_names: &BTreeMap<String, String>,
) -> Result<RenameBatchPlan, RenamePlanningError> {
    let hub_locations = system_hub_locations(devices);
    let mut seen_ids = BTreeSet::new();
    let mut classified = Vec::with_capacity(replicants.len());

    for (index, replicant) in replicants.iter().enumerate() {
        let replicant_id = replicant.key.id.as_str().trim();
        if replicant_id.is_empty() {
            return Err(RenamePlanningError::MissingReplicantIdentity { index });
        }
        if !seen_ids.insert(replicant_id.to_owned()) {
            return Err(RenamePlanningError::DuplicateReplicantIdentity {
                replicant_id: replicant_id.to_owned(),
            });
        }

        let classification = match assignments.get(replicant_id).and_then(Option::as_deref) {
            Some(region) => {
                let region = canonical_region(region);
                if region.is_empty() {
                    return Err(RenamePlanningError::InvalidAssignment {
                        replicant_id: replicant_id.to_owned(),
                        reason: "assigned region is empty".to_owned(),
                    });
                }
                Classification::Region(region)
            }
            None => classify_unassigned(replicant, devices, &hub_locations)?,
        };

        classified.push(ClassifiedReplicant {
            replicant_id: replicant_id.to_owned(),
            old_name: replicant.name.clone(),
            classification,
        });
    }

    let regions = classified
        .iter()
        .filter_map(|entry| match &entry.classification {
            Classification::Region(region) => Some(region.clone()),
            Classification::Hub | Classification::Unassigned => None,
        })
        .collect::<BTreeSet<_>>();
    let abbreviations = region_abbreviations(&regions);

    let mut grouped = BTreeMap::<Classification, Vec<ClassifiedReplicant>>::new();
    for entry in classified {
        grouped
            .entry(entry.classification.clone())
            .or_default()
            .push(entry);
    }

    let mut entries = Vec::with_capacity(replicants.len());
    for (classification, mut members) in grouped {
        members.sort_by(|left, right| left.replicant_id.cmp(&right.replicant_id));
        for (index, member) in members.into_iter().enumerate() {
            let ordinal = index + 1;
            let target_name = target_name(&classification, &abbreviations, ordinal);
            validate_target_name(&target_name)?;
            entries.push(RenameReplicantPlan {
                replicant_id: member.replicant_id,
                old_name: member.old_name,
                classification: classification.label(),
                target_name,
                temporary_name: None,
            });
        }
    }
    entries.sort_by(|left, right| left.replicant_id.cmp(&right.replicant_id));

    validate_target_collisions(&entries, external_occupied_names)?;
    add_staging_names(&mut entries, external_occupied_names);
    Ok(RenameBatchPlan {
        replicants: entries,
    })
}

/// Executes the explicit region-oriented maintenance action through managed
/// reads, durable Replicant update operations, and projection refreshes.
pub async fn rename_replicants_by_region(
    client: &Client,
    repository: &WorkflowRepository,
    action: &RenameReplicantsByRegionAction,
) -> ActionResult<RenameReplicantsByRegionActionResult> {
    client.sync().domain(SyncDomain::Devices).await?;
    let handles = client.replicants().refresh_owned().await?;
    let mut snapshots = Vec::with_capacity(handles.len());
    for handle in &handles {
        snapshots.push(handle.snapshot().await?);
    }

    let assignments = crate::orchestration::assigned_replicant_regions(repository)?;
    let external_occupied_names = client
        .directory()
        .search_all(&raw::replicants::ReplicantListQuery {
            limit: Some(100),
            ..Default::default()
        })
        .await?
        .into_iter()
        .filter_map(|profile| {
            profile
                .name
                .map(|name| (profile.id.as_str().to_owned(), name))
        })
        .collect::<BTreeMap<_, _>>();
    let plan = plan_renames(
        &snapshots,
        &assignments,
        &client.state().owned_devices()?,
        &external_occupied_names,
    )?;
    let changes_required = plan.changes_required();
    let already_correct = plan.replicants.len().saturating_sub(changes_required);
    let mut report = ActionReport::default();

    for entry in &plan.replicants {
        if entry.old_name.as_deref() == Some(entry.target_name.as_str()) {
            report.events.push(ActionEvent::new(
                ActionEventKind::Skipped,
                &entry.replicant_id,
                format!("already named {}", entry.target_name),
            ));
        } else {
            report.events.push(ActionEvent::new(
                ActionEventKind::Planned,
                &entry.replicant_id,
                format!(
                    "{} -> {}",
                    entry.old_name.as_deref().unwrap_or("<unnamed>"),
                    entry.target_name
                ),
            ));
        }
    }

    let mut result = RenameReplicantsByRegionActionResult {
        scanned_replicants: plan.replicants.len(),
        changes_required,
        renamed_successfully: 0,
        already_correct,
        failures: Vec::new(),
        plan,
        report,
    };
    if action.dry_run || changes_required == 0 {
        return Ok(result);
    }

    let handles = handles
        .into_iter()
        .map(|handle| (handle.id().as_str().to_owned(), handle))
        .collect::<BTreeMap<_, _>>();

    if result
        .plan
        .replicants
        .iter()
        .any(|entry| entry.temporary_name.is_some())
    {
        let mut staging_failed = false;
        let entries = result
            .plan
            .replicants
            .iter()
            .filter(|entry| entry.old_name.as_deref() != Some(entry.target_name.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        for entry in &entries {
            let Some(temporary_name) = entry.temporary_name.as_deref() else {
                continue;
            };
            if entry.old_name.as_deref() == Some(temporary_name) {
                continue;
            }
            let Some(handle) = handles.get(&entry.replicant_id) else {
                staging_failed = true;
                record_failure(
                    &mut result,
                    entry,
                    "managed handle disappeared after planning".to_owned(),
                );
                continue;
            };
            match apply_name(handle, temporary_name).await {
                Ok(operation) => result.report.events.push(
                    ActionEvent::new(
                        ActionEventKind::Succeeded,
                        &entry.replicant_id,
                        format!("staged as {temporary_name}"),
                    )
                    .operation(&operation),
                ),
                Err(error) => {
                    staging_failed = true;
                    record_failure_with_operation(&mut result, entry, error);
                }
            }
        }
        if staging_failed {
            return Ok(result);
        }
    }

    let entries = result
        .plan
        .replicants
        .iter()
        .filter(|entry| entry.old_name.as_deref() != Some(entry.target_name.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    for entry in &entries {
        let Some(handle) = handles.get(&entry.replicant_id) else {
            record_failure(
                &mut result,
                entry,
                "managed handle disappeared after planning".to_owned(),
            );
            continue;
        };
        match apply_name(handle, &entry.target_name).await {
            Ok(operation) => {
                result.renamed_successfully += 1;
                result.report.events.push(
                    ActionEvent::new(
                        ActionEventKind::Succeeded,
                        &entry.replicant_id,
                        format!(
                            "{} -> {}",
                            entry.old_name.as_deref().unwrap_or("<unnamed>"),
                            entry.target_name
                        ),
                    )
                    .operation(&operation),
                );
            }
            Err(error) => record_failure_with_operation(&mut result, entry, error),
        }
    }

    Ok(result)
}

struct RenameApplyError {
    detail: String,
    operation: Option<Operation>,
}

async fn apply_name(
    handle: &replicant_client::managed::ReplicantHandle,
    name: &str,
) -> Result<Operation, RenameApplyError> {
    let operation = handle
        .update(raw::replicants::ReplicantUpdateRequest {
            name: Some(name.to_owned()),
            ..Default::default()
        })
        .await
        .map_err(|error| RenameApplyError {
            detail: error.to_string(),
            operation: None,
        })?;
    let outcome = operation
        .outcome()
        .await
        .map_err(|error| RenameApplyError {
            detail: format!("could not read operation outcome: {error}"),
            operation: Some(operation.clone()),
        })?;
    if matches!(
        outcome.status,
        OperationStatus::Rejected | OperationStatus::Failed | OperationStatus::Cancelled
    ) {
        return Err(RenameApplyError {
            detail: format!(
                "managed rename operation {:?}: {:?}",
                outcome.status, outcome.response
            ),
            operation: Some(operation),
        });
    }
    if outcome.status == OperationStatus::Ambiguous {
        return Err(RenameApplyError {
            detail: "managed rename operation is ambiguous; rerun after reconciliation".to_owned(),
            operation: Some(operation),
        });
    }

    let refreshed = handle.refresh().await.map_err(|error| RenameApplyError {
        detail: format!("rename was submitted but projection refresh failed: {error}"),
        operation: Some(operation.clone()),
    })?;
    let current = refreshed
        .snapshot()
        .await
        .map_err(|error| RenameApplyError {
            detail: format!("rename was submitted but refreshed snapshot was unavailable: {error}"),
            operation: Some(operation.clone()),
        })?;
    if current.name.as_deref() != Some(name) {
        return Err(RenameApplyError {
            detail: format!(
                "rename operation did not reconcile to `{name}` (observed `{}`)",
                current.name.as_deref().unwrap_or("<unnamed>")
            ),
            operation: Some(operation),
        });
    }
    let outcome = operation
        .reconcile()
        .await
        .map_err(|error| RenameApplyError {
            detail: format!("rename applied but durable reconciliation failed: {error}"),
            operation: Some(operation.clone()),
        })?;
    if outcome.status != OperationStatus::Completed {
        return Err(RenameApplyError {
            detail: format!("durable rename operation remains {:?}", outcome.status),
            operation: Some(operation),
        });
    }
    Ok(operation)
}

fn record_failure(
    result: &mut RenameReplicantsByRegionActionResult,
    entry: &RenameReplicantPlan,
    detail: String,
) {
    record_failure_inner(result, entry, detail, None);
}

fn record_failure_with_operation(
    result: &mut RenameReplicantsByRegionActionResult,
    entry: &RenameReplicantPlan,
    error: RenameApplyError,
) {
    record_failure_inner(result, entry, error.detail, error.operation.as_ref());
}

fn record_failure_inner(
    result: &mut RenameReplicantsByRegionActionResult,
    entry: &RenameReplicantPlan,
    detail: String,
    operation: Option<&Operation>,
) {
    result.failures.push(RenameFailure {
        replicant_id: entry.replicant_id.clone(),
        old_name: entry.old_name.clone(),
        intended_name: entry.target_name.clone(),
        error: detail.clone(),
    });
    let event = ActionEvent::new(ActionEventKind::Failed, &entry.replicant_id, detail);
    result.report.events.push(match operation {
        Some(operation) => event.operation(operation),
        None => event,
    });
}

fn classify_unassigned(
    replicant: &Replicant,
    devices: &[Device],
    hub_locations: &BTreeSet<String>,
) -> Result<Classification, RenamePlanningError> {
    let replicant_id = replicant.key.id.as_str().to_owned();
    let mut locations = Vec::new();
    if let Some(location) = &replicant.location {
        locations.push(location.id.as_str().to_owned());
    }
    if let Some(hosted_device) = &replicant.hosted_device {
        let Some(device) = devices
            .iter()
            .find(|device| device.key.id == hosted_device.id)
        else {
            if locations.is_empty() {
                return Err(RenamePlanningError::MissingHostedDevice {
                    replicant_id,
                    device_id: hosted_device.id.as_str().to_owned(),
                });
            }
            return Ok(Classification::Unassigned);
        };
        if let Some(location) = &device.location {
            locations.push(location.id.as_str().to_owned());
        }
    }
    for device in devices
        .iter()
        .filter(|device| device.relationships.hosting_replicant.as_ref() == Some(&replicant.key))
    {
        if let Some(location) = &device.location {
            locations.push(location.id.as_str().to_owned());
        }
    }

    if locations.is_empty() {
        return Err(RenamePlanningError::MissingLocation {
            replicant_id,
            reason: "current location and hosted-device location are both unknown".to_owned(),
        });
    }
    if locations
        .iter()
        .any(|location| hub_locations.contains(&normalize_location(location)))
    {
        Ok(Classification::Hub)
    } else {
        Ok(Classification::Unassigned)
    }
}

fn system_hub_locations(devices: &[Device]) -> BTreeSet<String> {
    devices
        .iter()
        .filter(|device| device.device_type == Some(DeviceType::SystemHub))
        .filter_map(|device| device.location.as_ref())
        .map(|location| normalize_location(location.id.as_str()))
        .collect()
}

fn normalize_location(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn target_name(
    classification: &Classification,
    abbreviations: &BTreeMap<String, String>,
    ordinal: usize,
) -> String {
    match classification {
        Classification::Region(region) => format!(
            "Chats-{}{ordinal:02}",
            abbreviations
                .get(region)
                .map_or_else(|| base_abbreviation(region), Clone::clone)
        ),
        Classification::Hub => format!("Chats-Hub-{ordinal:02}"),
        Classification::Unassigned => format!("Chats-U{ordinal:02}"),
    }
}

fn validate_target_collisions(
    entries: &[RenameReplicantPlan],
    external_occupied_names: &BTreeMap<String, String>,
) -> Result<(), RenamePlanningError> {
    let mut targets = BTreeMap::<String, Vec<String>>::new();
    for entry in entries {
        targets
            .entry(fold_name(&entry.target_name))
            .or_default()
            .push(entry.replicant_id.clone());
    }
    if let Some((_, mut replicants)) = targets.into_iter().find(|(_, ids)| ids.len() > 1) {
        replicants.sort();
        let name = entries
            .iter()
            .find(|entry| replicants.contains(&entry.replicant_id))
            .map_or_else(String::new, |entry| entry.target_name.clone());
        return Err(RenamePlanningError::DuplicateTargetName { name, replicants });
    }
    for (replicant_id, name) in external_occupied_names {
        if entries
            .iter()
            .any(|entry| entry.replicant_id == *replicant_id)
        {
            continue;
        }
        if let Some(entry) = entries
            .iter()
            .find(|entry| fold_name(&entry.target_name) == fold_name(name))
        {
            return Err(RenamePlanningError::OccupiedTargetName {
                name: entry.target_name.clone(),
                replicant_id: replicant_id.clone(),
            });
        }
    }
    Ok(())
}

fn add_staging_names(
    entries: &mut [RenameReplicantPlan],
    external_occupied_names: &BTreeMap<String, String>,
) {
    let current_owners = entries
        .iter()
        .filter_map(|entry| {
            entry
                .old_name
                .as_ref()
                .map(|name| (fold_name(name), entry.replicant_id.clone()))
        })
        .collect::<BTreeMap<_, _>>();
    let changing = entries
        .iter()
        .filter(|entry| entry.old_name.as_deref() != Some(entry.target_name.as_str()))
        .map(|entry| entry.replicant_id.clone())
        .collect::<BTreeSet<_>>();
    let requires_staging = entries.iter().any(|entry| {
        entry.old_name.as_ref().is_some_and(|_| {
            current_owners
                .get(&fold_name(&entry.target_name))
                .is_some_and(|owner| owner != &entry.replicant_id && changing.contains(owner))
        })
    });
    if !requires_staging {
        return;
    }

    let mut occupied = current_owners.keys().cloned().collect::<BTreeSet<_>>();
    occupied.extend(external_occupied_names.values().map(|name| fold_name(name)));
    occupied.extend(entries.iter().map(|entry| fold_name(&entry.target_name)));
    let mut used_temporary = BTreeSet::new();
    for entry in entries
        .iter_mut()
        .filter(|entry| changing.contains(&entry.replicant_id))
    {
        let digest = stable_hex(&entry.replicant_id);
        let mut candidate = format!("Chats-TMP-{}", &digest[..8]);
        let mut serial = 1;
        while occupied.contains(&fold_name(&candidate))
            || !used_temporary.insert(fold_name(&candidate))
        {
            candidate = format!("Chats-TMP-{}-{serial:02}", &digest[..8]);
            serial += 1;
        }
        occupied.insert(fold_name(&candidate));
        entry.temporary_name = Some(candidate);
    }
}

fn region_abbreviations(regions: &BTreeSet<String>) -> BTreeMap<String, String> {
    let mut by_base = BTreeMap::<String, Vec<String>>::new();
    for region in regions {
        by_base
            .entry(base_abbreviation(region))
            .or_default()
            .push(region.clone());
    }
    let mut used = BTreeSet::from(["H".to_owned(), "U".to_owned()]);
    let mut result = BTreeMap::new();
    for (base, names) in by_base {
        let colliding_base = names.len() != 1;
        for region in names {
            let candidate = if !colliding_base && !used.contains(&base) {
                base.clone()
            } else {
                unique_hashed_abbreviation(&base, &region, &used)
            };
            used.insert(candidate.clone());
            result.insert(region, candidate);
        }
    }
    result
}

fn base_abbreviation(region: &str) -> String {
    region
        .chars()
        .find(|character| character.is_ascii_alphanumeric())
        .map(|character| character.to_ascii_uppercase().to_string())
        .unwrap_or_else(|| "R".to_owned())
}

fn unique_hashed_abbreviation(base: &str, region: &str, used: &BTreeSet<String>) -> String {
    let digest = stable_hex(region);
    for length in [4, 6, 8, 10, 12, 16, 32, 64] {
        let candidate = format!("{base}{}", &digest[..length]);
        if !used.contains(&candidate) {
            return candidate;
        }
    }
    let mut serial = 2;
    loop {
        let candidate = format!("{base}{digest}-{serial}");
        if !used.contains(&candidate) {
            return candidate;
        }
        serial += 1;
    }
}

fn stable_hex(value: &str) -> String {
    Sha256::digest(value.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02X}"))
        .collect()
}

fn fold_name(value: &str) -> String {
    value.to_ascii_lowercase()
}

fn validate_target_name(name: &str) -> Result<(), RenamePlanningError> {
    if name.is_empty() {
        return Err(RenamePlanningError::InvalidTargetName {
            name: name.to_owned(),
            reason: "name must not be empty".to_owned(),
        });
    }
    if !name
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || character == '-')
    {
        return Err(RenamePlanningError::InvalidTargetName {
            name: name.to_owned(),
            reason: "only ASCII letters, digits, and hyphens are used".to_owned(),
        });
    }
    if !name
        .chars()
        .next()
        .is_some_and(|character| character.is_ascii_alphanumeric())
    {
        return Err(RenamePlanningError::InvalidTargetName {
            name: name.to_owned(),
            reason: "name must begin with an ASCII letter or digit".to_owned(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use replicant_client::domain::{
        AccessScope, DeviceId, DeviceKey, DeviceRelationships, LocationId, LocationKey,
        ReplicantId, ReplicantKey,
    };
    use replicant_client::{SecretString, StartupPolicy, raw::Url};
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    fn replicant(id: &str, name: &str, location: Option<&str>) -> Replicant {
        Replicant {
            key: ReplicantKey::live(ReplicantId::from(id)),
            name: Some(name.to_owned()),
            is_npc: Some(false),
            status: None,
            location: location.map(|value| LocationKey::live(LocationId::from(value))),
            hosted_device: None,
            travel: None,
            private: Some(Default::default()),
            access: AccessScope::Owned,
        }
    }

    fn hub(location: &str) -> Device {
        Device {
            key: DeviceKey::live(DeviceId::from("HUB-1")),
            device_type: Some(DeviceType::SystemHub),
            status: None,
            location: Some(LocationKey::live(LocationId::from(location))),
            deployed_at: None,
            in_control_range: None,
            features: Vec::new(),
            available_commands: Vec::new(),
            available_directives: Vec::new(),
            tags: Vec::new(),
            settings: BTreeMap::new(),
            relationships: DeviceRelationships::default(),
            cargo: BTreeMap::new(),
            cargo_capacity: None,
            attach_capacity: None,
            stow_capacity: None,
            stow_used: None,
            operational_capacity: None,
            grace_period_remaining: None,
            upkeep_requirements: Vec::new(),
            system_status: None,
            active_directive: None,
            travel: None,
            runtime: Default::default(),
            access: AccessScope::Owned,
        }
    }

    fn plan(replicants: &[Replicant], assignments: &[(&str, &str)]) -> RenameBatchPlan {
        let assignments = assignments
            .iter()
            .map(|(id, region)| ((*id).to_owned(), Some((*region).to_owned())))
            .collect();
        plan_renames(replicants, &assignments, &[], &BTreeMap::new()).expect("plan")
    }

    #[test]
    fn assigned_regions_use_canonical_known_abbreviations() {
        let result = plan(
            &[
                replicant("R-A", "old-a", Some("OUTSIDE")),
                replicant("R-B", "old-b", Some("OUTSIDE")),
                replicant("R-D", "old-d", Some("OUTSIDE")),
            ],
            &[("R-A", "Alpha"), ("R-B", "Beta"), ("R-D", "Delta")],
        );
        assert_eq!(
            result
                .replicants
                .iter()
                .map(|entry| entry.target_name.as_str())
                .collect::<Vec<_>>(),
            ["Chats-A01", "Chats-B01", "Chats-D01"]
        );
    }

    #[test]
    fn assigned_region_wins_over_temporary_hub_location() {
        let result = plan_renames(
            &[replicant("R-1", "old", Some("HUB-LOC"))],
            &BTreeMap::from([(String::from("R-1"), Some(String::from("Alpha")))]),
            &[hub("HUB-LOC")],
            &BTreeMap::new(),
        )
        .expect("plan");
        assert_eq!(result.replicants[0].classification, "alpha");
        assert_eq!(result.replicants[0].target_name, "Chats-A01");
    }

    #[test]
    fn unassigned_hub_and_non_hub_use_reserved_groups() {
        let result = plan_renames(
            &[
                replicant("R-H", "old-h", Some("HUB-LOC")),
                replicant("R-U", "old-u", Some("OUTSIDE")),
            ],
            &BTreeMap::new(),
            &[hub("HUB-LOC")],
            &BTreeMap::new(),
        )
        .expect("plan");
        assert_eq!(result.replicants[0].target_name, "Chats-Hub-01");
        assert_eq!(result.replicants[1].target_name, "Chats-U01");
    }

    #[test]
    fn ordering_is_identity_based_and_repeatable() {
        let reps = [
            replicant("R-2", "Chats-A01", Some("OUTSIDE")),
            replicant("R-1", "Chats-A02", Some("OUTSIDE")),
        ];
        let first = plan(&reps, &[("R-1", "Alpha"), ("R-2", "Alpha")]);
        let second = plan(&reps, &[("R-1", "Alpha"), ("R-2", "Alpha")]);
        assert_eq!(first, second);
        assert_eq!(first.replicants[0].target_name, "Chats-A01");
        assert_eq!(first.replicants[1].target_name, "Chats-A02");
    }

    #[test]
    fn numbering_pads_and_continues_past_nine() {
        let reps = (1..=11)
            .rev()
            .map(|id| replicant(&format!("R-{id:02}"), "old", Some("OUTSIDE")))
            .collect::<Vec<_>>();
        let assignments = (1..=11)
            .map(|id| (format!("R-{id:02}"), Some(String::from("Alpha"))))
            .collect();
        let result = plan_renames(&reps, &assignments, &[], &BTreeMap::new()).expect("plan");
        assert_eq!(result.replicants[0].target_name, "Chats-A01");
        assert_eq!(result.replicants[8].target_name, "Chats-A09");
        assert_eq!(result.replicants[9].target_name, "Chats-A10");
        assert_eq!(result.replicants[10].target_name, "Chats-A11");
    }

    #[test]
    fn already_correct_names_require_no_change() {
        let result = plan(
            &[replicant("R-1", "Chats-A01", Some("OUTSIDE"))],
            &[("R-1", "Alpha")],
        );
        assert_eq!(result.changes_required(), 0);
    }

    #[test]
    fn external_collision_is_rejected_before_mutation() {
        let error = plan_renames(
            &[replicant("R-1", "old", Some("OUTSIDE"))],
            &BTreeMap::from([(String::from("R-1"), Some(String::from("Alpha")))]),
            &[],
            &BTreeMap::from([(String::from("OTHER"), String::from("Chats-A01"))]),
        )
        .expect_err("collision should fail planning");
        assert!(matches!(
            error,
            RenamePlanningError::OccupiedTargetName { .. }
        ));
    }

    #[test]
    fn duplicate_target_validation_reports_all_conflicting_ids() {
        let entries = [
            RenameReplicantPlan {
                replicant_id: String::from("R-1"),
                old_name: Some(String::from("old-1")),
                classification: String::from("alpha"),
                target_name: String::from("Chats-A01"),
                temporary_name: None,
            },
            RenameReplicantPlan {
                replicant_id: String::from("R-2"),
                old_name: Some(String::from("old-2")),
                classification: String::from("beta"),
                target_name: String::from("chats-a01"),
                temporary_name: None,
            },
        ];
        let error = validate_target_collisions(&entries, &BTreeMap::new())
            .expect_err("duplicate target names should fail");
        assert!(matches!(
            error,
            RenamePlanningError::DuplicateTargetName { ref replicants, .. }
                if replicants == &["R-1", "R-2"]
        ));
    }

    #[test]
    fn target_name_validation_rejects_api_unsafe_values() {
        let error = validate_target_name("Chats A01").expect_err("spaces are unsafe");
        assert!(matches!(
            error,
            RenamePlanningError::InvalidTargetName { .. }
        ));
    }

    #[tokio::test]
    async fn api_rejection_keeps_a_durable_operation_for_rerun() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/replicants/R-1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "replicant_code": "R-1",
                "name": "old",
                "location": "OUTSIDE"
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("PATCH"))
            .and(path("/v1/replicants/R-1"))
            .respond_with(
                ResponseTemplate::new(409)
                    .set_body_json(serde_json::json!({"error": "name is already in use"})),
            )
            .expect(1)
            .mount(&server)
            .await;
        let client = Client::builder()
            .authentication_token(SecretString::from("test-token"))
            .base_url(Url::parse(&server.uri()).expect("mock URL"))
            .in_memory()
            .startup_policy(StartupPolicy::RestoreOnly)
            .start()
            .await
            .expect("start client");
        let handle = client
            .replicants()
            .get_owned("R-1")
            .await
            .expect("owned handle");
        let error = apply_name(&handle, "Chats-U01")
            .await
            .expect_err("API rejection should be reported");
        assert!(error.detail.contains("Rejected"));
        assert!(error.operation.is_some());
        client.close().await.expect("close client");
    }

    #[test]
    fn swaps_receive_temporary_names() {
        let reps = [
            replicant("R-1", "Chats-B01", Some("OUTSIDE")),
            replicant("R-2", "Chats-A01", Some("OUTSIDE")),
        ];
        let result = plan(&reps, &[("R-1", "Alpha"), ("R-2", "Beta")]);
        assert!(
            result
                .replicants
                .iter()
                .all(|entry| entry.temporary_name.is_some())
        );
        assert_ne!(
            result.replicants[0].temporary_name,
            result.replicants[1].temporary_name
        );
    }

    #[test]
    fn unknown_regions_get_deterministic_non_reserved_abbreviations() {
        let reps = [
            replicant("R-1", "old-1", Some("OUTSIDE")),
            replicant("R-2", "old-2", Some("OUTSIDE")),
        ];
        let assignments = BTreeMap::from([
            (String::from("R-1"), Some(String::from("Orion"))),
            (String::from("R-2"), Some(String::from("Ocean"))),
        ]);
        let first = plan_renames(&reps, &assignments, &[], &BTreeMap::new()).expect("plan");
        let second = plan_renames(&reps, &assignments, &[], &BTreeMap::new()).expect("plan");
        assert_eq!(first, second);
        assert_ne!(
            first.replicants[0].target_name,
            first.replicants[1].target_name
        );
        assert!(
            first
                .replicants
                .iter()
                .all(|entry| !entry.target_name.starts_with("Chats-H")
                    && !entry.target_name.starts_with("Chats-U"))
        );
    }

    #[test]
    fn missing_unassigned_location_is_not_silently_classified() {
        let error = plan_renames(
            &[replicant("R-1", "old", None)],
            &BTreeMap::new(),
            &[],
            &BTreeMap::new(),
        )
        .expect_err("unknown location should fail planning");
        assert!(matches!(error, RenamePlanningError::MissingLocation { .. }));
    }
}
