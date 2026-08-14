//! Finite application actions.

use std::{
    io,
    time::{Duration, Instant},
};

use replicant_client::{Client, DeviceType, Operation, OperationStatus, SyncDomain, raw};
use serde::{Deserialize, Serialize};

use crate::ActionResult;

/// Machine-readable category for one finite action event.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionEventKind {
    /// A mutation that would be or is about to be submitted.
    Planned,
    /// A mutation accepted by the managed operation layer.
    Succeeded,
    /// Work omitted because the requested state already holds or no work matched.
    Skipped,
    /// Work that could not be completed.
    Failed,
}

/// One frontend-renderable event produced by a finite action.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ActionEvent {
    /// Event category.
    pub kind: ActionEventKind,
    /// Device, location, or other subject of the event.
    pub subject: String,
    /// Useful human-readable context.
    pub detail: String,
    /// Managed operation ID when a mutation was submitted.
    pub operation_id: Option<String>,
}

impl ActionEvent {
    fn new(kind: ActionEventKind, subject: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            kind,
            subject: subject.into(),
            detail: detail.into(),
            operation_id: None,
        }
    }

    fn operation(mut self, operation: &Operation) -> Self {
        self.operation_id = Some(operation.id().as_str().to_owned());
        self
    }
}

/// Standard machine-readable report shared by finite actions.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ActionReport {
    /// Ordered events emitted while planning and applying the action.
    pub events: Vec<ActionEvent>,
}

impl ActionReport {
    /// Returns whether any work failed.
    #[must_use]
    pub fn failed(&self) -> bool {
        self.events
            .iter()
            .any(|event| event.kind == ActionEventKind::Failed)
    }
}

/// Inputs for [`clear_tags`].
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ClearTagsAction {
    /// Prefix selecting tags to remove.
    pub tag_prefix: String,
    /// When true, report matching tags without submitting mutations.
    pub dry_run: bool,
}

impl ClearTagsAction {
    /// Creates a mutating clear-tags action for a prefix.
    #[must_use]
    pub fn new(tag_prefix: impl Into<String>) -> Self {
        Self {
            tag_prefix: tag_prefix.into(),
            dry_run: false,
        }
    }
}

/// Clear-tags outcome for one matching device.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ClearedDeviceTags {
    /// Device code.
    pub device: String,
    /// Matching tags selected for removal.
    pub tags: Vec<String>,
    /// Whether a managed mutation was submitted.
    pub changed: bool,
}

/// Typed result of a clear-tags action.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ClearTagsActionResult {
    /// Total owned devices scanned.
    pub scanned_devices: usize,
    /// Per-device outcomes for devices carrying matching tags.
    pub devices: Vec<ClearedDeviceTags>,
    /// Standard action events for non-stdout frontends.
    pub report: ActionReport,
}

impl ClearTagsActionResult {
    /// Number of matching tags found.
    #[must_use]
    pub fn removed_tags(&self) -> usize {
        self.devices.iter().map(|device| device.tags.len()).sum()
    }

    /// Number of devices changed through managed operations.
    #[must_use]
    pub fn changed_devices(&self) -> usize {
        self.devices.iter().filter(|device| device.changed).count()
    }
}

/// Inputs for adding one tag to every owned device of a type.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TagDevicesAction {
    /// Device type selected for tagging.
    pub device_type: String,
    /// Tag added to matching devices that do not already carry it.
    pub tag: String,
    /// When true, report matching devices without submitting mutations.
    pub dry_run: bool,
}

impl TagDevicesAction {
    /// Creates a mutating tag-devices action.
    #[must_use]
    pub fn new(device_type: impl Into<String>, tag: impl Into<String>) -> Self {
        Self {
            device_type: device_type.into(),
            tag: tag.into(),
            dry_run: false,
        }
    }
}

/// Typed result of a tag-devices action.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TagDevicesActionResult {
    /// Total owned devices of the requested type scanned.
    pub scanned_devices: usize,
    /// Devices that received or would receive the tag.
    pub changed_devices: usize,
    /// Devices that already carried the tag.
    pub already_tagged_devices: usize,
    /// Standard action events for non-stdout frontends.
    pub report: ActionReport,
}

/// Adds one tag to every owned device of a type through managed operations.
pub async fn tag_devices(
    client: &Client,
    action: &TagDevicesAction,
) -> ActionResult<TagDevicesActionResult> {
    if action.device_type.is_empty() || action.tag.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "device_type and tag must not be empty",
        )
        .into());
    }

    let handles = client
        .devices()
        .refresh_many()
        .of_type(DeviceType::from(action.device_type.as_str()))
        .collect()
        .await?;
    let scanned_devices = handles.len();
    let mut changed_devices = 0;
    let mut already_tagged_devices = 0;
    let mut report = ActionReport::default();

    for handle in handles {
        let snapshot = handle.snapshot().await?;
        if snapshot.tags.iter().any(|tag| tag == &action.tag) {
            already_tagged_devices += 1;
            report.events.push(ActionEvent::new(
                ActionEventKind::Skipped,
                handle.id().as_str(),
                format!("already tagged {}", action.tag),
            ));
            continue;
        }

        let event = if action.dry_run {
            ActionEvent::new(
                ActionEventKind::Planned,
                handle.id().as_str(),
                format!("add tag {}", action.tag),
            )
        } else {
            let operation = handle
                .configure(raw::devices::DeviceConfiguration {
                    add_tags: Some(vec![action.tag.clone()]),
                    ..Default::default()
                })
                .await?;
            ensure_operation_accepted(&operation).await?;
            ActionEvent::new(
                ActionEventKind::Succeeded,
                handle.id().as_str(),
                format!("added tag {}", action.tag),
            )
            .operation(&operation)
        };
        changed_devices += 1;
        report.events.push(event);
    }

    Ok(TagDevicesActionResult {
        scanned_devices,
        changed_devices,
        already_tagged_devices,
        report,
    })
}

/// Removes matching tags from every owned device through managed operations.
pub async fn clear_tags(
    client: &Client,
    action: &ClearTagsAction,
) -> ActionResult<ClearTagsActionResult> {
    if action.tag_prefix.is_empty() {
        return Err(
            io::Error::new(io::ErrorKind::InvalidInput, "tag_prefix must not be empty").into(),
        );
    }

    let handles = client.devices().refresh_many().collect().await?;
    let scanned_devices = handles.len();
    let mut devices = Vec::new();
    let mut report = ActionReport::default();

    for handle in handles {
        let snapshot = handle.snapshot().await?;
        let tags = matching_tags(&snapshot.tags, &action.tag_prefix);
        if tags.is_empty() {
            continue;
        }

        if action.dry_run {
            report.events.push(ActionEvent::new(
                ActionEventKind::Planned,
                handle.id().as_str(),
                format!("remove tags: {}", tags.join(", ")),
            ));
        } else {
            let operation = handle
                .configure(raw::devices::DeviceConfiguration {
                    remove_tags: Some(tags.clone()),
                    ..Default::default()
                })
                .await?;
            ensure_operation_accepted(&operation).await?;
            report.events.push(
                ActionEvent::new(
                    ActionEventKind::Succeeded,
                    handle.id().as_str(),
                    format!("removed tags: {}", tags.join(", ")),
                )
                .operation(&operation),
            );
        }
        devices.push(ClearedDeviceTags {
            device: handle.id().as_str().to_owned(),
            tags,
            changed: !action.dry_run,
        });
    }

    Ok(ClearTagsActionResult {
        scanned_devices,
        devices,
        report,
    })
}

/// Inputs for planning and applying a device contribution.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContributeDevicesAction {
    /// Location receiving the contribution.
    pub destination: String,
    /// Device type selected at the destination.
    pub device_type: String,
    /// Owned replicant name or code that must own each selected device.
    pub owner: String,
    /// Optional device tag required in addition to type and location.
    pub tag: Option<String>,
    /// Optional maximum number of devices, selected in stable code order.
    pub count: Option<usize>,
    /// When true, return the plan without submitting mutations.
    pub dry_run: bool,
}

impl ContributeDevicesAction {
    /// Creates an action selecting every matching device.
    #[must_use]
    pub fn new(
        destination: impl Into<String>,
        device_type: impl Into<String>,
        owner: impl Into<String>,
    ) -> Self {
        Self {
            destination: destination.into(),
            device_type: device_type.into(),
            owner: owner.into(),
            tag: None,
            count: None,
            dry_run: false,
        }
    }
}

/// Planned ownership state for one selected contribution device.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContributionDevicePlan {
    /// Device code.
    pub device: String,
    /// Current assigned replicant code, if any.
    pub current_owner: Option<String>,
    /// Whether ownership must change before contribution.
    pub owner_change_required: bool,
}

/// Fully resolved preview of a device contribution.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContributionPlan {
    /// Destination location.
    pub destination: String,
    /// Selected device type.
    pub device_type: String,
    /// Optional device tag required by the action.
    pub tag: Option<String>,
    /// Resolved owned replicant code.
    pub owner: String,
    /// Selected devices in stable code order.
    pub devices: Vec<ContributionDevicePlan>,
}

/// Typed result of a finite device contribution action.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContributeDevicesActionResult {
    /// Plan resolved from current managed state before mutation.
    pub plan: ContributionPlan,
    /// Standard action events, including partial success or failure.
    pub report: ActionReport,
}

/// Resolves parameters and current state without submitting mutations.
pub async fn plan_device_contribution(
    client: &Client,
    action: &ContributeDevicesAction,
) -> ActionResult<ContributionPlan> {
    validate_contribution(action)?;
    let owner = resolve_owned_replicant(client, &action.owner, &action.destination).await?;
    let handles = client
        .devices()
        .refresh_many()
        .of_type(DeviceType::from(action.device_type.as_str()))
        .at(&action.destination)
        .collect()
        .await?;
    let mut candidates = Vec::with_capacity(handles.len());
    for handle in handles {
        let snapshot = handle.snapshot().await?;
        candidates.push(ContributionCandidate {
            device: handle.id().as_str().to_owned(),
            owner: assigned_replicant(&snapshot).map(str::to_owned),
            tags: snapshot.tags,
        });
    }

    Ok(ContributionPlan {
        destination: action.destination.clone(),
        device_type: action.device_type.clone(),
        tag: action.tag.clone(),
        devices: select_contribution_devices(
            candidates,
            action.tag.as_deref(),
            action.count,
            &owner,
        ),
        owner,
    })
}

/// Applies a freshly resolved finite device contribution through managed operations.
pub async fn contribute_devices(
    client: &Client,
    action: &ContributeDevicesAction,
) -> ActionResult<ContributeDevicesActionResult> {
    let plan = plan_device_contribution(client, action).await?;
    let mut report = ActionReport::default();

    if plan.devices.is_empty() {
        report.events.push(ActionEvent::new(
            ActionEventKind::Skipped,
            &plan.destination,
            "no matching devices remain to contribute",
        ));
        return Ok(ContributeDevicesActionResult { plan, report });
    }

    for device in &plan.devices {
        if device.owner_change_required {
            report.events.push(ActionEvent::new(
                ActionEventKind::Planned,
                &device.device,
                format!("change owner to {}", plan.owner),
            ));
        } else {
            report.events.push(ActionEvent::new(
                ActionEventKind::Skipped,
                &device.device,
                format!("already owned by {}", plan.owner),
            ));
        }
    }
    report.events.push(ActionEvent::new(
        ActionEventKind::Planned,
        &plan.destination,
        format!("contribute {} device(s)", plan.devices.len()),
    ));

    if action.dry_run {
        return Ok(ContributeDevicesActionResult { plan, report });
    }

    for device in &plan.devices {
        if !device.owner_change_required {
            continue;
        }
        let handle = client.devices().get(&device.device).await?;
        let operation = handle.change_owner(&plan.owner).await?;
        match wait_for_owner(client, &plan, &device.device, &operation).await? {
            None => report.events.push(
                ActionEvent::new(
                    ActionEventKind::Succeeded,
                    &device.device,
                    format!("changed owner to {}", plan.owner),
                )
                .operation(&operation),
            ),
            Some(detail) => {
                report.events.push(
                    ActionEvent::new(ActionEventKind::Failed, &device.device, detail)
                        .operation(&operation),
                );
                return Ok(ContributeDevicesActionResult { plan, report });
            }
        }
    }

    for device in &plan.devices {
        let snapshot = client
            .devices()
            .get(&device.device)
            .await?
            .snapshot()
            .await?;
        if let Err(error) = ensure_ready_for_contribution(&plan, &device.device, &snapshot) {
            report.events.push(ActionEvent::new(
                ActionEventKind::Failed,
                &device.device,
                error.to_string(),
            ));
            return Ok(ContributeDevicesActionResult { plan, report });
        }
    }

    let operation = client
        .locations()
        .contribute(
            &plan.destination,
            plan.devices
                .iter()
                .map(|device| device.device.clone())
                .collect(),
        )
        .await?;
    let outcome = operation.outcome().await?;
    let (kind, detail) = match outcome.status {
        OperationStatus::Cancelled | OperationStatus::Rejected | OperationStatus::Failed => (
            ActionEventKind::Failed,
            format!(
                "contribution ended as {:?}: {:?}",
                outcome.status, outcome.response
            ),
        ),
        OperationStatus::Ambiguous => (
            ActionEventKind::Failed,
            "contribution outcome is ambiguous; refresh state before deciding whether to retry"
                .into(),
        ),
        _ => (
            ActionEventKind::Succeeded,
            format!("contributed {} device(s)", plan.devices.len()),
        ),
    };
    report
        .events
        .push(ActionEvent::new(kind, &plan.destination, detail).operation(&operation));

    Ok(ContributeDevicesActionResult { plan, report })
}

#[derive(Debug)]
struct ContributionCandidate {
    device: String,
    owner: Option<String>,
    tags: Vec<String>,
}

fn select_contribution_devices(
    mut candidates: Vec<ContributionCandidate>,
    tag: Option<&str>,
    count: Option<usize>,
    owner: &str,
) -> Vec<ContributionDevicePlan> {
    candidates.sort_by(|left, right| left.device.cmp(&right.device));
    candidates
        .into_iter()
        .filter(|candidate| tag.is_none_or(|tag| candidate.tags.iter().any(|value| value == tag)))
        .take(count.unwrap_or(usize::MAX))
        .map(|candidate| ContributionDevicePlan {
            owner_change_required: candidate.owner.as_deref() != Some(owner),
            device: candidate.device,
            current_owner: candidate.owner,
        })
        .collect()
}

fn validate_contribution(action: &ContributeDevicesAction) -> ActionResult<()> {
    if action.destination.is_empty() || action.device_type.is_empty() || action.owner.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "destination, device_type, and owner must not be empty",
        )
        .into());
    }
    if action.tag.as_deref() == Some("") || action.count == Some(0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "tag must not be empty and count must be greater than zero",
        )
        .into());
    }
    Ok(())
}

async fn resolve_owned_replicant(
    client: &Client,
    requested: &str,
    destination: &str,
) -> ActionResult<String> {
    client.sync().domain(SyncDomain::Replicants).await?;
    let handles = client.replicants().find().owned().collect().await?;
    let mut matches = Vec::new();
    for handle in handles {
        let snapshot = handle.snapshot().await?;
        if snapshot.key.id.as_str().eq_ignore_ascii_case(requested)
            || snapshot
                .name
                .as_deref()
                .is_some_and(|name| name.eq_ignore_ascii_case(requested))
        {
            matches.push(snapshot);
        }
    }
    let replicant = match matches.as_slice() {
        [replicant] => replicant,
        [] => {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("no owned replicant matches {requested:?}"),
            )
            .into());
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("owned replicant name {requested:?} is ambiguous; use its code"),
            )
            .into());
        }
    };
    let actual = replicant
        .location
        .as_ref()
        .map(|location| location.id.as_str());
    if actual != Some(destination) {
        return Err(io::Error::other(format!(
            "owner {} must be at {destination}, current location={actual:?}",
            replicant.key.id
        ))
        .into());
    }
    Ok(replicant.key.id.as_str().to_owned())
}

async fn wait_for_owner(
    client: &Client,
    plan: &ContributionPlan,
    device: &str,
    operation: &Operation,
) -> ActionResult<Option<String>> {
    let started = Instant::now();
    let mut delay = Duration::from_millis(250);
    loop {
        let snapshot = client.devices().get(device).await?.snapshot().await?;
        ensure_at_destination(plan, device, &snapshot)?;
        if assigned_replicant(&snapshot) == Some(plan.owner.as_str()) {
            return Ok(None);
        }
        let status = operation.status().await?;
        if matches!(
            status,
            OperationStatus::Cancelled | OperationStatus::Rejected | OperationStatus::Failed
        ) {
            return Ok(Some(format!("change owner ended as {status:?}")));
        }
        if started.elapsed() >= Duration::from_secs(30) {
            return Ok(Some(format!(
                "owner did not become {} within 30s; operation status={status:?}",
                plan.owner
            )));
        }
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(Duration::from_secs(2));
    }
}

fn assigned_replicant(device: &replicant_client::Device) -> Option<&str> {
    device
        .relationships
        .assigned_replicant
        .as_ref()
        .map(|owner| owner.id.as_str())
}

fn ensure_at_destination(
    plan: &ContributionPlan,
    device: &str,
    snapshot: &replicant_client::Device,
) -> ActionResult<()> {
    let actual = snapshot
        .location
        .as_ref()
        .map(|location| location.id.as_str());
    if actual != Some(plan.destination.as_str()) {
        return Err(io::Error::other(format!(
            "device {device} moved before contribution; expected {}, current location={actual:?}",
            plan.destination
        ))
        .into());
    }
    Ok(())
}

fn ensure_ready_for_contribution(
    plan: &ContributionPlan,
    device: &str,
    snapshot: &replicant_client::Device,
) -> ActionResult<()> {
    ensure_at_destination(plan, device, snapshot)?;
    if assigned_replicant(snapshot) != Some(plan.owner.as_str()) {
        return Err(io::Error::other(format!(
            "device {device} is not assigned to {} immediately before contribution",
            plan.owner
        ))
        .into());
    }
    if let Some(tag) = &plan.tag
        && !snapshot.tags.iter().any(|value| value == tag)
    {
        return Err(io::Error::other(format!(
            "device {device} no longer carries required tag {tag:?}"
        ))
        .into());
    }
    Ok(())
}

fn matching_tags(tags: &[String], prefix: &str) -> Vec<String> {
    tags.iter()
        .filter(|tag| tag.starts_with(prefix))
        .cloned()
        .collect()
}

async fn ensure_operation_accepted(operation: &Operation) -> ActionResult<()> {
    let outcome = operation.outcome().await?;
    if matches!(
        outcome.status,
        OperationStatus::Cancelled | OperationStatus::Rejected | OperationStatus::Failed
    ) {
        return Err(io::Error::other(format!(
            "operation {} ended as {:?}: {:?}",
            operation.id().as_str(),
            outcome.status,
            outcome.response
        ))
        .into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selects_only_prefixed_tags_and_summarizes_result() {
        let tags = vec!["keep".into(), "evt-one".into(), "evt-two".into()];
        assert_eq!(matching_tags(&tags, "evt-"), ["evt-one", "evt-two"]);

        let result = ClearTagsActionResult {
            scanned_devices: 2,
            devices: vec![ClearedDeviceTags {
                device: "DEV-1".into(),
                tags: matching_tags(&tags, "evt-"),
                changed: true,
            }],
            report: ActionReport::default(),
        };
        assert_eq!(result.removed_tags(), 2);
        assert_eq!(result.changed_devices(), 1);
    }

    #[test]
    fn contribution_selection_filters_sorts_limits_and_marks_existing_owner() {
        let candidates = vec![
            ContributionCandidate {
                device: "DEV-2".into(),
                owner: Some("OWNER-1".into()),
                tags: vec!["event".into()],
            },
            ContributionCandidate {
                device: "DEV-1".into(),
                owner: None,
                tags: vec!["event".into()],
            },
            ContributionCandidate {
                device: "DEV-0".into(),
                owner: None,
                tags: vec!["other".into()],
            },
        ];

        let selected = select_contribution_devices(candidates, Some("event"), Some(2), "OWNER-1");

        assert_eq!(
            selected
                .iter()
                .map(|device| device.device.as_str())
                .collect::<Vec<_>>(),
            ["DEV-1", "DEV-2"]
        );
        assert!(selected[0].owner_change_required);
        assert!(!selected[1].owner_change_required);
    }

    #[test]
    fn action_report_exposes_already_satisfied_and_failed_work() {
        let report = ActionReport {
            events: vec![
                ActionEvent::new(ActionEventKind::Skipped, "DEV-1", "already owned"),
                ActionEvent::new(ActionEventKind::Failed, "DEST", "rejected"),
            ],
        };

        assert!(report.failed());
        assert_eq!(
            serde_json::to_value(&report).expect("serialize")["events"][0]["kind"],
            "skipped"
        );
    }
}
