use std::{
    collections::{BTreeMap, BTreeSet},
    io,
};

use crate::automation::{EventCampaignCheckpoint, EventDeliveryCheckpoint, EventTourCheckpoint};
use replicant_client::{Client, OperationStatus, Star, raw};
use replicant_event_planner::{MAX_TAG_CHARACTERS, mission_tag};
use replicant_workflow::WorkflowRepository;
use serde::Serialize;
use tracing::{info, warn};

use super::{AnyResult, EVENT_MISSION_TAG_PREFIX, EventMissionPlan, app_error};

const EVENT_STOCK_TAG_PREFIX: &str = "evt-stock:";

#[derive(Clone, Debug)]
struct LegacyMissionIdentity {
    deterministic_tag: String,
    event_designation: String,
    active: bool,
}

/// Options controlling event-stock reconciliation.
#[derive(Clone, Copy, Debug, Default)]
pub struct EventStockReconcileOptions {
    /// Apply tag mutations rather than only reporting them.
    pub execute: bool,
    /// Convert unknown opaque legacy event tags into regional stock.
    ///
    /// Automatic daemon reconciliation deliberately leaves unknown tags alone;
    /// the explicit maintenance command may opt into reclaiming them.
    pub reclaim_unknown_orphans: bool,
}

/// One device considered by event-stock reconciliation.
#[derive(Clone, Debug, Serialize)]
pub struct EventStockReconcileDevice {
    /// Owned device code.
    pub device: String,
    /// Physical location when known.
    pub location: Option<String>,
    /// Legacy event mission tags found on the device.
    pub old_tags: Vec<String>,
    /// Stable event tag or regional stock tag selected by reconciliation.
    pub target_tag: Option<String>,
    /// Whether a mutation was submitted.
    pub changed: bool,
    /// Human-readable classification.
    pub disposition: String,
}

/// Summary of event-stock reconciliation.
#[derive(Clone, Debug, Serialize)]
pub struct EventStockReconcileReport {
    /// Number of owned devices scanned.
    pub scanned_devices: usize,
    /// Number of devices carrying at least one `evt-m:` tag.
    pub event_tagged_devices: usize,
    /// Number of terminal-workflow devices mapped back to their exact event.
    pub event_reclaims: usize,
    /// Number of unknown legacy devices moved/would move to regional stock.
    pub regional_stock_reclaims: usize,
    /// Number of devices retained because an active workflow still owns them.
    pub active_reservations: usize,
    /// Number of devices skipped because their legacy identity was ambiguous.
    pub ambiguous: usize,
    /// Whether mutations were enabled.
    pub executed: bool,
    /// Per-device reconciliation decisions.
    pub devices: Vec<EventStockReconcileDevice>,
}

/// Reconciles legacy per-workflow event tags into durable per-event identity.
///
/// Terminal workflow archives provide an exact mapping from old opaque
/// `evt-m:<hash>` tags to the game event that originally provisioned the
/// hardware. Unknown opaque tags can optionally fall back to a regional
/// `evt-stock:<region>` pool so they remain reusable instead of permanently
/// reserved by a dead mission.
pub async fn reconcile_event_stock(
    client: &Client,
    repository: &WorkflowRepository,
    options: EventStockReconcileOptions,
) -> AnyResult<EventStockReconcileReport> {
    client.ready().await?;
    let identities = legacy_mission_identities(repository)?;
    // Region assignment uses the managed/local catalogue. Reconciliation is
    // intentionally a device-tag maintenance pass and must not trigger a fresh
    // account-wide star-catalogue traversal just to classify orphan stock.
    let catalogue = client.galaxy().catalogue();
    let handles = client.devices().find().owned().collect().await?;
    let scanned_devices = handles.len();
    let mut report = EventStockReconcileReport {
        scanned_devices,
        event_tagged_devices: 0,
        event_reclaims: 0,
        regional_stock_reclaims: 0,
        active_reservations: 0,
        ambiguous: 0,
        executed: options.execute,
        devices: Vec::new(),
    };

    for handle in handles {
        let snapshot = handle.snapshot().await?;
        let old_tags = snapshot
            .tags
            .iter()
            .filter(|tag| tag.starts_with(EVENT_MISSION_TAG_PREFIX))
            .cloned()
            .collect::<Vec<_>>();
        if old_tags.is_empty() {
            continue;
        }
        report.event_tagged_devices += 1;

        let known = old_tags
            .iter()
            .filter_map(|tag| identities.get(tag).map(|identity| (tag, identity)))
            .collect::<Vec<_>>();
        let active = known
            .iter()
            .filter(|(_, identity)| identity.active)
            .map(|(tag, _)| (*tag).clone())
            .collect::<Vec<_>>();
        if !active.is_empty() {
            report.active_reservations += 1;
            report.devices.push(EventStockReconcileDevice {
                device: handle.id().as_str().to_owned(),
                location: snapshot_location_string(&snapshot.location),
                old_tags,
                target_tag: None,
                changed: false,
                disposition: format!(
                    "retained active event reservation(s): {}",
                    active.join(", ")
                ),
            });
            continue;
        }

        let targets = known
            .iter()
            .map(|(_, identity)| identity.deterministic_tag.clone())
            .collect::<BTreeSet<_>>();
        let reusable_support = terminal_support_asset(&snapshot.tags);
        let (target_tag, disposition) = if targets.len() == 1 && reusable_support {
            let region = snapshot
                .location
                .as_ref()
                .and_then(|location| region_for_location(&catalogue, location.id.as_str()))
                .unwrap_or_else(|| "unknown".to_owned());
            report.regional_stock_reclaims += 1;
            (
                Some(event_stock_tag(&region)),
                format!("release terminal support asset into {region} regional event stock"),
            )
        } else if targets.len() == 1 {
            let target = targets.iter().next().cloned().expect("one target");
            let identity = known
                .iter()
                .find(|(_, identity)| identity.deterministic_tag == target)
                .map(|(_, identity)| *identity)
                .expect("matching identity");
            report.event_reclaims += 1;
            (
                Some(target),
                format!("reclaim for event {}", identity.event_designation),
            )
        } else if targets.len() > 1 {
            report.ambiguous += 1;
            report.devices.push(EventStockReconcileDevice {
                device: handle.id().as_str().to_owned(),
                location: snapshot_location_string(&snapshot.location),
                old_tags,
                target_tag: None,
                changed: false,
                disposition: "multiple legacy tags map to different events; manual review required"
                    .to_owned(),
            });
            continue;
        } else if old_tags.iter().any(|tag| !is_opaque_legacy_tag(tag)) {
            // A readable deterministic tag is already the desired steady state.
            report.devices.push(EventStockReconcileDevice {
                device: handle.id().as_str().to_owned(),
                location: snapshot_location_string(&snapshot.location),
                old_tags,
                target_tag: None,
                changed: false,
                disposition: "already uses a readable event identity".to_owned(),
            });
            continue;
        } else if options.reclaim_unknown_orphans {
            let region = snapshot
                .location
                .as_ref()
                .and_then(|location| region_for_location(&catalogue, location.id.as_str()))
                .unwrap_or_else(|| "unknown".to_owned());
            report.regional_stock_reclaims += 1;
            (
                Some(event_stock_tag(&region)),
                format!("reclaim orphan into {region} regional event stock"),
            )
        } else {
            report.devices.push(EventStockReconcileDevice {
                device: handle.id().as_str().to_owned(),
                location: snapshot_location_string(&snapshot.location),
                old_tags,
                target_tag: None,
                changed: false,
                disposition: "unknown legacy reservation left unchanged".to_owned(),
            });
            continue;
        };

        let target_tag = target_tag.expect("reclaim target exists");
        let already_target = snapshot.tags.iter().any(|tag| tag == &target_tag);
        let target_is_regional_stock = target_tag.starts_with(EVENT_STOCK_TAG_PREFIX);
        let removable = snapshot
            .tags
            .iter()
            .filter(|tag| {
                (tag.starts_with(EVENT_MISSION_TAG_PREFIX) && **tag != target_tag)
                    || tag.starts_with("evt-b:")
                    || tag.starts_with("evt-p:")
                    || (target_is_regional_stock && tag.starts_with("evt-role:"))
            })
            .cloned()
            .collect::<Vec<_>>();
        let changed = !already_target || !removable.is_empty();
        if changed && options.execute {
            let operation = handle
                .configure(raw::devices::DeviceConfiguration {
                    add_tags: (!already_target).then_some(vec![target_tag.clone()]),
                    remove_tags: (!removable.is_empty()).then_some(removable),
                    ..Default::default()
                })
                .await?;
            ensure_operation_accepted(&operation).await?;
            info!(
                device = %handle.id().as_str(),
                target_tag = %target_tag,
                "reconciled legacy event-stock reservation"
            );
        }
        report.devices.push(EventStockReconcileDevice {
            device: handle.id().as_str().to_owned(),
            location: snapshot_location_string(&snapshot.location),
            old_tags,
            target_tag: Some(target_tag),
            changed: changed && options.execute,
            disposition,
        });
    }

    Ok(report)
}

fn legacy_mission_identities(
    repository: &WorkflowRepository,
) -> AnyResult<BTreeMap<String, LegacyMissionIdentity>> {
    let mut identities = BTreeMap::<String, LegacyMissionIdentity>::new();
    for workflow in repository.list()? {
        let active = !workflow.status.is_terminal();
        match workflow.kind.as_str() {
            "event.campaign" => {
                let checkpoint = match workflow.checkpoint::<EventCampaignCheckpoint>() {
                    Ok(checkpoint) => checkpoint,
                    Err(error) => {
                        warn!(
                            workflow_id = %workflow.id,
                            error = %error,
                            "skipping unreadable event campaign checkpoint during stock reconciliation"
                        );
                        continue;
                    }
                };
                if let Some(archive) = checkpoint.archive {
                    for plan_json in archive.mission_json.values() {
                        if let Ok(plan) = serde_json::from_str::<EventMissionPlan>(plan_json) {
                            merge_identity(&mut identities, &plan, active);
                        }
                    }
                }
            }
            "event.delivery" => {
                let checkpoint = match workflow.checkpoint::<EventDeliveryCheckpoint>() {
                    Ok(checkpoint) => checkpoint,
                    Err(error) => {
                        warn!(
                            workflow_id = %workflow.id,
                            error = %error,
                            "skipping unreadable event delivery checkpoint during stock reconciliation"
                        );
                        continue;
                    }
                };
                if let Some(plan_json) = checkpoint.plan_json
                    && let Ok(plan) = serde_json::from_str::<EventMissionPlan>(&plan_json)
                {
                    merge_identity(&mut identities, &plan, active);
                }
            }
            "event.tour" => {
                let checkpoint = match workflow.checkpoint::<EventTourCheckpoint>() {
                    Ok(checkpoint) => checkpoint,
                    Err(error) => {
                        warn!(
                            workflow_id = %workflow.id,
                            error = %error,
                            "skipping unreadable event tour checkpoint during stock reconciliation"
                        );
                        continue;
                    }
                };
                if let Some(plan_json) = checkpoint.plan_json
                    && let Ok(plan) = serde_json::from_str::<EventMissionPlan>(&plan_json)
                {
                    merge_identity(&mut identities, &plan, active);
                }
            }
            _ => {}
        }
    }
    Ok(identities)
}

fn merge_identity(
    identities: &mut BTreeMap<String, LegacyMissionIdentity>,
    plan: &EventMissionPlan,
    active: bool,
) {
    let deterministic_tag = mission_tag(&plan.event.designation);
    identities
        .entry(plan.mission_tag.clone())
        .and_modify(|identity| identity.active |= active)
        .or_insert_with(|| LegacyMissionIdentity {
            deterministic_tag,
            event_designation: plan.event.designation.clone(),
            active,
        });
}

fn terminal_support_asset(tags: &[String]) -> bool {
    tags.iter().any(|tag| {
        matches!(
            tag.as_str(),
            "evt-role:cargo" | "evt-role:carrier" | "evt-role:beacon"
        )
    })
}

fn is_opaque_legacy_tag(tag: &str) -> bool {
    tag.strip_prefix(EVENT_MISSION_TAG_PREFIX)
        .is_some_and(|suffix| {
            suffix.len() == 16 && suffix.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
}

fn event_stock_tag(region: &str) -> String {
    let normalized = region
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    let direct = format!("{EVENT_STOCK_TAG_PREFIX}{normalized}");
    if direct.chars().count() <= MAX_TAG_CHARACTERS {
        direct
    } else {
        let hash = stable_hash(&normalized);
        format!("{EVENT_STOCK_TAG_PREFIX}{hash:016x}")
    }
}

fn snapshot_location_string(
    location: &Option<replicant_client::domain::LocationKey>,
) -> Option<String> {
    location
        .as_ref()
        .map(|location| location.id.as_str().to_owned())
}

fn region_for_location(catalogue: &[Star], location: &str) -> Option<String> {
    catalogue
        .iter()
        .filter(|star| {
            let designation = star.key.id.as_str();
            location.eq_ignore_ascii_case(designation)
                || location
                    .get(..designation.len())
                    .is_some_and(|prefix| prefix.eq_ignore_ascii_case(designation))
                    && location
                        .get(designation.len()..)
                        .is_some_and(|suffix| suffix.starts_with('-'))
        })
        .max_by_key(|star| star.key.id.as_str().len())
        .and_then(|star| star.region.as_deref())
        .map(|region| region.trim().to_ascii_lowercase())
}

fn stable_hash(value: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

async fn ensure_operation_accepted(operation: &replicant_client::Operation) -> AnyResult<()> {
    let outcome = operation.outcome().await?;
    if matches!(
        outcome.status,
        OperationStatus::Cancelled | OperationStatus::Rejected | OperationStatus::Failed
    ) {
        return Err(app_error(
            io::ErrorKind::Other,
            format!(
                "operation {} ended as {:?}: {:?}",
                operation.id().as_str(),
                outcome.status,
                outcome.response
            ),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opaque_legacy_detection_does_not_match_readable_event_tags() {
        assert!(is_opaque_legacy_tag("evt-m:62d07388a13aa742"));
        assert!(!is_opaque_legacy_tag("evt-m:khuxkrixx-3-evt-008"));
    }

    #[test]
    fn support_role_tags_are_released_to_regional_stock() {
        assert!(terminal_support_asset(&["evt-role:cargo".to_owned()]));
        assert!(terminal_support_asset(&["evt-role:carrier".to_owned()]));
        assert!(terminal_support_asset(&["evt-role:beacon".to_owned()]));
        assert!(!terminal_support_asset(&["evt-role:payload".to_owned()]));
    }

    #[test]
    fn regional_stock_tags_are_bounded_and_deterministic() {
        let region = "An Extremely Long Region Name That Cannot Fit";
        assert_eq!(event_stock_tag(region), event_stock_tag(region));
        assert!(event_stock_tag(region).len() <= MAX_TAG_CHARACTERS);
        assert_eq!(event_stock_tag("Beta"), "evt-stock:beta");
    }
}
