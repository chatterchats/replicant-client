//! Migration of legacy UUID-scoped mission reservations into reusable system stock.
//!
//! Event missions already retain durable, readable event identities. Older
//! bootstrap, mining, and relay checkpoints used UUID-derived `*-m:<hash>`
//! reservations, which could strand otherwise reusable printed devices after a
//! workflow was replaced. This module reconstructs the old-to-new mapping from
//! durable workflow history and rewrites owned device tags without touching the
//! finer-grained batch, site, or role tags.

use std::{
    collections::{BTreeMap, BTreeSet},
    io,
};

use crate::{
    AnyResult,
    automation::{MiningDeployCheckpoint, RegionEstablishCheckpoint},
    bootstrap::BootstrapMission,
    mining::{MiningMission, is_opaque_mining_mission_tag, mining_mission_tag},
    workflows::RelayWorkflowCheckpoint,
};
use replicant_bootstrap_planner::mission_tag as bootstrap_mission_tag;
use replicant_client::{Client, OperationStatus, raw};
use replicant_workflow::WorkflowRepository;
use serde::Serialize;
use tracing::{info, warn};

/// Summary of one daemon-side legacy mission-tag migration pass.
#[derive(Clone, Debug, Default, Serialize)]
pub struct MissionTagReconcileReport {
    /// Number of owned devices inspected.
    pub scanned_devices: usize,
    /// Number of legacy tag identities reconstructed from workflow history.
    pub mapped_legacy_tags: usize,
    /// Number of devices whose tags were rewritten.
    pub migrated_devices: usize,
    /// Number of tagged devices skipped because one old tag mapped ambiguously.
    pub ambiguous_devices: usize,
}

/// Rewrites UUID-derived bootstrap/mining/relay mission tags using durable
/// workflow history.
///
/// The migration is idempotent. It only removes legacy `*-m:<16 hex>` tags for
/// which the runtime can recover an exact system target from a persisted
/// checkpoint. Batch/site/role tags are intentionally preserved.
pub async fn reconcile_legacy_mission_tags(
    client: &Client,
    repository: &WorkflowRepository,
) -> AnyResult<MissionTagReconcileReport> {
    client.ready().await?;
    let mappings = legacy_tag_mappings(repository)?;
    let handles = client.devices().find().owned().collect().await?;
    let mut report = MissionTagReconcileReport {
        scanned_devices: handles.len(),
        mapped_legacy_tags: mappings.len(),
        ..MissionTagReconcileReport::default()
    };

    for handle in handles {
        let snapshot = handle.snapshot().await?;
        let old_tags = snapshot
            .tags
            .iter()
            .filter(|tag| mappings.contains_key(*tag))
            .cloned()
            .collect::<Vec<_>>();
        if old_tags.is_empty() {
            continue;
        }

        let targets = old_tags
            .iter()
            .flat_map(|tag| mappings.get(tag).into_iter().flatten().cloned())
            .collect::<BTreeSet<_>>();
        if targets.len() != 1 {
            report.ambiguous_devices += 1;
            warn!(
                device = %handle.id().as_str(),
                old_tags = ?old_tags,
                targets = ?targets,
                "legacy mission tags map to multiple systems; leaving device unchanged"
            );
            continue;
        }

        let target = targets.into_iter().next().expect("one target");
        let removable = old_tags
            .into_iter()
            .filter(|tag| {
                mappings
                    .get(tag)
                    .is_some_and(|targets| targets.contains(&target))
            })
            .collect::<Vec<_>>();
        let add_tags = (!snapshot.tags.contains(&target)).then_some(vec![target.clone()]);
        let operation = handle
            .configure(raw::devices::DeviceConfiguration {
                add_tags,
                remove_tags: Some(removable.clone()),
                ..Default::default()
            })
            .await?;
        ensure_operation_accepted(&operation).await?;
        report.migrated_devices += 1;
        info!(
            device = %handle.id().as_str(),
            old_tags = ?removable,
            new_tag = %target,
            "migrated legacy mission reservation"
        );
    }

    Ok(report)
}

fn legacy_tag_mappings(
    repository: &WorkflowRepository,
) -> AnyResult<BTreeMap<String, BTreeSet<String>>> {
    let mut mappings = BTreeMap::<String, BTreeSet<String>>::new();
    for workflow in repository.list()? {
        match workflow.kind.as_str() {
            "mining.deploy" | "mining.campaign" => {
                let checkpoint = match workflow.checkpoint::<MiningDeployCheckpoint>() {
                    Ok(checkpoint) => checkpoint,
                    Err(error) => {
                        warn!(
                            workflow_id = %workflow.id,
                            error = %error,
                            "skipping unreadable mining checkpoint during mission-tag migration"
                        );
                        continue;
                    }
                };
                if let Some(plan_json) = checkpoint.plan_json
                    && let Ok(plan) = serde_json::from_str::<MiningMission>(&plan_json)
                {
                    let target = mining_mission_tag(&plan.hub_location);
                    add_mapping(&mut mappings, &plan.mission_tag, &target, "mine-m:");
                    for tag in &plan.legacy_mission_tags {
                        add_mapping(&mut mappings, tag, &target, "mine-m:");
                    }
                }
            }
            "region.establish" => {
                let checkpoint = match workflow.checkpoint::<RegionEstablishCheckpoint>() {
                    Ok(checkpoint) => checkpoint,
                    Err(error) => {
                        warn!(
                            workflow_id = %workflow.id,
                            error = %error,
                            "skipping unreadable bootstrap checkpoint during mission-tag migration"
                        );
                        continue;
                    }
                };
                if let Some(mission_json) = checkpoint.mission_json
                    && let Ok(mission) = serde_json::from_str::<BootstrapMission>(&mission_json)
                {
                    let target = bootstrap_mission_tag(&mission.landing_star);
                    add_mapping(&mut mappings, &mission.mission_tag, &target, "boot-m:");
                    for tag in &mission.legacy_mission_tags {
                        add_mapping(&mut mappings, tag, &target, "boot-m:");
                    }
                }
            }
            "relay.expansion" => {
                let checkpoint = match workflow.checkpoint::<RelayWorkflowCheckpoint>() {
                    Ok(checkpoint) => checkpoint,
                    Err(error) => {
                        warn!(
                            workflow_id = %workflow.id,
                            error = %error,
                            "skipping unreadable relay checkpoint during mission-tag migration"
                        );
                        continue;
                    }
                };
                if let Some(state) = checkpoint.state {
                    let (target, legacy) = state.mission_tag_migration();
                    for tag in legacy {
                        add_mapping(&mut mappings, &tag, &target, "relay-m:");
                    }
                }
            }
            _ => {}
        }
    }
    Ok(mappings)
}

fn add_mapping(
    mappings: &mut BTreeMap<String, BTreeSet<String>>,
    legacy: &str,
    target: &str,
    prefix: &str,
) {
    if legacy == target || !legacy.starts_with(prefix) || !opaque_suffix(legacy, prefix) {
        return;
    }
    if prefix == "mine-m:" && !is_opaque_mining_mission_tag(legacy) {
        return;
    }
    mappings
        .entry(legacy.to_owned())
        .or_default()
        .insert(target.to_owned());
}

fn opaque_suffix(tag: &str, prefix: &str) -> bool {
    tag.strip_prefix(prefix).is_some_and(|suffix| {
        suffix.len() == 16 && suffix.bytes().all(|byte| byte.is_ascii_hexdigit())
    })
}

async fn ensure_operation_accepted(operation: &replicant_client::Operation) -> AnyResult<()> {
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
    fn opaque_mission_tags_are_detected_without_matching_readable_tags() {
        assert!(opaque_suffix("boot-m:0123456789abcdef", "boot-m:"));
        assert!(opaque_suffix("relay-m:0123456789abcdef", "relay-m:"));
        assert!(!opaque_suffix("boot-m:scepturum", "boot-m:"));
    }
}
