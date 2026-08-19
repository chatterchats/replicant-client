//! Durable shared prerequisites raised by Automation Director goals.
//!
//! Standing goals describe desired empire state. When a goal cannot advance
//! because another capability is missing, it raises a requirement here instead
//! of calling another planner directly. Requirements have deterministic
//! identities, merge requesters across goals, survive restart, and can later be
//! resolved by dedicated subsystems without coupling the original planners.

use std::collections::{BTreeMap, BTreeSet};

use replicant_protocol::{
    DirectorRequirementKind, DirectorRequirementRequester, DirectorRequirementStatus,
    DirectorRequirementSummary, WorkflowId as ProtocolWorkflowId,
};
use replicant_transport::DeviceRequest;
use replicant_workflow::{WorkflowId, WorkflowRepository};
use serde::{Deserialize, Serialize};

use crate::ApplicationError;

/// Runtime-document namespace for durable Director requirements.
pub const REQUIREMENT_NS: &str = "director.requirement";

/// One semantic prerequisite that can be shared by multiple standing goals.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum DirectorRequirement {
    /// An account-wide printable device blueprint must be unlocked.
    Blueprint {
        /// Device type whose blueprint is required.
        device_type: String,
    },
    /// A mixed resource/device manifest must reach one destination.
    Logistics {
        /// Optional regional ownership hint.
        region: Option<String>,
        /// Origin location or system scope understood by transport planning.
        origin_scope: String,
        /// Exact destination location.
        destination: String,
        /// Resource quantities required at the destination.
        #[serde(default)]
        resources: BTreeMap<String, i64>,
        /// Device quantities required at the destination.
        #[serde(default)]
        devices: Vec<DeviceRequest>,
    },
    /// One region needs additional useful worker capacity.
    WorkerCapacity {
        /// Canonical operating region.
        region: String,
        /// Number of additional simultaneously useful workers required.
        count: usize,
        /// Optional role affinity explaining the pressure source.
        affinity: Option<String>,
    },
    /// One strategic system must become reachable through the FTL mesh.
    Connectivity {
        /// Canonical operating region.
        region: String,
        /// System that must become connected.
        target_system: String,
    },
}

impl DirectorRequirement {
    /// Returns the requirement category used by protocol/UI summaries.
    #[must_use]
    pub const fn kind(&self) -> DirectorRequirementKind {
        match self {
            Self::Blueprint { .. } => DirectorRequirementKind::Blueprint,
            Self::Logistics { .. } => DirectorRequirementKind::Logistics,
            Self::WorkerCapacity { .. } => DirectorRequirementKind::WorkerCapacity,
            Self::Connectivity { .. } => DirectorRequirementKind::Connectivity,
        }
    }

    /// Returns a deterministic identity used for durable deduplication.
    pub fn identity(&self) -> Result<String, ApplicationError> {
        Ok(match self {
            Self::Blueprint { device_type } => {
                format!("blueprint:{}", canonical_component(device_type))
            }
            Self::WorkerCapacity {
                region, affinity, ..
            } => format!(
                "worker:{}:{}",
                canonical_component(region),
                affinity
                    .as_deref()
                    .map(canonical_component)
                    .unwrap_or_else(|| "general".to_owned())
            ),
            Self::Connectivity {
                region,
                target_system,
            } => format!(
                "connectivity:{}:{}",
                canonical_component(region),
                canonical_component(target_system)
            ),
            Self::Logistics { .. } => {
                let normalized = self.normalized();
                let encoded = serde_json::to_vec(&normalized)?;
                format!("logistics:{:016x}", stable_hash(&encoded))
            }
        })
    }

    fn normalized(&self) -> Self {
        let Self::Logistics {
            region,
            origin_scope,
            destination,
            resources,
            devices,
        } = self
        else {
            return self.clone();
        };
        let resources = resources
            .iter()
            .filter(|(_, quantity)| **quantity != 0)
            .map(|(resource, quantity)| (resource.clone(), *quantity))
            .collect();
        let mut device_quantities = BTreeMap::<String, i64>::new();
        for device in devices {
            *device_quantities
                .entry(device.device_type.clone())
                .or_default() += device.quantity;
        }
        let devices = device_quantities
            .into_iter()
            .filter(|(_, quantity)| *quantity != 0)
            .map(|(device_type, quantity)| DeviceRequest {
                quantity,
                device_type,
            })
            .collect();
        Self::Logistics {
            region: region.as_deref().map(canonical_component),
            origin_scope: origin_scope.trim().to_owned(),
            destination: destination.trim().to_owned(),
            resources,
            devices,
        }
    }

    fn region(&self) -> Option<&str> {
        match self {
            Self::Logistics { region, .. } => region.as_deref(),
            Self::WorkerCapacity { region, .. } | Self::Connectivity { region, .. } => Some(region),
            Self::Blueprint { .. } => None,
        }
    }

    fn target(&self) -> String {
        match self {
            Self::Blueprint { device_type } => device_type.clone(),
            Self::Logistics {
                origin_scope,
                destination,
                resources,
                devices,
                ..
            } => format!(
                "{origin_scope} -> {destination} ({} resource type(s), {} device request(s))",
                resources.len(),
                devices.len()
            ),
            Self::WorkerCapacity {
                region,
                count,
                affinity,
            } => match affinity {
                Some(affinity) => format!("{region}: +{count} {affinity} worker(s)"),
                None => format!("{region}: +{count} worker(s)"),
            },
            Self::Connectivity { target_system, .. } => target_system.clone(),
        }
    }
}

/// One goal's reason for requesting a shared prerequisite.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct RequirementRequesterRecord {
    goal_id: String,
    reason: String,
    priority: u32,
    last_seen_at_ms: i64,
}

/// Durable requirement state stored independently from the Director snapshot.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct RequirementRecord {
    id: String,
    requirement: DirectorRequirement,
    status: DirectorRequirementStatus,
    #[serde(default)]
    requesters: Vec<RequirementRequesterRecord>,
    #[serde(default)]
    active_workflows: Vec<WorkflowId>,
    first_raised_at_ms: i64,
    last_raised_at_ms: i64,
    resolved_at_ms: Option<i64>,
}

/// In-memory reconciliation view of the durable requirement graph.
pub struct DirectorRequirementGraph {
    records: BTreeMap<String, RequirementRecord>,
    seen_requirements: BTreeSet<String>,
    seen_requesters: BTreeSet<(String, String)>,
    now: i64,
}

impl DirectorRequirementGraph {
    /// Loads every durable requirement before one Director reconciliation.
    pub fn load(repository: &WorkflowRepository, now: i64) -> Result<Self, ApplicationError> {
        let records = repository
            .list_documents(REQUIREMENT_NS)?
            .into_iter()
            .map(|(key, value, _)| {
                let record = serde_json::from_value::<RequirementRecord>(value)?;
                Ok((key, record))
            })
            .collect::<Result<BTreeMap<_, _>, ApplicationError>>()?;
        Ok(Self {
            records,
            seen_requirements: BTreeSet::new(),
            seen_requesters: BTreeSet::new(),
            now,
        })
    }

    /// Raises or refreshes one semantic requirement for one goal.
    ///
    /// Requesters with the same deterministic requirement identity are merged;
    /// the highest requester priority becomes the effective requirement priority.
    pub fn raise(
        &mut self,
        requirement: DirectorRequirement,
        goal_id: &str,
        reason: impl Into<String>,
        priority: u32,
    ) -> Result<String, ApplicationError> {
        let requirement = requirement.normalized();
        let id = requirement.identity()?;
        let reason = reason.into();
        let first_raise_this_pass = self.seen_requirements.insert(id.clone());
        let record = self
            .records
            .entry(id.clone())
            .or_insert_with(|| RequirementRecord {
                id: id.clone(),
                requirement: requirement.clone(),
                status: DirectorRequirementStatus::Pending,
                requesters: Vec::new(),
                active_workflows: Vec::new(),
                first_raised_at_ms: self.now,
                last_raised_at_ms: self.now,
                resolved_at_ms: None,
            });
        record.requirement = if first_raise_this_pass {
            requirement
        } else {
            merge_requirement(&record.requirement, &requirement)
        };
        record.last_raised_at_ms = self.now;
        if record.status == DirectorRequirementStatus::Satisfied {
            record.status = DirectorRequirementStatus::Pending;
            record.resolved_at_ms = None;
        }
        if let Some(requester) = record
            .requesters
            .iter_mut()
            .find(|requester| requester.goal_id == goal_id)
        {
            requester.reason = reason;
            requester.priority = priority;
            requester.last_seen_at_ms = self.now;
        } else {
            record.requesters.push(RequirementRequesterRecord {
                goal_id: goal_id.to_owned(),
                reason,
                priority,
                last_seen_at_ms: self.now,
            });
        }
        self.seen_requesters
            .insert((id.clone(), goal_id.to_owned()));
        tracing::debug!(
            event = "director.requirement.raised",
            requirement_id = %id,
            goal_id,
            kind = ?record.requirement.kind(),
            priority,
            "Director requirement raised"
        );
        Ok(id)
    }

    /// Returns Blueprint requirements raised by current goal blockers, keyed
    /// by device type with their effective requester priority.
    #[must_use]
    pub fn current_blueprint_priorities(&self) -> BTreeMap<String, u32> {
        let mut priorities: BTreeMap<String, u32> = BTreeMap::new();
        for record in self.records.values() {
            let requested_now = record.requesters.iter().any(|requester| {
                self.seen_requesters
                    .contains(&(record.id.clone(), requester.goal_id.clone()))
            });
            if !requested_now
                || !matches!(
                    record.status,
                    DirectorRequirementStatus::Pending
                        | DirectorRequirementStatus::Active
                        | DirectorRequirementStatus::Blocked
                )
            {
                continue;
            }
            let DirectorRequirement::Blueprint { device_type } = &record.requirement else {
                continue;
            };
            let priority = record
                .requesters
                .iter()
                .filter(|requester| {
                    self.seen_requesters
                        .contains(&(record.id.clone(), requester.goal_id.clone()))
                })
                .map(|requester| requester.priority)
                .max()
                .unwrap_or_default();
            priorities
                .entry(device_type.clone())
                .and_modify(|existing| *existing = (*existing).max(priority))
                .or_insert(priority);
        }
        priorities
    }

    /// Associates resolver work with a requirement raised during this pass.
    /// This is purely explanatory/deduplication state; satisfaction still
    /// comes from observing that requesting goals no longer raise it.
    pub fn attach_workflow(
        &mut self,
        requirement_id: &str,
        workflow_id: WorkflowId,
    ) -> Result<(), ApplicationError> {
        let Some(record) = self.records.get_mut(requirement_id) else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("unknown Director requirement {requirement_id}"),
            )
            .into());
        };
        if !record.active_workflows.contains(&workflow_id) {
            record.active_workflows.push(workflow_id);
            record
                .active_workflows
                .sort_by_key(|workflow_id| workflow_id.to_string());
        }
        record.status = DirectorRequirementStatus::Active;
        Ok(())
    }

    /// Returns current worker pressure grouped by region.
    #[must_use]
    pub fn worker_demand_by_region(&self) -> BTreeMap<String, usize> {
        let mut demand = BTreeMap::new();
        for record in self.records.values() {
            let requested_now = record.requesters.iter().any(|requester| {
                self.seen_requesters
                    .contains(&(record.id.clone(), requester.goal_id.clone()))
            });
            if !requested_now
                || !matches!(
                    record.status,
                    DirectorRequirementStatus::Pending
                        | DirectorRequirementStatus::Active
                        | DirectorRequirementStatus::Blocked
                )
            {
                continue;
            }
            if let DirectorRequirement::WorkerCapacity { region, count, .. } = &record.requirement {
                *demand.entry(region.clone()).or_default() += *count;
            }
        }
        demand
    }

    /// Persists the successful reconciliation and resolves requirements that no
    /// longer have any requesting goal in this pass.
    pub fn persist(
        mut self,
        repository: &WorkflowRepository,
    ) -> Result<Vec<DirectorRequirementSummary>, ApplicationError> {
        for record in self.records.values_mut() {
            record.requesters.retain(|requester| {
                self.seen_requesters
                    .contains(&(record.id.clone(), requester.goal_id.clone()))
            });
            if record.requesters.is_empty() {
                if record.status != DirectorRequirementStatus::Satisfied {
                    tracing::debug!(
                        event = "director.requirement.satisfied",
                        requirement_id = %record.id,
                        kind = ?record.requirement.kind(),
                        "Director requirement satisfied because no current goal remains blocked on it"
                    );
                }
                record.status = DirectorRequirementStatus::Satisfied;
                record.active_workflows.clear();
                record.resolved_at_ms = Some(self.now);
            }
            repository.put_document(REQUIREMENT_NS, &record.id, record)?;
        }
        Ok(self.summaries())
    }

    /// Builds frontend-safe summaries from the current in-memory graph.
    #[must_use]
    pub fn summaries(&self) -> Vec<DirectorRequirementSummary> {
        records_to_summaries(self.records.values())
    }
}

/// Reads durable requirement summaries without running a Director reconciliation.
pub fn load_requirement_summaries(
    repository: &WorkflowRepository,
) -> Result<Vec<DirectorRequirementSummary>, ApplicationError> {
    let records = repository
        .list_documents(REQUIREMENT_NS)?
        .into_iter()
        .map(|(_, value, _)| serde_json::from_value::<RequirementRecord>(value))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(records_to_summaries(records.iter()))
}

fn records_to_summaries<'a>(
    records: impl IntoIterator<Item = &'a RequirementRecord>,
) -> Vec<DirectorRequirementSummary> {
    let mut summaries = records
        .into_iter()
        .map(|record| {
            let mut requesters = record
                .requesters
                .iter()
                .map(|requester| DirectorRequirementRequester {
                    goal_id: requester.goal_id.clone(),
                    reason: requester.reason.clone(),
                    priority: requester.priority,
                })
                .collect::<Vec<_>>();
            requesters.sort_by(|left, right| {
                right
                    .priority
                    .cmp(&left.priority)
                    .then_with(|| left.goal_id.cmp(&right.goal_id))
            });
            DirectorRequirementSummary {
                id: record.id.clone(),
                kind: record.requirement.kind(),
                status: record.status,
                region: record.requirement.region().map(str::to_owned),
                target: record.requirement.target(),
                priority: requesters
                    .iter()
                    .map(|requester| requester.priority)
                    .max()
                    .unwrap_or_default(),
                requesters,
                active_workflows: record
                    .active_workflows
                    .iter()
                    .map(|id| ProtocolWorkflowId(id.to_string()))
                    .collect(),
            }
        })
        .collect::<Vec<_>>();
    summaries.sort_by(|left, right| {
        right
            .priority
            .cmp(&left.priority)
            .then_with(|| left.id.cmp(&right.id))
    });
    summaries
}

fn merge_requirement(
    existing: &DirectorRequirement,
    incoming: &DirectorRequirement,
) -> DirectorRequirement {
    match (existing, incoming) {
        (
            DirectorRequirement::WorkerCapacity {
                region,
                count: existing_count,
                affinity,
            },
            DirectorRequirement::WorkerCapacity {
                count: incoming_count,
                ..
            },
        ) => DirectorRequirement::WorkerCapacity {
            region: region.clone(),
            count: (*existing_count).max(*incoming_count),
            affinity: affinity.clone(),
        },
        _ => incoming.clone(),
    }
}

fn canonical_component(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn stable_hash(bytes: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
    bytes.iter().fold(FNV_OFFSET, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(FNV_PRIME)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repository() -> WorkflowRepository {
        WorkflowRepository::open_in_memory().expect("open runtime repository")
    }

    #[test]
    fn duplicate_blueprint_requests_merge_requesters_and_priority() {
        let repository = repository();
        let mut graph = DirectorRequirementGraph::load(&repository, 100).expect("load graph");
        let requirement = DirectorRequirement::Blueprint {
            device_type: "deep_space_relay_station".to_owned(),
        };
        let first = graph
            .raise(
                requirement.clone(),
                "expand_ftl_network:gamma",
                "Gamma FTL expansion needs a DSR blueprint",
                700,
            )
            .expect("raise first request");
        let second = graph
            .raise(
                requirement,
                "establish_regions",
                "Bootstrap planning also needs the relay capability",
                900,
            )
            .expect("raise duplicate request");
        assert_eq!(first, second);
        let summaries = graph.summaries();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].requesters.len(), 2);
        assert_eq!(summaries[0].priority, 900);
    }

    #[test]
    fn current_blueprint_priorities_only_include_requirements_refreshed_this_pass() {
        let repository = repository();
        let mut graph = DirectorRequirementGraph::load(&repository, 100).expect("load graph");
        graph
            .raise(
                DirectorRequirement::Blueprint {
                    device_type: "galactic_observatory".to_owned(),
                },
                "expand_star_catalogue",
                "catalogue blocker",
                400,
            )
            .expect("raise blueprint");
        assert_eq!(
            graph
                .current_blueprint_priorities()
                .get("galactic_observatory"),
            Some(&400)
        );
        graph.persist(&repository).expect("persist graph");

        let graph = DirectorRequirementGraph::load(&repository, 200).expect("reload graph");
        assert!(graph.current_blueprint_priorities().is_empty());
    }

    #[test]
    fn logistics_identity_is_order_independent_for_device_requests() {
        let left = DirectorRequirement::Logistics {
            region: Some("alpha".to_owned()),
            origin_scope: "SCEPTURUM".to_owned(),
            destination: "SCEPTURUM-7-L4".to_owned(),
            resources: BTreeMap::from([("carbon".to_owned(), 80), ("structural".to_owned(), 400)]),
            devices: vec![
                DeviceRequest {
                    quantity: 2,
                    device_type: "survey_drone".to_owned(),
                },
                DeviceRequest {
                    quantity: 1,
                    device_type: "maintenance_drone".to_owned(),
                },
            ],
        };
        let mut right = left.clone();
        if let DirectorRequirement::Logistics { devices, .. } = &mut right {
            devices.reverse();
        }
        assert_eq!(
            left.identity().expect("left identity"),
            right.identity().expect("right identity")
        );
    }

    #[test]
    fn worker_capacity_uses_maximum_for_same_affinity_and_sums_distinct_roles() {
        let repository = repository();
        let mut graph = DirectorRequirementGraph::load(&repository, 100).expect("load graph");
        graph
            .raise(
                DirectorRequirement::WorkerCapacity {
                    region: "beta".to_owned(),
                    count: 1,
                    affinity: Some("catalogue".to_owned()),
                },
                "enhance_star_catalogue:beta",
                "survey backlog",
                500,
            )
            .expect("raise catalogue");
        graph
            .raise(
                DirectorRequirement::WorkerCapacity {
                    region: "beta".to_owned(),
                    count: 3,
                    affinity: Some("catalogue".to_owned()),
                },
                "another_catalogue_goal",
                "larger survey backlog",
                550,
            )
            .expect("raise duplicate catalogue");
        graph
            .raise(
                DirectorRequirement::WorkerCapacity {
                    region: "beta".to_owned(),
                    count: 1,
                    affinity: Some("events".to_owned()),
                },
                "event_completion:beta",
                "event runner unavailable",
                700,
            )
            .expect("raise event");
        assert_eq!(graph.worker_demand_by_region().get("beta"), Some(&4));
    }

    #[test]
    fn worker_capacity_can_decrease_on_a_later_reconciliation() {
        let repository = repository();
        let mut graph = DirectorRequirementGraph::load(&repository, 100).expect("load graph");
        graph
            .raise(
                DirectorRequirement::WorkerCapacity {
                    region: "beta".to_owned(),
                    count: 3,
                    affinity: Some("catalogue".to_owned()),
                },
                "enhance_star_catalogue:beta",
                "large survey backlog",
                500,
            )
            .expect("raise initial demand");
        graph.persist(&repository).expect("persist initial demand");

        let mut graph = DirectorRequirementGraph::load(&repository, 200).expect("reload graph");
        graph
            .raise(
                DirectorRequirement::WorkerCapacity {
                    region: "beta".to_owned(),
                    count: 1,
                    affinity: Some("catalogue".to_owned()),
                },
                "enhance_star_catalogue:beta",
                "smaller survey backlog",
                500,
            )
            .expect("refresh smaller demand");
        assert_eq!(graph.worker_demand_by_region().get("beta"), Some(&1));
    }

    #[test]
    fn requirements_survive_restart_and_resolve_when_not_refreshed() {
        let repository = repository();
        let mut graph = DirectorRequirementGraph::load(&repository, 100).expect("load graph");
        graph
            .raise(
                DirectorRequirement::Connectivity {
                    region: "gamma".to_owned(),
                    target_system: "GAMMA-CAPITAL".to_owned(),
                },
                "expand_ftl_network:gamma",
                "Regional capital is disconnected",
                800,
            )
            .expect("raise connectivity");
        graph.persist(&repository).expect("persist graph");

        let loaded = DirectorRequirementGraph::load(&repository, 200).expect("reload graph");
        assert_eq!(loaded.summaries().len(), 1);
        loaded
            .persist(&repository)
            .expect("persist unrefreshed graph");

        let summaries = load_requirement_summaries(&repository).expect("load summaries");
        assert_eq!(summaries[0].status, DirectorRequirementStatus::Satisfied);
        assert!(summaries[0].requesters.is_empty());
    }
}
