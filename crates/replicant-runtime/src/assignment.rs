use std::{collections::BTreeMap, sync::Arc};

use replicant_client::{
    Client, ManagedStateSnapshot,
    domain::{Device, DeviceKey, InventoryOwner, Replicant, Star},
};
use replicant_workflow::{
    AllocationCandidate, AllocationId, AllocationLocation, AllocationSet, ReplacementOutcome,
    RepositoryError, ResourceKey, WorkItemId, WorkflowRepository,
};

use crate::worker_state::{OPERATIONAL_REGIONAL_WORKER_CAPABILITY, classify_regional_worker};

/// Runtime failure while assigning resources to durable work.
#[derive(Debug, thiserror::Error)]
pub enum AssignmentError {
    /// Durable allocation failed.
    #[error(transparent)]
    Repository(#[from] RepositoryError),
    /// Managed candidate discovery failed.
    #[error(transparent)]
    Managed(#[from] replicant_client::Error),
}

/// Runtime adapter between managed candidate discovery and atomic workflow allocation.
#[derive(Clone)]
pub struct ResourceBroker {
    repository: Arc<WorkflowRepository>,
    client: Option<Client>,
}

impl ResourceBroker {
    /// Creates a broker for explicitly supplied candidate observations.
    #[must_use]
    pub fn new(repository: Arc<WorkflowRepository>) -> Self {
        Self {
            repository,
            client: None,
        }
    }

    /// Creates a broker that discovers candidates from one managed client snapshot.
    #[must_use]
    pub fn with_managed_client(repository: Arc<WorkflowRepository>, client: Client) -> Self {
        Self {
            repository,
            client: Some(client),
        }
    }

    /// Discovers exclusive, inventory, stow, and attachment pools from committed managed state.
    pub fn discover_candidates(&self) -> Result<Vec<AllocationCandidate>, AssignmentError> {
        let client = self
            .client
            .as_ref()
            .ok_or(replicant_client::Error::Closed)?;
        let ManagedStateSnapshot {
            revision,
            owned_devices: devices,
            owned_replicants,
            inventories,
            ..
        } = client.state().snapshot()?;
        let observed_at_ms = unix_millis();
        let hosted_capabilities = hosted_device_capabilities(&devices);
        let catalogue = client.galaxy().catalogue();
        let mut candidates = Vec::new();
        for replicant in owned_replicants {
            let vessel = operational_vessel_for(&replicant, &devices);
            let mut capabilities = capabilities_for_replicant(&replicant, &hosted_capabilities);
            if classify_regional_worker(&replicant, vessel, None, None, None, false)
                .is_operational()
            {
                capabilities.push(OPERATIONAL_REGIONAL_WORKER_CAPABILITY.to_owned());
                capabilities.sort();
                capabilities.dedup();
            }
            let location = replicant
                .location
                .as_ref()
                .map(|location| allocation_location(location.id.as_str(), &catalogue));
            candidates.push(AllocationCandidate {
                resource: ResourceKey::Replicant(replicant.key.id.to_string()),
                kind: "replicant".into(),
                capabilities,
                location,
                available_quantity: 1,
                observed_revision: revision,
                observed_at_ms,
            });
        }
        for device in devices {
            let device_code = device.key.id.to_string();
            let mut capabilities = json_string_values(&device.features);
            if let Some(device_type) = &device.device_type {
                capabilities.extend(json_string_values(std::slice::from_ref(device_type)));
            }
            capabilities.sort();
            capabilities.dedup();
            let location = device
                .location
                .as_ref()
                .map(|location| allocation_location(location.id.as_str(), &catalogue));
            candidates.push(AllocationCandidate {
                resource: ResourceKey::Device(device_code.clone()),
                kind: "device".into(),
                capabilities: capabilities.clone(),
                location: location.clone(),
                available_quantity: 1,
                observed_revision: revision,
                observed_at_ms,
            });
            if device.device_type.as_ref().is_some_and(|kind| {
                json_string_values(std::slice::from_ref(kind))
                    .iter()
                    .any(|value| value == "autofactory")
            }) {
                candidates.push(AllocationCandidate {
                    resource: ResourceKey::Autofactory(device_code.clone()),
                    kind: "autofactory".into(),
                    capabilities: capabilities.clone(),
                    location: location.clone(),
                    available_quantity: 1,
                    observed_revision: revision,
                    observed_at_ms,
                });
            }
            let free_stow = device.free_stow_capacity();
            if free_stow > 0 {
                candidates.push(AllocationCandidate {
                    resource: ResourceKey::Namespaced {
                        namespace: "stow".into(),
                        key: device_code.clone(),
                    },
                    kind: "stow".into(),
                    capabilities: Vec::new(),
                    location: location.clone(),
                    available_quantity: u64::try_from(free_stow).unwrap_or(0),
                    observed_revision: revision,
                    observed_at_ms,
                });
            }
            let free_attach = free_attach_capacity(&device);
            if free_attach > 0 {
                candidates.push(AllocationCandidate {
                    resource: ResourceKey::Namespaced {
                        namespace: "attach".into(),
                        key: device_code,
                    },
                    kind: "attach".into(),
                    capabilities: Vec::new(),
                    location,
                    available_quantity: u64::try_from(free_attach).unwrap_or(0),
                    observed_revision: revision,
                    observed_at_ms,
                });
            }
        }
        for inventory in inventories {
            let owner = match inventory.owner {
                InventoryOwner::Account(id) => format!("account:{id}"),
                InventoryOwner::Replicant(key) => format!("replicant:{}", key.id),
                InventoryOwner::Location(key) => format!("location:{}", key.id),
                _ => continue,
            };
            let location = inventory.location.map(|location| AllocationLocation {
                designation: Some(location.id.to_string()),
                ..AllocationLocation::default()
            });
            for item in inventory.items {
                if item.quantity <= 0 {
                    continue;
                }
                candidates.push(AllocationCandidate {
                    resource: ResourceKey::Namespaced {
                        namespace: "inventory".into(),
                        key: format!("{owner}:{}", item.resource),
                    },
                    kind: "material".into(),
                    capabilities: vec![item.resource],
                    location: location.clone(),
                    available_quantity: u64::try_from(item.quantity).unwrap_or(0),
                    observed_revision: revision,
                    observed_at_ms,
                });
            }
        }
        candidates.sort_by_key(|candidate| {
            serde_json::to_string(&candidate.resource).unwrap_or_default()
        });
        Ok(candidates)
    }

    /// Discovers candidates and allocates them in one broker call.
    pub fn allocate_discovered(
        &self,
        item_id: WorkItemId,
        expected_revision: u64,
    ) -> Result<AllocationSet, AssignmentError> {
        let candidates = self.discover_candidates()?;
        self.allocate(item_id, expected_revision, &candidates)
    }

    /// Atomically allocates a caller's managed-state candidate observation.
    ///
    /// Executor adapters discover candidates from one managed snapshot, including
    /// precomputed range facts, then pass the entire observation here. Identity,
    /// quantity, stale-observation, and legacy exact-claim exclusion are enforced
    /// by the repository's single immediate transaction.
    pub fn allocate(
        &self,
        item_id: WorkItemId,
        expected_revision: u64,
        candidates: &[AllocationCandidate],
    ) -> Result<AllocationSet, AssignmentError> {
        self.repository
            .allocate_requirements(item_id, expected_revision, candidates)
            .map_err(Into::into)
    }

    /// Atomically allocates candidates while requiring selected requirement
    /// pairs to share the same underlying resource identity.
    pub fn allocate_with_affinity(
        &self,
        item_id: WorkItemId,
        expected_revision: u64,
        candidates: &[AllocationCandidate],
        affinities: &[(&str, &str)],
    ) -> Result<AllocationSet, AssignmentError> {
        self.repository
            .allocate_requirements_with_affinity(item_id, expected_revision, candidates, affinities)
            .map_err(Into::into)
    }

    /// Atomically allocates candidates with runtime-only affinity and compatibility policy.
    ///
    /// Ignored requirement keys remain in an immutable durable work-item schema, but are not
    /// allocated. This lets runtime fixes retire obsolete requirements without invalidating active
    /// work items created by an older version.
    pub fn allocate_with_policy(
        &self,
        item_id: WorkItemId,
        expected_revision: u64,
        candidates: &[AllocationCandidate],
        affinities: &[(&str, &str)],
        ignored_requirement_keys: &[&str],
    ) -> Result<AllocationSet, AssignmentError> {
        self.repository
            .allocate_requirements_with_policy(
                item_id,
                expected_revision,
                candidates,
                affinities,
                ignored_requirement_keys,
            )
            .map_err(Into::into)
    }

    /// Replaces a resource proven permanently missing using current managed candidates.
    pub fn replace_dead_allocation(
        &self,
        item_id: WorkItemId,
        allocation_id: AllocationId,
    ) -> Result<ReplacementOutcome, AssignmentError> {
        let candidates = self.discover_candidates()?;
        self.repository
            .replace_dead_allocation(item_id, allocation_id, &candidates, unix_millis())
            .map_err(Into::into)
    }

    /// Replaces a missing allocation from caller-annotated current candidates.
    ///
    /// Campaign adapters use this form when requirement scope facts such as
    /// Director region membership are not intrinsic managed-state fields.
    pub fn replace_dead_allocation_from(
        &self,
        item_id: WorkItemId,
        allocation_id: AllocationId,
        candidates: &[AllocationCandidate],
    ) -> Result<ReplacementOutcome, AssignmentError> {
        self.repository
            .replace_dead_allocation(item_id, allocation_id, candidates, unix_millis())
            .map_err(Into::into)
    }

    /// Replaces a missing allocation while preserving requirement affinity.
    pub fn replace_dead_allocation_from_with_affinity(
        &self,
        item_id: WorkItemId,
        allocation_id: AllocationId,
        candidates: &[AllocationCandidate],
        affinities: &[(&str, &str)],
    ) -> Result<ReplacementOutcome, AssignmentError> {
        self.repository
            .replace_dead_allocation_with_affinity(
                item_id,
                allocation_id,
                candidates,
                unix_millis(),
                affinities,
            )
            .map_err(Into::into)
    }
}

fn allocation_location(designation: &str, catalogue: &[Star]) -> AllocationLocation {
    let star = catalogue
        .iter()
        .filter(|star| {
            let system = star.key.id.as_str();
            designation == system
                || designation
                    .strip_prefix(system)
                    .is_some_and(|suffix| suffix.starts_with('-'))
        })
        .max_by_key(|star| star.key.id.as_str().len());
    AllocationLocation {
        region: star.and_then(|star| star.region.clone()),
        system: star.map(|star| star.key.id.as_str().to_owned()),
        designation: Some(designation.to_owned()),
        distances_ly: BTreeMap::new(),
    }
}

fn json_string_values<T: serde::Serialize>(values: &[T]) -> Vec<String> {
    values
        .iter()
        .filter_map(|value| serde_json::to_value(value).ok())
        .filter_map(|value| value.as_str().map(str::to_owned))
        .collect()
}

fn operational_vessel_for<'a>(replicant: &Replicant, devices: &'a [Device]) -> Option<&'a Device> {
    devices.iter().find(|device| {
        device
            .device_type
            .as_ref()
            .is_some_and(|kind| kind.as_str() == "racing_vessel")
            && (replicant.hosted_device.as_ref() == Some(&device.key)
                || device.relationships.hosting_replicant.as_ref() == Some(&replicant.key))
    })
}

fn hosted_device_capabilities(devices: &[Device]) -> BTreeMap<DeviceKey, Vec<String>> {
    devices
        .iter()
        .map(|device| {
            let mut capabilities = json_string_values(&device.features);
            capabilities.extend(json_string_values(&device.available_commands));
            capabilities.sort();
            capabilities.dedup();
            (device.key.clone(), capabilities)
        })
        .collect()
}

fn free_attach_capacity(device: &Device) -> i64 {
    let attached = i64::try_from(device.relationships.attached_devices.len()).unwrap_or(i64::MAX);
    device
        .attach_capacity
        .unwrap_or_default()
        .saturating_sub(attached)
        .max(0)
}

fn capabilities_for_replicant(
    replicant: &Replicant,
    hosted_capabilities: &BTreeMap<DeviceKey, Vec<String>>,
) -> Vec<String> {
    replicant
        .hosted_device
        .as_ref()
        .and_then(|host| hosted_capabilities.get(host))
        .cloned()
        .unwrap_or_default()
}

fn unix_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use replicant_client::domain::{
        AccessScope, DeviceCommand, DeviceFeature, DeviceKey, DeviceRelationships, ReplicantKey,
    };
    use replicant_workflow::{
        NewWorkflow, RequirementScope, ResourceKey, ResourceRequirement, WorkItemSpec, WorkflowKind,
    };
    use serde_json::json;

    use super::*;

    fn managed_device(
        code: &str,
        features: &[&str],
        available_commands: &[&str],
    ) -> replicant_client::domain::Device {
        replicant_client::domain::Device {
            key: DeviceKey::live(code.into()),
            device_type: None,
            status: None,
            location: None,
            deployed_at: None,
            in_control_range: None,
            features: features
                .iter()
                .map(|feature| DeviceFeature::from(*feature))
                .collect(),
            available_commands: available_commands
                .iter()
                .map(|command| DeviceCommand::from(*command))
                .collect(),
            available_directives: Vec::new(),
            tags: Vec::new(),
            settings: Default::default(),
            relationships: DeviceRelationships::default(),
            cargo: Default::default(),
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

    fn managed_replicant(hosted_device: Option<&str>) -> replicant_client::domain::Replicant {
        replicant_client::domain::Replicant {
            key: ReplicantKey::live("R-1".into()),
            name: None,
            is_npc: None,
            status: None,
            location: None,
            hosted_device: hosted_device.map(|code| DeviceKey::live(code.into())),
            travel: None,
            private: None,
            access: AccessScope::Owned,
        }
    }

    fn projected_capabilities(
        replicant: &replicant_client::domain::Replicant,
        devices: &[replicant_client::domain::Device],
    ) -> Vec<String> {
        let hosted_capabilities = hosted_device_capabilities(devices);
        capabilities_for_replicant(replicant, &hosted_capabilities)
    }

    #[test]
    fn attachment_capacity_uses_current_attached_relationships() {
        let mut carrier = managed_device("CARRIER-1", &[], &[]);
        carrier.attach_capacity = Some(10);
        carrier.relationships.attached_devices = ["A", "B", "C"]
            .into_iter()
            .map(|code| DeviceKey::live(code.into()))
            .collect();

        assert_eq!(free_attach_capacity(&carrier), 7);
    }

    #[test]
    fn replicant_without_host_has_no_projected_capabilities() {
        let replicant = managed_replicant(None);
        let devices = [managed_device("HOST-1", &["scanning"], &["activate"])];

        assert!(projected_capabilities(&replicant, &devices).is_empty());
    }

    #[test]
    fn replicant_inherits_sorted_deduplicated_host_features_and_commands() {
        let replicant = managed_replicant(Some("HOST-1"));
        let devices = [managed_device(
            "HOST-1",
            &["travel", "scanning"],
            &["travel", "activate"],
        )];

        assert_eq!(
            projected_capabilities(&replicant, &devices),
            vec![
                "activate".to_owned(),
                "scanning".to_owned(),
                "travel".to_owned()
            ]
        );
    }

    #[test]
    fn stale_host_key_has_no_projected_capabilities() {
        let replicant = managed_replicant(Some("STALE-HOST"));
        let devices = [managed_device("CURRENT-HOST", &["scanning"], &["activate"])];

        assert!(projected_capabilities(&replicant, &devices).is_empty());
    }

    #[test]
    fn broker_allocates_capability_and_range_matched_candidate() {
        let repository =
            Arc::new(WorkflowRepository::open_in_memory().expect("open workflow repository"));
        let campaign = repository
            .create(NewWorkflow {
                kind: WorkflowKind::new("test.broker").expect("valid campaign kind"),
                schema_version: 1,
                config: json!({}),
                checkpoint: json!({}),
                current_step: None,
                parent_id: None,
            })
            .expect("create campaign");
        let requirements = vec![ResourceRequirement {
            key: "worker".into(),
            kind: "replicant".into(),
            capabilities: vec!["survey".into()],
            scope: RequirementScope::WithinLy {
                origin: "SOL".into(),
                range_ly: 5.0,
            },
            count: 1,
            quantity: 1,
        }];
        let item = repository
            .reconcile_work_items(
                campaign.id,
                &[WorkItemSpec {
                    workflow_id: campaign.id,
                    dedupe_key: "item".into(),
                    kind: WorkflowKind::new("test.broker-item").expect("valid item kind"),
                    sort_key: "item".into(),
                    payload_json: json!({}),
                    preconditions_json: json!([]),
                    requirements_json: serde_json::to_value(requirements)
                        .expect("encode requirements"),
                    deadline_at_ms: None,
                }],
                1,
            )
            .expect("reconcile item")
            .remove(0);
        let candidates = [
            AllocationCandidate {
                resource: ResourceKey::Replicant("OUT-OF-RANGE".into()),
                kind: "replicant".into(),
                capabilities: vec!["survey".into()],
                location: Some(replicant_workflow::AllocationLocation {
                    distances_ly: [("SOL".into(), 8.0)].into(),
                    ..replicant_workflow::AllocationLocation::default()
                }),
                available_quantity: 1,
                observed_revision: 1,
                observed_at_ms: 10,
            },
            AllocationCandidate {
                resource: ResourceKey::Replicant("IN-RANGE".into()),
                kind: "replicant".into(),
                capabilities: vec!["survey".into()],
                location: Some(replicant_workflow::AllocationLocation {
                    distances_ly: [("SOL".into(), 4.0)].into(),
                    ..replicant_workflow::AllocationLocation::default()
                }),
                available_quantity: 1,
                observed_revision: 1,
                observed_at_ms: 10,
            },
        ];

        let allocations = ResourceBroker::new(repository)
            .allocate(item.id, item.state.revision, &candidates)
            .expect("allocate matched candidate");
        assert_eq!(
            allocations.by_requirement["worker"][0].resource,
            ResourceKey::Replicant("IN-RANGE".into())
        );
    }
}
