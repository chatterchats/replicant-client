//! Declarative desired-state requirements and pure gap evaluation.

use std::collections::BTreeMap;

use replicant_client::{Client, domain::Device};
use replicant_workflow::{ResourceKey, WorkflowStatus};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Stable place at which a desired state must hold.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum RequirementScope {
    /// Any location in one star system.
    System(String),
    /// One exact in-system location.
    Location(String),
}

/// Optional state constraints for matching devices.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct DeviceRequirementState {
    /// Required device status, such as `active`.
    pub status: Option<String>,
    /// Required assigned replicant.
    pub owner: Option<String>,
    /// Required adopting controller device.
    pub controller: Option<String>,
    /// Whether the device must be deployed rather than stowed.
    pub deployed: Option<bool>,
}

/// Desired asset or infrastructure category.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum RequirementTarget {
    /// Count devices of one type with optional lifecycle/ownership constraints.
    Device {
        /// Managed device type.
        device_type: String,
        /// State constraints applied before a device counts.
        #[serde(default)]
        state: DeviceRequirementState,
    },
    /// Count relay or mining infrastructure devices.
    Infrastructure {
        /// `relay` or `mining`.
        infrastructure: InfrastructureKind,
    },
    /// Count available device or resource units.
    Availability {
        /// Device type or resource type.
        asset: AvailabilityKind,
    },
}

/// Higher-level infrastructure categories.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InfrastructureKind {
    /// Relay infrastructure.
    Relay,
    /// Mining infrastructure.
    Mining,
}

/// Availability constraint category.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum AvailabilityKind {
    /// Available managed devices of one type.
    Device(String),
    /// Available units of one resource type.
    Resource(String),
}

/// Registered lower-level operation used to close a requirement gap.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FulfillmentOperation {
    /// Whether the child is a finite action or durable workflow.
    pub operation_class: FulfillmentOperationClass,
    /// Registered operation kind.
    pub kind: String,
    /// Typed descriptor values at the persistence boundary.
    #[serde(default)]
    pub parameters: BTreeMap<String, Value>,
    /// Resources the child must reserve before it starts.
    #[serde(default)]
    pub claims: Vec<ResourceKey>,
}

/// Lifecycle class for a lower-level fulfillment operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FulfillmentOperationClass {
    /// A bounded mutation, persisted as a child workflow for visibility.
    Action,
    /// A registered durable workflow.
    Workflow,
}

/// One persisted desired-state declaration.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Requirement {
    /// Stable user/application-defined identity.
    pub id: String,
    /// Human-readable label.
    pub name: String,
    /// Place where the requirement applies.
    pub scope: RequirementScope,
    /// State being counted.
    pub target: RequirementTarget,
    /// Minimum desired count or quantity.
    pub desired: u64,
    /// Lower-level work used when a gap exists.
    pub fulfillment: FulfillmentOperation,
}

/// One relevant managed-state observation used by the pure evaluator.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequirementFact {
    /// Place where the fact currently holds.
    pub scope: RequirementScope,
    /// Kind of observed state.
    pub target: RequirementTarget,
    /// Count or quantity represented by this fact.
    pub quantity: u64,
}

/// Active equivalent work already contributing toward a desired state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActiveFulfillment {
    /// Stable requirement identity.
    pub requirement_id: String,
    /// Quantity expected from this active work.
    pub quantity: u64,
    /// Current child lifecycle state.
    pub status: WorkflowStatus,
}

/// Typed non-mutating output of requirement evaluation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FulfillmentPlan {
    /// Stable requirement identity.
    pub requirement_id: String,
    /// Desired quantity.
    pub desired: u64,
    /// Quantity already present in managed state.
    pub actual: u64,
    /// Quantity covered by equivalent active work.
    pub in_progress: u64,
    /// Remaining quantity to fulfill.
    pub missing: u64,
    /// Child operation to create when `missing` is non-zero.
    pub step: Option<FulfillmentStep>,
}

/// One concrete lower-level child operation in a fulfillment plan.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FulfillmentStep {
    /// Quantity this child is expected to supply.
    pub quantity: u64,
    /// Registered operation invocation.
    pub operation: FulfillmentOperation,
}

/// Evaluates desired state without performing mutations.
#[must_use]
pub fn evaluate_requirement(
    requirement: &Requirement,
    facts: &[RequirementFact],
    active: &[ActiveFulfillment],
) -> FulfillmentPlan {
    let actual = facts
        .iter()
        .filter(|fact| {
            fact.scope == requirement.scope && target_matches(&requirement.target, &fact.target)
        })
        .map(|fact| fact.quantity)
        .sum::<u64>();
    let in_progress = active
        .iter()
        .filter(|work| {
            work.requirement_id == requirement.id
                && !matches!(
                    work.status,
                    WorkflowStatus::Succeeded | WorkflowStatus::Failed | WorkflowStatus::Cancelled
                )
        })
        .map(|work| work.quantity)
        .sum::<u64>();
    let missing = requirement
        .desired
        .saturating_sub(actual.saturating_add(in_progress));
    FulfillmentPlan {
        requirement_id: requirement.id.clone(),
        desired: requirement.desired,
        actual,
        in_progress,
        missing,
        step: (missing != 0).then(|| FulfillmentStep {
            quantity: missing,
            operation: requirement.fulfillment.clone(),
        }),
    }
}

/// Builds evaluator facts from the daemon-owned managed client.
pub async fn managed_facts(
    client: &Client,
) -> Result<Vec<RequirementFact>, replicant_client::Error> {
    let handles = client.devices().find().collect().await?;
    let mut facts = Vec::new();
    for handle in handles {
        let device = handle.snapshot().await?;
        facts.extend(device_facts(&device));
    }
    for inventory in client.state().inventories()? {
        let Some(location) = inventory.location else {
            continue;
        };
        let location = location.id.as_str().to_owned();
        let system = location.split('-').next().unwrap_or(&location).to_owned();
        for item in inventory.items {
            let quantity = u64::try_from(item.quantity).unwrap_or(0);
            for scope in [
                RequirementScope::System(system.clone()),
                RequirementScope::Location(location.clone()),
            ] {
                facts.push(RequirementFact {
                    scope,
                    target: RequirementTarget::Availability {
                        asset: AvailabilityKind::Resource(item.resource.clone()),
                    },
                    quantity,
                });
            }
        }
    }
    Ok(facts)
}

fn device_facts(device: &Device) -> Vec<RequirementFact> {
    let Some(location) = &device.location else {
        return Vec::new();
    };
    let Some(device_type) = device
        .device_type
        .as_ref()
        .map(|value| value.as_str().to_owned())
    else {
        return Vec::new();
    };
    let location = location.id.as_str().to_owned();
    let system = location.split('-').next().unwrap_or(&location).to_owned();
    let state = DeviceRequirementState {
        status: device
            .status
            .as_ref()
            .map(|value| value.as_str().to_owned()),
        owner: device
            .relationships
            .assigned_replicant
            .as_ref()
            .map(|key| key.id.as_str().to_owned()),
        controller: device
            .relationships
            .controller
            .as_ref()
            .map(|key| key.id.as_str().to_owned()),
        deployed: Some(device.relationships.stowed_in.is_none()),
    };
    [
        RequirementScope::System(system),
        RequirementScope::Location(location),
    ]
    .into_iter()
    .flat_map(|scope| {
        let mut targets = vec![
            RequirementTarget::Device {
                device_type: device_type.clone(),
                state: state.clone(),
            },
            RequirementTarget::Availability {
                asset: AvailabilityKind::Device(device_type.clone()),
            },
        ];
        let lower = device_type.to_ascii_lowercase();
        if lower.contains("relay") {
            targets.push(RequirementTarget::Infrastructure {
                infrastructure: InfrastructureKind::Relay,
            });
        }
        if lower.contains("mining") {
            targets.push(RequirementTarget::Infrastructure {
                infrastructure: InfrastructureKind::Mining,
            });
        }
        targets.into_iter().map(move |target| RequirementFact {
            scope: scope.clone(),
            target,
            quantity: 1,
        })
    })
    .collect()
}

fn target_matches(desired: &RequirementTarget, actual: &RequirementTarget) -> bool {
    match (desired, actual) {
        (
            RequirementTarget::Device {
                device_type: desired_type,
                state: desired_state,
            },
            RequirementTarget::Device {
                device_type: actual_type,
                state: actual_state,
            },
        ) => {
            desired_type == actual_type
                && desired_state
                    .status
                    .as_ref()
                    .is_none_or(|value| actual_state.status.as_ref() == Some(value))
                && desired_state
                    .owner
                    .as_ref()
                    .is_none_or(|value| actual_state.owner.as_ref() == Some(value))
                && desired_state
                    .controller
                    .as_ref()
                    .is_none_or(|value| actual_state.controller.as_ref() == Some(value))
                && desired_state
                    .deployed
                    .is_none_or(|value| actual_state.deployed == Some(value))
        }
        _ => desired == actual,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn requirement() -> Requirement {
        Requirement {
            id: "relay-sol".to_owned(),
            name: "SOL relay coverage".to_owned(),
            scope: RequirementScope::System("SOL".to_owned()),
            target: RequirementTarget::Infrastructure {
                infrastructure: InfrastructureKind::Relay,
            },
            desired: 2,
            fulfillment: FulfillmentOperation {
                operation_class: FulfillmentOperationClass::Workflow,
                kind: "relay.expansion".to_owned(),
                parameters: BTreeMap::new(),
                claims: Vec::new(),
            },
        }
    }

    #[test]
    fn evaluation_is_idempotent_and_accounts_for_active_work() {
        let requirement = requirement();
        let facts = [RequirementFact {
            scope: requirement.scope.clone(),
            target: requirement.target.clone(),
            quantity: 1,
        }];
        let active = [ActiveFulfillment {
            requirement_id: requirement.id.clone(),
            quantity: 1,
            status: WorkflowStatus::Running,
        }];
        let first = evaluate_requirement(&requirement, &facts, &active);
        assert_eq!(first, evaluate_requirement(&requirement, &facts, &active));
        assert_eq!((first.actual, first.in_progress, first.missing), (1, 1, 0));
        assert!(first.step.is_none());
    }
}
