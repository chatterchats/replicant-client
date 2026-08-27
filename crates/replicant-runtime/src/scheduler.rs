use std::collections::{BTreeMap, BTreeSet};

use replicant_workflow::{
    AllocationCandidate, RepositoryError, RequirementScope, ResourceKey, ResourceRequirement,
    WorkItemStatus, WorkflowRepository,
};
use serde::{Deserialize, Serialize};

/// Loss model used to compare deadline pressure across automations.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum LatenessCost {
    /// One-time loss after a missed deadline.
    Total {
        /// Units lost after the deadline.
        loss_units: f64,
    },
    /// Continuing loss while work remains late.
    Rate {
        /// Units lost per hour.
        loss_units_per_hour: f64,
    },
}

impl LatenessCost {
    fn one_hour_loss(self) -> f64 {
        match self {
            Self::Total { loss_units } => loss_units,
            Self::Rate {
                loss_units_per_hour,
            } => loss_units_per_hour,
        }
        .max(0.0)
    }
}

/// Scheduler metadata declared adjacent to a Director automation goal.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AutomationDeclaration {
    /// Stable automation key.
    pub key: String,
    /// Resource-flow facts produced by successful work.
    pub produces: Vec<String>,
    /// Resource-flow facts consumed by work.
    pub consumes: Vec<String>,
    /// Minimum worker count while enabled runnable work exists.
    pub one_worker_floor: u32,
    /// Cost of missing the automation deadline.
    pub lateness_cost: LatenessCost,
}

/// Returns the migrated automation resource-flow declarations.
#[must_use]
pub fn automation_declarations() -> Vec<AutomationDeclaration> {
    let cost = LatenessCost::Rate {
        loss_units_per_hour: 1.0,
    };
    vec![
        declaration("belt_search", &["known_unexploited_belts"], &[], cost),
        declaration(
            "mining.expansion",
            &["resource_flow"],
            &["known_unexploited_belts", "devices", "reachability"],
            cost,
        ),
        declaration("printing", &["devices"], &["resource_flow"], cost),
        declaration(
            "event.campaign",
            &[],
            &["resource_flow", "devices", "reachability"],
            cost,
        ),
        declaration(
            "survey",
            &["scanned_bodies", "salvage_sites"],
            &["reachability"],
            cost,
        ),
        declaration(
            "salvage.recovery",
            &["resource_flow"],
            &["salvage_sites", "reachability"],
            cost,
        ),
        declaration("relay.expansion", &["reachability"], &[], cost),
    ]
}

fn declaration(
    key: &str,
    produces: &[&str],
    consumes: &[&str],
    lateness_cost: LatenessCost,
) -> AutomationDeclaration {
    AutomationDeclaration {
        key: key.into(),
        produces: produces.iter().map(|value| (*value).into()).collect(),
        consumes: consumes.iter().map(|value| (*value).into()).collect(),
        one_worker_floor: 1,
        lateness_cost,
    }
}

/// Observed buffer and burn facts for one resource edge.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BufferObservation {
    /// Resource-flow edge key.
    pub resource: String,
    /// Available units.
    pub buffer: f64,
    /// Consumption rate in units per hour.
    pub burn_rate_per_hour: f64,
    /// Observation time in Unix milliseconds.
    pub observed_at_ms: i64,
}

impl BufferObservation {
    /// Derives a starvation deadline; zero burn has no deadline.
    #[must_use]
    pub fn deadline_at_ms(&self) -> Option<i64> {
        if !self.buffer.is_finite()
            || !self.burn_rate_per_hour.is_finite()
            || self.buffer < 0.0
            || self.burn_rate_per_hour < 0.0
        {
            return None;
        }
        if self.buffer == 0.0 {
            return Some(self.observed_at_ms);
        }
        if self.burn_rate_per_hour == 0.0 {
            return None;
        }
        let millis = self.buffer / self.burn_rate_per_hour * 3_600_000.0;
        (millis < i64::MAX as f64)
            .then(|| self.observed_at_ms.saturating_add(millis.round() as i64))
    }
}

/// Current runnable and worker facts for one campaign.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AutomationDemand {
    /// Stable automation key.
    pub automation: String,
    /// Stable campaign workflow identity.
    pub campaign: String,
    /// Head pending item considered by this decision.
    pub item: Option<String>,
    /// Runnable pending items.
    pub runnable_items: u32,
    /// Currently eligible workers.
    pub eligible_workers: u32,
    /// Current durable grants.
    pub current_grants: u32,
    /// Whether automation is enabled.
    pub enabled: bool,
    /// Earliest explicit pending-item deadline.
    pub explicit_deadline_at_ms: Option<i64>,
}

/// Scheduler action for one campaign.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScheduleAction {
    /// Preserve grants.
    Hold,
    /// Add grants.
    Grant,
    /// Reclaim grants at safe boundaries.
    Reclaim,
    /// Report unmet grow-only workforce floor.
    GrowWorkforce,
    /// No runnable enabled work.
    Idle,
}
/// Reads the latest persisted automation schedule report.
pub fn automation_schedule_report(
    repository: &WorkflowRepository,
) -> Result<serde_json::Value, RepositoryError> {
    Ok(repository
        .read_document("automation.scheduler", "latest")?
        .map_or_else(|| serde_json::Value::Array(Vec::new()), |(value, _)| value))
}

/// Derives schedule demand from durable migrated campaign work items.
pub fn repository_schedule(
    repository: &WorkflowRepository,
    candidates: &[AllocationCandidate],
    physical_workers: u32,
    now_ms: i64,
) -> Result<Vec<ScheduleDecision>, RepositoryError> {
    let buffers = repository
        .read_document("automation.scheduler", "buffers")?
        .map(|(value, _)| serde_json::from_value::<Vec<BufferObservation>>(value))
        .transpose()?
        .unwrap_or_default();
    let mut demands = Vec::new();
    for workflow in repository.list_active()? {
        let Some(automation) = automation_for_workflow_kind(workflow.kind.as_str()) else {
            continue;
        };
        let items = repository.list_work_items(workflow.id)?;
        let pending: Vec<_> = items
            .iter()
            .filter(|item| item.state.status == WorkItemStatus::Pending)
            .collect();
        let runnable_items = u32::try_from(pending.len()).unwrap_or(u32::MAX);
        let eligible_workers = u32::try_from(
            candidates
                .iter()
                .filter(|candidate| matches!(candidate.resource, ResourceKey::Replicant(_)))
                .filter(|candidate| {
                    pending.iter().any(|item| {
                        serde_json::from_value::<Vec<ResourceRequirement>>(
                            item.spec.requirements_json.clone(),
                        )
                        .is_ok_and(|requirements| {
                            requirements.iter().any(|requirement| {
                                requirement.kind == "replicant"
                                    && candidate_satisfies(candidate, requirement)
                            })
                        })
                    })
                })
                .count(),
        )
        .unwrap_or(u32::MAX);
        let explicit_deadline_at_ms = pending
            .iter()
            .filter_map(|item| item.spec.deadline_at_ms)
            .min();
        let item = pending.first().map(|item| item.id.to_string());
        let current_grants = u32::try_from(
            repository
                .claims(workflow.id)?
                .iter()
                .filter(|claim| matches!(claim.resource, ResourceKey::Replicant(_)))
                .count(),
        )
        .unwrap_or(u32::MAX);
        demands.push(AutomationDemand {
            automation: automation.into(),
            campaign: workflow.id.to_string(),
            item,
            runnable_items,
            eligible_workers,
            current_grants,
            enabled: true,
            explicit_deadline_at_ms,
        });
    }
    let mut decisions = schedule(
        &automation_declarations(),
        &demands,
        &buffers,
        physical_workers,
        now_ms,
    );
    for decision in &mut decisions {
        if decision.automation != "salvage.recovery" {
            continue;
        }
        if let Some((evidence, _)) =
            repository.read_document("automation.scheduler.salvage", &decision.campaign)?
        {
            decision.discovery_count = evidence["discovery_count"].as_u64();
            decision.depleted_count = evidence["depleted_count"].as_u64();
            decision.ledger_count = evidence["ledger_count"].as_u64();
            decision.worklist_count = evidence["worklist_count"].as_u64();
        }
    }
    Ok(decisions)
}

fn candidate_satisfies(candidate: &AllocationCandidate, requirement: &ResourceRequirement) -> bool {
    candidate.kind == requirement.kind
        && requirement
            .capabilities
            .iter()
            .all(|required| candidate.capabilities.contains(required))
        && match &requirement.scope {
            RequirementScope::Anywhere => true,
            RequirementScope::Region(region) => {
                candidate
                    .location
                    .as_ref()
                    .and_then(|location| location.region.as_ref())
                    == Some(region)
            }
            RequirementScope::System(system) => {
                candidate
                    .location
                    .as_ref()
                    .and_then(|location| location.system.as_ref())
                    == Some(system)
            }
            RequirementScope::Location(designation) => {
                candidate
                    .location
                    .as_ref()
                    .and_then(|location| location.designation.as_ref())
                    == Some(designation)
            }
            RequirementScope::WithinLy { origin, range_ly } => candidate
                .location
                .as_ref()
                .and_then(|location| location.distances_ly.get(origin))
                .is_some_and(|distance| distance <= range_ly),
        }
}

fn automation_for_workflow_kind(kind: &str) -> Option<&'static str> {
    match kind {
        "belt_search.campaign" => Some("belt_search"),
        "relay.expansion" => Some("relay.expansion"),
        "survey.route" => Some("survey"),
        "mining.expansion" => Some("mining.expansion"),
        "event.campaign" | "event.fulfillment" => Some("event.campaign"),
        "salvage.recovery" => Some("salvage.recovery"),
        _ => None,
    }
}

/// Explanation facts for one scheduler decision.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ScheduleDecision {
    /// Stable automation key.
    pub automation: String,
    /// Stable campaign identity.
    pub campaign: String,
    /// Head pending item considered by this decision.
    pub item: Option<String>,
    /// Buffer fact, when available.
    pub buffer: Option<f64>,
    /// Burn-rate fact, when available.
    pub burn_rate_per_hour: Option<f64>,
    /// Derived or explicit deadline.
    pub deadline_at_ms: Option<i64>,
    /// One-hour lateness loss.
    pub loss_over_one_hour: f64,
    /// Declared lateness-cost model.
    pub lateness_cost: LatenessCost,
    /// Enabled runnable floor.
    pub floor: u32,
    /// Runnable/eligible ceiling.
    pub ceiling: u32,
    /// Existing grants.
    pub current_grants: u32,
    /// Computed target grants.
    pub target_grants: u32,
    /// Derived urgency.
    pub urgency: f64,
    /// Declaration SCC index.
    pub component: usize,
    /// Recipient/donor urgency ratio when hysteresis applies.
    pub hysteresis_ratio: Option<f64>,
    /// Selected action.
    pub action: ScheduleAction,
    /// Deterministic short reasons.
    pub reasons: Vec<String>,
    /// Remote salvage discoveries observed for this campaign.
    #[serde(default)]
    pub discovery_count: Option<u64>,
    /// Remote depleted salvage events observed for this campaign.
    #[serde(default)]
    pub depleted_count: Option<u64>,
    /// Durable completed-site ledger count.
    #[serde(default)]
    pub ledger_count: Option<u64>,
    /// Remaining salvage worklist count.
    #[serde(default)]
    pub worklist_count: Option<u64>,
}

/// Calculates deadline pressure using a one-hour comparison horizon.
#[must_use]
pub fn urgency(now_ms: i64, deadline_at_ms: Option<i64>, cost: LatenessCost) -> f64 {
    let Some(deadline_at_ms) = deadline_at_ms else {
        return 0.0;
    };
    let slack_hours = (deadline_at_ms.saturating_sub(now_ms) as f64 / 3_600_000.0).max(1.0 / 60.0);
    cost.one_hour_loss() / slack_hours
}

/// Computes floors first, then elastic grants, with 25% reclaim hysteresis.
#[must_use]
pub fn schedule(
    declarations: &[AutomationDeclaration],
    demands: &[AutomationDemand],
    buffers: &[BufferObservation],
    physical_workers: u32,
    now_ms: i64,
) -> Vec<ScheduleDecision> {
    let declarations_by_key: BTreeMap<_, _> = declarations
        .iter()
        .map(|value| (value.key.as_str(), value))
        .collect();
    let observations: BTreeMap<_, _> = buffers
        .iter()
        .map(|value| (value.resource.as_str(), value))
        .collect();
    let components = strongly_connected_components(declarations);
    let mut decisions = Vec::new();
    for demand in demands {
        let Some(declaration) = declarations_by_key.get(demand.automation.as_str()) else {
            continue;
        };
        let observed = declaration
            .produces
            .iter()
            .filter_map(|resource| observations.get(resource.as_str()).copied())
            .filter(|value| {
                now_ms >= value.observed_at_ms
                    && now_ms.saturating_sub(value.observed_at_ms) <= 86_400_000
            })
            .min_by_key(|value| value.deadline_at_ms().unwrap_or(i64::MAX));
        let derived = observed.and_then(BufferObservation::deadline_at_ms);
        let deadline = match (demand.explicit_deadline_at_ms, derived) {
            (Some(explicit), Some(derived)) => Some(explicit.min(derived)),
            (Some(explicit), None) => Some(explicit),
            (None, derived) => derived,
        };
        let floor = if demand.enabled && demand.runnable_items != 0 {
            declaration.one_worker_floor.min(demand.runnable_items)
        } else {
            0
        };
        let ceiling = demand.runnable_items.min(demand.eligible_workers);
        let mut reasons = Vec::new();
        if observed.is_none() && deadline.is_none() {
            if declaration.produces.is_empty() {
                reasons.push("deadline observation unavailable; floor only".into());
            } else {
                reasons.extend(
                    declaration
                        .produces
                        .iter()
                        .map(|resource| format!("{resource} observation unavailable; floor only")),
                );
            }
        }
        decisions.push(ScheduleDecision {
            automation: demand.automation.clone(),
            campaign: demand.campaign.clone(),
            item: demand.item.clone(),
            buffer: observed.map(|value| value.buffer),
            burn_rate_per_hour: observed.map(|value| value.burn_rate_per_hour),
            deadline_at_ms: deadline,
            loss_over_one_hour: declaration.lateness_cost.one_hour_loss(),
            lateness_cost: declaration.lateness_cost,
            floor,
            ceiling,
            current_grants: demand.current_grants,
            target_grants: 0,
            urgency: urgency(now_ms, deadline, declaration.lateness_cost),
            component: components
                .get(&demand.automation)
                .copied()
                .unwrap_or(usize::MAX),
            hysteresis_ratio: None,
            action: if ceiling == 0 {
                ScheduleAction::Idle
            } else {
                ScheduleAction::Hold
            },
            reasons,
            discovery_count: None,
            depleted_count: None,
            ledger_count: None,
            worklist_count: None,
        });
    }
    let mut order: Vec<_> = (0..decisions.len()).collect();
    order.sort_by(|left, right| decision_order(&decisions[*left], &decisions[*right]));
    for decision in &mut decisions {
        decision.target_grants = decision.current_grants.min(decision.floor);
    }
    let incumbent_floors = decisions
        .iter()
        .map(|decision| decision.target_grants)
        .sum::<u32>();
    let mut remaining = physical_workers.saturating_sub(incumbent_floors);
    for index in &order {
        let needed = decisions[*index]
            .floor
            .saturating_sub(decisions[*index].target_grants);
        let grant = needed.min(remaining);
        decisions[*index].target_grants += grant;
        remaining -= grant;
        if decisions[*index].target_grants < decisions[*index].floor {
            decisions[*index].action = ScheduleAction::GrowWorkforce;
            decisions[*index]
                .reasons
                .push("runnable floor unmet; incumbent floor grants preserved".into());
        }
    }
    while remaining != 0 {
        let Some(index) = order
            .iter()
            .copied()
            .find(|index| decisions[*index].target_grants < decisions[*index].ceiling)
        else {
            break;
        };
        decisions[index].target_grants += 1;
        remaining -= 1;
    }
    for decision in &mut decisions {
        if decision.action == ScheduleAction::GrowWorkforce {
            continue;
        }
        decision.action = match decision.target_grants.cmp(&decision.current_grants) {
            std::cmp::Ordering::Greater => ScheduleAction::Grant,
            std::cmp::Ordering::Equal if decision.ceiling == 0 => ScheduleAction::Idle,
            std::cmp::Ordering::Equal => ScheduleAction::Hold,
            std::cmp::Ordering::Less => ScheduleAction::Reclaim,
        };
    }
    for recipient_index in &order {
        if decisions[*recipient_index].target_grants >= decisions[*recipient_index].ceiling {
            continue;
        }
        let donor_index = decisions.iter().enumerate().find_map(|(index, donor)| {
            (donor.current_grants > donor.floor
                && decisions[*recipient_index].urgency >= donor.urgency * 1.25)
                .then_some(index)
        });
        if let Some(donor_index) = donor_index {
            let donor_urgency = decisions[donor_index].urgency;
            decisions[*recipient_index].hysteresis_ratio = Some(if donor_urgency == 0.0 {
                f64::INFINITY
            } else {
                decisions[*recipient_index].urgency / donor_urgency
            });
            decisions[donor_index].action = ScheduleAction::Reclaim;
            decisions[donor_index]
                .reasons
                .push("recipient urgency exceeded 25% hysteresis".into());
        }
    }
    decisions.sort_by(|left, right| left.automation.cmp(&right.automation));
    decisions
}

fn decision_order(left: &ScheduleDecision, right: &ScheduleDecision) -> std::cmp::Ordering {
    left.deadline_at_ms
        .unwrap_or(i64::MAX)
        .cmp(&right.deadline_at_ms.unwrap_or(i64::MAX))
        .then_with(|| right.urgency.total_cmp(&left.urgency))
        .then_with(|| left.automation.cmp(&right.automation))
}

fn strongly_connected_components(
    declarations: &[AutomationDeclaration],
) -> BTreeMap<String, usize> {
    let mut producers: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for declaration in declarations {
        for resource in &declaration.produces {
            producers
                .entry(resource)
                .or_default()
                .push(&declaration.key);
        }
    }
    let mut edges: BTreeMap<String, BTreeSet<String>> = declarations
        .iter()
        .map(|value| (value.key.clone(), BTreeSet::new()))
        .collect();
    for consumer in declarations {
        for resource in &consumer.consumes {
            for producer in producers.get(resource.as_str()).into_iter().flatten() {
                edges
                    .entry((*producer).into())
                    .or_default()
                    .insert(consumer.key.clone());
            }
        }
    }
    let mut reachable = BTreeMap::new();
    for key in edges.keys() {
        let mut reached = BTreeSet::new();
        let mut stack = vec![key.clone()];
        while let Some(node) = stack.pop() {
            if reached.insert(node.clone()) {
                stack.extend(edges.get(&node).into_iter().flatten().cloned());
            }
        }
        reachable.insert(key.clone(), reached);
    }
    let mut result = BTreeMap::new();
    let mut component = 0;
    for key in edges.keys() {
        if result.contains_key(key) {
            continue;
        }
        for other in edges.keys() {
            if reachable[key].contains(other) && reachable[other].contains(key) {
                result.insert(other.clone(), component);
            }
        }
        component += 1;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn declaration(key: &str, produces: &[&str], consumes: &[&str]) -> AutomationDeclaration {
        AutomationDeclaration {
            key: key.into(),
            produces: produces.iter().map(|value| (*value).into()).collect(),
            consumes: consumes.iter().map(|value| (*value).into()).collect(),
            one_worker_floor: 1,
            lateness_cost: LatenessCost::Rate {
                loss_units_per_hour: 100.0,
            },
        }
    }

    fn demand(key: &str) -> AutomationDemand {
        AutomationDemand {
            automation: key.into(),
            campaign: format!("{key}-campaign"),
            item: Some(format!("{key}-item")),
            runnable_items: 4,
            eligible_workers: 4,
            current_grants: 0,
            enabled: true,
            explicit_deadline_at_ms: None,
        }
    }

    #[test]
    fn scheduler_zero_buffer_and_zero_burn_deadlines() {
        let now = 1_000_000;
        assert_eq!(
            BufferObservation {
                resource: "flow".into(),
                buffer: 0.0,
                burn_rate_per_hour: 1_850.0 / 24.0,
                observed_at_ms: now,
            }
            .deadline_at_ms(),
            Some(now)
        );
        assert_eq!(
            BufferObservation {
                resource: "flow".into(),
                buffer: 100.0,
                burn_rate_per_hour: 0.0,
                observed_at_ms: now,
            }
            .deadline_at_ms(),
            None
        );
    }
    #[test]
    fn scheduler_healthy_stale_and_explicit_deadline_facts() {
        let declarations = [declaration("producer", &["flow"], &[])];
        let healthy = schedule(
            &declarations,
            &[demand("producer")],
            &[BufferObservation {
                resource: "flow".into(),
                buffer: 10.0,
                burn_rate_per_hour: 1.0,
                observed_at_ms: 0,
            }],
            1,
            1_000,
        );
        assert_eq!(healthy[0].deadline_at_ms, Some(36_000_000));
        assert!(healthy[0].urgency > 0.0);
        assert_eq!(healthy[0].item.as_deref(), Some("producer-item"));

        let stale = schedule(
            &declarations,
            &[demand("producer")],
            &[BufferObservation {
                resource: "flow".into(),
                buffer: 0.0,
                burn_rate_per_hour: 1.0,
                observed_at_ms: 0,
            }],
            1,
            86_400_001,
        );
        assert_eq!(stale[0].deadline_at_ms, None);
        assert!(
            stale[0]
                .reasons
                .iter()
                .any(|reason| reason.contains("unavailable"))
        );

        let mut explicit = demand("producer");
        explicit.explicit_deadline_at_ms = Some(120_000);
        let explicit = schedule(&declarations, &[explicit], &[], 1, 60_000);
        assert_eq!(explicit[0].deadline_at_ms, Some(120_000));
        assert_eq!(explicit[0].urgency, 6_000.0);
    }

    #[test]
    fn automation_schedule_report_returns_required_fields() {
        let repository = WorkflowRepository::open_in_memory().expect("open repository");
        let decisions = schedule(
            &[declaration("producer", &["flow"], &[])],
            &[demand("producer")],
            &[BufferObservation {
                resource: "flow".into(),
                buffer: 2.0,
                burn_rate_per_hour: 1.0,
                observed_at_ms: 0,
            }],
            1,
            0,
        );
        repository
            .put_document("automation.scheduler", "latest", &decisions)
            .expect("persist schedule");
        let report = automation_schedule_report(&repository).expect("read schedule report");
        let decision = report
            .as_array()
            .and_then(|values| values.first())
            .and_then(serde_json::Value::as_object)
            .expect("report decision");
        for field in [
            "automation",
            "campaign",
            "item",
            "buffer",
            "burn_rate_per_hour",
            "deadline_at_ms",
            "lateness_cost",
            "loss_over_one_hour",
            "floor",
            "ceiling",
            "current_grants",
            "target_grants",
            "urgency",
            "hysteresis_ratio",
            "action",
            "reasons",
            "discovery_count",
            "depleted_count",
            "ledger_count",
            "worklist_count",
        ] {
            assert!(decision.contains_key(field), "missing report field {field}");
        }
    }

    #[test]
    fn scheduler_floors_under_scarcity_and_cycle_reporting() {
        let declarations = [
            declaration("belt_search", &["belts"], &[]),
            declaration("mining", &["flow"], &["belts", "devices"]),
            declaration("printing", &["devices"], &["flow"]),
        ];
        let decisions = schedule(
            &declarations,
            &[demand("belt_search"), demand("mining"), demand("printing")],
            &[BufferObservation {
                resource: "flow".into(),
                buffer: 0.0,
                burn_rate_per_hour: 1.0,
                observed_at_ms: 0,
            }],
            2,
            0,
        );
        assert_eq!(
            decisions
                .iter()
                .map(|value| value.target_grants)
                .sum::<u32>(),
            2
        );
        assert_eq!(
            decisions
                .iter()
                .filter(|value| value.action == ScheduleAction::GrowWorkforce)
                .count(),
            1
        );
        assert_eq!(decisions[1].component, decisions[2].component);
    }

    #[test]
    fn scheduler_preserves_incumbent_floor_under_scarcity() {
        let declarations = [
            declaration("incumbent", &[], &[]),
            declaration("newcomer", &[], &[]),
        ];
        let mut incumbent = demand("incumbent");
        incumbent.current_grants = 1;
        let decisions = schedule(&declarations, &[incumbent, demand("newcomer")], &[], 1, 0);
        let incumbent = decisions
            .iter()
            .find(|decision| decision.automation == "incumbent")
            .expect("incumbent decision");
        let newcomer = decisions
            .iter()
            .find(|decision| decision.automation == "newcomer")
            .expect("newcomer decision");
        assert_eq!(incumbent.target_grants, 1);
        assert_ne!(incumbent.action, ScheduleAction::Reclaim);
        assert_eq!(newcomer.target_grants, 0);
        assert_eq!(newcomer.action, ScheduleAction::GrowWorkforce);
    }

    #[test]
    fn scheduler_hysteresis_requires_twenty_five_percent() {
        let declarations = [
            declaration("donor", &["healthy"], &[]),
            declaration("recipient", &["urgent"], &[]),
        ];
        let mut donor = demand("donor");
        donor.current_grants = 2;
        donor.runnable_items = 2;
        let decisions = schedule(
            &declarations,
            &[donor, demand("recipient")],
            &[
                BufferObservation {
                    resource: "healthy".into(),
                    buffer: 1.0,
                    burn_rate_per_hour: 1.0,
                    observed_at_ms: 0,
                },
                BufferObservation {
                    resource: "urgent".into(),
                    buffer: 0.5,
                    burn_rate_per_hour: 1.0,
                    observed_at_ms: 0,
                },
            ],
            2,
            0,
        );
        assert_eq!(decisions[0].action, ScheduleAction::Reclaim);
        assert!(
            decisions[1]
                .hysteresis_ratio
                .is_some_and(|ratio| ratio >= 1.25)
        );
    }
}
