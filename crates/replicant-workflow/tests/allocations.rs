//! Atomic resource allocation acceptance tests.

use std::sync::{Arc, Barrier};

use replicant_workflow::{
    AllocationCandidate, AllocationLocation, NewWorkflow, ReplacementOutcome, RepositoryError,
    RequirementScope, ResourceKey, ResourceRequirement, WorkItemSpec, WorkItemStatus,
    WorkItemTransition, WorkflowKind, WorkflowRepository, WorkflowState, WorkflowStatus,
};
use serde_json::json;

fn campaign(repository: &WorkflowRepository) -> replicant_workflow::WorkflowInstance {
    repository
        .create(NewWorkflow {
            kind: WorkflowKind::new("test.allocation").expect("valid kind"),
            schema_version: 1,
            config: json!({}),
            checkpoint: json!({}),
            current_step: None,
            parent_id: None,
        })
        .expect("create campaign")
}

fn item(
    repository: &WorkflowRepository,
    workflow_id: replicant_workflow::WorkflowId,
    key: &str,
    requirements: Vec<ResourceRequirement>,
) -> replicant_workflow::WorkItem {
    repository
        .reconcile_work_items(
            workflow_id,
            &[WorkItemSpec {
                workflow_id,
                dedupe_key: key.into(),
                kind: WorkflowKind::new("test.allocation-item").expect("valid item kind"),
                sort_key: key.into(),
                payload_json: json!({}),
                preconditions_json: json!([]),
                requirements_json: serde_json::to_value(requirements).expect("encode requirements"),
                deadline_at_ms: None,
            }],
            1,
        )
        .expect("reconcile item")
        .into_iter()
        .find(|item| item.spec.dedupe_key == key)
        .expect("item exists")
}

fn requirement(key: &str, kind: &str, count: u32, quantity: u64) -> ResourceRequirement {
    ResourceRequirement {
        key: key.into(),
        kind: kind.into(),
        capabilities: Vec::new(),
        scope: RequirementScope::Anywhere,
        count,
        quantity,
    }
}

fn candidate(resource: ResourceKey, kind: &str, quantity: u64) -> AllocationCandidate {
    AllocationCandidate {
        resource,
        kind: kind.into(),
        capabilities: Vec::new(),
        location: None,
        available_quantity: quantity,
        observed_revision: 1,
        observed_at_ms: 10,
    }
}

#[test]
fn allocation_exclusive_identity_has_one_owner() {
    let repository = WorkflowRepository::open_in_memory().expect("open repository");
    let first_campaign = campaign(&repository);
    let second_campaign = campaign(&repository);
    let first = item(
        &repository,
        first_campaign.id,
        "first",
        vec![requirement("worker", "replicant", 1, 1)],
    );
    let second = item(
        &repository,
        second_campaign.id,
        "second",
        vec![requirement("worker", "replicant", 1, 1)],
    );
    let candidates = [candidate(
        ResourceKey::Replicant("R-1".into()),
        "replicant",
        1,
    )];

    repository
        .allocate_requirements(first.id, first.state.revision, &candidates)
        .expect("allocate first owner");
    assert!(matches!(
        repository.allocate_requirements(second.id, second.state.revision, &candidates),
        Err(RepositoryError::AllocationShortage { .. })
    ));
    assert_eq!(
        repository
            .claims(first_campaign.id)
            .expect("list claims")
            .len(),
        1
    );
    assert!(
        repository
            .claims(second_campaign.id)
            .expect("list claims")
            .is_empty()
    );
}

#[test]
fn allocation_quantity_never_exceeds_pool_and_shortage_rolls_back() {
    let repository = WorkflowRepository::open_in_memory().expect("open repository");
    let campaign = campaign(&repository);
    let allocation_item = item(
        &repository,
        campaign.id,
        "materials",
        vec![
            requirement("ore", "material", 1, 7),
            requirement("missing", "material", 1, 5),
        ],
    );
    let candidates = [
        candidate(
            ResourceKey::Namespaced {
                namespace: "inventory".into(),
                key: "SOL:IRON".into(),
            },
            "material",
            10,
        ),
        candidate(
            ResourceKey::Namespaced {
                namespace: "inventory".into(),
                key: "SOL:COPPER".into(),
            },
            "other",
            10,
        ),
    ];
    assert!(matches!(
        repository.allocate_requirements(
            allocation_item.id,
            allocation_item.state.revision,
            &candidates
        ),
        Err(RepositoryError::AllocationShortage { requirement_key, .. })
            if requirement_key == "missing"
    ));

    let rollback_probe = item(
        &repository,
        campaign.id,
        "rollback-probe",
        vec![requirement("all-ore", "material", 1, 10)],
    );
    let allocated = repository
        .allocate_requirements(
            rollback_probe.id,
            rollback_probe.state.revision,
            std::slice::from_ref(&candidates[0]),
        )
        .expect("rolled-back partial allocation left full capacity");
    assert_eq!(allocated.by_requirement["all-ore"][0].quantity, 10);
}

#[test]
fn allocation_stow_capacity_releases_on_terminal_item() {
    let repository = WorkflowRepository::open_in_memory().expect("open repository");
    let campaign = campaign(&repository);
    let first = item(
        &repository,
        campaign.id,
        "first-stow",
        vec![requirement("stow", "stow", 1, 3)],
    );
    let second = item(
        &repository,
        campaign.id,
        "second-stow",
        vec![requirement("stow", "stow", 1, 3)],
    );
    let candidates = [candidate(
        ResourceKey::Namespaced {
            namespace: "stow".into(),
            key: "VESSEL-1".into(),
        },
        "stow",
        5,
    )];
    repository
        .allocate_requirements(first.id, first.state.revision, &candidates)
        .expect("allocate first stow reservation");
    assert!(matches!(
        repository.allocate_requirements(second.id, second.state.revision, &candidates),
        Err(RepositoryError::AllocationShortage { .. })
    ));
    let first = repository
        .read_work_item(first.id)
        .expect("read first item")
        .expect("first item exists");
    repository
        .transition_work_item(
            first.id,
            first.state.revision,
            WorkItemTransition::Skipped {
                reason: "complete".into(),
                result_json: None,
            },
            20,
        )
        .expect("finish first item");
    repository
        .allocate_requirements(second.id, second.state.revision, &candidates)
        .expect("released stow capacity is reusable");
}

#[test]
fn allocation_affinity_pairs_stow_with_selected_carrier() {
    let repository = WorkflowRepository::open_in_memory().expect("open repository");
    let campaign = campaign(&repository);
    let mut carrier = requirement("carrier", "device", 1, 1);
    carrier.capabilities.push("surge_carrier".into());
    let stow = requirement("stow", "stow", 1, 2);
    let allocation_item = item(&repository, campaign.id, "paired-stow", vec![carrier, stow]);
    let mut carrier_b = candidate(ResourceKey::Device("B-CARRIER".into()), "device", 1);
    carrier_b.capabilities.push("surge_carrier".into());
    let candidates = [
        carrier_b,
        candidate(
            ResourceKey::Namespaced {
                namespace: "stow".into(),
                key: "A-OTHER".into(),
            },
            "stow",
            10,
        ),
        candidate(
            ResourceKey::Namespaced {
                namespace: "stow".into(),
                key: "B-CARRIER".into(),
            },
            "stow",
            10,
        ),
    ];

    let allocated = repository
        .allocate_requirements_with_affinity(
            allocation_item.id,
            allocation_item.state.revision,
            &candidates,
            &[("stow", "carrier")],
        )
        .expect("allocate paired carrier and stow");
    assert_eq!(
        allocated.by_requirement["carrier"][0].resource,
        ResourceKey::Device("B-CARRIER".into())
    );
    assert_eq!(
        allocated.by_requirement["stow"][0].resource,
        ResourceKey::Namespaced {
            namespace: "stow".into(),
            key: "B-CARRIER".into(),
        }
    );
}

#[test]
fn allocation_affinity_skips_transport_without_required_stow_capacity() {
    let repository = WorkflowRepository::open_in_memory().expect("open repository");
    let campaign = campaign(&repository);
    let mut carrier = requirement("carrier", "device", 1, 1);
    carrier.capabilities.push("surge_carrier".into());
    let stow = requirement("stow", "stow", 1, 2);
    let allocation_item = item(
        &repository,
        campaign.id,
        "paired-stow-capacity",
        vec![carrier, stow],
    );
    let mut carrier_a = candidate(ResourceKey::Device("A-CARRIER".into()), "device", 1);
    carrier_a.capabilities.push("surge_carrier".into());
    let mut carrier_b = candidate(ResourceKey::Device("B-CARRIER".into()), "device", 1);
    carrier_b.capabilities.push("surge_carrier".into());
    let candidates = [
        carrier_a,
        carrier_b,
        candidate(
            ResourceKey::Namespaced {
                namespace: "stow".into(),
                key: "A-CARRIER".into(),
            },
            "stow",
            1,
        ),
        candidate(
            ResourceKey::Namespaced {
                namespace: "stow".into(),
                key: "B-CARRIER".into(),
            },
            "stow",
            10,
        ),
    ];

    let allocated = repository
        .allocate_requirements_with_affinity(
            allocation_item.id,
            allocation_item.state.revision,
            &candidates,
            &[("stow", "carrier")],
        )
        .expect("allocate carrier with enough paired stow");
    assert_eq!(
        allocated.by_requirement["carrier"][0].resource,
        ResourceKey::Device("B-CARRIER".into())
    );
    assert_eq!(
        allocated.by_requirement["stow"][0].resource,
        ResourceKey::Namespaced {
            namespace: "stow".into(),
            key: "B-CARRIER".into(),
        }
    );
}

#[test]
fn allocation_affinity_survives_transport_replacement() {
    let repository = WorkflowRepository::open_in_memory().expect("open repository");
    let campaign = campaign(&repository);
    let mut carrier = requirement("carrier", "device", 1, 1);
    carrier.capabilities.push("surge_carrier".into());
    let stow = requirement("stow", "stow", 1, 2);
    let allocation_item = item(
        &repository,
        campaign.id,
        "replacement-affinity",
        vec![carrier, stow],
    );
    let mut carrier_b = candidate(ResourceKey::Device("B-CARRIER".into()), "device", 1);
    carrier_b.capabilities.push("surge_carrier".into());
    let initial_candidates = [
        carrier_b,
        candidate(
            ResourceKey::Namespaced {
                namespace: "stow".into(),
                key: "B-CARRIER".into(),
            },
            "stow",
            10,
        ),
    ];
    let allocated = repository
        .allocate_requirements_with_affinity(
            allocation_item.id,
            allocation_item.state.revision,
            &initial_candidates,
            &[("stow", "carrier")],
        )
        .expect("allocate initial pair");
    let old_carrier = allocated.by_requirement["carrier"][0].clone();
    let old_stow = allocated.by_requirement["stow"][0].clone();

    let mut carrier_c = candidate(ResourceKey::Device("C-CARRIER".into()), "device", 1);
    carrier_c.capabilities.push("surge_carrier".into());
    let replacement_candidates = [
        carrier_c,
        candidate(
            ResourceKey::Namespaced {
                namespace: "stow".into(),
                key: "B-CARRIER".into(),
            },
            "stow",
            10,
        ),
        candidate(
            ResourceKey::Namespaced {
                namespace: "stow".into(),
                key: "C-CARRIER".into(),
            },
            "stow",
            10,
        ),
    ];
    assert!(matches!(
        repository
            .replace_dead_allocation_with_affinity(
                allocation_item.id,
                old_carrier.id,
                &replacement_candidates,
                20,
                &[("stow", "carrier")],
            )
            .expect("replace carrier"),
        ReplacementOutcome::Replaced(ref allocation)
            if allocation.resource == ResourceKey::Device("C-CARRIER".into())
    ));
    assert!(matches!(
        repository
            .replace_dead_allocation_with_affinity(
                allocation_item.id,
                old_stow.id,
                &replacement_candidates,
                21,
                &[("stow", "carrier")],
            )
            .expect("replace paired stow"),
        ReplacementOutcome::Replaced(ref allocation)
            if matches!(
                &allocation.resource,
                ResourceKey::Namespaced { namespace, key }
                    if namespace == "stow" && key == "C-CARRIER"
            )
    ));
}

#[test]
fn allocation_does_not_reuse_a_worker_with_an_active_assignment() {
    let repository = WorkflowRepository::open_in_memory().expect("open repository");
    let first_campaign = campaign(&repository);
    let second_campaign = campaign(&repository);
    let first = item(
        &repository,
        first_campaign.id,
        "first-worker",
        vec![requirement("worker", "replicant", 1, 1)],
    );
    let second = item(
        &repository,
        second_campaign.id,
        "second-worker",
        vec![requirement("worker", "replicant", 1, 1)],
    );
    let workers = [
        candidate(ResourceKey::Replicant("R-1".into()), "replicant", 1),
        candidate(ResourceKey::Replicant("R-2".into()), "replicant", 1),
    ];
    let allocated = repository
        .allocate_requirements(first.id, first.state.revision, &workers)
        .expect("allocate first worker");
    let assigned = repository
        .claim_next_work_item(first_campaign.id, 20)
        .expect("claim first")
        .expect("first assigned");
    repository
        .assign_work_item(
            assigned.id,
            assigned.state.revision,
            "first-assignment",
            &ResourceKey::Replicant("R-1".into()),
            21,
        )
        .expect("persist first assignment");
    repository
        .replace_dead_allocation(
            first.id,
            allocated.by_requirement["worker"][0].id,
            &[candidate(
                ResourceKey::Replicant("R-2".into()),
                "replicant",
                1,
            )],
            22,
        )
        .expect("move first allocation away from assigned worker");

    assert!(matches!(
        repository.allocate_requirements(
            second.id,
            second.state.revision,
            &[candidate(
                ResourceKey::Replicant("R-1".into()),
                "replicant",
                1,
            )],
        ),
        Err(RepositoryError::AllocationShortage { requirement_key, .. })
            if requirement_key == "worker"
    ));
}

#[test]
fn allocation_dead_replacement_preserves_checkpoint() {
    let repository = WorkflowRepository::open_in_memory().expect("open repository");
    let campaign = campaign(&repository);
    let allocation_item = item(
        &repository,
        campaign.id,
        "replacement",
        vec![requirement("worker", "replicant", 1, 1)],
    );
    let allocated = repository
        .allocate_requirements(
            allocation_item.id,
            allocation_item.state.revision,
            &[candidate(
                ResourceKey::Replicant("DEAD".into()),
                "replicant",
                1,
            )],
        )
        .expect("allocate original");
    let assigned = repository
        .claim_next_work_item(campaign.id, 20)
        .expect("claim item")
        .expect("assigned item");
    let running = repository
        .start_work_item(assigned.id, assigned.state.revision, "R-1", "grant", 21)
        .expect("start item");
    let checkpointed = repository
        .transition_work_item(
            running.id,
            running.state.revision,
            WorkItemTransition::CheckpointCommitted {
                checkpoint_json: json!({ "step": 3 }),
            },
            22,
        )
        .expect("commit checkpoint");
    let replacement = repository
        .replace_dead_allocation(
            allocation_item.id,
            allocated.by_requirement["worker"][0].id,
            &[candidate(
                ResourceKey::Replicant("LIVE".into()),
                "replicant",
                1,
            )],
            23,
        )
        .expect("replace missing resource");
    assert!(matches!(
        replacement,
        ReplacementOutcome::Replaced(ref allocation)
            if allocation.resource == ResourceKey::Replicant("LIVE".into())
    ));
    assert_eq!(
        repository
            .read_work_item(checkpointed.id)
            .expect("read item")
            .expect("item exists")
            .state
            .checkpoint_json,
        Some(json!({ "step": 3 }))
    );
}

#[test]
fn allocation_replacement_distinguishes_waiting_from_unavailable() {
    let repository = WorkflowRepository::open_in_memory().expect("open repository");
    let waiting_campaign = campaign(&repository);
    let mut ranged = requirement("worker", "replicant", 1, 1);
    ranged.scope = RequirementScope::WithinLy {
        origin: "SOL".into(),
        range_ly: 5.0,
    };
    let waiting_item = item(&repository, waiting_campaign.id, "waiting", vec![ranged]);
    let mut original = candidate(ResourceKey::Replicant("DEAD".into()), "replicant", 1);
    original.location = Some(AllocationLocation {
        distances_ly: [("SOL".into(), 1.0)].into(),
        ..AllocationLocation::default()
    });
    let allocated = repository
        .allocate_requirements(
            waiting_item.id,
            waiting_item.state.revision,
            std::slice::from_ref(&original),
        )
        .expect("allocate original");
    let mut out_of_range = candidate(ResourceKey::Replicant("BUSY".into()), "replicant", 1);
    out_of_range.location = Some(AllocationLocation {
        distances_ly: [("SOL".into(), 9.0)].into(),
        ..AllocationLocation::default()
    });
    assert_eq!(
        repository
            .replace_dead_allocation(
                waiting_item.id,
                allocated.by_requirement["worker"][0].id,
                &[out_of_range],
                20,
            )
            .expect("classify temporary blocker"),
        ReplacementOutcome::Waiting
    );

    let unavailable_campaign = campaign(&repository);
    let unavailable_item = item(
        &repository,
        unavailable_campaign.id,
        "unavailable",
        vec![requirement("worker", "replicant", 1, 1)],
    );
    let sibling = item(
        &repository,
        unavailable_campaign.id,
        "sibling",
        vec![requirement("worker", "replicant", 1, 1)],
    );
    let allocated = repository
        .allocate_requirements(
            unavailable_item.id,
            unavailable_item.state.revision,
            &[candidate(
                ResourceKey::Replicant("SECOND-DEAD".into()),
                "replicant",
                1,
            )],
        )
        .expect("allocate second original");
    assert_eq!(
        repository
            .replace_dead_allocation(
                unavailable_item.id,
                allocated.by_requirement["worker"][0].id,
                &[],
                30,
            )
            .expect("classify unavailable replacement"),
        ReplacementOutcome::Unavailable
    );
    assert_eq!(
        repository
            .read_work_item(unavailable_item.id)
            .expect("read unavailable item")
            .expect("item exists")
            .state
            .status,
        WorkItemStatus::Failed
    );
    assert_eq!(
        repository
            .read_work_item(sibling.id)
            .expect("read sibling")
            .expect("sibling exists")
            .state
            .status,
        WorkItemStatus::Pending
    );
}

#[test]
fn allocation_safe_reclaim_preserves_checkpoint_and_nonworker_capacity() {
    let repository = WorkflowRepository::open_in_memory().expect("open repository");
    let campaign = campaign(&repository);
    let allocation_item = item(
        &repository,
        campaign.id,
        "reclaim",
        vec![
            requirement("worker", "replicant", 1, 1),
            requirement("material", "material", 1, 2),
        ],
    );
    let candidates = [
        candidate(ResourceKey::Replicant("R-1".into()), "replicant", 1),
        candidate(
            ResourceKey::Namespaced {
                namespace: "inventory".into(),
                key: "SOL:IRON".into(),
            },
            "material",
            2,
        ),
    ];
    repository
        .allocate_requirements(
            allocation_item.id,
            allocation_item.state.revision,
            &candidates,
        )
        .expect("allocate resources");
    let assigned = repository
        .claim_next_work_item(campaign.id, 20)
        .expect("claim item")
        .expect("assigned item");
    repository
        .assign_work_item(
            assigned.id,
            assigned.state.revision,
            "grant",
            &ResourceKey::Replicant("R-1".into()),
            21,
        )
        .expect("persist assignment");
    let running = repository
        .start_work_item(assigned.id, assigned.state.revision, "R-1", "grant", 22)
        .expect("start item");
    let checkpointed = repository
        .transition_work_item(
            running.id,
            running.state.revision,
            WorkItemTransition::CheckpointCommitted {
                checkpoint_json: json!({ "safe": true }),
            },
            23,
        )
        .expect("commit safe boundary");
    assert!(
        repository
            .request_work_item_reclaim(checkpointed.id, 24)
            .expect("request reclaim")
    );
    let reclaimed = repository
        .transition_work_item(
            checkpointed.id,
            checkpointed.state.revision,
            WorkItemTransition::Reclaimed {
                checkpoint_json: None,
            },
            25,
        )
        .expect("finish safe reclaim");
    assert_eq!(reclaimed.state.status, WorkItemStatus::Pending);
    assert_eq!(
        reclaimed.state.checkpoint_json,
        Some(json!({ "safe": true }))
    );
    assert!(
        !repository
            .work_item_reclaim_requested(reclaimed.id)
            .expect("assignment released")
    );
    assert!(
        repository
            .claims(campaign.id)
            .expect("worker claim released")
            .is_empty()
    );
    let retained = repository
        .allocate_requirements(reclaimed.id, reclaimed.state.revision, &candidates)
        .expect("existing nonworker allocation remains");
    assert!(retained.by_requirement.contains_key("material"));
    assert!(retained.by_requirement.contains_key("worker"));
}

#[test]
fn retryable_failure_releases_worker_assignment_for_next_attempt() {
    let repository = WorkflowRepository::open_in_memory().expect("open repository");
    let campaign = campaign(&repository);
    let item = item(
        &repository,
        campaign.id,
        "retry",
        vec![requirement("worker", "replicant", 1, 1)],
    );
    let candidates = [candidate(
        ResourceKey::Replicant("R-1".into()),
        "replicant",
        1,
    )];
    let allocations = repository
        .allocate_requirements(item.id, item.state.revision, &candidates)
        .expect("allocate worker");
    let assigned = repository
        .claim_next_work_item(campaign.id, 20)
        .expect("claim item")
        .expect("assigned item");
    repository
        .assign_work_item(
            assigned.id,
            assigned.state.revision,
            "first-grant",
            &ResourceKey::Replicant("R-1".into()),
            21,
        )
        .expect("persist first assignment");
    let running = repository
        .start_work_item(
            assigned.id,
            assigned.state.revision,
            "R-1",
            "first-grant",
            22,
        )
        .expect("start first attempt");
    repository
        .transition_work_item(
            running.id,
            running.state.revision,
            WorkItemTransition::RetryableFailure {
                checkpoint_json: None,
                error: "temporary failure".into(),
            },
            23,
        )
        .expect("schedule retry");

    let retry = repository
        .claim_next_work_item(campaign.id, i64::MAX)
        .expect("claim retry")
        .expect("retry is assigned");
    assert_eq!(
        repository
            .allocate_requirements(retry.id, retry.state.revision, &candidates)
            .expect("reuse retained worker allocation"),
        allocations
    );
    repository
        .assign_work_item(
            retry.id,
            retry.state.revision,
            "second-grant",
            &ResourceKey::Replicant("R-1".into()),
            i64::MAX,
        )
        .expect("persist replacement assignment");
}

#[test]
fn allocation_restart_releases_exclusive_material_and_stow_pools() {
    let repository = WorkflowRepository::open_in_memory().expect("open repository");
    let first_campaign = campaign(&repository);
    let requirements = vec![
        requirement("worker", "replicant", 1, 1),
        requirement("material", "material", 1, 4),
        requirement("stow", "stow", 1, 3),
    ];
    let first = item(
        &repository,
        first_campaign.id,
        "first",
        requirements.clone(),
    );
    let candidates = [
        candidate(ResourceKey::Replicant("R-1".into()), "replicant", 1),
        candidate(
            ResourceKey::Namespaced {
                namespace: "inventory".into(),
                key: "SOL:IRON".into(),
            },
            "material",
            4,
        ),
        candidate(
            ResourceKey::Namespaced {
                namespace: "stow".into(),
                key: "VESSEL-1".into(),
            },
            "stow",
            3,
        ),
    ];
    repository
        .allocate_requirements(first.id, first.state.revision, &candidates)
        .expect("allocate all pool kinds");
    let assigned = repository
        .claim_next_work_item(first_campaign.id, 20)
        .expect("claim first item")
        .expect("assigned first item");
    repository
        .assign_work_item(
            assigned.id,
            assigned.state.revision,
            "grant",
            &ResourceKey::Replicant("R-1".into()),
            21,
        )
        .expect("persist assignment");
    repository
        .start_work_item(assigned.id, assigned.state.revision, "R-1", "grant", 22)
        .expect("start first item");
    assert_eq!(
        repository
            .reconcile_orphaned_work_items(Some(first_campaign.id), 30)
            .expect("restart reconcile"),
        1
    );
    assert!(
        repository
            .claims(first_campaign.id)
            .expect("claims released")
            .is_empty()
    );

    let second_campaign = campaign(&repository);
    let second = item(&repository, second_campaign.id, "second", requirements);
    repository
        .allocate_requirements(second.id, second.state.revision, &candidates)
        .expect("all restarted pool capacity is reusable");
}

#[test]
fn allocation_file_backed_races_never_overbook_pools() {
    for (label, resource, kind, available, requested) in [
        (
            "exclusive",
            ResourceKey::Replicant("R-RACE".into()),
            "replicant",
            1,
            1,
        ),
        (
            "material",
            ResourceKey::Namespaced {
                namespace: "inventory".into(),
                key: "SOL:IRON".into(),
            },
            "material",
            4,
            3,
        ),
        (
            "stow",
            ResourceKey::Namespaced {
                namespace: "stow".into(),
                key: "VESSEL-RACE".into(),
            },
            "stow",
            4,
            3,
        ),
    ] {
        let path = std::env::temp_dir().join(format!(
            "replicant-allocation-{label}-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let repository = WorkflowRepository::open(&path).expect("open race repository");
        let first_campaign = campaign(&repository);
        let second_campaign = campaign(&repository);
        let first = item(
            &repository,
            first_campaign.id,
            "first",
            vec![requirement("pool", kind, 1, requested)],
        );
        let second = item(
            &repository,
            second_campaign.id,
            "second",
            vec![requirement("pool", kind, 1, requested)],
        );
        drop(repository);
        let barrier = Arc::new(Barrier::new(2));
        let handles: Vec<_> = [first, second]
            .into_iter()
            .map(|item| {
                let path = path.clone();
                let barrier = barrier.clone();
                let candidate = candidate(resource.clone(), kind, available);
                std::thread::spawn(move || {
                    let repository = WorkflowRepository::open(path).expect("open racing handle");
                    barrier.wait();
                    repository
                        .allocate_requirements(
                            item.id,
                            item.state.revision,
                            std::slice::from_ref(&candidate),
                        )
                        .is_ok()
                })
            })
            .collect();
        let winners = handles
            .into_iter()
            .map(|handle| handle.join().expect("allocation thread"))
            .filter(|won| *won)
            .count();
        assert_eq!(winners, 1, "{label} pool overbooked");
        std::fs::remove_file(path).expect("remove race database");
    }
}

#[test]
fn allocation_workflow_cancellation_releases_every_pool_and_assignment() {
    let repository = WorkflowRepository::open_in_memory().expect("open repository");
    let first_campaign = campaign(&repository);
    let requirements = vec![
        requirement("worker", "replicant", 1, 1),
        requirement("material", "material", 1, 4),
        requirement("stow", "stow", 1, 3),
    ];
    let first = item(
        &repository,
        first_campaign.id,
        "cancelled",
        requirements.clone(),
    );
    let candidates = [
        candidate(ResourceKey::Replicant("R-CANCEL".into()), "replicant", 1),
        candidate(
            ResourceKey::Namespaced {
                namespace: "inventory".into(),
                key: "SOL:CANCEL-IRON".into(),
            },
            "material",
            4,
        ),
        candidate(
            ResourceKey::Namespaced {
                namespace: "stow".into(),
                key: "VESSEL-CANCEL".into(),
            },
            "stow",
            3,
        ),
    ];
    repository
        .allocate_requirements(first.id, first.state.revision, &candidates)
        .expect("allocate cancellation fixture");
    let assigned = repository
        .claim_next_work_item(first_campaign.id, 20)
        .expect("claim item")
        .expect("assigned item");
    repository
        .assign_work_item(
            assigned.id,
            assigned.state.revision,
            "cancel-grant",
            &ResourceKey::Replicant("R-CANCEL".into()),
            21,
        )
        .expect("persist assignment");
    repository
        .update(
            first_campaign.id,
            first_campaign.revision,
            WorkflowState::<_, ()> {
                status: WorkflowStatus::Cancelled,
                current_step: None,
                checkpoint: json!({}),
                last_error: None,
                result: None,
            },
        )
        .expect("cancel workflow");
    assert!(
        repository
            .claims(first_campaign.id)
            .expect("claims released")
            .is_empty()
    );
    assert!(
        !repository
            .work_item_reclaim_requested(first.id)
            .expect("assignment released")
    );
    assert_eq!(
        repository
            .read_work_item(first.id)
            .expect("read cancelled item")
            .expect("item exists")
            .state
            .status,
        WorkItemStatus::Abandoned
    );

    let second_campaign = campaign(&repository);
    let second = item(&repository, second_campaign.id, "replacement", requirements);
    repository
        .allocate_requirements(second.id, second.state.revision, &candidates)
        .expect("cancelled pool capacity is reusable");
}
