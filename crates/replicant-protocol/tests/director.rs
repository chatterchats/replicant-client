//! Backward-compatible Director observations and stable goal identity.

use replicant_protocol::{DirectorGoalKind, DirectorSnapshot};
use serde_json::{Value, json};

fn legacy_snapshot() -> Value {
    json!({
        "metadata": {"revision": 1, "generated_at_ms": 10},
        "mode": "advisory",
        "regions": [],
        "goals": [{
            "id": "expand_mining_ops:beta", "kind": "expand_mining_ops", "region": "beta",
            "status": "active", "objective": "Expand mining operations", "blocker": null,
            "next_action": null, "progress_current": 1, "progress_total": 2,
            "active_workflows": [], "enabled": true
        }],
        "replicants": [],
        "workforce": {
            "total": 0, "busy": 0, "idle": 0, "idle_ratio": 1.0,
            "pending_worker_demand": 0, "scale_up_recommended": false, "scale_reason": null
        }
    })
}

#[test]
fn legacy_director_keeps_mining_identity_without_observed_health() {
    let snapshot: DirectorSnapshot =
        serde_json::from_value(legacy_snapshot()).expect("legacy snapshot");
    assert!(snapshot.mining_ops.is_empty());
    assert!(snapshot.mining_policies.is_empty());
    assert_eq!(snapshot.goals[0].kind, DirectorGoalKind::ExpandMiningOps);
    let wire = serde_json::to_value(snapshot).expect("serialize snapshot");
    assert_eq!(wire["goals"][0]["id"], "expand_mining_ops:beta");
    assert_eq!(wire["goals"][0]["kind"], "expand_mining_ops");
}

#[test]
fn mining_health_preserves_backlog_and_unknown_hub_without_changing_policy() {
    let mut wire = legacy_snapshot();
    let health = json!({
        "region": "beta", "healthy_sites": 6, "total_sites": 6,
        "healthy_routes": 5, "total_routes": 6, "active_cargo_freighters": 9,
        "backlogged_routes": 1, "worst_backlog": {"location": "KHIKHKUWU-BELT-1", "quantity": 54979},
        "backlog_known": true, "healthy_remote_maintenance": 5, "total_remote_sites": 6,
        "hub_ready": null, "hub_minimum": 2, "hub_target": 3,
        "priority_protected": 4, "priority_target": 4, "expansion_candidates": 3
    });
    wire["mining_ops"] = json!([health]);
    let snapshot: DirectorSnapshot = serde_json::from_value(wire).expect("structured snapshot");
    assert_eq!(snapshot.mining_ops[0].hub_ready, None);
    assert_eq!(
        snapshot.mining_ops[0]
            .worst_backlog
            .as_ref()
            .expect("backlog")
            .quantity,
        54979
    );
    assert!(snapshot.mining_policies.is_empty());
    assert_eq!(
        serde_json::to_value(snapshot).expect("serialize")["mining_ops"][0],
        health
    );
}
