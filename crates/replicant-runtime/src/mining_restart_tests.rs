use super::*;
use replicant_client::{SecretString, StartupPolicy, raw::Url};
use replicant_workflow::{WorkflowInstance, WorkflowRepository, WorkflowState, WorkflowSupervisor};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

fn rotation_intent() -> MiningMaintenanceRotationIntent {
    MiningMaintenanceRotationIntent {
        region: "alpha".into(),
        system: "REMOTE".into(),
        site_belt: "REMOTE-BELT-1".into(),
        hub_belt: "HUB-BELT-1".into(),
        worn_drone: "WORN".into(),
        replacement_candidates: vec!["SPARE".into()],
    }
}

fn capacity_intent() -> MiningTransportCapacityIntent {
    MiningTransportCapacityIntent {
        region: "alpha".into(),
        system: "REMOTE".into(),
        collect: "REMOTE-BELT-1".into(),
        deliver: "HUB-BELT-1".into(),
        controller: "TC".into(),
        target_freighters: 1,
        reuse_candidates: Vec::new(),
    }
}

async fn client_at(server: &MockServer) -> Client {
    Client::builder()
        .base_url(Url::parse(&server.uri()).expect("URL"))
        .authentication_token(SecretString::from("fixture"))
        .in_memory()
        .startup_policy(StartupPolicy::RestoreOnly)
        .start()
        .await
        .expect("client")
}

async fn seed_device(server: &MockServer, client: &Client, value: Value) {
    let code = value["device_code"].as_str().expect("device code");
    Mock::given(method("GET"))
        .and(path(format!("/v1/devices/{code}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(value.clone()))
        .mount(server)
        .await;
    client
        .devices()
        .refresh(code)
        .await
        .expect("seed owned device");
}

fn settle_child(repository: &WorkflowRepository, child: WorkflowInstance, status: WorkflowStatus) {
    let running = repository
        .update(
            child.id,
            child.revision,
            WorkflowState {
                status: WorkflowStatus::Running,
                current_step: child.current_step.clone(),
                checkpoint: child.checkpoint::<Value>().expect("child checkpoint"),
                last_error: None,
                result: None::<Value>,
            },
        )
        .expect("running child");
    repository
        .update(
            running.id,
            running.revision,
            WorkflowState {
                status,
                current_step: running.current_step.clone(),
                checkpoint: running.checkpoint::<Value>().expect("child checkpoint"),
                last_error: None,
                result: None::<Value>,
            },
        )
        .expect("settled child");
}

async fn resume_once(
    repository: Arc<WorkflowRepository>,
    client: Client,
    id: WorkflowId,
) -> WorkflowInstance {
    let mut registry = WorkflowRegistry::new();
    register(&mut registry).expect("runtime registry");
    let supervisor =
        WorkflowSupervisor::with_managed_client(repository.clone(), Arc::new(registry), client);
    for _ in 0..1000 {
        supervisor.tick().await.expect("tick");
        let row = repository.read(id).expect("read").expect("workflow");
        if row.status == WorkflowStatus::Waiting || row.status.is_terminal() {
            assert_ne!(row.status, WorkflowStatus::Failed, "{:?}", row.last_error);
            return row;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    panic!("workflow did not settle");
}

#[tokio::test]
async fn regional_dispatch_restart_adopts_tagged_output_without_reprinting() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/blueprints"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"blueprints": []})),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/inventory"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"locations": [], "next_cursor": null})),
        )
        .mount(&server)
        .await;
    let client = client_at(&server).await;
    seed_device(
        &server,
        &client,
        serde_json::json!({
            "device_code": "PRINTED",
            "device_type": "maintenance_drone",
            "location": "HUB-BELT-1",
            "status": "idle",
            "tags": ["dispatch:crash"]
        }),
    )
    .await;
    let repository = Arc::new(WorkflowRepository::open_in_memory().expect("repository"));
    let mut request = new_regional_dispatch_workflow(RegionalDispatchIntent {
        source: "HUB-BELT-1".into(),
        destination: "HUB-BELT-1".into(),
        devices: vec![DeviceRequest {
            device_type: "maintenance_drone".into(),
            quantity: 1,
        }],
        ..Default::default()
    });
    request.checkpoint = RegionalDispatchCheckpoint {
        source_location: Some("HUB-BELT-1".into()),
        print_tag: "dispatch:crash".into(),
        selection_complete: true,
        print_requests: vec![PrintRequest::new("maintenance_drone", 1)],
        plan: Some(DeliveryPlan {
            origin: "HUB-BELT-1".into(),
            destination: "HUB-BELT-1".into(),
            payload_devices: vec![PayloadDevice {
                code: "PRINTED".into(),
                device_type: "maintenance_drone".into(),
                origin: "HUB-BELT-1".into(),
            }],
            device_carriers: vec!["UNUSED".into()],
            ..Default::default()
        }),
        ..Default::default()
    };
    let workflow = repository.create(request).expect("regional dispatch");

    let resumed = resume_once(repository, client.clone(), workflow.id).await;
    assert_eq!(resumed.status, WorkflowStatus::Succeeded);
    let checkpoint: RegionalDispatchCheckpoint = resumed.checkpoint().expect("checkpoint");
    assert!(checkpoint.manufacturing_complete);
    assert_eq!(checkpoint.devices, ["PRINTED"]);
    assert!(
        server
            .received_requests()
            .await
            .expect("requests")
            .iter()
            .all(|request| request.method == "GET"),
        "accepted tagged output must suppress another print submission"
    );
    client.close().await.expect("close");
}

#[tokio::test]
async fn mining_restart_keeps_provisioned_replacement_when_fresh_stock_appears() {
    let server = MockServer::start().await;
    let client = client_at(&server).await;
    seed_device(
        &server,
        &client,
        serde_json::json!({
            "device_code": "SPARE", "device_type": "maintenance_drone",
            "location": "HUB-BELT-1", "status": "idle", "operational_capacity": 100.0
        }),
    )
    .await;
    let database =
        std::env::temp_dir().join(format!("mining-restart-{}.sqlite", uuid::Uuid::new_v4()));
    let repository = Arc::new(WorkflowRepository::open(&database).expect("repository"));
    let parent = repository
        .create(new_mining_maintenance_rotation_workflow(rotation_intent()))
        .expect("rotation");
    let mut request = new_regional_dispatch_workflow(RegionalDispatchIntent {
        source: "HUB-BELT-1".into(),
        destination: "REMOTE-BELT-1".into(),
        devices: vec![DeviceRequest {
            device_type: "maintenance_drone".into(),
            quantity: 1,
        }],
        ..Default::default()
    });
    request.parent_id = Some(parent.id);
    let child = repository
        .create(request)
        .expect("provision child before parent checkpoint");
    let child_id = child.id;
    settle_child(&repository, child, WorkflowStatus::Paused);
    drop(repository);
    let repository = Arc::new(WorkflowRepository::open(&database).expect("reopened repository"));
    let resumed = resume_once(repository.clone(), client.clone(), parent.id).await;
    let checkpoint: MiningMaintenanceRotationCheckpoint = resumed.checkpoint().expect("checkpoint");
    assert_eq!(
        resumed.current_step.as_deref(),
        Some("provisioning_replacement")
    );
    assert_eq!(checkpoint.provision_child, Some(child_id));
    assert!(checkpoint.replacement.is_none());
    assert!(checkpoint.delivery_child.is_none());
    assert_eq!(repository.list().expect("workflows").len(), 2);
    client.close().await.expect("close");
    drop(repository);
    std::fs::remove_file(database).expect("remove fixture database");
}

#[tokio::test]
async fn mining_restart_after_replacement_delivery_and_patrol_recovers_only_worn_drone() {
    let server = MockServer::start().await;
    let client = client_at(&server).await;
    let intent = rotation_intent();
    seed_device(&server, &client, serde_json::json!({
        "device_code": "READY", "device_type": "maintenance_drone",
        "location": "REMOTE-BELT-1", "status": "patrolling",
        "tags": [replicant_mining_planner::site_tag("REMOTE"), replicant_mining_planner::role_tag("maintenance")],
        "ami_directive": {"directive": "patrol"}, "ami_directive_status": "active"
    })).await;
    seed_device(&server, &client, serde_json::json!({
        "device_code": "WORN", "device_type": "maintenance_drone",
        "location": "REMOTE-BELT-1", "status": "deactivated", "attached_to_device_code": "CARRIER"
    })).await;
    let repository = Arc::new(WorkflowRepository::open_in_memory().expect("repository"));
    let mut request = new_mining_maintenance_rotation_workflow(intent.clone());
    request.checkpoint.replacement = Some("READY".into());
    // Delivery and patrol succeeded remotely, but neither parent flag was saved.
    let parent = repository.create(request).expect("rotation");
    let mut recovery = new_logistics_manifest_workflow(maintenance_recovery_manifest(
        &intent,
        intent.site_belt.clone(),
    ));
    recovery.parent_id = Some(parent.id);
    repository
        .acquire_claim(parent.id, ResourceKey::Device(intent.worn_drone.clone()))
        .expect("parent worn-drone custody");
    let child = repository
        .create_with_parent_claims(recovery, &[ResourceKey::Device(intent.worn_drone.clone())])
        .expect("recovery created before checkpoint");
    let child_id = child.id;
    settle_child(&repository, child, WorkflowStatus::Paused);
    let resumed = resume_once(repository.clone(), client.clone(), parent.id).await;
    let checkpoint: MiningMaintenanceRotationCheckpoint = resumed.checkpoint().expect("checkpoint");
    assert!(checkpoint.replacement_delivered);
    assert!(checkpoint.replacement_patrol_verified);
    assert_eq!(checkpoint.recovery_child, Some(child_id));
    assert!(!checkpoint.worn_returned);
    assert_eq!(
        resumed.current_step.as_deref(),
        Some("recovering_worn_drone")
    );
    assert_eq!(repository.list().expect("workflows").len(), 2);
    assert!(
        server
            .received_requests()
            .await
            .expect("requests")
            .iter()
            .all(|request| request.method == "GET")
    );
    client.close().await.expect("close");
}

#[tokio::test]
async fn mining_restart_excess_pickup_requires_authoritative_hub_return() {
    for (location, attached) in [
        ("REMOTE-BELT-1", true),
        ("HUB-BELT-1", true),
        ("HUB-BELT-1", false),
    ] {
        let server = MockServer::start().await;
        let client = client_at(&server).await;
        seed_device(
            &server,
            &client,
            serde_json::json!({
                "device_code": "EXTRA", "device_type": "cargo_freighter",
                "location": location, "status": "idle",
                "attached_to_device_code": if attached { Some("CARRIER") } else { None }
            }),
        )
        .await;
        let repository = Arc::new(WorkflowRepository::open_in_memory().expect("repository"));
        let intent = capacity_intent();
        let mut request = new_mining_transport_capacity_workflow(intent.clone());
        request.checkpoint.release_freighter = Some("EXTRA".into());
        request.checkpoint.release_complete = true;
        let parent = repository.create(request).expect("capacity");
        let mut request = new_logistics_manifest_workflow(LogisticsManifestIntent {
            origin: "REMOTE-BELT-1".into(),
            destination: intent.deliver.clone(),
            device_codes: vec!["EXTRA".into()],
            purpose: format!(
                "mining-transport-capacity:return:{}:EXTRA",
                mining_transport_capacity_route_key(&intent)
            ),
            ..Default::default()
        });
        request.parent_id = Some(parent.id);
        let child = repository.create(request).expect("return child");
        let child_id = child.id;
        settle_child(&repository, child, WorkflowStatus::Succeeded);
        let resumed = resume_once(repository.clone(), client.clone(), parent.id).await;
        assert_eq!(
            resumed
                .checkpoint::<MiningTransportCapacityCheckpoint>()
                .expect("checkpoint")
                .return_child,
            Some(child_id)
        );
        assert_eq!(
            resumed.status,
            if location == "HUB-BELT-1" && !attached {
                WorkflowStatus::Succeeded
            } else {
                WorkflowStatus::Waiting
            }
        );
        assert_eq!(repository.list().expect("workflows").len(), 2);
        assert!(
            server
                .received_requests()
                .await
                .expect("requests")
                .iter()
                .all(|request| request.method == "GET")
        );
        client.close().await.expect("close");
    }
}

#[test]
fn mining_restart_legacy_empty_checkpoints_have_no_invented_history() {
    let capacity: MiningTransportCapacityCheckpoint =
        serde_json::from_str("{}").expect("legacy capacity checkpoint");
    assert!(capacity.release_freighter.is_none());
    assert!(!capacity.release_complete);
    let rotation: MiningMaintenanceRotationCheckpoint =
        serde_json::from_str("{}").expect("legacy rotation checkpoint");
    assert!(!maintenance_rotation_recovery_allowed(&rotation));
    let trend: crate::mining::TransportCapacityTrend =
        serde_json::from_str("{}").expect("legacy trend");
    assert_eq!(trend, crate::mining::TransportCapacityTrend::default());
}

async fn seed_route(server: &MockServer, client: &Client, adopted_new: bool) {
    let controlled = if adopted_new {
        vec!["BASE", "NEW"]
    } else {
        vec!["BASE"]
    };
    seed_device(server, client, serde_json::json!({
        "device_code": "TC", "device_type": "ami_transport_controller",
        "location": "REMOTE-BELT-1", "status": "coordinating",
        "tags": [replicant_mining_planner::site_tag("REMOTE"), replicant_mining_planner::role_tag("transport-controller")],
        "ami_directive": {"directive": "ferry", "config": {
            "collect": "REMOTE-BELT-1", "deliver": "HUB-BELT-1"
        }},
        "ami_directive_status": "active",
        "controlled_devices": controlled.iter().map(|code| serde_json::json!({"device_code": code})).collect::<Vec<_>>()
    })).await;
    for code in ["BASE", "NEW"] {
        seed_device(server, client, serde_json::json!({
            "device_code": code, "device_type": "cargo_freighter",
            "location": "REMOTE-BELT-1", "status": "idle",
            "controller_device_code": if code == "BASE" || adopted_new { Some("TC") } else { None }
        })).await;
    }
    Mock::given(method("GET"))
        .and(path("/v1/devices"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "devices": (["BASE", "NEW"].iter().map(|code| serde_json::json!({
                "device_code": code, "device_type": "cargo_freighter",
                "location": "REMOTE-BELT-1", "status": "idle",
                "controller_device_code": if *code == "BASE" || adopted_new { Some("TC") } else { None }
            })).collect::<Vec<_>>()),
            "next_cursor": null
        })))
        .mount(server).await;
}

#[tokio::test]
async fn mining_authority_remote_census_discovers_hidden_freighter_before_provisioning() {
    let server = MockServer::start().await;
    let client = client_at(&server).await;
    seed_device(&server, &client, serde_json::json!({
        "device_code": "TC", "device_type": "ami_transport_controller",
        "location": "REMOTE-BELT-1", "status": "coordinating",
        "tags": [replicant_mining_planner::site_tag("REMOTE"), replicant_mining_planner::role_tag("transport-controller")],
        "ami_directive": {"directive": "ferry", "config": {
            "collect": "REMOTE-BELT-1", "deliver": "HUB-BELT-1"
        }},
        "ami_directive_status": "active",
        "controlled_devices": [{"device_code": "BASE"}]
    })).await;
    let freighters = ["BASE", "HIDDEN"].map(|code| {
        serde_json::json!({
            "device_code": code, "device_type": "cargo_freighter",
            "location": "REMOTE-BELT-1", "status": "idle", "controller_device_code": "TC"
        })
    });
    seed_device(&server, &client, freighters[0].clone()).await;
    Mock::given(method("GET"))
        .and(path("/v1/devices/HIDDEN"))
        .respond_with(ResponseTemplate::new(200).set_body_json(freighters[1].clone()))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/devices"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "devices": freighters, "next_cursor": null
        })))
        .expect(1)
        .mount(&server)
        .await;
    let repository = Arc::new(WorkflowRepository::open_in_memory().expect("repository"));
    let mut intent = capacity_intent();
    intent.target_freighters = 3;
    let workflow = repository
        .create(new_mining_transport_capacity_workflow(intent))
        .expect("capacity");
    let resumed = resume_once(repository.clone(), client.clone(), workflow.id).await;
    assert_eq!(resumed.status, WorkflowStatus::Waiting);
    assert_eq!(repository.list().expect("workflows").len(), 1);
    assert!(
        server
            .received_requests()
            .await
            .expect("requests")
            .iter()
            .all(|request| request.method == "GET")
    );
    client.close().await.expect("close");
}

#[tokio::test]
async fn mining_authority_hub_patrol_health_never_becomes_a_spurious_print_deficit() {
    for (capacity, expected) in [
        (None, WorkflowStatus::Waiting),
        (Some(94.9), WorkflowStatus::Waiting),
        (Some(95.0), WorkflowStatus::Succeeded),
    ] {
        let server = MockServer::start().await;
        let client = client_at(&server).await;
        let drones = ["HUB-1", "HUB-2"].map(|code| {
            serde_json::json!({
                "device_code": code, "device_type": "maintenance_drone",
                "location": "HUB-BELT-1", "status": "idle", "operational_capacity": capacity,
                "tags": [replicant_mining_planner::role_tag(HUB_MAINTENANCE_ROLE)],
                "ami_directive": {"directive": "patrol"}, "ami_directive_status": "active"
            })
        });
        for drone in &drones {
            seed_device(&server, &client, drone.clone()).await;
        }
        Mock::given(method("GET"))
            .and(path("/v1/devices"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "devices": drones, "next_cursor": null
            })))
            .mount(&server)
            .await;
        let repository = Arc::new(WorkflowRepository::open_in_memory().expect("repository"));
        let workflow = repository
            .create(new_mining_maintenance_pool_workflow(
                MiningMaintenancePoolIntent {
                    region: "alpha".into(),
                    hub_belt: "HUB-BELT-1".into(),
                    target_healthy: 2,
                    reuse_candidates: Vec::new(),
                },
            ))
            .expect("pool");
        let resumed = resume_once(repository.clone(), client.clone(), workflow.id).await;
        assert_eq!(resumed.status, expected, "{capacity:?}");
        assert_eq!(repository.list().expect("workflows").len(), 1);
        assert!(
            server
                .received_requests()
                .await
                .expect("requests")
                .iter()
                .all(|request| request.method == "GET")
        );
        client.close().await.expect("close");
    }
}

#[tokio::test]
async fn mining_rotation_rechecks_wear_and_discovers_dynamic_hub_stock() {
    for (capacity, location, stocked, expected_status, needs_child) in [
        (None, "REMOTE-BELT-1", false, WorkflowStatus::Waiting, false),
        (
            Some(30.0),
            "REMOTE-BELT-1",
            false,
            WorkflowStatus::Succeeded,
            false,
        ),
        (
            Some(29.9),
            "HUB-BELT-1",
            false,
            WorkflowStatus::Succeeded,
            false,
        ),
        (
            Some(29.9),
            "REMOTE-BELT-1",
            false,
            WorkflowStatus::Waiting,
            true,
        ),
        (
            Some(29.9),
            "REMOTE-BELT-1",
            true,
            WorkflowStatus::Waiting,
            true,
        ),
    ] {
        let server = MockServer::start().await;
        let client = client_at(&server).await;
        seed_device(
            &server,
            &client,
            serde_json::json!({
                "device_code": "WORN", "device_type": "maintenance_drone",
                "location": location, "status": "idle", "operational_capacity": capacity,
                "ami_directive": {"directive": "patrol"}, "ami_directive_status": "active"
            }),
        )
        .await;
        let drones = (0..if stocked { 3 } else { 0 })
            .map(|index| {
                serde_json::json!({
                    "device_code": format!("POOL-{index}"), "device_type": "maintenance_drone",
                    "location": "HUB-BELT-1", "status": "idle", "operational_capacity": 95,
                    "tags": [replicant_mining_planner::role_tag(HUB_MAINTENANCE_ROLE)],
                    "ami_directive": {"directive": "patrol"}, "ami_directive_status": "active"
                })
            })
            .collect::<Vec<_>>();
        for drone in &drones {
            Mock::given(method("GET"))
                .and(path(format!(
                    "/v1/devices/{}",
                    drone["device_code"].as_str().expect("code")
                )))
                .respond_with(ResponseTemplate::new(200).set_body_json(drone))
                .mount(&server)
                .await;
        }
        Mock::given(method("GET"))
            .and(path("/v1/devices"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "devices": drones, "next_cursor": null
            })))
            .mount(&server)
            .await;
        let repository = Arc::new(WorkflowRepository::open_in_memory().expect("repository"));
        let workflow = repository
            .create(new_mining_maintenance_rotation_workflow(rotation_intent()))
            .expect("rotation");
        let resumed = resume_once(repository.clone(), client.clone(), workflow.id).await;
        assert_eq!(
            resumed.status, expected_status,
            "{capacity:?}, location={location}, stocked={stocked}"
        );
        let children = repository.list_children(workflow.id).expect("children");
        assert_eq!(children.len(), usize::from(needs_child));
        if let Some(child) = children.first() {
            assert_eq!(
                child.kind,
                if stocked {
                    logistics_manifest_workflow_kind()
                } else {
                    regional_dispatch_workflow_kind()
                }
            );
            let checkpoint: MiningMaintenanceRotationCheckpoint =
                resumed.checkpoint().expect("checkpoint");
            assert_eq!(checkpoint.replacement.is_some(), stocked);
            if !stocked {
                assert_eq!(
                    child
                        .config::<RegionalDispatchIntent>()
                        .expect("dispatch")
                        .devices[0]
                        .quantity,
                    1
                );
            }
        }
        assert!(
            server
                .received_requests()
                .await
                .expect("requests")
                .iter()
                .all(|request| request.method == "GET")
        );
        client.close().await.expect("close");
    }
}

#[tokio::test]
async fn mining_restart_after_freighter_delivery_recovers_selected_stock_before_printing() {
    for arrived in [true, false] {
        let server = MockServer::start().await;
        let client = client_at(&server).await;
        seed_route(&server, &client, false).await;
        if !arrived {
            // The collection projection says arrived; the exact device read disagrees.
            Mock::given(method("GET"))
                .and(path("/v1/devices/NEW"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "device_code": "NEW", "device_type": "cargo_freighter",
                    "location": "HUB-BELT-1", "status": "idle"
                })))
                .with_priority(1)
                .mount(&server)
                .await;
        }
        let repository = Arc::new(WorkflowRepository::open_in_memory().expect("repository"));
        let mut intent = capacity_intent();
        intent.target_freighters = 3;
        let parent = repository
            .create(new_mining_transport_capacity_workflow(intent.clone()))
            .expect("capacity");
        let mut delivery = new_logistics_manifest_workflow(LogisticsManifestIntent {
            origin: "HUB-BELT-1".into(),
            destination: "REMOTE-BELT-1".into(),
            device_codes: vec!["NEW".into()],
            purpose: format!(
                "mining-transport-capacity:stage:{}:NEW",
                mining_transport_capacity_route_key(&intent)
            ),
            ..Default::default()
        });
        delivery.parent_id = Some(parent.id);
        let delivered = repository
            .create(delivery)
            .expect("delivery before parent checkpoint");
        let delivered_id = delivered.id;
        settle_child(&repository, delivered, WorkflowStatus::Succeeded);
        let resumed = resume_once(repository.clone(), client.clone(), parent.id).await;
        let checkpoint: MiningTransportCapacityCheckpoint =
            resumed.checkpoint().expect("checkpoint");
        assert_eq!(checkpoint.selected_freighters, ["NEW"]);
        assert_eq!(checkpoint.staging_children.get("NEW"), Some(&delivered_id));
        if arrived {
            let dispatch = repository
                .read(checkpoint.dispatch_child.expect("remaining deficit"))
                .expect("read")
                .expect("dispatch");
            let config: RegionalDispatchIntent = dispatch.config().expect("dispatch config");
            assert_eq!(
                config.devices[0].quantity, 1,
                "delivered freighter must not be printed again"
            );
        } else {
            assert_eq!(resumed.status, WorkflowStatus::Waiting);
            assert!(checkpoint.dispatch_child.is_none());
            assert_eq!(
                repository.list_children(parent.id).expect("children").len(),
                1
            );
        }
        client.close().await.expect("close");
    }
}

#[tokio::test]
async fn mining_restart_after_adoption_verifies_route_without_another_delivery() {
    let server = MockServer::start().await;
    let client = client_at(&server).await;
    seed_route(&server, &client, true).await;
    let repository = Arc::new(WorkflowRepository::open_in_memory().expect("repository"));
    let mut intent = capacity_intent();
    intent.target_freighters = 2;
    let parent = repository
        .create(new_mining_transport_capacity_workflow(intent))
        .expect("capacity");
    let resumed = resume_once(repository.clone(), client.clone(), parent.id).await;
    assert_eq!(resumed.status, WorkflowStatus::Succeeded);
    assert_eq!(repository.list().expect("workflows").len(), 1);
    assert!(
        server
            .received_requests()
            .await
            .expect("requests")
            .iter()
            .all(|request| request.method == "GET")
    );
    client.close().await.expect("close");
}

#[tokio::test]
async fn mining_restart_after_excess_release_never_selects_a_second_freighter() {
    let server = MockServer::start().await;
    let client = client_at(&server).await;
    seed_route(&server, &client, false).await;
    seed_device(
        &server,
        &client,
        serde_json::json!({
            "device_code": "EXTRA", "device_type": "cargo_freighter",
            "location": "HUB-BELT-1", "status": "idle"
        }),
    )
    .await;
    let repository = Arc::new(WorkflowRepository::open_in_memory().expect("repository"));
    let mut request = new_mining_transport_capacity_workflow(capacity_intent());
    request.checkpoint.release_freighter = Some("EXTRA".into());
    let parent = repository
        .create(request)
        .expect("capacity before release-complete checkpoint");
    let resumed = resume_once(repository.clone(), client.clone(), parent.id).await;
    assert_eq!(resumed.status, WorkflowStatus::Succeeded);
    let checkpoint: MiningTransportCapacityCheckpoint = resumed.checkpoint().expect("checkpoint");
    assert_eq!(checkpoint.release_freighter.as_deref(), Some("EXTRA"));
    assert!(checkpoint.release_complete);
    assert!(
        server
            .received_requests()
            .await
            .expect("requests")
            .iter()
            .all(|request| request.method == "GET")
    );
    client.close().await.expect("close");
}

#[tokio::test]
async fn mining_restart_print_with_lost_operation_callback_submits_only_once() {
    use replicant_printing::managed::{
        QueueOptions, TrackedPrintRequest, TrackedPrintUpdate, queue_tracked_prints_once,
    };

    let server = MockServer::start().await;
    let client = client_at(&server).await;
    seed_device(
        &server,
        &client,
        serde_json::json!({
            "device_code": "AF-RESTART", "device_type": "autofactory",
            "location": "HUB-BELT-1", "status": "idle", "queue_size": 4
        }),
    )
    .await;
    Mock::given(method("POST"))
        .and(path("/v1/devices/AF-RESTART"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .expect(1)
        .mount(&server)
        .await;
    let blueprints = BTreeMap::from([(
        "maintenance_drone".to_owned(),
        replicant_printing::Blueprint {
            device_type: "maintenance_drone".into(),
            ..Default::default()
        },
    )]);
    let options = QueueOptions::at("HUB-BELT-1");
    for lost_callback in [true, false] {
        // Reconstructed from the durable mission/batch identity, not the lost callback.
        let mut request =
            TrackedPrintRequest::new("maintenance_drone", 1).authoritative_factory_check();
        request.operation_id = Some(OperationId::from("mining:legacy:print:mine-b:one"));
        let result =
            queue_tracked_prints_once(&client, &[request], &options, &blueprints, |update| {
                match update {
                    TrackedPrintUpdate::Preparing(_) => Ok(Some(vec!["mine-b:one".into()])),
                    TrackedPrintUpdate::OperationRecorded { .. } if lost_callback => {
                        Err("crash before workflow checkpoint".into())
                    }
                    _ => Ok(None),
                }
            })
            .await;
        if lost_callback {
            assert!(result.is_err());
        } else {
            let report = result.expect("resume the accepted operation");
            assert_eq!(report.submissions.len(), 1);
        }
    }
    server.verify().await;
    client.close().await.expect("close");
}

#[tokio::test]
async fn mining_pool_reuses_stock_after_claim_conflicts() {
    let server = MockServer::start().await;
    let client = client_at(&server).await;
    let idle = |code: &str| {
        serde_json::json!({
            "device_code": code, "device_type": "maintenance_drone",
            "location": "HUB-BELT-1", "status": "idle", "operational_capacity": 100,
            "tags": [replicant_mining_planner::role_tag(HUB_MAINTENANCE_ROLE)]
        })
    };
    for code in ["POOL-A", "POOL-B"] {
        Mock::given(method("GET"))
            .and(path(format!("/v1/devices/{code}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(idle(code)))
            .mount(&server)
            .await;
    }
    Mock::given(method("GET"))
        .and(path("/v1/devices/POOL-C"))
        .respond_with(ResponseTemplate::new(200).set_body_json(idle("POOL-C")))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&server)
        .await;
    let mut patrolling = idle("POOL-C");
    patrolling["ami_directive"] = serde_json::json!({"directive": "patrol"});
    patrolling["ami_directive_status"] = serde_json::json!("active");
    Mock::given(method("GET"))
        .and(path("/v1/devices/POOL-C"))
        .respond_with(ResponseTemplate::new(200).set_body_json(patrolling))
        .with_priority(2)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/devices"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "devices": [idle("POOL-A"), idle("POOL-B"), idle("POOL-C")], "next_cursor": null
        })))
        .mount(&server)
        .await;
    let repository = Arc::new(WorkflowRepository::open_in_memory().expect("repository"));
    let holder = repository
        .create(new_mining_transport_capacity_workflow(capacity_intent()))
        .expect("holder");
    let holder_id = holder.id;
    settle_child(&repository, holder, WorkflowStatus::Paused);
    for code in ["POOL-A", "POOL-B"] {
        repository
            .acquire_claim(holder_id, ResourceKey::Device(code.into()))
            .expect("foreign custody");
    }
    let pool = repository
        .create(new_mining_maintenance_pool_workflow(
            MiningMaintenancePoolIntent {
                region: "alpha".into(),
                hub_belt: "HUB-BELT-1".into(),
                target_healthy: 2,
                reuse_candidates: vec!["POOL-A".into(), "POOL-B".into(), "POOL-C".into()],
            },
        ))
        .expect("pool");
    let resumed = resume_once(repository.clone(), client.clone(), pool.id).await;
    let checkpoint: MiningMaintenancePoolCheckpoint = resumed.checkpoint().expect("checkpoint");
    let provision = repository
        .read(checkpoint.provision_child.expect("one remaining deficit"))
        .expect("read")
        .expect("provisioning");
    assert_eq!(
        provision
            .config::<RegionalDispatchIntent>()
            .expect("dispatch")
            .devices[0]
            .quantity,
        1
    );
    client.close().await.expect("close");
}
