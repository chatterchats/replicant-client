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
    let child = repository
        .create(recovery)
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
}

#[tokio::test]
async fn mining_restart_after_freighter_delivery_recovers_selected_stock_before_printing() {
    let server = MockServer::start().await;
    let client = client_at(&server).await;
    seed_route(&server, &client, false).await;
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
    let checkpoint: MiningTransportCapacityCheckpoint = resumed.checkpoint().expect("checkpoint");
    assert_eq!(checkpoint.selected_freighters, ["NEW"]);
    assert_eq!(checkpoint.staging_children.get("NEW"), Some(&delivered_id));
    let dispatch = repository
        .read(checkpoint.dispatch_child.expect("remaining deficit"))
        .expect("read")
        .expect("dispatch");
    let config: RegionalDispatchIntent = dispatch.config().expect("dispatch config");
    assert_eq!(
        config.devices[0].quantity, 1,
        "delivered freighter must not be printed again"
    );
    client.close().await.expect("close");
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
