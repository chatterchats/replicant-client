use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{self, BufWriter, Write},
    path::{Path, PathBuf},
    time::Duration,
};

use replicant_client::Client;
use replicant_event_planner::{
    BeaconAction, CriterionAssessment, EventDefinition, FactoryWorkload, PlanningContext,
    ResourceMap, mission_tag, plan_event,
};
use replicant_printing::managed::{
    discover_factories as discover_print_factories, fetch_blueprints as fetch_print_blueprints,
};
use serde::{Deserialize, Serialize};
use tokio::{task::JoinHandle, time::sleep};
use tracing::{info, warn};

use super::{
    AnyResult, ClaimedDevice, Config, EVENT_MISSION_TAG_PREFIX, EventMissionPlan, EventScope,
    MissionPhase, PLAN_VERSION, app_error, build_factory_workloads, executor,
    fetch_active_events_in_scope, fetch_blueprints, fetch_devices, fetch_earned_achievements,
    fetch_inventory, load_plan, normalize_event, save_plan, select_replicant,
};

const CAMPAIGN_VERSION: u32 = 1;
const CAMPAIGN_KIND: &str = "all_events_campaign";
const CAMPAIGN_POLL_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct EventCampaignPlan {
    version: u32,
    kind: String,
    campaign_id: String,
    selected_replicant: String,
    home_location: String,
    #[serde(default)]
    event_scope: EventScope,
    missions: Vec<CampaignMission>,
    blocked_events: Vec<BlockedEvent>,
    #[serde(default)]
    warnings: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CampaignMission {
    event_designation: String,
    event_title: String,
    mission_path: PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BlockedEvent {
    event_designation: String,
    event_title: String,
    reasons: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
struct CampaignStatusReport {
    campaign_id: String,
    selected_replicant: String,
    home_location: String,
    event_scope: EventScope,
    completed: usize,
    total: usize,
    missions: Vec<CampaignMissionStatus>,
    blocked_events: Vec<BlockedEvent>,
    warnings: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
struct CampaignMissionStatus {
    event_designation: String,
    event_title: String,
    criterion: String,
    phase: MissionPhase,
    achievement: bool,
    recommendation_badges: usize,
    prints_produced: usize,
    prints_total: usize,
    prestaged: bool,
}

struct PlanningPool {
    home_inventory: ResourceMap,
    blueprints: BTreeMap<String, replicant_event_planner::BlueprintSpec>,
    devices: Vec<replicant_event_planner::DeviceStock>,
    factories: Vec<FactoryWorkload>,
    earned_achievements: BTreeSet<String>,
    home_location: String,
}

enum PlannedEvent {
    Mission(CampaignMission),
    Blocked(BlockedEvent),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CampaignWorkerKind {
    ResourceStage,
    Prestage,
    Finish,
}

struct CampaignWorker {
    kind: CampaignWorkerKind,
    assets: BTreeSet<String>,
    handle: Option<JoinHandle<AnyResult<()>>>,
}

impl Drop for CampaignWorker {
    fn drop(&mut self) {
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }
}

pub(crate) fn is_campaign_file(path: &Path) -> AnyResult<bool> {
    if !path.exists() {
        return Ok(false);
    }
    let value: serde_json::Value = serde_json::from_slice(&fs::read(path)?)?;
    Ok(value.get("kind").and_then(serde_json::Value::as_str) == Some(CAMPAIGN_KIND))
}

pub(crate) fn load_campaign(path: &Path) -> AnyResult<EventCampaignPlan> {
    let campaign: EventCampaignPlan = serde_json::from_slice(&fs::read(path)?)?;
    if campaign.version != CAMPAIGN_VERSION || campaign.kind != CAMPAIGN_KIND {
        return Err(app_error(
            io::ErrorKind::InvalidData,
            format!(
                "unsupported event campaign version or kind at {}",
                path.display()
            ),
        ));
    }
    Ok(campaign)
}

fn save_campaign(path: &Path, campaign: &EventCampaignPlan) -> AnyResult<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)?;
    }
    let temporary = path.with_extension("json.tmp");
    let file = File::create(&temporary)?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, campaign)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    writer.get_ref().sync_all()?;
    fs::rename(&temporary, path)?;
    Ok(())
}

pub(crate) async fn create_campaign(
    client: &Client,
    config: &Config,
    mut events: Vec<EventDefinition>,
    earned: &BTreeSet<String>,
) -> AnyResult<()> {
    ensure_replace_allowed(config)?;
    let replicant = select_replicant(client, config.replicant.as_deref(), config.command).await?;
    let replicant_code = replicant.key.id.as_str().to_owned();
    let campaign_id = uuid::Uuid::new_v4().simple().to_string();
    let mut campaign = EventCampaignPlan {
        version: CAMPAIGN_VERSION,
        kind: CAMPAIGN_KIND.to_owned(),
        campaign_id,
        selected_replicant: replicant_code,
        home_location: config.home.clone(),
        event_scope: config.event_scope(),
        missions: Vec::new(),
        blocked_events: Vec::new(),
        warnings: Vec::new(),
    };
    let mut pool = build_planning_pool(client, &campaign.home_location, earned).await?;
    sort_campaign_events(&mut events, earned);
    for event in events {
        match plan_campaign_event(client, config, &campaign, event, &mut pool).await? {
            PlannedEvent::Mission(mission) => campaign.missions.push(mission),
            PlannedEvent::Blocked(blocked) => campaign.blocked_events.push(blocked),
        }
    }
    save_campaign(&config.plan_path, &campaign)?;
    show_campaign_status(config, &campaign)?;
    println!("Execute with: replicant-cli event --run");
    Ok(())
}

fn ensure_replace_allowed(config: &Config) -> AnyResult<()> {
    if !config.plan_path.exists() || config.replace_plan {
        return Ok(());
    }
    if is_campaign_file(&config.plan_path)? {
        let campaign = load_campaign(&config.plan_path)?;
        if !campaign_is_terminal(&campaign)? {
            return Err(app_error(
                io::ErrorKind::AlreadyExists,
                format!(
                    "active campaign {} already exists at {}; use run, status, or plan --replace-plan",
                    campaign.campaign_id,
                    config.plan_path.display()
                ),
            ));
        }
    } else {
        let mission = load_plan(&config.plan_path)?;
        if !mission.phase.is_terminal() {
            return Err(app_error(
                io::ErrorKind::AlreadyExists,
                format!(
                    "active mission {} already exists at {}; use run, status, or plan --replace-plan",
                    mission.mission_id,
                    config.plan_path.display()
                ),
            ));
        }
    }
    Ok(())
}

async fn build_planning_pool(
    client: &Client,
    home: &str,
    earned: &BTreeSet<String>,
) -> AnyResult<PlanningPool> {
    let blueprints = fetch_blueprints(client).await?;
    let mut live_devices = fetch_devices(client, &blueprints, home).await?;
    super::hydrate_factory_workloads(client, &mut live_devices, home).await?;
    let factories = build_factory_workloads(&live_devices, &blueprints, home);
    Ok(PlanningPool {
        home_inventory: fetch_inventory(client, home).await?,
        blueprints,
        devices: live_devices
            .into_iter()
            .map(|device| device.stock)
            .collect(),
        factories,
        earned_achievements: earned.clone(),
        home_location: home.to_owned(),
    })
}

fn sort_campaign_events(events: &mut [EventDefinition], earned: &BTreeSet<String>) {
    events.sort_by(|left, right| {
        let left_new = left
            .rewards
            .completion_achievement
            .as_ref()
            .is_some_and(|achievement| !earned.contains(achievement));
        let right_new = right
            .rewards
            .completion_achievement
            .as_ref()
            .is_some_and(|achievement| !earned.contains(achievement));
        right_new
            .cmp(&left_new)
            .then_with(|| left.designation.cmp(&right.designation))
    });
}

async fn plan_campaign_event(
    client: &Client,
    config: &Config,
    campaign: &EventCampaignPlan,
    event: EventDefinition,
    pool: &mut PlanningPool,
) -> AnyResult<PlannedEvent> {
    let context = PlanningContext {
        home_inventory: pool.home_inventory.clone(),
        event_inventory: fetch_inventory(client, &event.location).await?,
        blueprints: pool.blueprints.clone(),
        devices: pool.devices.clone(),
        factories: pool.factories.clone(),
        earned_achievements: pool.earned_achievements.clone(),
        home_location: pool.home_location.clone(),
        mission_tag_prefix: EVENT_MISSION_TAG_PREFIX.into(),
    };
    let event_plan = plan_event(event, &context)?;
    let Some(selected_criterion) = best_criterion(&event_plan.criteria).cloned() else {
        let reasons = event_plan
            .criteria
            .iter()
            .map(|criterion| {
                format!(
                    "{}: {}",
                    criterion.criterion_name,
                    criterion.blockers.join("; ")
                )
            })
            .collect::<Vec<_>>();
        return Ok(PlannedEvent::Blocked(BlockedEvent {
            event_designation: event_plan.event.designation,
            event_title: event_plan.event.title,
            reasons,
        }));
    };

    let mission_id = uuid::Uuid::new_v4().simple().to_string();
    let mission_path = campaign_mission_path(
        &config.plan_path,
        &campaign.campaign_id,
        &event_plan.event.designation,
    );
    let mission = EventMissionPlan {
        version: PLAN_VERSION,
        mission_id: mission_id.clone(),
        mission_tag: mission_tag(&mission_id),
        phase: MissionPhase::Planned,
        selected_replicant: campaign.selected_replicant.clone(),
        home_location: campaign.home_location.clone(),
        event_scope: campaign.event_scope.clone(),
        event: event_plan.event,
        selected_criterion,
        grants_unearned_achievement: event_plan.grants_unearned_achievement,
        claimed_devices: Vec::<ClaimedDevice>::new(),
        execution: executor::ExecutionState::default(),
    };
    reserve_mission(pool, &mission);
    save_plan(&mission_path, &mission)?;
    info!(
        event = %mission.event.designation,
        criterion = %mission.selected_criterion.criterion_name,
        badges = mission.selected_criterion.recommendations.len(),
        prints = mission.selected_criterion.print_count(),
        "planned event for all-events campaign"
    );
    Ok(PlannedEvent::Mission(CampaignMission {
        event_designation: mission.event.designation.clone(),
        event_title: mission.event.title.clone(),
        mission_path,
    }))
}

fn best_criterion(criteria: &[CriterionAssessment]) -> Option<&CriterionAssessment> {
    criteria
        .iter()
        .filter(|criterion| criterion.feasible)
        .max_by(|left, right| {
            left.recommendations
                .len()
                .cmp(&right.recommendations.len())
                .then_with(|| right.print_count().cmp(&left.print_count()))
                .then_with(|| {
                    right
                        .print_schedule
                        .makespan_seconds
                        .total_cmp(&left.print_schedule.makespan_seconds)
                })
                .then_with(|| right.total_trips().cmp(&left.total_trips()))
                .then_with(|| right.criterion_name.cmp(&left.criterion_name))
        })
}

fn reserve_mission(pool: &mut PlanningPool, mission: &EventMissionPlan) {
    let mut resources = mission.selected_criterion.remaining_resources.clone();
    merge_resources(
        &mut resources,
        &mission.selected_criterion.manufacturing_resources,
    );
    subtract_resources(&mut pool.home_inventory, &resources);

    let mut consumed_devices = mission
        .selected_criterion
        .reused_devices
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    if !matches!(
        mission.selected_criterion.beacon.action,
        BeaconAction::AlreadyActive
    ) && let Some(code) = &mission.selected_criterion.beacon.device_code
    {
        consumed_devices.insert(code.clone());
    }
    // All-events campaigns intentionally reserve transport devices across
    // missions so resource feeders, device carriers, and reward recovery can
    // run concurrently without two missions fighting over the same vessel.
    for transport in mission
        .selected_criterion
        .cargo
        .transports
        .iter()
        .chain(mission.selected_criterion.carriers.transports.iter())
        .filter(|transport| !transport.must_print)
    {
        consumed_devices.insert(transport.code.clone());
    }
    pool.devices
        .retain(|device| !consumed_devices.contains(&device.code));

    for batch in &mission.selected_criterion.print_schedule.batches {
        if let Some(factory) = pool
            .factories
            .iter_mut()
            .find(|factory| factory.code == batch.factory_code)
        {
            factory.remaining_seconds = factory
                .remaining_seconds
                .max(batch.projected_finish_seconds);
        }
    }
}

fn merge_resources(target: &mut ResourceMap, source: &ResourceMap) {
    for (resource, quantity) in source {
        *target.entry(resource.clone()).or_default() += quantity;
    }
}

fn subtract_resources(target: &mut ResourceMap, source: &ResourceMap) {
    for (resource, quantity) in source {
        let remaining = target
            .get(resource)
            .copied()
            .unwrap_or(0)
            .saturating_sub(*quantity);
        if remaining == 0 {
            target.remove(resource);
        } else {
            target.insert(resource.clone(), remaining);
        }
    }
}

fn campaign_mission_path(campaign_path: &Path, campaign_id: &str, event: &str) -> PathBuf {
    let stem = campaign_path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("event-mission");
    let campaign_prefix = campaign_id.chars().take(8).collect::<String>();
    let directory = campaign_path
        .parent()
        .unwrap_or_else(|| Path::new(""))
        .join(format!("{stem}-campaign-{campaign_prefix}"));
    directory.join(format!(
        "{}-{:016x}.json",
        sanitize_filename(event),
        stable_hash(event)
    ))
}

fn sanitize_filename(value: &str) -> String {
    let cleaned = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    cleaned.trim_matches('-').to_owned()
}

fn stable_hash(value: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

pub(crate) async fn execute_campaign(
    client: &Client,
    config: &Config,
    campaign: &mut EventCampaignPlan,
) -> AnyResult<()> {
    let mut workers = BTreeMap::<PathBuf, CampaignWorker>::new();

    loop {
        reap_finished_campaign_workers(client, config, &mut workers).await?;
        let busy_paths = worker_paths(&workers);
        prefill_print_queues(client, config, campaign, &busy_paths).await?;
        reap_finished_campaign_workers(client, config, &mut workers).await?;
        spawn_campaign_workers(client, config, campaign, &mut workers, None).await?;

        if let Some(record) = next_replicant_ready_mission(campaign, &workers)? {
            let path = record.mission_path.clone();
            let mut mission = load_plan(&path)?;
            let assets = mission_transport_codes(&mission);
            if !worker_assets(&workers).is_disjoint(&assets) {
                sleep(CAMPAIGN_POLL_INTERVAL).await;
                continue;
            }

            info!(
                event = %record.event_designation,
                "dispatching campaign replicant to prestaged event"
            );
            let mission_config = mission_config(config, &path);
            let mut feeder_skip = worker_paths(&workers);
            feeder_skip.insert(path.clone());
            let feeder_client = client.clone();
            let feeder_config = config.clone();
            let feeder_campaign = campaign.clone();
            let feeder: JoinHandle<AnyResult<()>> = tokio::spawn(async move {
                loop {
                    prefill_print_queues(
                        &feeder_client,
                        &feeder_config,
                        &feeder_campaign,
                        &feeder_skip,
                    )
                    .await?;
                    sleep(CAMPAIGN_POLL_INTERVAL).await;
                }
            });
            let reservations = campaign_replan_reservations(campaign, &path)?;
            let resolve_result = executor::resolve_prestaged_campaign_mission(
                client,
                &mission_config,
                &mut mission,
                &reservations,
            )
            .await;
            let feeder_finished = feeder.is_finished();
            if !feeder_finished {
                feeder.abort();
            }
            let feeder_result = feeder.await;
            resolve_result?;
            if feeder_finished {
                feeder_result.map_err(|error| {
                    app_error(
                        io::ErrorKind::Other,
                        format!("campaign print feeder failed to join: {error}"),
                    )
                })??;
            }
            // On the next scheduler turn this resolved mission becomes an
            // independent reward/return worker while the replicant moves on.
            continue;
        }

        if campaign_has_incomplete_missions(campaign)? || !workers.is_empty() {
            sleep(CAMPAIGN_POLL_INTERVAL).await;
            continue;
        }

        executor::return_campaign_replicant_home(
            client,
            config,
            &campaign.selected_replicant,
            &campaign.home_location,
        )
        .await?;

        if campaign.blocked_events.is_empty() {
            save_campaign(&config.plan_path, campaign)?;
            println!(
                "Campaign {} completed every planned event.",
                campaign.campaign_id
            );
            return Ok(());
        }

        let added = retry_blocked_events(client, config, campaign).await?;
        save_campaign(&config.plan_path, campaign)?;
        if added == 0 {
            show_campaign_status(config, campaign)?;
            return Err(app_error(
                io::ErrorKind::Other,
                "all currently feasible events completed, but blocked events remain; replenish resources or resolve the listed blockers and run again",
            ));
        }
    }
}

async fn spawn_campaign_workers(
    client: &Client,
    config: &Config,
    campaign: &EventCampaignPlan,
    workers: &mut BTreeMap<PathBuf, CampaignWorker>,
    exclude: Option<&Path>,
) -> AnyResult<()> {
    let mut busy_assets = worker_assets(workers);
    for (record_index, record) in campaign.missions.iter().enumerate() {
        if exclude.is_some_and(|path| path == record.mission_path.as_path())
            || workers.contains_key(&record.mission_path)
        {
            continue;
        }
        let mut mission = load_plan(&record.mission_path)?;
        if mission.phase.is_terminal() {
            continue;
        }

        // Check persisted transport conflicts before doing any live asset
        // reconciliation. A deferred mission used to spend network requests
        // every scheduler tick only to discover a conflict already visible in
        // the campaign JSON.
        let preliminary_finish = mission.execution.event_resolved;
        let preliminary_prints_complete = persisted_prints_complete(&mission);
        let preliminary_stage_resources = !preliminary_finish
            && !mission.execution.resources_staged
            && !preliminary_prints_complete;
        let preliminary_prestage = !preliminary_finish
            && !mission.execution.prestage_complete
            && preliminary_prints_complete;
        if !preliminary_finish && !preliminary_stage_resources && !preliminary_prestage {
            continue;
        }
        let preliminary_assets = if preliminary_stage_resources {
            mission_resource_transport_codes(&mission)
        } else {
            mission_transport_codes(&mission)
        };
        if has_prior_incomplete_transport_conflict(campaign, record_index, &preliminary_assets)? {
            info!(
                event = %record.event_designation,
                "campaign worker deferred behind an older mission sharing a transport"
            );
            continue;
        }
        if !busy_assets.is_disjoint(&preliminary_assets) {
            info!(
                event = %record.event_designation,
                assets = %preliminary_assets.iter().cloned().collect::<Vec<_>>().join(","),
                "campaign worker deferred because a transport is already busy"
            );
            continue;
        }

        let reservations = campaign_replan_reservations(campaign, &record.mission_path)?;
        if !mission.execution.event_resolved
            && !mission.execution.prestage_complete
            && persisted_prints_complete(&mission)
            && executor::reconcile_campaign_asset_plan(
                client,
                &mission_config(config, &record.mission_path),
                &mut mission,
                &reservations,
            )
            .await?
        {
            // The mission's stale logical reservations were replaced against
            // current live stock while protecting sibling campaign claims.
            // Re-enter the scheduler so printer lanes/resource workers can be
            // recalculated from the new plan before any concurrent work starts.
            continue;
        }

        // Reconciliation can repair resource-staging state, so derive the
        // final worker intent again before dispatch.
        let finish = mission.execution.event_resolved;
        let prints_complete = persisted_prints_complete(&mission);
        let stage_resources = !finish && !mission.execution.resources_staged && !prints_complete;
        let prestage = !finish && !mission.execution.prestage_complete && prints_complete;
        if !finish && !stage_resources && !prestage {
            continue;
        }

        let assets = if stage_resources {
            mission_resource_transport_codes(&mission)
        } else {
            mission_transport_codes(&mission)
        };
        if has_prior_incomplete_transport_conflict(campaign, record_index, &assets)? {
            continue;
        }
        if !busy_assets.is_disjoint(&assets) {
            continue;
        }
        if stage_resources
            && !executor::prepare_campaign_resource_stage(
                client,
                &mission_config(config, &record.mission_path),
                &mut mission,
            )
            .await?
        {
            continue;
        }
        busy_assets.extend(assets.iter().cloned());

        let path = record.mission_path.clone();
        let worker_path = path.clone();
        let worker_client = client.clone();
        let worker_config = mission_config(config, &path);
        let event = record.event_designation.clone();
        let kind = if finish {
            CampaignWorkerKind::Finish
        } else if stage_resources {
            CampaignWorkerKind::ResourceStage
        } else {
            CampaignWorkerKind::Prestage
        };
        let handle = tokio::spawn(async move {
            if kind == CampaignWorkerKind::ResourceStage {
                let mission = load_plan(&worker_path)?;
                info!(event = %event, "starting independent event resource feeder");
                executor::deliver_campaign_resources(&worker_client, &worker_config, &mission).await
            } else {
                let mut mission = load_plan(&worker_path)?;
                if kind == CampaignWorkerKind::Finish {
                    info!(event = %event, "starting independent event reward/return worker");
                    executor::finish_resolved_campaign_mission(
                        &worker_client,
                        &worker_config,
                        &mut mission,
                    )
                    .await
                } else {
                    info!(event = %event, "starting independent event device prestaging worker");
                    let _ = executor::prestage_campaign_mission(
                        &worker_client,
                        &worker_config,
                        &mut mission,
                        &reservations,
                    )
                    .await?;
                    Ok(())
                }
            }
        });
        workers.insert(
            path,
            CampaignWorker {
                kind,
                assets,
                handle: Some(handle),
            },
        );
    }
    Ok(())
}

async fn reap_finished_campaign_workers(
    client: &Client,
    config: &Config,
    workers: &mut BTreeMap<PathBuf, CampaignWorker>,
) -> AnyResult<()> {
    let finished = workers
        .iter()
        .filter_map(|(path, worker)| {
            worker
                .handle
                .as_ref()
                .is_some_and(|handle| handle.is_finished())
                .then_some(path.clone())
        })
        .collect::<Vec<_>>();
    for path in finished {
        let mut worker = workers
            .remove(&path)
            .ok_or_else(|| app_error(io::ErrorKind::NotFound, "campaign worker disappeared"))?;
        let handle = worker
            .handle
            .take()
            .ok_or_else(|| app_error(io::ErrorKind::NotFound, "campaign worker handle missing"))?;
        handle.await.map_err(|error| {
            app_error(
                io::ErrorKind::Other,
                format!("campaign background worker failed to join: {error}"),
            )
        })??;
        if worker.kind == CampaignWorkerKind::ResourceStage {
            let mut mission = load_plan(&path)?;
            let worker_config = mission_config(config, &path);
            executor::confirm_campaign_resources_staged(client, &worker_config, &mut mission)
                .await?;
        }
    }
    Ok(())
}

fn worker_assets(workers: &BTreeMap<PathBuf, CampaignWorker>) -> BTreeSet<String> {
    workers
        .values()
        .flat_map(|worker| worker.assets.iter().cloned())
        .collect()
}

fn worker_paths(workers: &BTreeMap<PathBuf, CampaignWorker>) -> BTreeSet<PathBuf> {
    workers
        .iter()
        .filter_map(|(path, worker)| {
            (worker.kind != CampaignWorkerKind::ResourceStage).then_some(path.clone())
        })
        .collect()
}

fn persisted_prints_complete(mission: &EventMissionPlan) -> bool {
    if mission.selected_criterion.print_schedule.batches.is_empty() {
        return true;
    }
    !mission.execution.print_batches.is_empty()
        && mission
            .execution
            .print_batches
            .iter()
            .all(|batch| i64::try_from(batch.produced_codes.len()).ok() == Some(batch.quantity))
}

fn mission_resource_transport_codes(mission: &EventMissionPlan) -> BTreeSet<String> {
    mission
        .selected_criterion
        .cargo
        .transports
        .iter()
        .filter(|transport| !transport.code.starts_with("<print:"))
        .map(|transport| transport.code.clone())
        .collect()
}

fn mission_transport_codes(mission: &EventMissionPlan) -> BTreeSet<String> {
    mission
        .selected_criterion
        .cargo
        .transports
        .iter()
        .chain(mission.selected_criterion.carriers.transports.iter())
        .filter(|transport| !transport.code.starts_with("<print:"))
        .map(|transport| transport.code.clone())
        .collect()
}

fn campaign_replan_reservations(
    campaign: &EventCampaignPlan,
    current_path: &Path,
) -> AnyResult<executor::CampaignReplanReservations> {
    let mut reservations = executor::CampaignReplanReservations::default();
    for record in &campaign.missions {
        if record.mission_path.as_path() == current_path {
            continue;
        }
        let mission = load_plan(&record.mission_path)?;
        if mission.phase.is_terminal() {
            continue;
        }

        reservations
            .device_codes
            .extend(mission.selected_criterion.reused_devices.iter().cloned());
        reservations
            .device_codes
            .extend(mission_transport_codes(&mission));
        reservations.device_codes.extend(
            mission
                .claimed_devices
                .iter()
                .filter(|claim| !claim.released)
                .map(|claim| claim.device_code.clone()),
        );
        reservations.device_codes.extend(
            mission
                .execution
                .payload_devices
                .iter()
                .map(|payload| payload.code.clone()),
        );
        for batch in &mission.execution.print_batches {
            reservations
                .device_codes
                .extend(batch.produced_codes.iter().cloned());
        }
        if !matches!(
            mission.selected_criterion.beacon.action,
            BeaconAction::AlreadyActive | BeaconAction::Unavailable
        ) && let Some(code) = &mission.selected_criterion.beacon.device_code
        {
            reservations.device_codes.insert(code.clone());
        }

        if !mission.execution.event_resolved {
            if !mission.execution.resources_staged {
                merge_resources(
                    &mut reservations.home_resources,
                    &mission.selected_criterion.remaining_resources,
                );
            }
            if !persisted_prints_complete(&mission) {
                merge_resources(
                    &mut reservations.home_resources,
                    &mission.selected_criterion.manufacturing_resources,
                );
            }
        }
    }
    Ok(reservations)
}

fn has_prior_incomplete_transport_conflict(
    campaign: &EventCampaignPlan,
    record_index: usize,
    assets: &BTreeSet<String>,
) -> AnyResult<bool> {
    if assets.is_empty() {
        return Ok(false);
    }
    for prior in &campaign.missions[..record_index] {
        let mission = load_plan(&prior.mission_path)?;
        if mission.phase.is_terminal() {
            continue;
        }
        if !assets.is_disjoint(&mission_transport_codes(&mission)) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn next_replicant_ready_mission<'a>(
    campaign: &'a EventCampaignPlan,
    workers: &BTreeMap<PathBuf, CampaignWorker>,
) -> AnyResult<Option<&'a CampaignMission>> {
    let busy = worker_assets(workers);
    for (record_index, record) in campaign.missions.iter().enumerate() {
        if workers.contains_key(&record.mission_path) {
            continue;
        }
        let mission = load_plan(&record.mission_path)?;
        if mission.phase.is_terminal()
            || mission.execution.event_resolved
            || !mission.execution.prestage_complete
        {
            continue;
        }
        let assets = mission_transport_codes(&mission);
        if has_prior_incomplete_transport_conflict(campaign, record_index, &assets)? {
            continue;
        }
        if busy.is_disjoint(&assets) {
            return Ok(Some(record));
        }
    }
    Ok(None)
}

async fn prefill_print_queues(
    client: &Client,
    config: &Config,
    campaign: &EventCampaignPlan,
    skip_paths: &BTreeSet<PathBuf>,
) -> AnyResult<()> {
    ensure_campaign_printer_lanes(client, campaign, skip_paths).await?;
    loop {
        let progress = prefill_print_round(client, config, campaign, skip_paths).await?;
        if progress.submitted == 0 {
            if progress.pending > 0 {
                info!(
                    pending = progress.pending,
                    "Autofactory queues are full; continuing ready event work"
                );
            }
            return Ok(());
        }
    }
}

async fn ensure_campaign_printer_lanes(
    client: &Client,
    campaign: &EventCampaignPlan,
    skip_paths: &BTreeSet<PathBuf>,
) -> AnyResult<()> {
    // Do not perform remote factory discovery on scheduler turns where every
    // mission is already done manufacturing. This used to trigger a complete
    // Autofactory scan even during pure reward recovery/return phases.
    let mut needs_lanes = false;
    for record in &campaign.missions {
        if skip_paths.contains(&record.mission_path) {
            continue;
        }
        let mission = load_plan(&record.mission_path)?;
        if !mission.phase.is_terminal()
            && !mission.selected_criterion.print_schedule.batches.is_empty()
            && !persisted_prints_complete(&mission)
        {
            needs_lanes = true;
            break;
        }
    }
    if !needs_lanes {
        return Ok(());
    }

    let blueprints = fetch_print_blueprints(client).await?;
    let mut factories =
        discover_print_factories(client, &campaign.home_location, &blueprints).await?;
    factories.sort_by(|left, right| {
        left.waiting_for_resources
            .cmp(&right.waiting_for_resources)
            .then_with(|| left.remaining_seconds.total_cmp(&right.remaining_seconds))
            .then_with(|| left.code.cmp(&right.code))
    });
    let factory_codes = factories
        .iter()
        .map(|factory| factory.code.clone())
        .collect::<BTreeSet<_>>();
    let mut reserved = BTreeSet::<String>::new();

    // Preserve active reservations first, including missions currently owned by
    // a logistics worker. This prevents a newly scheduled event from leasing a
    // factory that is already part of another event's dependency lanes.
    for record in &campaign.missions {
        let mission = load_plan(&record.mission_path)?;
        if mission.phase.is_terminal()
            || mission.selected_criterion.print_schedule.batches.is_empty()
            || persisted_prints_complete(&mission)
        {
            continue;
        }
        for code in &mission.execution.printer_lanes {
            if factory_codes.contains(code) {
                reserved.insert(code.clone());
            }
        }
    }

    for record in &campaign.missions {
        if skip_paths.contains(&record.mission_path) {
            continue;
        }
        let mut mission = load_plan(&record.mission_path)?;
        if mission.phase.is_terminal()
            || mission.selected_criterion.print_schedule.batches.is_empty()
            || persisted_prints_complete(&mission)
        {
            continue;
        }

        let previous_lanes = mission.execution.printer_lanes.clone();
        let total_units = mission
            .selected_criterion
            .print_devices
            .iter()
            .map(|item| item.count.max(0))
            .sum::<i64>();
        let has_components = mission.selected_criterion.print_devices.iter().any(|item| {
            blueprints
                .get(&item.device_type)
                .is_some_and(|blueprint| !blueprint.components.is_empty())
        });
        let desired: usize = if has_components || total_units > 1 {
            2
        } else {
            1
        };
        let mut lane_set = mission
            .execution
            .printer_lanes
            .iter()
            .filter(|code| factory_codes.contains(*code))
            .cloned()
            .collect::<BTreeSet<_>>();
        lane_set.extend(
            mission
                .execution
                .print_batches
                .iter()
                .filter(|batch| batch.submitted)
                .filter(|batch| {
                    i64::try_from(batch.produced_codes.len()).ok() != Some(batch.quantity)
                })
                .map(|batch| batch.factory_code.clone())
                .filter(|code| factory_codes.contains(code)),
        );
        let mut lanes = lane_set.into_iter().collect::<Vec<_>>();
        let needed = desired.saturating_sub(lanes.len());
        let additional_lanes = factories
            .iter()
            .filter(|factory| !factory.waiting_for_resources)
            .filter(|factory| !reserved.contains(&factory.code))
            .filter(|factory| !lanes.contains(&factory.code))
            .take(needed)
            .map(|factory| factory.code.clone())
            .collect::<Vec<_>>();
        lanes.extend(additional_lanes);
        lanes.sort();
        lanes.dedup();
        reserved.extend(lanes.iter().cloned());
        if previous_lanes != lanes {
            mission.execution.printer_lanes = lanes.clone();
            save_plan(&record.mission_path, &mission)?;
            if lanes.is_empty() {
                info!(
                    event = %record.event_designation,
                    previous_lanes = %previous_lanes.join(","),
                    "released unavailable event Autofactory lane(s); waiting for a live printer"
                );
            } else {
                info!(
                    event = %record.event_designation,
                    lanes = %lanes.join(","),
                    "reconciled event Autofactory lane(s)"
                );
            }
        }
    }
    Ok(())
}

async fn prefill_print_round(
    client: &Client,
    config: &Config,
    campaign: &EventCampaignPlan,
    skip_paths: &BTreeSet<PathBuf>,
) -> AnyResult<executor::PrintKickoff> {
    let mut progress = executor::PrintKickoff::default();
    for record in &campaign.missions {
        if skip_paths.contains(&record.mission_path) {
            continue;
        }
        let mut mission = load_plan(&record.mission_path)?;
        if mission.phase.is_terminal()
            || mission.selected_criterion.print_schedule.batches.is_empty()
            || persisted_prints_complete(&mission)
        {
            continue;
        }
        let mission_config = mission_config(config, &record.mission_path);
        let mission_progress =
            executor::kickoff_printing(client, &mission_config, &mut mission, 1).await?;
        progress.submitted += mission_progress.submitted;
        progress.pending += mission_progress.pending;
    }
    Ok(progress)
}

fn mission_config(config: &Config, path: &Path) -> Config {
    let mut mission_config = config.clone();
    mission_config.plan_path = path.to_owned();
    mission_config.all_events = false;
    mission_config
}

async fn retry_blocked_events(
    client: &Client,
    config: &Config,
    campaign: &mut EventCampaignPlan,
) -> AnyResult<usize> {
    let active = fetch_active_events_in_scope(client, &campaign.event_scope)
        .await?
        .iter()
        .map(normalize_event)
        .collect::<Result<Vec<_>, _>>()?;
    let active = active
        .into_iter()
        .map(|event| (event.designation.clone(), event))
        .collect::<BTreeMap<_, _>>();
    let earned = fetch_earned_achievements(client).await?;
    let mut pool = build_planning_pool(client, &campaign.home_location, &earned).await?;
    let previous = std::mem::take(&mut campaign.blocked_events);
    let mut added = 0usize;
    for blocked in previous {
        let Some(event) = active.get(&blocked.event_designation).cloned() else {
            let warning = format!(
                "event {} is no longer active and was removed from the campaign blocker list",
                blocked.event_designation
            );
            warn!(warning = %warning);
            campaign.warnings.push(warning);
            continue;
        };
        match plan_campaign_event(client, config, campaign, event, &mut pool).await? {
            PlannedEvent::Mission(mission) => {
                campaign.missions.push(mission);
                added += 1;
            }
            PlannedEvent::Blocked(blocked) => campaign.blocked_events.push(blocked),
        }
    }
    Ok(added)
}

fn campaign_is_terminal(campaign: &EventCampaignPlan) -> AnyResult<bool> {
    Ok(campaign.blocked_events.is_empty() && !campaign_has_incomplete_missions(campaign)?)
}

fn campaign_has_incomplete_missions(campaign: &EventCampaignPlan) -> AnyResult<bool> {
    for record in &campaign.missions {
        if !load_plan(&record.mission_path)?.phase.is_terminal() {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(crate) fn show_campaign_status(config: &Config, campaign: &EventCampaignPlan) -> AnyResult<()> {
    let mut statuses = Vec::new();
    for record in &campaign.missions {
        let mission = load_plan(&record.mission_path)?;
        statuses.push(CampaignMissionStatus {
            event_designation: record.event_designation.clone(),
            event_title: record.event_title.clone(),
            criterion: mission.selected_criterion.criterion_name.clone(),
            phase: mission.phase,
            achievement: mission.grants_unearned_achievement,
            recommendation_badges: mission.selected_criterion.recommendations.len(),
            prints_produced: mission
                .execution
                .print_batches
                .iter()
                .map(|batch| batch.produced_codes.len())
                .sum(),
            prints_total: mission
                .selected_criterion
                .print_schedule
                .batches
                .iter()
                .filter_map(|batch| usize::try_from(batch.quantity).ok())
                .sum(),
            prestaged: mission.execution.prestage_complete,
        });
    }
    let completed = statuses
        .iter()
        .filter(|status| status.phase.is_terminal())
        .count();
    let report = CampaignStatusReport {
        campaign_id: campaign.campaign_id.clone(),
        selected_replicant: campaign.selected_replicant.clone(),
        home_location: campaign.home_location.clone(),
        event_scope: campaign.event_scope.clone(),
        completed,
        total: statuses.len() + campaign.blocked_events.len(),
        missions: statuses,
        blocked_events: campaign.blocked_events.clone(),
        warnings: campaign.warnings.clone(),
    };
    if config.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }

    println!("Campaign:   {}", report.campaign_id);
    println!("Replicant:  {}", report.selected_replicant);
    println!("Home:       {}", report.home_location);
    println!("Scope:      {}", report.event_scope.description());
    println!("Completed:  {}/{}", report.completed, report.total);
    for mission in report.missions {
        let phase = format!("{:?}", mission.phase);
        println!(
            "  {:<25} {:<25} {:<12} badges={} prints={}/{}{}{}",
            mission.event_designation,
            mission.criterion,
            phase,
            mission.recommendation_badges,
            mission.prints_produced,
            mission.prints_total,
            if mission.achievement { " NEW ACH" } else { "" },
            if mission.prestaged { " READY" } else { "" }
        );
    }
    if !report.blocked_events.is_empty() {
        println!("Blocked events:");
        for blocked in report.blocked_events {
            println!(
                "  {}: {}",
                blocked.event_designation,
                blocked.reasons.join(" | ")
            );
        }
    }
    for warning in report.warnings {
        println!("Warning: {warning}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use replicant_event_planner::{BeaconPlan, PrintSchedule, Recommendation, TransportPlan};

    fn assessment(name: &str, badges: &[Recommendation], prints: i64) -> CriterionAssessment {
        CriterionAssessment {
            criterion_name: name.into(),
            remaining_resources: ResourceMap::new(),
            remaining_devices: Vec::new(),
            reused_devices: Vec::new(),
            print_devices: (prints > 0)
                .then(|| replicant_event_planner::DeviceRequirement {
                    device_type: "device".into(),
                    count: prints,
                })
                .into_iter()
                .collect(),
            manufacturing_resources: ResourceMap::new(),
            print_schedule: PrintSchedule::default(),
            cargo: TransportPlan::default(),
            carriers: TransportPlan::default(),
            beacon: BeaconPlan {
                action: BeaconAction::AlreadyActive,
                device_code: None,
                transport_slots: 0,
                warning: None,
            },
            feasible: true,
            blockers: Vec::new(),
            recommendations: badges.iter().copied().collect(),
            warnings: Vec::new(),
        }
    }

    #[test]
    fn most_recommendation_badges_wins() {
        let criteria = [
            assessment("one", &[Recommendation::Fastest], 0),
            assessment(
                "two",
                &[Recommendation::FewestPrints, Recommendation::FewestTrips],
                3,
            ),
        ];
        assert_eq!(best_criterion(&criteria).unwrap().criterion_name, "two");
    }

    #[test]
    fn fewer_prints_breaks_a_badge_tie() {
        let criteria = [
            assessment("prints", &[Recommendation::Fastest], 2),
            assessment("stock", &[Recommendation::FewestTrips], 0),
        ];
        assert_eq!(best_criterion(&criteria).unwrap().criterion_name, "stock");
    }
}
