use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{self, BufWriter, Write},
    path::{Path, PathBuf},
};

use replicant_client::Client;
use replicant_event_planner::{
    BeaconAction, CriterionAssessment, EventDefinition, FactoryWorkload, PlanningContext,
    ResourceMap, mission_tag, plan_event,
};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use super::{
    AnyResult, ClaimedDevice, Config, EVENT_MISSION_TAG_PREFIX, EventMissionPlan, MissionPhase,
    PLAN_VERSION, app_error, build_factory_workloads, executor, fetch_active_events,
    fetch_blueprints, fetch_devices, fetch_earned_achievements, fetch_inventory, load_plan,
    normalize_event, save_plan, select_replicant,
};

const CAMPAIGN_VERSION: u32 = 1;
const CAMPAIGN_KIND: &str = "all_events_campaign";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct EventCampaignPlan {
    version: u32,
    kind: String,
    campaign_id: String,
    selected_replicant: String,
    home_location: String,
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
    println!("Execute with: replicant-events run");
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
    let live_devices = fetch_devices(client, &blueprints).await?;
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
    loop {
        prefill_print_queues(client, config, campaign).await?;

        let mut material_only = Vec::new();
        let mut printing_required = Vec::new();
        for (index, record) in campaign.missions.iter().enumerate() {
            let mission = load_plan(&record.mission_path)?;
            if mission.phase.is_terminal() {
                continue;
            }
            if mission.selected_criterion.print_schedule.batches.is_empty() {
                material_only.push(index);
            } else {
                printing_required.push((
                    index,
                    mission.selected_criterion.print_schedule.makespan_seconds,
                ));
            }
        }

        for index in material_only {
            prefill_print_queues(client, config, campaign).await?;
            execute_campaign_mission(client, config, &campaign.missions[index]).await?;
        }

        printing_required.sort_by(|left, right| {
            left.1
                .total_cmp(&right.1)
                .then_with(|| left.0.cmp(&right.0))
        });
        for (index, _) in printing_required {
            prefill_print_queues(client, config, campaign).await?;
            execute_campaign_mission(client, config, &campaign.missions[index]).await?;
        }

        if campaign_has_incomplete_missions(campaign)? {
            continue;
        }
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

async fn prefill_print_queues(
    client: &Client,
    config: &Config,
    campaign: &EventCampaignPlan,
) -> AnyResult<()> {
    loop {
        let progress = prefill_print_round(client, config, campaign).await?;
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

async fn prefill_print_round(
    client: &Client,
    config: &Config,
    campaign: &EventCampaignPlan,
) -> AnyResult<executor::PrintKickoff> {
    let mut progress = executor::PrintKickoff::default();
    for record in &campaign.missions {
        let mut mission = load_plan(&record.mission_path)?;
        if mission.phase.is_terminal()
            || mission.selected_criterion.print_schedule.batches.is_empty()
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

async fn execute_campaign_mission(
    client: &Client,
    config: &Config,
    record: &CampaignMission,
) -> AnyResult<()> {
    let mut mission = load_plan(&record.mission_path)?;
    if mission.phase.is_terminal() {
        return Ok(());
    }
    info!(
        event = %record.event_designation,
        phase = ?mission.phase,
        "executing campaign event"
    );
    let mission_config = mission_config(config, &record.mission_path);
    executor::execute_saved_plan(client, &mission_config, &mut mission).await
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
    let active = fetch_active_events(client)
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
    println!("Completed:  {}/{}", report.completed, report.total);
    for mission in report.missions {
        let phase = format!("{:?}", mission.phase);
        println!(
            "  {:<18} {:<24} {:<22} badges={} prints={}/{}{}",
            mission.event_designation,
            mission.criterion,
            phase,
            mission.recommendation_badges,
            mission.prints_produced,
            mission.prints_total,
            if mission.achievement { " NEW ACH" } else { "" }
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
