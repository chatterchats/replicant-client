export const PROTOCOL_VERSION = 1;

export type HealthStatus = "healthy" | "degraded" | "unhealthy";
export type SyncPhase =
  "starting" | "syncing" | "ready" | "degraded" | "offline";
export type WorkflowStatus =
  | "queued"
  | "running"
  | "waiting"
  | "paused"
  | "reconciling"
  | "succeeded"
  | "failed"
  | "cancelled";
export type EntityKind =
  | "system"
  | "location"
  | "replicant"
  | "device"
  | "inventory"
  | "autofactory"
  | "cargo"
  | "operation"
  | "workflow";
export type DomainSlice =
  | "entities"
  | "universe"
  | "overview"
  | "devices"
  | "inventory"
  | "autofactories"
  | "cargo"
  | "missions"
  | "history"
  | "events"
  | "activity"
  | "trade"
  | "simulations"
  | "blueprints"
  | "directory"
  | "tutorials"
  | "messages"
  | "bobnet"
  | "network"
  | "standing"
  | "leaderboards"
  | "workflows"
  | "operations"
  | "refresh"
  | "director";

export interface DaemonHealth {
  status: HealthStatus;
  daemon_version: string;
  detail: string | null;
}

export interface RuntimeSyncStatus {
  phase: SyncPhase;
  revision: number;
  last_event_at_ms: number | null;
  detail: string | null;
}

export type RefreshPhase =
  | "account"
  | "devices"
  | "replicants"
  | "stars"
  | "systems"
  | "bodies"
  | "events"
  | "messages"
  | "locations"
  | "inventory"
  | "simulations";

export interface StartRefreshRequest {
  phases: RefreshPhase[];
  dry_run: boolean;
  read_requests_per_minute: number | null;
}

export interface ApproveRefreshRequest {
  phase: RefreshPhase;
  digest: string;
}

export interface RefreshDelta {
  proposed_inserts: number;
  proposed_updates: number;
  proposed_tombstones: number;
  applied_inserts: number;
  applied_updates: number;
  applied_tombstones: number;
}

export interface RefreshPhaseSummary {
  phase: RefreshPhase;
  status: string;
  pages: number;
  items: number;
  request_attempts: number;
  delta: RefreshDelta;
  retry_not_before: number | null;
  approval_digest: string | null;
  failure_kind: string | null;
}

export interface RefreshRunSummary {
  run_id: string;
  mode: "apply" | "dry_run";
  status: string;
  readiness: "unavailable" | "rest_baseline" | "complete";
  current_phase: RefreshPhase | null;
  read_requests_per_minute: number;
  request_attempts: number;
  delta: RefreshDelta;
  updated_at: number;
}

export interface RefreshRunDetail {
  summary: RefreshRunSummary;
  requested_phases: RefreshPhase[];
  phases: RefreshPhaseSummary[];
}

export interface AutomationStatus {
  automatic_triggers_enabled: boolean;
  workflows_paused: boolean;
}

export type AutomationControlAction =
  | "enable_triggers"
  | "disable_triggers"
  | "pause_all"
  | "resume_all"
  | "cancel";

export interface AutomationControlResponse {
  automation: AutomationStatus;
  affected_workflows: number;
}

export type DirectorMode = "off" | "advisory" | "automatic";
export type DirectorGoalKind =
  | "establish_regions"
  | "expand_star_catalogue"
  | "enhance_star_catalogue"
  | "discover_belts"
  | "expand_mining_ops"
  | "salvage_recovery"
  | "event_completion"
  | "asteroid_diversion"
  | "blueprint_acquisition"
  | "maintain_system_hubs"
  | "stranded_device_recovery"
  | "unserviced_resources"
  | "expand_ftl_network"
  | "establish_beacons";
export type DirectorGoalStatus = "satisfied" | "active" | "blocked" | "waiting";
export type DirectorRegionStatus =
  "discovered" | "establishing" | "established";
export type DirectorRequirementKind =
  "blueprint" | "logistics" | "worker_capacity" | "connectivity";
export type DirectorRequirementStatus =
  "pending" | "active" | "blocked" | "satisfied" | "unavailable";

export interface DirectorRequirementRequester {
  goal_id: string;
  reason: string;
  priority: number;
}

export interface DirectorRequirementSummary {
  id: string;
  kind: DirectorRequirementKind;
  status: DirectorRequirementStatus;
  region: string | null;
  target: string;
  priority: number;
  requesters: DirectorRequirementRequester[];
  active_workflows: string[];
}

export interface DirectorReplicantAssignment {
  code: string;
  name: string | null;
  region: string | null;
  busy: boolean;
  workflow_id: string | null;
  role_affinity: string | null;
}

export interface DirectorRegionSummary {
  region: string;
  status: DirectorRegionStatus;
  hub_system: string | null;
  hub_location: string | null;
  replicants: string[];
  known_systems: number;
}

export interface DirectorMiningPolicySummary {
  region: string;
  expand_moderate: boolean;
  expand_sparse: boolean;
}

export interface DirectorGoalSummary {
  id: string;
  kind: DirectorGoalKind;
  region: string | null;
  status: DirectorGoalStatus;
  objective: string;
  blocker: string | null;
  next_action: string | null;
  progress_current: number;
  progress_total: number;
  active_workflows: string[];
  enabled: boolean;
}

export interface DirectorWorkforceSummary {
  total: number;
  busy: number;
  idle: number;
  idle_ratio: number;
  pending_worker_demand: number;
  scale_up_recommended: boolean;
  scale_reason: string | null;
}

export interface DirectorUrgencyFact {
  automation: string;
  campaign: string;
  item: string | null;
  buffer: number | null;
  burn_rate_per_hour: number | null;
  deadline_at_ms: number | null;
  lateness_cost: Record<string, unknown>;
  loss_over_one_hour: number;
  floor: number;
  ceiling: number;
  current_grants: number;
  target_grants: number;
  urgency: number;
  hysteresis_ratio: number | null;
  action: string;
  reasons: string[];
}

export interface DirectorSnapshot {
  metadata: SnapshotMetadata;
  mode: DirectorMode;
  regions: DirectorRegionSummary[];
  goals: DirectorGoalSummary[];
  mining_policies: DirectorMiningPolicySummary[];
  replicants: DirectorReplicantAssignment[];
  requirements: DirectorRequirementSummary[];
  workforce: DirectorWorkforceSummary;
  urgency?: DirectorUrgencyFact[];
}

export interface SnapshotMetadata {
  revision: number;
  generated_at_ms: number;
}

export interface OverviewReplicant {
  entity: EntityRef;
  name: string | null;
  system: string | null;
  location: string | null;
  status: string | null;
}

export interface OverviewTravel {
  entity: EntityRef;
  from: string | null;
  to: string | null;
  arrives_at: string | null;
}

export interface WorkflowStatusCount {
  status: WorkflowStatus;
  count: number;
}

export interface OverviewSnapshot {
  metadata: SnapshotMetadata;
  health: DaemonHealth;
  sync: RuntimeSyncStatus;
  automation: AutomationStatus;
  replicants: OverviewReplicant[];
  active_travel: OverviewTravel[];
  active_workflows: WorkflowSummary[];
  workflow_counts: WorkflowStatusCount[];
  attention_workflows: WorkflowSummary[];
  notifications: Notification[];
  recent_activity: WorkflowActivity[];
}

export interface DeviceClaim {
  workflow_id: string;
  workflow_kind: string;
  workflow_status: WorkflowStatus;
}

export interface DeviceSummary {
  entity: EntityRef;
  device_type: string | null;
  status: string | null;
  ownership: string;
  owner: string | null;
  owner_name: string | null;
  system: string | null;
  region: string | null;
  location: string | null;
  available_commands: string[];
  available_directives?: string[];
  features?: string[];
  tags: string[];
  attached_to: string | null;
  stowed_in: string | null;
  controller: string | null;
  linked_device: string | null;
  attached_devices: string[];
  controlled_devices: string[];
  stowed_devices: string[];
  attach_capacity: number | null;
  cargo_capacity: number | null;
  cargo_used: number | null;
  cargo?: CargoResourceSummary[];
  stow_capacity?: number | null;
  stow_used?: number | null;
  operational_capacity_percent: number | null;
  grace_period_remaining?: number | null;
  upkeep_requirements?: Record<string, unknown>[];
  system_status?: Record<string, unknown> | null;
  active_directive: string | null;
  directive_status: string | null;
  travel_destination: string | null;
  claim: DeviceClaim | null;
}

export interface EntityProvenance {
  observed_at_ms: number;
  stale: boolean;
  reachability: string;
  source_operation: string;
}

export interface EntityStatusCount {
  status: string | null;
  count: number;
}

export interface EntityGroupSummary {
  entity_kind: EntityKind;
  entity_type: string | null;
  count: number;
  statuses: EntityStatusCount[];
}

export interface EntityCollectionSummary {
  total: number;
  items: EntitySummary[];
  groups: EntityGroupSummary[];
}

export interface SystemInspectorSummary {
  name: string | null;
  spectral_type: string | null;
  region: string | null;
  entry_point: string | null;
  position: GalaxyPoint | null;
  explored: boolean | null;
  has_hub: boolean | null;
  has_ward: boolean | null;
  has_life: boolean | null;
  children: EntityCollectionSummary;
}

export interface LocationEnvironmentSummary {
  atmosphere: string | null;
  magnetic_field: boolean | null;
  gravity_g: number | null;
  surface_temperature_c: number | null;
  habitable_zone: boolean | null;
  life_stage: string | null;
  axial_tilt_degrees: number | null;
  rotation_state: string | null;
  star_spectral_type: string | null;
  nearby_belt_richness: string | null;
  distance_from_sol_light_years: number | null;
}

export interface LocationSurveySummary {
  planets_total: number | null;
  planets_scanned: number | null;
  moons_total: number | null;
  moons_scanned: number | null;
  moons_total_estimated: boolean | null;
}

export interface LocationInspectorSummary {
  location_type: string | null;
  system: string | null;
  parent: string | null;
  scanned: boolean | null;
  system_scanned: boolean | null;
  system_tags: string[];
  survey: LocationSurveySummary;
  environment: LocationEnvironmentSummary;
  contents: EntityCollectionSummary;
}

export type EntityInspectorDetail =
  | { kind: "device"; detail: DeviceSummary }
  | { kind: "system"; detail: SystemInspectorSummary }
  | { kind: "location"; detail: LocationInspectorSummary };

export interface EntityInspectorSnapshot {
  metadata: SnapshotMetadata;
  summary: EntitySummary;
  provenance: EntityProvenance | null;
  detail: EntityInspectorDetail;
}

export interface DevicesSnapshot {
  metadata: SnapshotMetadata;
  devices: DeviceSummary[];
}

export interface SurveyMissionSummary {
  workflow: WorkflowSummary;
  replicant: string;
  vessel: string;
  center: string;
  phase: string;
  completed_systems: number;
  total_systems: number;
  next_system: string | null;
  controller: string | null;
  drones: string[];
}

export interface SurveySnapshot {
  metadata: SnapshotMetadata;
  missions: SurveyMissionSummary[];
  fleet: DeviceSummary[];
}

export type MiningInstallationStatus = "complete" | "partial";

export interface MiningInstallationSummary {
  id: string;
  system: string | null;
  location: string | null;
  controller: DeviceSummary | null;
  miners: DeviceSummary[];
  survey_controller: DeviceSummary | null;
  survey_drones: DeviceSummary[];
  maintenance_device: DeviceSummary | null;
  missing: string[];
  status: MiningInstallationStatus;
}

export interface MiningSnapshot {
  metadata: SnapshotMetadata;
  installations: MiningInstallationSummary[];
  workflows: WorkflowSummary[];
}

export interface RelayExpansionSummary {
  workflow: WorkflowSummary;
  replicant: string;
  hub: string;
  targets: string[];
  phase: string;
  completed_stops: number;
  total_stops: number | null;
  next_system: string | null;
  pending_relays: number | null;
}

export interface RelaySnapshot {
  metadata: SnapshotMetadata;
  relays: DeviceSummary[];
  staged_relays: DeviceSummary[];
  connected_systems: number;
  relay_edges: { from: string; to: string }[];
  expansions: RelayExpansionSummary[];
}

export interface BootstrapMissionSummary {
  mission_id: string;
  execution_id: string;
  region: string;
  source_hub: string;
  target_system: string;
  target_location: string;
  phase: string;
  reserved_devices: number;
  loaded_devices: number;
  capital_system: string | null;
  selected_sites: number;
  warnings: string[];
  completed: boolean;
  updated_at_ms: number;
}

export interface BootstrapSnapshot {
  metadata: SnapshotMetadata;
  missions: BootstrapMissionSummary[];
}

export interface EventRequirementSummary {
  kind: "resource" | "device";
  item: string;
  required: number;
  completed: number;
  remaining: number;
}

export interface EventCriterionSummary {
  name: string;
  requirements: EventRequirementSummary[];
  complete: boolean;
}

export interface EventRewardItem {
  item: string;
  quantity: number;
}

export interface EventRewardsSummary {
  resources: EventRewardItem[];
  devices: EventRewardItem[];
  xp: number | null;
  civilisation_points: number | null;
  completion_achievement: string | null;
}

export interface EventSummary {
  designation: string;
  title: string;
  event_type: string | null;
  category: string | null;
  tier: number | null;
  system: string;
  location: string;
  description: string | null;
  criteria: EventCriterionSummary[];
  rewards: EventRewardsSummary;
  status: string | null;
  discovered_at: string | null;
  completed_at: string | null;
}

export interface EventsSnapshot {
  metadata: SnapshotMetadata;
  events: EventSummary[];
}

export interface AccountEventSummary {
  id: string;
  name: string;
  category: string;
  device: EntityRef | null;
  replicant: EntityRef | null;
  system: string | null;
  location: string | null;
  occurred_at: string;
  payload: Record<string, unknown>;
  ami_digest: boolean;
}

export interface AccountEventsSnapshot {
  metadata: SnapshotMetadata;
  cursor: string | null;
  events: AccountEventSummary[];
}

export interface DeviceLogSummary {
  id: number | null;
  created_at: string | null;
  device_code: string | null;
  device_type: string | null;
  event_type: string | null;
  message: string | null;
  payload: Record<string, unknown>;
}

export interface DeviceLogsSnapshot {
  metadata: SnapshotMetadata;
  device: EntityRef;
  events: DeviceLogSummary[];
  next_cursor: number | null;
}

export interface SimulationScenarioSummary {
  code: string;
  name: string | null;
  description: string | null;
  long_description: string | null;
  objective_type: string | null;
  objective_target: number | null;
  timeout_hours: number | null;
  version: number | null;
  entry_cost: InventoryQuantity[];
}

export interface SimulationRunSummary {
  id: number;
  interface: EntityRef | null;
  is_mine: boolean;
  replicant: EntityRef | null;
  replicant_name: string | null;
  scenario_code: string | null;
  scenario_name: string | null;
  lifecycle: string | null;
  started_at: string | null;
  completed_at: string | null;
  abandoned_at: string | null;
  timed_out_at: string | null;
  score_seconds: number | null;
  resources_mined: number | null;
  devices_printed: number | null;
  timeout_hours: number | null;
}

export interface SimulationInterfaceSummary {
  device: DeviceSummary;
  scenarios: SimulationScenarioSummary[];
  active: SimulationRunSummary[];
  error: string | null;
}

export interface SimulationsSnapshot {
  metadata: SnapshotMetadata;
  interfaces: SimulationInterfaceSummary[];
  managed_history: SimulationRunSummary[];
  account_history: SimulationRunSummary[];
}

export interface BlueprintSummary {
  device_type: string;
  short_description: string | null;
  description: string | null;
  print_time_seconds: number | null;
  resources: InventoryQuantity[];
  components: InventoryQuantity[];
  features: string[];
  directives: string[];
  cargo_capacity: number | null;
  attach_capacity: number | null;
  stow_capacity: number | null;
  queue_size: number | null;
}

export interface BlueprintsSnapshot {
  metadata: SnapshotMetadata;
  blueprints: BlueprintSummary[];
}

export interface DirectoryReplicantSummary {
  entity: EntityRef;
  name: string | null;
  last_location: string | null;
  is_npc: boolean | null;
}

export interface DirectorySnapshot {
  metadata: SnapshotMetadata;
  query: string | null;
  replicants: DirectoryReplicantSummary[];
}

export interface DirectoryReplicantDetail {
  entity: EntityRef;
  name: string | null;
  is_npc: boolean | null;
  status: string | null;
  location: string | null;
  hosted_device: EntityRef | null;
}

export interface DirectoryReplicantDetailSnapshot {
  metadata: SnapshotMetadata;
  replicant: DirectoryReplicantDetail;
}

export interface TutorialStepSummary {
  key: string | null;
  description: string | null;
  hint: string | null;
  completed: boolean | null;
  current: boolean | null;
}

export interface TutorialSummary {
  slug: string;
  name: string | null;
  description: string | null;
  order: number | null;
  completed: boolean | null;
  current_step: number | null;
  total_steps: number | null;
  steps: TutorialStepSummary[];
}

export interface TutorialsSnapshot {
  metadata: SnapshotMetadata;
  tutorials: TutorialSummary[];
  selected: TutorialSummary | null;
}

export interface TradeItemSummary {
  kind: string;
  item: string;
  quantity: number | null;
}

export interface TradeSummary {
  trade_code: string;
  name: string | null;
  current_stock: number | null;
  initial_stock: number | null;
  requested: TradeItemSummary[];
  offered: TradeItemSummary[];
  created_at: string | null;
}

export interface TradeControllerSummary {
  entity: EntityRef;
  shop_name: string | null;
  description: string | null;
  is_local: boolean;
  owner_name: string | null;
  owner_replicant: string | null;
  system: string | null;
  location: string | null;
  total_stock: number | null;
  trade_count: number | null;
  trade_details_status: string;
  trades: TradeSummary[];
  workflow: WorkflowSummary | null;
}

export interface TradeSnapshot {
  metadata: SnapshotMetadata;
  viewer: EntityRef | null;
  controllers: TradeControllerSummary[];
}

export interface BillFinderRequest {
  tracking_beacon?: string | null;
  expand?: boolean;
  target_system?: string | null;
}

export interface BillDepartureSummary {
  tracking_beacon: string;
  replicant_code: string;
  vessel_code: string | null;
  vessel_type: string | null;
  origin_location: string;
  origin_system: string;
  logged_at: string | null;
  vector: [number, number, number];
}

export interface BillCandidateSummary {
  system: string;
  angular_error_deg: number;
  distance_ly: number;
  projected_distance_ly: number;
  cross_track_ly: number;
}

export interface BillExpansionSummary {
  status: string;
  target_system: string | null;
  workflow: WorkflowSummary | null;
  message: string;
}

export interface BillFinderResponse {
  metadata: SnapshotMetadata;
  departure: BillDepartureSummary;
  candidates: BillCandidateSummary[];
  recommended_system: string | null;
  confidence: string;
  ambiguous: boolean;
  expansion: BillExpansionSummary;
}

export interface ReportsSnapshot {
  metadata: SnapshotMetadata;
  reports: ReportDescriptor[];
  executions: FiniteExecution[];
}

export interface InboxMessageSummary {
  id: number | null;
  title: string | null;
  body: string | null;
  category: string | null;
  message_type: string | null;
  is_read: boolean | null;
  created_at: string | null;
}

export interface BobnetChannelSummary {
  name: string;
  last_active: string | null;
}

export interface BobnetMessageSummary {
  id: number | null;
  channel: string | null;
  body: string | null;
  sender: string | null;
  sender_name: string | null;
  is_npc_or_system: boolean;
  current_system: string | null;
  created_at: string | null;
}

export interface MessageFreshness {
  /** Timestamp of the last successful refresh, in Unix milliseconds. */
  last_refresh_at: number | null;
  /** Whether the cached inbox is stale. */
  stale: boolean;
  /** Error from the most recent attempted refresh, when one occurred. */
  last_error: string | null;
}

export interface MessagesSnapshot {
  metadata: SnapshotMetadata;
  inbox: InboxMessageSummary[];
  unread_count: number | null;
  last_cursor: number | null;
  freshness: MessageFreshness;
}

export interface BobnetReplicantSummary {
  entity: EntityRef;
  name: string | null;
  status: string | null;
  location: string | null;
}

export interface BobnetSnapshot {
  metadata: SnapshotMetadata;
  sources: DeviceSummary[];
  selected_source: string | null;
  channels: BobnetChannelSummary[];
  messages: BobnetMessageSummary[];
  replicants: BobnetReplicantSummary[];
  next_cursor: number | null;
  total_messages: number | null;
  error: string | null;
}

export interface NetworkRelaySummary {
  device: DeviceSummary;
  channels: BobnetChannelSummary[];
  error: string | null;
}

export interface AccountReplicantSummary {
  entity: EntityRef;
  name: string | null;
  system: string | null;
  location: string | null;
  hosted_device: EntityRef | null;
}

export interface NetworkSnapshot {
  metadata: SnapshotMetadata;
  account_name: string | null;
  account_status: string | null;
  subscribed_channels: string[];
  replicants: AccountReplicantSummary[];
  relays: NetworkRelaySummary[];
}

export interface AchievementSummary {
  key: string;
  title: string | null;
  description: string | null;
  category: string | null;
  xp_reward: number | null;
  achieved_at: string | null;
}

export interface ReputationSummary {
  species: string;
  name: string | null;
  value: number | null;
  description: string | null;
  trait_name: string | null;
}

export interface StandingSnapshot {
  metadata: SnapshotMetadata;
  experience_points_total: number | null;
  civilisation_points: number | null;
  achievements: AchievementSummary[];
  reputation: ReputationSummary[];
}

export interface LeaderboardBoardSummary {
  key: string;
  name: string | null;
  description: string | null;
  board_type: string | null;
}

export interface LeaderboardEntrySummary {
  rank: number | null;
  replicant: EntityRef | null;
  name: string | null;
  designation: string | null;
  value: number | null;
  contribution_count: number | null;
}

export interface LeaderboardsSnapshot {
  metadata: SnapshotMetadata;
  boards: LeaderboardBoardSummary[];
  selected_board: string | null;
  entries: LeaderboardEntrySummary[];
}

export type ApiTokenSource = "environment" | "secret_file" | "unset";

export interface SettingsSnapshot {
  metadata: SnapshotMetadata;
  profile: string;
  bind_address: string;
  managed_database_path: string;
  history_database_path: string;
  telemetry_database_path: string;
  runtime_database_path: string;
  log_filter: string;
  docker: boolean;
  api_token_source: ApiTokenSource;
  daemon_settings_require_restart: boolean;
}

export interface FactoryJobSummary {
  device_type: string;
  quantity: number;
  eta_seconds: number | null;
  tags: string[];
}

export type AutofactoryAvailability = "available" | "busy" | "unavailable";

export interface AutofactorySummary {
  device: DeviceSummary;
  availability: AutofactoryAvailability;
  queue_capacity: number | null;
  queued_units: number;
  current_job: FactoryJobSummary | null;
  queued_jobs: FactoryJobSummary[];
}

export interface AutofactorySnapshot {
  metadata: SnapshotMetadata;
  utilization: {
    total: number;
    busy: number;
    available: number;
    unavailable: number;
    queued_units: number;
    utilization_percent: number;
  };
  factories: AutofactorySummary[];
}

export interface CargoResourceSummary {
  resource: string;
  quantity: number;
}

export interface CargoCarrierSummary {
  device: DeviceSummary;
  resources: CargoResourceSummary[];
  attachment_used: number;
}

export interface CargoSnapshot {
  metadata: SnapshotMetadata;
  cargo_used: number;
  cargo_capacity: number;
  attachment_used: number;
  attachment_capacity: number;
  carriers: CargoCarrierSummary[];
}

export type InventoryOwnerKind = "account" | "replicant" | "location";

export interface InventoryQuantity {
  resource: string;
  quantity: number;
}

export interface InventoryLocationSummary {
  owner_kind: InventoryOwnerKind;
  owner: string;
  system: string | null;
  location: string | null;
  total_quantity: number;
  resources: InventoryQuantity[];
}

export interface InventoryDistribution {
  owner_kind: InventoryOwnerKind;
  owner: string;
  system: string | null;
  location: string | null;
  quantity: number;
}

export interface InventoryResourceSummary {
  resource: string;
  total_quantity: number;
  distribution: InventoryDistribution[];
}

export interface InventorySnapshot {
  metadata: SnapshotMetadata;
  total_quantity: number;
  locations: InventoryLocationSummary[];
  resources: InventoryResourceSummary[];
}

export interface WorkflowSummary {
  id: string;
  kind: string;
  status: WorkflowStatus;
  current_step: string | null;
  revision: number;
  updated_at_ms: number;
}

export type TriggerCondition =
  | { kind: "manual" }
  | { kind: "schedule"; interval_seconds: number }
  | { kind: "game_event"; event_name: string; device_code: string | null }
  | { kind: "state_condition"; minimum_revision: number }
  | {
      kind: "parent_workflow";
      parent_kind: string | null;
      status: WorkflowStatus;
    };

export interface TriggerTarget {
  operation_class: "action" | "workflow";
  kind: string;
  parameters: Record<string, unknown>;
}

export interface AutomationTrigger {
  id: string;
  name: string;
  condition: TriggerCondition;
  target: TriggerTarget;
  enabled: boolean;
  created_at_ms: number;
  updated_at_ms: number;
  last_fired_at_ms: number | null;
  next_run_at_ms: number | null;
  last_error: string | null;
  revision: number;
}

export interface TriggerRequest {
  name: string;
  condition: TriggerCondition;
  target: TriggerTarget;
  enabled: boolean;
}

export type ParameterKind =
  | {
      type:
        | "string"
        | "integer"
        | "number"
        | "boolean"
        | "enum"
        | "resource_manifest"
        | "device_manifest";
    }
  | {
      type:
        "system" | "location" | "replicant" | "device" | "device_type" | "tag";
    }
  | { type: "entity"; entity_kind: EntityKind };

export interface ParameterDescriptor {
  name: string;
  label: string;
  description: string;
  kind: ParameterKind;
  required: boolean;
  default: unknown;
  options: { value: string; label: string }[];
  validation: {
    minimum: number | null;
    maximum: number | null;
    min_length: number | null;
    max_length: number | null;
  };
}

interface Descriptor {
  kind: string;
  display_name: string;
  aliases: string[];
  description: string;
  category: string;
  operation_class: "report" | "action" | "workflow";
  applicable_to: EntityKind[];
  parameters: ParameterDescriptor[];
}

export interface ReportDescriptor extends Descriptor {
  operation_class: "report";
  risk: "none";
}

export interface DeviceCommandBinding {
  command: string;
  parameters: Record<string, unknown>;
}

export interface ActionDescriptor extends Descriptor {
  operation_class: "action";
  risk: "none" | "low" | "elevated";
  device_commands?: DeviceCommandBinding[];
}

export interface WorkflowDescriptor extends Descriptor {
  operation_class: "workflow";
  risk: "none" | "low" | "elevated";
  supported_triggers: (
    "manual" | "schedule" | "game_event" | "state_condition" | "parent_workflow"
  )[];
}

export interface DescriptorCatalog {
  reports: ReportDescriptor[];
  actions: ActionDescriptor[];
  workflows: WorkflowDescriptor[];
}

export type OperationDescriptor =
  ReportDescriptor | ActionDescriptor | WorkflowDescriptor;

export type FiniteExecutionStatus =
  "running" | "succeeded" | "skipped" | "failed" | "cancelled";

export interface ResultSummary {
  succeeded: number;
  skipped: number;
  failed: number;
}

export interface FiniteExecution {
  id: string;
  operation_class: "report" | "action";
  kind: string;
  status: FiniteExecutionStatus;
  summary: ResultSummary;
  started_at_ms: number;
  finished_at_ms: number;
  result: unknown;
  error: string | null;
  links: EntityRef[];
}

export interface WorkflowDetail {
  summary: WorkflowSummary;
  schema_version: number;
  parameters: Record<string, unknown>;
  wait_reason: string | null;
  parent_id: string | null;
  claims: EntityRef[];
  created_at_ms: number;
  finished_at_ms: number | null;
  error: string | null;
}

export interface RuntimeSnapshot {
  metadata: SnapshotMetadata;
  sync: RuntimeSyncStatus;
  automation: AutomationStatus;
  workflows: WorkflowSummary[];
  requirements: RequirementSummary[];
  notifications: Notification[];
  /** Revision each domain slice had reached when the snapshot was produced. */
  slice_revisions: Partial<Record<DomainSlice, number>>;
  refreshes: RefreshRunSummary[];
}

export interface RequirementSummary {
  id: string;
  name: string;
  target: string;
  scope: string;
  desired: number;
  actual: number;
  in_progress: number;
  missing: number;
  workflow_id: string;
  status: WorkflowStatus;
}

export interface GalaxyPoint {
  x: number;
  y: number;
  z: number;
}

export interface GalaxyStar {
  id: string;
  name: string | null;
  spectral_type: string | null;
  region?: string | null;
  position: GalaxyPoint;
  exploration: "undiscovered" | "partial" | "explored";
  current: boolean;
  has_hub: boolean;
  has_life: boolean;
  has_relay: boolean;
  has_megastructure?: boolean;
}

export interface GalaxySceneSnapshot {
  revision: number;
  generated_at_ms: number;
  stars: GalaxyStar[];
  relay_edges: { from: string; to: string }[];
  active_travel: {
    entity: EntityRef;
    from: string;
    to: string;
    started_at: string | null;
    arrives_at: string | null;
  }[];
  signals: { id: string; label: string | null; position: GalaxyPoint }[];
  highlights: { workflow_id: string; from: string; to: string }[];
  overlays: {
    kind: "life" | "device" | "influence";
    system: string;
    position: GalaxyPoint;
    count: number;
  }[];
  workflow_targets: {
    workflow_id: string;
    workflow_kind: string;
    system: string;
  }[];
}

export type SystemMarkerKind =
  | "star"
  | "planet"
  | "moon"
  | "belt"
  | "lagrange"
  | "location"
  | "vessel"
  | "device"
  | "factory"
  | "relay"
  | "event"
  | "resource_site"
  | "megastructure";

export interface SystemPoint {
  x: number;
  y: number;
}

export interface SystemMarker {
  id: string;
  label: string;
  kind: SystemMarkerKind;
  entity: EntityRef;
  location: string;
  parent: string | null;
  in_habitable_zone: boolean | null;
  position: SystemPoint;
  count: number;
}

export interface SystemSceneSnapshot {
  system: string;
  revision: number;
  generated_at_ms: number;
  markers: SystemMarker[];
  active_travel: {
    entity: EntityRef;
    from: string;
    to: string;
    started_at: string | null;
    arrives_at: string | null;
  }[];
  workflow_markers: {
    workflow_id: string;
    workflow_kind: string;
    location: string;
  }[];
}

export interface EntityRef {
  kind: EntityKind;
  id: string;
}

export interface EntitySummary {
  entity: EntityRef;
  label: string;
  secondary_label: string | null;
  system: string | null;
  location: string | null;
  entity_type: string | null;
  status: string | null;
}

export interface EntityIndexSnapshot {
  metadata: SnapshotMetadata;
  entities: EntitySummary[];
}

export interface WorkflowActivity {
  id: number;
  workflow_id: string;
  occurred_at_ms: number;
  level: "debug" | "info" | "warning" | "error";
  step: string | null;
  message: string;
}

export interface OperationUpdate {
  id: string;
  workflow_id: string | null;
  status: "pending" | "running" | "succeeded" | "failed" | "ambiguous";
  message: string | null;
  updated_at_ms: number;
}

export type NotificationLevel = "info" | "warning" | "error";

export interface Notification {
  id: string;
  level: NotificationLevel;
  title: string;
  message: string;
  created_at_ms: number;
}

export type LiveDelta =
  | { type: "snapshot"; data: SnapshotMetadata }
  | {
      type: "entity_upsert";
      data: { entity: EntityRef; value: EntitySummary };
    }
  | { type: "entity_remove"; data: { entity: EntityRef } }
  | { type: "domain_invalidated"; data: { slice: DomainSlice } }
  | {
      type: "domains_invalidated";
      data: { slices: Partial<Record<DomainSlice, number>> };
    }
  | { type: "workflow_created"; data: WorkflowSummary }
  | { type: "workflow_updated"; data: WorkflowSummary }
  | { type: "workflow_activity"; data: WorkflowActivity }
  | { type: "operation_updated"; data: OperationUpdate }
  | { type: "notification"; data: Notification }
  | { type: "automation_changed"; data: AutomationStatus }
  | {
      type: "daemon_status_changed";
      data: { health: DaemonHealth; sync: RuntimeSyncStatus };
    };

export interface LiveMessage {
  protocol_version: number;
  revision: number;
  delta: LiveDelta;
}

export interface Versioned<T> {
  protocol_version: number;
  payload: T;
}

function record(value: unknown, name: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null)
    throw new Error(`Invalid ${name}`);
  return value as Record<string, unknown>;
}

function oneOf<T extends string>(
  value: unknown,
  values: readonly T[],
  name: string,
): T {
  if (typeof value !== "string" || !values.includes(value as T))
    throw new Error(`Invalid ${name}`);
  return value as T;
}

function nullableString(value: unknown, name: string): string | null {
  if (value !== null && typeof value !== "string")
    throw new Error(`Invalid ${name}`);
  return value;
}

function requiredString(value: unknown, name: string): string {
  if (typeof value !== "string") throw new Error(`Invalid ${name}`);
  return value;
}

function number(value: unknown, name: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value))
    throw new Error(`Invalid ${name}`);
  return value;
}

function optionalFiniteNumber(value: unknown, name: string): number | null {
  if (value === null) return null;
  if (typeof value !== "number" || !Number.isFinite(value))
    throw new Error(`Invalid ${name}`);
  return value;
}

function finiteNumber(value: unknown, name: string): number {
  const parsed = optionalFiniteNumber(value, name);
  if (parsed === null) throw new Error(`Invalid ${name}`);
  return parsed;
}

function array(value: unknown, name: string): unknown[] {
  if (!Array.isArray(value)) throw new Error(`Invalid ${name}`);
  return value;
}

function stringArray(value: unknown, name: string): string[] {
  const values = array(value, name);
  if (!values.every((item) => typeof item === "string"))
    throw new Error(`Invalid ${name}`);
  return values;
}

function boolean(value: unknown, name: string): boolean {
  if (typeof value !== "boolean") throw new Error(`Invalid ${name}`);
  return value;
}

const healthStatuses = ["healthy", "degraded", "unhealthy"] as const;
const syncPhases = [
  "starting",
  "syncing",
  "ready",
  "degraded",
  "offline",
] as const;
const workflowStatuses = [
  "queued",
  "running",
  "waiting",
  "paused",
  "reconciling",
  "succeeded",
  "failed",
  "cancelled",
] as const;
const entityKinds = [
  "system",
  "location",
  "replicant",
  "device",
  "inventory",
  "autofactory",
  "cargo",
  "operation",
  "workflow",
] as const;
const domainSlices = [
  "entities",
  "universe",
  "overview",
  "devices",
  "inventory",
  "autofactories",
  "cargo",
  "missions",
  "history",
  "events",
  "activity",
  "trade",
  "simulations",
  "blueprints",
  "directory",
  "tutorials",
  "messages",
  "bobnet",
  "network",
  "standing",
  "leaderboards",
  "workflows",
  "operations",
  "refresh",
  "director",
] as const;
const refreshPhases = [
  "account",
  "devices",
  "replicants",
  "stars",
  "systems",
  "bodies",
  "events",
  "messages",
  "locations",
  "inventory",
  "simulations",
] as const;
const inventoryOwnerKinds = ["account", "replicant", "location"] as const;
const apiTokenSources = ["environment", "secret_file", "unset"] as const;

function health(value: unknown): DaemonHealth {
  const item = record(value, "daemon health");
  if (typeof item.daemon_version !== "string")
    throw new Error("Invalid daemon version");
  return {
    status: oneOf(item.status, healthStatuses, "health status"),
    daemon_version: item.daemon_version,
    detail: nullableString(item.detail, "health detail"),
  };
}

function sync(value: unknown): RuntimeSyncStatus {
  const item = record(value, "sync status");
  return {
    phase: oneOf(item.phase, syncPhases, "sync phase"),
    revision: number(item.revision, "sync revision"),
    last_event_at_ms:
      item.last_event_at_ms === null
        ? null
        : number(item.last_event_at_ms, "last event time"),
    detail: nullableString(item.detail, "sync detail"),
  };
}

function refreshDelta(value: unknown): RefreshDelta {
  const item = record(value, "refresh delta");
  return {
    proposed_inserts: number(item.proposed_inserts, "proposed inserts"),
    proposed_updates: number(item.proposed_updates, "proposed updates"),
    proposed_tombstones: number(
      item.proposed_tombstones,
      "proposed tombstones",
    ),
    applied_inserts: number(item.applied_inserts, "applied inserts"),
    applied_updates: number(item.applied_updates, "applied updates"),
    applied_tombstones: number(item.applied_tombstones, "applied tombstones"),
  };
}

function refreshRunSummary(value: unknown): RefreshRunSummary {
  const item = record(value, "refresh run");
  if (typeof item.run_id !== "string" || typeof item.status !== "string")
    throw new Error("Invalid refresh run identity");
  return {
    run_id: item.run_id,
    mode: oneOf(item.mode, ["apply", "dry_run"] as const, "refresh mode"),
    status: item.status,
    readiness: oneOf(
      item.readiness,
      ["unavailable", "rest_baseline", "complete"] as const,
      "refresh readiness",
    ),
    current_phase:
      item.current_phase === null
        ? null
        : oneOf(item.current_phase, refreshPhases, "refresh current phase"),
    read_requests_per_minute: number(
      item.read_requests_per_minute,
      "refresh read budget",
    ),
    request_attempts: number(item.request_attempts, "refresh attempts"),
    delta: refreshDelta(item.delta),
    updated_at: number(item.updated_at, "refresh update time"),
  };
}

function automation(value: unknown): AutomationStatus {
  const item = record(value, "automation status");
  if (
    typeof item.automatic_triggers_enabled !== "boolean" ||
    typeof item.workflows_paused !== "boolean"
  )
    throw new Error("Invalid automation status");
  return {
    automatic_triggers_enabled: item.automatic_triggers_enabled,
    workflows_paused: item.workflows_paused,
  };
}

function metadata(value: unknown): SnapshotMetadata {
  const item = record(value, "snapshot metadata");
  return {
    revision: number(item.revision, "snapshot revision"),
    generated_at_ms: number(item.generated_at_ms, "snapshot time"),
  };
}

function workflow(value: unknown): WorkflowSummary {
  const item = record(value, "workflow");
  if (typeof item.id !== "string" || typeof item.kind !== "string")
    throw new Error("Invalid workflow identity");
  return {
    id: item.id,
    kind: item.kind,
    status: oneOf(item.status, workflowStatuses, "workflow status"),
    current_step: nullableString(item.current_step, "workflow step"),
    revision: number(item.revision, "workflow revision"),
    updated_at_ms: number(item.updated_at_ms, "workflow update time"),
  };
}

function trigger(value: unknown): AutomationTrigger {
  const item = record(value, "automation trigger");
  const rawCondition = record(item.condition, "trigger condition");
  const kind = oneOf(
    rawCondition.kind,
    [
      "manual",
      "schedule",
      "game_event",
      "state_condition",
      "parent_workflow",
    ] as const,
    "trigger kind",
  );
  let condition: TriggerCondition;
  switch (kind) {
    case "manual":
      condition = { kind };
      break;
    case "schedule":
      condition = {
        kind,
        interval_seconds: number(
          rawCondition.interval_seconds,
          "schedule interval",
        ),
      };
      break;
    case "game_event":
      if (typeof rawCondition.event_name !== "string")
        throw new Error("Invalid game event name");
      condition = {
        kind,
        event_name: rawCondition.event_name,
        device_code: nullableString(
          rawCondition.device_code,
          "event device code",
        ),
      };
      break;
    case "state_condition":
      condition = {
        kind,
        minimum_revision: number(
          rawCondition.minimum_revision,
          "minimum state revision",
        ),
      };
      break;
    case "parent_workflow":
      condition = {
        kind,
        parent_kind: nullableString(rawCondition.parent_kind, "parent kind"),
        status: oneOf(
          rawCondition.status,
          workflowStatuses,
          "parent workflow status",
        ),
      };
      break;
  }
  const rawTarget = record(item.target, "trigger target");
  if (
    typeof item.id !== "string" ||
    typeof item.name !== "string" ||
    typeof item.enabled !== "boolean" ||
    typeof rawTarget.kind !== "string"
  )
    throw new Error("Invalid trigger");
  return {
    id: item.id,
    name: item.name,
    condition,
    target: {
      operation_class: oneOf(
        rawTarget.operation_class,
        ["action", "workflow"] as const,
        "trigger target class",
      ),
      kind: rawTarget.kind,
      parameters: record(rawTarget.parameters, "trigger parameters"),
    },
    enabled: item.enabled,
    created_at_ms: number(item.created_at_ms, "trigger creation time"),
    updated_at_ms: number(item.updated_at_ms, "trigger update time"),
    last_fired_at_ms:
      item.last_fired_at_ms === null
        ? null
        : number(item.last_fired_at_ms, "last trigger time"),
    next_run_at_ms:
      item.next_run_at_ms === null
        ? null
        : number(item.next_run_at_ms, "next trigger time"),
    last_error: nullableString(item.last_error, "trigger error"),
    revision: number(item.revision, "trigger revision"),
  };
}

function parameter(value: unknown): ParameterDescriptor {
  const item = record(value, "parameter descriptor");
  const rawKind = record(item.kind, "parameter kind");
  const kindType = oneOf(
    rawKind.type,
    [
      "string",
      "integer",
      "number",
      "boolean",
      "enum",
      "system",
      "location",
      "replicant",
      "device",
      "device_type",
      "tag",
      "resource_manifest",
      "device_manifest",
      "entity",
    ] as const,
    "parameter kind",
  );
  const kind: ParameterKind =
    kindType === "entity"
      ? {
          type: kindType,
          entity_kind: oneOf(
            rawKind.entity_kind,
            entityKinds,
            "parameter entity kind",
          ),
        }
      : { type: kindType };
  if (
    typeof item.name !== "string" ||
    typeof item.label !== "string" ||
    typeof item.description !== "string" ||
    typeof item.required !== "boolean" ||
    !Array.isArray(item.options)
  )
    throw new Error("Invalid parameter descriptor");
  const validation = record(item.validation, "parameter validation");
  return {
    name: item.name,
    label: item.label,
    description: item.description,
    kind,
    required: item.required,
    default: item.default,
    options: item.options.map((option) => {
      const value = record(option, "parameter option");
      if (typeof value.value !== "string" || typeof value.label !== "string")
        throw new Error("Invalid parameter option");
      return { value: value.value, label: value.label };
    }),
    validation: {
      minimum: optionalFiniteNumber(validation.minimum, "parameter minimum"),
      maximum: optionalFiniteNumber(validation.maximum, "parameter maximum"),
      min_length:
        validation.min_length === null
          ? null
          : number(validation.min_length, "parameter minimum length"),
      max_length:
        validation.max_length === null
          ? null
          : number(validation.max_length, "parameter maximum length"),
    },
  };
}

function descriptor(value: unknown, label: string) {
  const item = record(value, label);
  if (
    typeof item.kind !== "string" ||
    typeof item.display_name !== "string" ||
    !Array.isArray(item.aliases) ||
    !item.aliases.every((alias) => typeof alias === "string") ||
    typeof item.description !== "string" ||
    typeof item.category !== "string" ||
    !Array.isArray(item.applicable_to) ||
    !Array.isArray(item.parameters)
  )
    throw new Error(`Invalid ${label}`);
  return {
    kind: item.kind,
    display_name: item.display_name,
    aliases: item.aliases,
    description: item.description,
    category: item.category,
    operation_class: oneOf(
      item.operation_class,
      ["report", "action", "workflow"] as const,
      "operation class",
    ),
    applicable_to: item.applicable_to.map((kind) =>
      oneOf(kind, entityKinds, "applicable entity kind"),
    ),
    parameters: item.parameters.map(parameter),
  };
}

function reportDescriptor(value: unknown): ReportDescriptor {
  const item = record(value, "report descriptor");
  return {
    ...descriptor(value, "report descriptor"),
    operation_class: oneOf(
      item.operation_class,
      ["report"] as const,
      "report operation class",
    ),
    risk: oneOf(item.risk, ["none"] as const, "report risk"),
  };
}

function workflowDescriptor(value: unknown): WorkflowDescriptor {
  const item = record(value, "workflow descriptor");
  if (!Array.isArray(item.supported_triggers))
    throw new Error("Invalid workflow descriptor");
  return {
    ...descriptor(value, "workflow descriptor"),
    operation_class: oneOf(
      item.operation_class,
      ["workflow"] as const,
      "workflow operation class",
    ),
    risk: oneOf(item.risk, ["none", "low", "elevated"] as const, "risk"),
    supported_triggers: item.supported_triggers.map((trigger) =>
      oneOf(
        trigger,
        [
          "manual",
          "schedule",
          "game_event",
          "state_condition",
          "parent_workflow",
        ] as const,
        "workflow trigger",
      ),
    ),
  };
}

function workflowDetail(value: unknown): WorkflowDetail {
  const item = record(value, "workflow detail");
  if (!Array.isArray(item.claims)) throw new Error("Invalid workflow claims");
  return {
    summary: workflow(item.summary),
    schema_version: number(item.schema_version, "workflow schema version"),
    parameters: record(item.parameters, "workflow parameters"),
    wait_reason: nullableString(item.wait_reason, "workflow wait reason"),
    parent_id: nullableString(item.parent_id, "workflow parent"),
    claims: item.claims.map(entity),
    created_at_ms: number(item.created_at_ms, "workflow creation time"),
    finished_at_ms:
      item.finished_at_ms === null
        ? null
        : number(item.finished_at_ms, "workflow finish time"),
    error: nullableString(item.error, "workflow error"),
  };
}

function entity(value: unknown): EntityRef {
  const item = record(value, "entity reference");
  if (typeof item.id !== "string") throw new Error("Invalid entity id");
  return { kind: oneOf(item.kind, entityKinds, "entity kind"), id: item.id };
}

function entitySummary(value: unknown): EntitySummary {
  const item = record(value, "entity summary");
  if (typeof item.label !== "string") throw new Error("Invalid entity label");
  return {
    entity: entity(item.entity),
    label: item.label,
    secondary_label: nullableString(
      item.secondary_label,
      "entity secondary label",
    ),
    system: nullableString(item.system, "entity system"),
    location: nullableString(item.location, "entity location"),
    entity_type: nullableString(item.entity_type, "entity type"),
    status: nullableString(item.status, "entity status"),
  };
}

function optionalString(value: unknown, name: string): string | null {
  return value === undefined ? null : nullableString(value, name);
}

function optionalBoolean(value: unknown, name: string): boolean | null {
  if (value === undefined || value === null) return null;
  if (typeof value !== "boolean") throw new Error(`Invalid ${name}`);
  return value;
}

function optionalNumber(value: unknown, name: string): number | null {
  return value === undefined ? null : optionalFiniteNumber(value, name);
}

function optionalInteger(value: unknown, name: string): number | null {
  if (value === undefined || value === null) return null;
  return number(value, name);
}

function entityCollection(value: unknown): EntityCollectionSummary {
  const item = value === undefined ? {} : record(value, "entity collection");
  return {
    total:
      item.total === undefined
        ? 0
        : number(item.total, "entity collection total"),
    items:
      item.items === undefined
        ? []
        : array(item.items, "entity collection items").map(entitySummary),
    groups:
      item.groups === undefined
        ? []
        : array(item.groups, "entity collection groups").map((value) => {
            const group = record(value, "entity collection group");
            return {
              entity_kind: oneOf(
                group.entity_kind,
                entityKinds,
                "entity group kind",
              ),
              entity_type: optionalString(
                group.entity_type,
                "entity group type",
              ),
              count: number(group.count, "entity group count"),
              statuses:
                group.statuses === undefined
                  ? []
                  : array(group.statuses, "entity group statuses").map(
                      (value) => {
                        const status = record(value, "entity status count");
                        return {
                          status: optionalString(
                            status.status,
                            "entity group status",
                          ),
                          count: number(status.count, "entity status count"),
                        };
                      },
                    ),
            };
          }),
  };
}

function locationSurvey(value: unknown): LocationSurveySummary {
  const item = value === undefined ? {} : record(value, "location survey");
  return {
    planets_total: optionalInteger(item.planets_total, "planet total"),
    planets_scanned: optionalInteger(item.planets_scanned, "planets scanned"),
    moons_total: optionalInteger(item.moons_total, "moon total"),
    moons_scanned: optionalInteger(item.moons_scanned, "moons scanned"),
    moons_total_estimated: optionalBoolean(
      item.moons_total_estimated,
      "estimated moon total",
    ),
  };
}

function locationEnvironment(value: unknown): LocationEnvironmentSummary {
  const item = value === undefined ? {} : record(value, "location environment");
  return {
    atmosphere: optionalString(item.atmosphere, "atmosphere"),
    magnetic_field: optionalBoolean(item.magnetic_field, "magnetic field"),
    gravity_g: optionalNumber(item.gravity_g, "gravity"),
    surface_temperature_c: optionalNumber(
      item.surface_temperature_c,
      "surface temperature",
    ),
    habitable_zone: optionalBoolean(item.habitable_zone, "habitable zone"),
    life_stage: optionalString(item.life_stage, "life stage"),
    axial_tilt_degrees: optionalNumber(item.axial_tilt_degrees, "axial tilt"),
    rotation_state: optionalString(item.rotation_state, "rotation state"),
    star_spectral_type: optionalString(
      item.star_spectral_type,
      "star spectral type",
    ),
    nearby_belt_richness: optionalString(
      item.nearby_belt_richness,
      "nearby belt richness",
    ),
    distance_from_sol_light_years: optionalNumber(
      item.distance_from_sol_light_years,
      "distance from Sol",
    ),
  };
}

function entityInspector(value: unknown): EntityInspectorSnapshot {
  const snapshot = record(value, "entity inspector snapshot");
  const tagged = record(snapshot.detail, "entity inspector detail");
  const kind = oneOf(
    tagged.kind,
    ["device", "system", "location"] as const,
    "entity inspector kind",
  );
  const detail = record(tagged.detail, "entity inspector kind detail");
  const provenance =
    snapshot.provenance === undefined || snapshot.provenance === null
      ? null
      : (() => {
          const item = record(snapshot.provenance, "entity provenance");
          if (
            typeof item.stale !== "boolean" ||
            typeof item.reachability !== "string" ||
            typeof item.source_operation !== "string"
          )
            throw new Error("Invalid entity provenance");
          return {
            observed_at_ms: number(
              item.observed_at_ms,
              "entity observation time",
            ),
            stale: item.stale,
            reachability: item.reachability,
            source_operation: item.source_operation,
          };
        })();
  let parsedDetail: EntityInspectorDetail;
  if (kind === "device") {
    parsedDetail = { kind, detail: parseDeviceSummary(detail) };
  } else if (kind === "system") {
    parsedDetail = {
      kind,
      detail: {
        name: optionalString(detail.name, "system name"),
        spectral_type: optionalString(detail.spectral_type, "spectral type"),
        region: optionalString(detail.region, "system region"),
        entry_point: optionalString(detail.entry_point, "region entry point"),
        position:
          detail.position === undefined || detail.position === null
            ? null
            : point(detail.position),
        explored: optionalBoolean(detail.explored, "system explored"),
        has_hub: optionalBoolean(detail.has_hub, "system hub"),
        has_ward: optionalBoolean(detail.has_ward, "system ward"),
        has_life: optionalBoolean(detail.has_life, "system life"),
        children: entityCollection(detail.children),
      },
    };
  } else {
    parsedDetail = {
      kind,
      detail: {
        location_type: optionalString(detail.location_type, "location type"),
        system: optionalString(detail.system, "location system"),
        parent: optionalString(detail.parent, "parent location"),
        scanned: optionalBoolean(detail.scanned, "location scanned"),
        system_scanned: optionalBoolean(
          detail.system_scanned,
          "system scanned",
        ),
        system_tags:
          detail.system_tags === undefined
            ? []
            : stringArray(detail.system_tags, "system tags"),
        survey: locationSurvey(detail.survey),
        environment: locationEnvironment(detail.environment),
        contents: entityCollection(detail.contents),
      },
    };
  }
  return {
    metadata: metadata(snapshot.metadata),
    summary: entitySummary(snapshot.summary),
    provenance,
    detail: parsedDetail,
  };
}

function finiteExecution(value: unknown): FiniteExecution {
  const item = record(value, "finite execution");
  const summary = record(item.summary, "result summary");
  if (
    typeof item.id !== "string" ||
    typeof item.kind !== "string" ||
    !Array.isArray(item.links)
  )
    throw new Error("Invalid finite execution");
  return {
    id: item.id,
    operation_class: oneOf(
      item.operation_class,
      ["report", "action"] as const,
      "finite execution class",
    ),
    kind: item.kind,
    status: oneOf(
      item.status,
      ["running", "succeeded", "skipped", "failed", "cancelled"] as const,
      "finite execution status",
    ),
    summary: {
      succeeded: number(summary.succeeded, "successful result count"),
      skipped: number(summary.skipped, "skipped result count"),
      failed: number(summary.failed, "failed result count"),
    },
    started_at_ms: number(item.started_at_ms, "execution start time"),
    finished_at_ms: number(item.finished_at_ms, "execution finish time"),
    result: item.result,
    error: nullableString(item.error, "execution error"),
    links: item.links.map(entity),
  };
}

function point(value: unknown): GalaxyPoint {
  const item = record(value, "galaxy point");
  return {
    x: finiteNumber(item.x, "galaxy x"),
    y: finiteNumber(item.y, "galaxy y"),
    z: finiteNumber(item.z, "galaxy z"),
  };
}

function stringPair(value: unknown, name: string) {
  const item = record(value, name);
  if (typeof item.from !== "string" || typeof item.to !== "string")
    throw new Error(`Invalid ${name}`);
  return { from: item.from, to: item.to };
}

function activity(value: unknown): WorkflowActivity {
  const item = record(value, "workflow activity");
  if (typeof item.workflow_id !== "string" || typeof item.message !== "string")
    throw new Error("Invalid workflow activity");
  return {
    id: number(item.id, "activity id"),
    workflow_id: item.workflow_id,
    occurred_at_ms: number(item.occurred_at_ms, "activity time"),
    level: oneOf(
      item.level,
      ["debug", "info", "warning", "error"] as const,
      "activity level",
    ),
    step: nullableString(item.step, "activity step"),
    message: item.message,
  };
}

function operation(value: unknown): OperationUpdate {
  const item = record(value, "operation update");
  if (typeof item.id !== "string") throw new Error("Invalid operation id");
  return {
    id: item.id,
    workflow_id: nullableString(item.workflow_id, "operation workflow"),
    status: oneOf(
      item.status,
      ["pending", "running", "succeeded", "failed", "ambiguous"] as const,
      "operation status",
    ),
    message: nullableString(item.message, "operation message"),
    updated_at_ms: number(item.updated_at_ms, "operation update time"),
  };
}

function notification(value: unknown): Notification {
  const item = record(value, "notification");
  if (
    typeof item.id !== "string" ||
    typeof item.title !== "string" ||
    typeof item.message !== "string"
  )
    throw new Error("Invalid notification");
  return {
    id: item.id,
    level: oneOf(
      item.level,
      ["info", "warning", "error"] as const,
      "notification level",
    ),
    title: item.title,
    message: item.message,
    created_at_ms: number(item.created_at_ms, "notification time"),
  };
}

function envelope<T>(
  value: unknown,
  parse: (payload: unknown) => T,
): Versioned<T> {
  const item = record(value, "daemon response");
  if (item.protocol_version !== PROTOCOL_VERSION)
    throw new Error("Unsupported daemon protocol version");
  return { protocol_version: PROTOCOL_VERSION, payload: parse(item.payload) };
}

export function parseHealthResponse(value: unknown): Versioned<DaemonHealth> {
  return envelope(value, health);
}

export function parseOverviewResponse(
  value: unknown,
): Versioned<OverviewSnapshot> {
  return envelope(value, (payload) => {
    const item = record(payload, "overview snapshot");
    return {
      metadata: metadata(item.metadata),
      health: health(item.health),
      sync: sync(item.sync),
      automation: automation(item.automation),
      replicants: array(item.replicants, "overview replicants").map((value) => {
        const replicant = record(value, "overview replicant");
        return {
          entity: entity(replicant.entity),
          name: nullableString(replicant.name, "replicant name"),
          system: nullableString(replicant.system, "replicant system"),
          location: nullableString(replicant.location, "replicant location"),
          status: nullableString(replicant.status, "replicant status"),
        };
      }),
      active_travel: array(item.active_travel, "overview travel").map(
        (value) => {
          const travel = record(value, "overview travel");
          return {
            entity: entity(travel.entity),
            from: nullableString(travel.from, "travel origin"),
            to: nullableString(travel.to, "travel destination"),
            arrives_at: nullableString(travel.arrives_at, "travel arrival"),
          };
        },
      ),
      active_workflows: array(item.active_workflows, "active workflows").map(
        workflow,
      ),
      workflow_counts: array(item.workflow_counts, "workflow counts").map(
        (value) => {
          const count = record(value, "workflow count");
          return {
            status: oneOf(count.status, workflowStatuses, "workflow status"),
            count: number(count.count, "workflow count"),
          };
        },
      ),
      attention_workflows: array(
        item.attention_workflows,
        "attention workflows",
      ).map(workflow),
      notifications: array(item.notifications, "overview notifications").map(
        notification,
      ),
      recent_activity: array(item.recent_activity, "recent activity").map(
        activity,
      ),
    };
  });
}

export function parseDevicesResponse(
  value: unknown,
): Versioned<DevicesSnapshot> {
  return envelope(value, (payload) => {
    const snapshot = record(payload, "devices snapshot");
    return {
      metadata: metadata(snapshot.metadata),
      devices: array(snapshot.devices, "devices").map(parseDeviceSummary),
    };
  });
}

function parseCargoResource(value: unknown): CargoResourceSummary {
  const resource = record(value, "cargo resource");
  if (typeof resource.resource !== "string")
    throw new Error("Invalid cargo resource");
  return {
    resource: resource.resource,
    quantity: number(resource.quantity, "cargo resource quantity"),
  };
}

function parseDeviceSummary(value: unknown): DeviceSummary {
  const device = record(value, "device");
  if (typeof device.ownership !== "string")
    throw new Error("Invalid device ownership");
  const claim =
    device.claim === null
      ? null
      : (() => {
          const item = record(device.claim, "device claim");
          if (
            typeof item.workflow_id !== "string" ||
            typeof item.workflow_kind !== "string"
          )
            throw new Error("Invalid device claim");
          return {
            workflow_id: item.workflow_id,
            workflow_kind: item.workflow_kind,
            workflow_status: oneOf(
              item.workflow_status,
              workflowStatuses,
              "workflow status",
            ),
          };
        })();
  return {
    entity: entity(device.entity),
    device_type: nullableString(device.device_type, "device type"),
    status: nullableString(device.status, "device status"),
    ownership: device.ownership,
    owner: nullableString(device.owner, "device owner"),
    owner_name:
      device.owner_name === undefined
        ? null
        : nullableString(device.owner_name, "device owner name"),
    system: nullableString(device.system, "device system"),
    region:
      device.region === undefined
        ? null
        : nullableString(device.region, "device region"),
    location: nullableString(device.location, "device location"),
    available_commands:
      device.available_commands === undefined
        ? []
        : stringArray(device.available_commands, "available device commands"),
    available_directives:
      device.available_directives === undefined
        ? []
        : stringArray(
            device.available_directives,
            "available device directives",
          ),
    features:
      device.features === undefined
        ? []
        : stringArray(device.features, "device features"),
    tags: stringArray(device.tags, "device tags"),
    attached_to: nullableString(device.attached_to, "attached device"),
    stowed_in: nullableString(device.stowed_in, "stowed device"),
    controller: nullableString(device.controller, "device controller"),
    linked_device: nullableString(device.linked_device, "linked device"),
    attached_devices: stringArray(device.attached_devices, "attached devices"),
    controlled_devices: stringArray(
      device.controlled_devices,
      "controlled devices",
    ),
    stowed_devices: stringArray(device.stowed_devices, "stowed devices"),
    attach_capacity:
      device.attach_capacity === null
        ? null
        : number(device.attach_capacity, "attach capacity"),
    cargo_capacity:
      device.cargo_capacity === null
        ? null
        : number(device.cargo_capacity, "cargo capacity"),
    cargo_used:
      device.cargo_used === null
        ? null
        : number(device.cargo_used, "cargo used"),
    cargo:
      device.cargo === undefined
        ? []
        : array(device.cargo, "device cargo").map(parseCargoResource),
    stow_capacity:
      device.stow_capacity === undefined || device.stow_capacity === null
        ? null
        : number(device.stow_capacity, "stow capacity"),
    stow_used:
      device.stow_used === undefined || device.stow_used === null
        ? null
        : number(device.stow_used, "stow used"),
    operational_capacity_percent: optionalFiniteNumber(
      device.operational_capacity_percent,
      "operational capacity",
    ),
    grace_period_remaining:
      device.grace_period_remaining === undefined ||
      device.grace_period_remaining === null
        ? null
        : number(device.grace_period_remaining, "grace period remaining"),
    upkeep_requirements:
      device.upkeep_requirements === undefined
        ? []
        : array(device.upkeep_requirements, "upkeep requirements").map(
            (value) => record(value, "upkeep requirement"),
          ),
    system_status:
      device.system_status === undefined || device.system_status === null
        ? null
        : record(device.system_status, "device system status"),
    active_directive: nullableString(
      device.active_directive,
      "active directive",
    ),
    directive_status: nullableString(
      device.directive_status,
      "directive status",
    ),
    travel_destination: nullableString(
      device.travel_destination,
      "travel destination",
    ),
    claim,
  };
}

export function parseSurveyResponse(value: unknown): Versioned<SurveySnapshot> {
  return envelope(value, (payload) => {
    const snapshot = record(payload, "survey snapshot");
    return {
      metadata: metadata(snapshot.metadata),
      missions: array(snapshot.missions, "survey missions").map((value) => {
        const mission = record(value, "survey mission");
        if (
          typeof mission.replicant !== "string" ||
          typeof mission.vessel !== "string" ||
          typeof mission.center !== "string" ||
          typeof mission.phase !== "string"
        )
          throw new Error("Invalid survey mission");
        return {
          workflow: workflow(mission.workflow),
          replicant: mission.replicant,
          vessel: mission.vessel,
          center: mission.center,
          phase: mission.phase,
          completed_systems: number(
            mission.completed_systems,
            "completed survey systems",
          ),
          total_systems: number(mission.total_systems, "survey systems"),
          next_system: nullableString(mission.next_system, "next system"),
          controller: nullableString(mission.controller, "survey controller"),
          drones: stringArray(mission.drones, "survey drones"),
        };
      }),
      fleet: array(snapshot.fleet, "survey fleet").map(parseDeviceSummary),
    };
  });
}

export function parseMiningResponse(value: unknown): Versioned<MiningSnapshot> {
  return envelope(value, (payload) => {
    const snapshot = record(payload, "mining snapshot");
    return {
      metadata: metadata(snapshot.metadata),
      installations: array(snapshot.installations, "mining installations").map(
        (value) => {
          const installation = record(value, "mining installation");
          if (typeof installation.id !== "string")
            throw new Error("Invalid mining installation");
          return {
            id: installation.id,
            system: nullableString(installation.system, "mining system"),
            location: nullableString(installation.location, "mining location"),
            controller:
              installation.controller === null
                ? null
                : parseDeviceSummary(installation.controller),
            miners: array(installation.miners, "mining drones").map(
              parseDeviceSummary,
            ),
            survey_controller:
              installation.survey_controller === null
                ? null
                : parseDeviceSummary(installation.survey_controller),
            survey_drones: array(
              installation.survey_drones,
              "mining survey drones",
            ).map(parseDeviceSummary),
            maintenance_device:
              installation.maintenance_device === null
                ? null
                : parseDeviceSummary(installation.maintenance_device),
            missing: stringArray(
              installation.missing,
              "missing mining devices",
            ),
            status: oneOf(
              installation.status,
              ["complete", "partial"] as const,
              "mining installation status",
            ),
          };
        },
      ),
      workflows: array(snapshot.workflows, "mining workflows").map(workflow),
    };
  });
}

export function parseEntityInspectorResponse(
  value: unknown,
): Versioned<EntityInspectorSnapshot> {
  return envelope(value, entityInspector);
}

export function parseRelayResponse(value: unknown): Versioned<RelaySnapshot> {
  return envelope(value, (payload) => {
    const snapshot = record(payload, "relay snapshot");
    return {
      metadata: metadata(snapshot.metadata),
      relays: array(snapshot.relays, "relays").map(parseDeviceSummary),
      staged_relays: array(snapshot.staged_relays, "staged relays").map(
        parseDeviceSummary,
      ),
      connected_systems: number(
        snapshot.connected_systems,
        "connected relay systems",
      ),
      relay_edges: array(snapshot.relay_edges, "relay edges").map((value) => {
        const edge = record(value, "relay edge");
        if (typeof edge.from !== "string" || typeof edge.to !== "string")
          throw new Error("Invalid relay edge");
        return { from: edge.from, to: edge.to };
      }),
      expansions: array(snapshot.expansions, "relay expansions").map(
        (value) => {
          const expansion = record(value, "relay expansion");
          if (
            typeof expansion.replicant !== "string" ||
            typeof expansion.hub !== "string" ||
            typeof expansion.phase !== "string"
          )
            throw new Error("Invalid relay expansion");
          return {
            workflow: workflow(expansion.workflow),
            replicant: expansion.replicant,
            hub: expansion.hub,
            targets: stringArray(expansion.targets, "relay targets"),
            phase: expansion.phase,
            completed_stops: number(
              expansion.completed_stops,
              "completed relay stops",
            ),
            total_stops: optionalFiniteNumber(
              expansion.total_stops,
              "relay stops",
            ),
            next_system: nullableString(
              expansion.next_system,
              "next relay system",
            ),
            pending_relays: optionalFiniteNumber(
              expansion.pending_relays,
              "pending relays",
            ),
          };
        },
      ),
    };
  });
}

export function parseBootstrapResponse(
  value: unknown,
): Versioned<BootstrapSnapshot> {
  return envelope(value, (payload) => {
    const snapshot = record(payload, "bootstrap snapshot");
    return {
      metadata: metadata(snapshot.metadata),
      missions: array(snapshot.missions, "bootstrap missions").map((value) => {
        const mission = record(value, "bootstrap mission");
        if (
          typeof mission.mission_id !== "string" ||
          typeof mission.execution_id !== "string" ||
          typeof mission.region !== "string" ||
          typeof mission.source_hub !== "string" ||
          typeof mission.target_system !== "string" ||
          typeof mission.target_location !== "string" ||
          typeof mission.phase !== "string" ||
          typeof mission.completed !== "boolean"
        )
          throw new Error("Invalid bootstrap mission");
        return {
          mission_id: mission.mission_id,
          execution_id: mission.execution_id,
          region: mission.region,
          source_hub: mission.source_hub,
          target_system: mission.target_system,
          target_location: mission.target_location,
          phase: mission.phase,
          reserved_devices: number(
            mission.reserved_devices,
            "reserved bootstrap devices",
          ),
          loaded_devices: number(
            mission.loaded_devices,
            "loaded bootstrap devices",
          ),
          capital_system: nullableString(
            mission.capital_system,
            "bootstrap capital",
          ),
          selected_sites: number(mission.selected_sites, "bootstrap sites"),
          warnings: stringArray(mission.warnings, "bootstrap warnings"),
          completed: mission.completed,
          updated_at_ms: number(mission.updated_at_ms, "bootstrap update time"),
        };
      }),
    };
  });
}

export function parseEventsResponse(value: unknown): Versioned<EventsSnapshot> {
  return envelope(value, (payload) => {
    const snapshot = record(payload, "events snapshot");
    return {
      metadata: metadata(snapshot.metadata),
      events: array(snapshot.events, "events").map((value) => {
        const event = record(value, "event");
        if (
          typeof event.designation !== "string" ||
          typeof event.title !== "string" ||
          typeof event.system !== "string" ||
          typeof event.location !== "string"
        )
          throw new Error("Invalid event");
        const rewards = record(event.rewards, "event rewards");
        const rewardItems = (value: unknown, name: string) =>
          array(value, name).map((value) => {
            const item = record(value, name);
            if (typeof item.item !== "string")
              throw new Error(`Invalid ${name}`);
            return {
              item: item.item,
              quantity: number(item.quantity, `${name} quantity`),
            };
          });
        return {
          designation: event.designation,
          title: event.title,
          event_type: nullableString(event.event_type, "event type"),
          category: nullableString(event.category, "event category"),
          tier: optionalFiniteNumber(event.tier, "event tier"),
          system: event.system,
          location: event.location,
          description: nullableString(event.description, "event description"),
          criteria: array(event.criteria, "event criteria").map((value) => {
            const criterion = record(value, "event criterion");
            if (
              typeof criterion.name !== "string" ||
              typeof criterion.complete !== "boolean"
            )
              throw new Error("Invalid event criterion");
            return {
              name: criterion.name,
              complete: criterion.complete,
              requirements: array(
                criterion.requirements,
                "event requirements",
              ).map((value) => {
                const requirement = record(value, "event requirement");
                if (typeof requirement.item !== "string")
                  throw new Error("Invalid event requirement");
                return {
                  kind: oneOf(
                    requirement.kind,
                    ["resource", "device"] as const,
                    "event requirement kind",
                  ),
                  item: requirement.item,
                  required: number(requirement.required, "event required"),
                  completed: number(requirement.completed, "event completed"),
                  remaining: number(requirement.remaining, "event remaining"),
                };
              }),
            };
          }),
          rewards: {
            resources: rewardItems(rewards.resources, "event resource reward"),
            devices: rewardItems(rewards.devices, "event device reward"),
            xp: optionalFiniteNumber(rewards.xp, "event XP"),
            civilisation_points: optionalFiniteNumber(
              rewards.civilisation_points,
              "event civilisation points",
            ),
            completion_achievement: nullableString(
              rewards.completion_achievement,
              "event achievement",
            ),
          },
          status: nullableString(event.status, "event status"),
          discovered_at: nullableString(
            event.discovered_at,
            "event discovery time",
          ),
          completed_at: nullableString(
            event.completed_at,
            "event completion time",
          ),
        };
      }),
    };
  });
}

export function parseActivityResponse(
  value: unknown,
): Versioned<AccountEventsSnapshot> {
  return envelope(value, (payload) => {
    const snapshot = record(payload, "account activity snapshot");
    return {
      metadata: metadata(snapshot.metadata),
      cursor: nullableString(snapshot.cursor, "account event cursor"),
      events: array(snapshot.events, "account events").map((value) => {
        const event = record(value, "account event");
        if (
          typeof event.id !== "string" ||
          typeof event.name !== "string" ||
          typeof event.category !== "string" ||
          typeof event.occurred_at !== "string" ||
          typeof event.ami_digest !== "boolean"
        )
          throw new Error("Invalid account event");
        return {
          id: event.id,
          name: event.name,
          category: event.category,
          device: event.device === null ? null : entity(event.device),
          replicant: event.replicant === null ? null : entity(event.replicant),
          system: nullableString(event.system, "event system"),
          location: nullableString(event.location, "event location"),
          occurred_at: event.occurred_at,
          payload: record(event.payload, "event payload"),
          ami_digest: event.ami_digest,
        };
      }),
    };
  });
}

export function parseDeviceLogsResponse(
  value: unknown,
): Versioned<DeviceLogsSnapshot> {
  return envelope(value, (payload) => {
    const snapshot = record(payload, "device logs snapshot");
    return {
      metadata: metadata(snapshot.metadata),
      device: entity(snapshot.device),
      events: array(snapshot.events, "device logs").map((value) => {
        const event = record(value, "device log");
        return {
          id: optionalFiniteNumber(event.id, "device log id"),
          created_at: nullableString(event.created_at, "device log time"),
          device_code: nullableString(event.device_code, "device log code"),
          device_type: nullableString(event.device_type, "device log type"),
          event_type: nullableString(event.event_type, "device log event type"),
          message: nullableString(event.message, "device log message"),
          payload: record(event.payload, "device log payload"),
        };
      }),
      next_cursor: optionalFiniteNumber(
        snapshot.next_cursor,
        "device log cursor",
      ),
    };
  });
}

function parseInventoryQuantities(
  value: unknown,
  name: string,
): InventoryQuantity[] {
  return array(value, name).map((value) => {
    const item = record(value, name);
    if (typeof item.resource !== "string") throw new Error(`Invalid ${name}`);
    return {
      resource: item.resource,
      quantity: number(item.quantity, `${name} quantity`),
    };
  });
}

function parseSimulationRun(value: unknown): SimulationRunSummary {
  const run = record(value, "simulation run");
  if (typeof run.is_mine !== "boolean")
    throw new Error("Invalid simulation run");
  return {
    id: number(run.id, "simulation id"),
    interface: run.interface === null ? null : entity(run.interface),
    is_mine: run.is_mine,
    replicant: run.replicant === null ? null : entity(run.replicant),
    replicant_name: nullableString(run.replicant_name, "simulation replicant"),
    scenario_code: nullableString(run.scenario_code, "scenario code"),
    scenario_name: nullableString(run.scenario_name, "scenario name"),
    lifecycle: nullableString(run.lifecycle, "simulation lifecycle"),
    started_at: nullableString(run.started_at, "simulation start"),
    completed_at: nullableString(run.completed_at, "simulation completion"),
    abandoned_at: nullableString(run.abandoned_at, "simulation abandonment"),
    timed_out_at: nullableString(run.timed_out_at, "simulation timeout"),
    score_seconds: optionalFiniteNumber(run.score_seconds, "simulation score"),
    resources_mined: optionalFiniteNumber(
      run.resources_mined,
      "resources mined",
    ),
    devices_printed: optionalFiniteNumber(
      run.devices_printed,
      "devices printed",
    ),
    timeout_hours: optionalFiniteNumber(
      run.timeout_hours,
      "simulation timeout hours",
    ),
  };
}

export function parseSimulationsResponse(
  value: unknown,
): Versioned<SimulationsSnapshot> {
  return envelope(value, (payload) => {
    const snapshot = record(payload, "simulations snapshot");
    return {
      metadata: metadata(snapshot.metadata),
      interfaces: array(snapshot.interfaces, "simulation interfaces").map(
        (value) => {
          const item = record(value, "simulation interface");
          return {
            device: parseDeviceSummary(item.device),
            scenarios: array(item.scenarios, "simulation scenarios").map(
              (value) => {
                const scenario = record(value, "simulation scenario");
                if (typeof scenario.code !== "string")
                  throw new Error("Invalid simulation scenario");
                return {
                  code: scenario.code,
                  name: nullableString(scenario.name, "scenario name"),
                  description: nullableString(
                    scenario.description,
                    "scenario description",
                  ),
                  long_description: nullableString(
                    scenario.long_description,
                    "scenario rules",
                  ),
                  objective_type: nullableString(
                    scenario.objective_type,
                    "scenario objective",
                  ),
                  objective_target: optionalFiniteNumber(
                    scenario.objective_target,
                    "scenario target",
                  ),
                  timeout_hours: optionalFiniteNumber(
                    scenario.timeout_hours,
                    "scenario timeout",
                  ),
                  version: optionalFiniteNumber(
                    scenario.version,
                    "scenario version",
                  ),
                  entry_cost: parseInventoryQuantities(
                    scenario.entry_cost,
                    "scenario entry cost",
                  ),
                };
              },
            ),
            active: array(item.active, "active simulations").map(
              parseSimulationRun,
            ),
            error: nullableString(item.error, "simulation interface error"),
          };
        },
      ),
      managed_history: array(
        snapshot.managed_history,
        "managed simulation history",
      ).map(parseSimulationRun),
      account_history: array(
        snapshot.account_history,
        "account simulation history",
      ).map(parseSimulationRun),
    };
  });
}

export function parseBlueprintsResponse(
  value: unknown,
): Versioned<BlueprintsSnapshot> {
  return envelope(value, (payload) => {
    const snapshot = record(payload, "blueprints snapshot");
    return {
      metadata: metadata(snapshot.metadata),
      blueprints: array(snapshot.blueprints, "blueprints").map((value) => {
        const blueprint = record(value, "blueprint");
        if (typeof blueprint.device_type !== "string")
          throw new Error("Invalid blueprint");
        return {
          device_type: blueprint.device_type,
          short_description: nullableString(
            blueprint.short_description,
            "blueprint short description",
          ),
          description: nullableString(
            blueprint.description,
            "blueprint description",
          ),
          print_time_seconds: optionalFiniteNumber(
            blueprint.print_time_seconds,
            "blueprint print time",
          ),
          resources: parseInventoryQuantities(
            blueprint.resources,
            "blueprint resource",
          ),
          components: parseInventoryQuantities(
            blueprint.components,
            "blueprint component",
          ),
          features: stringArray(blueprint.features, "blueprint features"),
          directives: stringArray(blueprint.directives, "blueprint directives"),
          cargo_capacity: optionalFiniteNumber(
            blueprint.cargo_capacity,
            "blueprint cargo capacity",
          ),
          attach_capacity: optionalFiniteNumber(
            blueprint.attach_capacity,
            "blueprint attach capacity",
          ),
          stow_capacity: optionalFiniteNumber(
            blueprint.stow_capacity,
            "blueprint stow capacity",
          ),
          queue_size: optionalFiniteNumber(
            blueprint.queue_size,
            "blueprint queue size",
          ),
        };
      }),
    };
  });
}

export function parseDirectoryResponse(
  value: unknown,
): Versioned<DirectorySnapshot> {
  return envelope(value, (payload) => {
    const snapshot = record(payload, "directory snapshot");
    return {
      metadata: metadata(snapshot.metadata),
      query: nullableString(snapshot.query, "directory query"),
      replicants: array(snapshot.replicants, "directory replicants").map(
        (value) => {
          const row = record(value, "directory replicant");
          return {
            entity: entity(row.entity),
            name: nullableString(row.name, "replicant name"),
            last_location: nullableString(
              row.last_location,
              "replicant location",
            ),
            is_npc:
              row.is_npc === null
                ? null
                : boolean(row.is_npc, "replicant NPC flag"),
          };
        },
      ),
    };
  });
}

export function parseDirectoryReplicantResponse(
  value: unknown,
): Versioned<DirectoryReplicantDetailSnapshot> {
  return envelope(value, (payload) => {
    const snapshot = record(payload, "directory replicant snapshot");
    const profile = record(snapshot.replicant, "directory replicant profile");
    return {
      metadata: metadata(snapshot.metadata),
      replicant: {
        entity: entity(profile.entity),
        name: nullableString(profile.name, "replicant name"),
        is_npc:
          profile.is_npc === null
            ? null
            : boolean(profile.is_npc, "replicant NPC flag"),
        status: nullableString(profile.status, "replicant status"),
        location: nullableString(profile.location, "replicant location"),
        hosted_device:
          profile.hosted_device === null ? null : entity(profile.hosted_device),
      },
    };
  });
}

function parseTutorial(value: unknown): TutorialSummary {
  const tutorial = record(value, "tutorial");
  if (typeof tutorial.slug !== "string") throw new Error("Invalid tutorial");
  return {
    slug: tutorial.slug,
    name: nullableString(tutorial.name, "tutorial name"),
    description: nullableString(tutorial.description, "tutorial description"),
    order: optionalFiniteNumber(tutorial.order, "tutorial order"),
    completed:
      tutorial.completed === null
        ? null
        : boolean(tutorial.completed, "tutorial completion"),
    current_step: optionalFiniteNumber(
      tutorial.current_step,
      "tutorial current step",
    ),
    total_steps: optionalFiniteNumber(
      tutorial.total_steps,
      "tutorial total steps",
    ),
    steps: array(tutorial.steps, "tutorial steps").map((value) => {
      const step = record(value, "tutorial step");
      return {
        key: nullableString(step.key, "tutorial step key"),
        description: nullableString(
          step.description,
          "tutorial step description",
        ),
        hint: nullableString(step.hint, "tutorial step hint"),
        completed:
          step.completed === null
            ? null
            : boolean(step.completed, "tutorial step completion"),
        current:
          step.current === null
            ? null
            : boolean(step.current, "tutorial current flag"),
      };
    }),
  };
}

export function parseTutorialsResponse(
  value: unknown,
): Versioned<TutorialsSnapshot> {
  return envelope(value, (payload) => {
    const snapshot = record(payload, "tutorials snapshot");
    return {
      metadata: metadata(snapshot.metadata),
      tutorials: array(snapshot.tutorials, "tutorials").map(parseTutorial),
      selected:
        snapshot.selected === null ? null : parseTutorial(snapshot.selected),
    };
  });
}

function parseTradeItems(value: unknown, name: string): TradeItemSummary[] {
  return array(value, name).map((value) => {
    const item = record(value, name);
    if (typeof item.kind !== "string" || typeof item.item !== "string")
      throw new Error(`Invalid ${name}`);
    return {
      kind: item.kind,
      item: item.item,
      quantity: optionalFiniteNumber(item.quantity, `${name} quantity`),
    };
  });
}

export function parseTradeResponse(value: unknown): Versioned<TradeSnapshot> {
  return envelope(value, (payload) => {
    const snapshot = record(payload, "trade snapshot");
    return {
      metadata: metadata(snapshot.metadata),
      viewer: snapshot.viewer === null ? null : entity(snapshot.viewer),
      controllers: array(snapshot.controllers, "trade controllers").map(
        (value) => {
          const controller = record(value, "trade controller");
          if (typeof controller.is_local !== "boolean")
            throw new Error("Invalid trade controller");
          return {
            entity: entity(controller.entity),
            shop_name: nullableString(controller.shop_name, "shop name"),
            description: nullableString(
              controller.description,
              "shop description",
            ),
            is_local: controller.is_local,
            owner_name: nullableString(controller.owner_name, "shop owner"),
            owner_replicant: nullableString(
              controller.owner_replicant,
              "shop owner replicant",
            ),
            system: nullableString(controller.system, "shop system"),
            location: nullableString(controller.location, "shop location"),
            total_stock: optionalFiniteNumber(
              controller.total_stock,
              "shop stock",
            ),
            trade_count: optionalFiniteNumber(
              controller.trade_count,
              "shop trade count",
            ),
            trade_details_status:
              typeof controller.trade_details_status === "string"
                ? controller.trade_details_status
                : "available",
            trades: array(controller.trades, "trades").map((value) => {
              const trade = record(value, "trade");
              if (typeof trade.trade_code !== "string")
                throw new Error("Invalid trade");
              return {
                trade_code: trade.trade_code,
                name: nullableString(trade.name, "trade name"),
                current_stock: optionalFiniteNumber(
                  trade.current_stock,
                  "trade stock",
                ),
                initial_stock: optionalFiniteNumber(
                  trade.initial_stock,
                  "initial trade stock",
                ),
                requested: parseTradeItems(trade.requested, "requested item"),
                offered: parseTradeItems(trade.offered, "offered item"),
                created_at: nullableString(
                  trade.created_at,
                  "trade creation time",
                ),
              };
            }),
            workflow:
              controller.workflow === null
                ? null
                : workflow(controller.workflow),
          };
        },
      ),
    };
  });
}

function bobnetChannel(value: unknown): BobnetChannelSummary {
  const channel = record(value, "BobNet channel");
  if (typeof channel.name !== "string")
    throw new Error("Invalid BobNet channel");
  return {
    name: channel.name,
    last_active: nullableString(channel.last_active, "channel activity"),
  };
}

function bobnetMessage(value: unknown): BobnetMessageSummary {
  const message = record(value, "BobNet message");
  if (typeof message.is_npc_or_system !== "boolean")
    throw new Error("Invalid BobNet message");
  return {
    id: optionalFiniteNumber(message.id, "BobNet message ID"),
    channel: nullableString(message.channel, "BobNet message channel"),
    body: nullableString(message.body, "BobNet message body"),
    sender: nullableString(message.sender, "BobNet message sender"),
    sender_name: nullableString(message.sender_name, "BobNet sender name"),
    is_npc_or_system: message.is_npc_or_system,
    current_system: nullableString(
      message.current_system,
      "BobNet sender system",
    ),
    created_at: nullableString(message.created_at, "BobNet message time"),
  };
}

export function parseBillFinderResponse(
  value: unknown,
): Versioned<BillFinderResponse> {
  return envelope(value, (payload) => {
    const result = record(payload, "Bill finder response");
    const departure = record(result.departure, "Bill departure");
    const vector = array(departure.vector, "Bill departure vector").map(
      (value) => {
        if (typeof value !== "number" || !Number.isFinite(value))
          throw new Error("Invalid Bill departure vector");
        return value;
      },
    );
    const [vectorX, vectorY, vectorZ] = vector;
    if (
      vector.length !== 3 ||
      vectorX === undefined ||
      vectorY === undefined ||
      vectorZ === undefined
    ) {
      throw new Error("Invalid Bill departure vector");
    }
    const expansion = record(result.expansion, "Bill expansion");
    if (
      typeof departure.tracking_beacon !== "string" ||
      typeof departure.replicant_code !== "string" ||
      typeof departure.origin_location !== "string" ||
      typeof departure.origin_system !== "string" ||
      typeof result.confidence !== "string" ||
      typeof result.ambiguous !== "boolean" ||
      typeof expansion.status !== "string" ||
      typeof expansion.message !== "string"
    ) {
      throw new Error("Invalid Bill finder response");
    }
    return {
      metadata: metadata(result.metadata),
      departure: {
        tracking_beacon: departure.tracking_beacon,
        replicant_code: departure.replicant_code,
        vessel_code: nullableString(departure.vessel_code, "Bill vessel code"),
        vessel_type: nullableString(departure.vessel_type, "Bill vessel type"),
        origin_location: departure.origin_location,
        origin_system: departure.origin_system,
        logged_at: nullableString(departure.logged_at, "Bill audit timestamp"),
        vector: [vectorX, vectorY, vectorZ],
      },
      candidates: array(result.candidates, "Bill candidates").map((value) => {
        const candidate = record(value, "Bill candidate");
        if (typeof candidate.system !== "string")
          throw new Error("Invalid Bill candidate");
        const angularError = optionalFiniteNumber(
          candidate.angular_error_deg,
          "Bill candidate angular error",
        );
        const distance = optionalFiniteNumber(
          candidate.distance_ly,
          "Bill candidate distance",
        );
        const projectedDistance = optionalFiniteNumber(
          candidate.projected_distance_ly,
          "Bill candidate projected distance",
        );
        const crossTrack = optionalFiniteNumber(
          candidate.cross_track_ly,
          "Bill candidate cross-track distance",
        );
        if (
          angularError === null ||
          distance === null ||
          projectedDistance === null ||
          crossTrack === null
        ) {
          throw new Error("Invalid Bill candidate");
        }
        return {
          system: candidate.system,
          angular_error_deg: angularError,
          distance_ly: distance,
          projected_distance_ly: projectedDistance,
          cross_track_ly: crossTrack,
        };
      }),
      recommended_system: nullableString(
        result.recommended_system,
        "Bill recommended system",
      ),
      confidence: result.confidence,
      ambiguous: result.ambiguous,
      expansion: {
        status: expansion.status,
        target_system: nullableString(
          expansion.target_system,
          "Bill expansion target",
        ),
        workflow:
          expansion.workflow === null ? null : workflow(expansion.workflow),
        message: expansion.message,
      },
    };
  });
}

export function parseReportsResponse(
  value: unknown,
): Versioned<ReportsSnapshot> {
  return envelope(value, (payload) => {
    const snapshot = record(payload, "reports snapshot");
    return {
      metadata: metadata(snapshot.metadata),
      reports: array(snapshot.reports, "reports").map(reportDescriptor),
      executions: array(snapshot.executions, "report executions").map(
        finiteExecution,
      ),
    };
  });
}

function messageFreshness(value: unknown): MessageFreshness {
  if (value === undefined) {
    return { last_refresh_at: null, stale: false, last_error: null };
  }
  const freshness = record(value, "message freshness");
  if (typeof freshness.stale !== "boolean")
    throw new Error("Invalid message freshness");
  return {
    last_refresh_at: optionalInteger(
      freshness.last_refresh_at,
      "message refresh time",
    ),
    stale: freshness.stale,
    last_error: optionalString(freshness.last_error, "message refresh error"),
  };
}

export function parseMessagesResponse(
  value: unknown,
): Versioned<MessagesSnapshot> {
  return envelope(value, (payload) => {
    const snapshot = record(payload, "messages snapshot");
    return {
      metadata: metadata(snapshot.metadata),
      inbox: array(snapshot.inbox, "inbox messages").map((value) => {
        const message = record(value, "inbox message");
        if (message.is_read !== null && typeof message.is_read !== "boolean")
          throw new Error("Invalid inbox message");
        return {
          id: optionalFiniteNumber(message.id, "inbox message ID"),
          title: nullableString(message.title, "message title"),
          body: nullableString(message.body, "message body"),
          category: nullableString(message.category, "message category"),
          message_type: nullableString(message.message_type, "message type"),
          is_read: message.is_read,
          created_at: nullableString(message.created_at, "message time"),
        };
      }),
      unread_count: optionalFiniteNumber(snapshot.unread_count, "unread count"),
      last_cursor: optionalInteger(snapshot.last_cursor, "message cursor"),
      freshness: messageFreshness(snapshot.freshness),
    };
  });
}

export function parseBobnetResponse(value: unknown): Versioned<BobnetSnapshot> {
  return envelope(value, (payload) => {
    const snapshot = record(payload, "BobNet snapshot");
    return {
      metadata: metadata(snapshot.metadata),
      sources: array(snapshot.sources, "BobNet sources").map(
        parseDeviceSummary,
      ),
      selected_source: nullableString(
        snapshot.selected_source,
        "BobNet source",
      ),
      channels: array(snapshot.channels, "BobNet channels").map(bobnetChannel),
      messages: array(snapshot.messages, "BobNet messages").map(bobnetMessage),
      replicants: array(snapshot.replicants, "BobNet replicants").map(
        (value) => {
          const replicant = record(value, "BobNet replicant");
          return {
            entity: entity(replicant.entity),
            name: nullableString(replicant.name, "BobNet replicant name"),
            status: nullableString(replicant.status, "BobNet replicant status"),
            location: nullableString(
              replicant.location,
              "BobNet replicant location",
            ),
          };
        },
      ),
      next_cursor: optionalFiniteNumber(snapshot.next_cursor, "BobNet cursor"),
      total_messages: optionalFiniteNumber(
        snapshot.total_messages,
        "BobNet total messages",
      ),
      error: nullableString(snapshot.error, "BobNet warning"),
    };
  });
}

export function parseNetworkResponse(
  value: unknown,
): Versioned<NetworkSnapshot> {
  return envelope(value, (payload) => {
    const snapshot = record(payload, "network snapshot");
    return {
      metadata: metadata(snapshot.metadata),
      account_name: nullableString(snapshot.account_name, "account name"),
      account_status: nullableString(snapshot.account_status, "account status"),
      subscribed_channels: stringArray(
        snapshot.subscribed_channels,
        "subscribed channels",
      ),
      replicants: array(snapshot.replicants, "account replicants").map(
        (value) => {
          const replicant = record(value, "account replicant");
          return {
            entity: entity(replicant.entity),
            name: nullableString(replicant.name, "replicant name"),
            system: nullableString(replicant.system, "replicant system"),
            location: nullableString(replicant.location, "replicant location"),
            hosted_device:
              replicant.hosted_device === null
                ? null
                : entity(replicant.hosted_device),
          };
        },
      ),
      relays: array(snapshot.relays, "network relays").map((value) => {
        const relay = record(value, "network relay");
        return {
          device: parseDeviceSummary(relay.device),
          channels: array(relay.channels, "relay channels").map(bobnetChannel),
          error: nullableString(relay.error, "relay error"),
        };
      }),
    };
  });
}

export function parseStandingResponse(
  value: unknown,
): Versioned<StandingSnapshot> {
  return envelope(value, (payload) => {
    const snapshot = record(payload, "standing snapshot");
    return {
      metadata: metadata(snapshot.metadata),
      experience_points_total: optionalFiniteNumber(
        snapshot.experience_points_total,
        "experience points",
      ),
      civilisation_points: optionalFiniteNumber(
        snapshot.civilisation_points,
        "civilisation points",
      ),
      achievements: array(snapshot.achievements, "achievements").map(
        (value) => {
          const achievement = record(value, "achievement");
          if (typeof achievement.key !== "string")
            throw new Error("Invalid achievement");
          return {
            key: achievement.key,
            title: nullableString(achievement.title, "achievement title"),
            description: nullableString(
              achievement.description,
              "achievement description",
            ),
            category: nullableString(
              achievement.category,
              "achievement category",
            ),
            xp_reward: optionalFiniteNumber(
              achievement.xp_reward,
              "achievement XP",
            ),
            achieved_at: nullableString(
              achievement.achieved_at,
              "achievement time",
            ),
          };
        },
      ),
      reputation: array(snapshot.reputation, "reputation").map((value) => {
        const reputation = record(value, "reputation");
        if (typeof reputation.species !== "string")
          throw new Error("Invalid reputation");
        return {
          species: reputation.species,
          name: nullableString(reputation.name, "species name"),
          value: optionalFiniteNumber(reputation.value, "reputation value"),
          description: nullableString(
            reputation.description,
            "reputation description",
          ),
          trait_name: nullableString(reputation.trait_name, "species trait"),
        };
      }),
    };
  });
}

export function parseLeaderboardsResponse(
  value: unknown,
): Versioned<LeaderboardsSnapshot> {
  return envelope(value, (payload) => {
    const snapshot = record(payload, "leaderboards snapshot");
    return {
      metadata: metadata(snapshot.metadata),
      selected_board: nullableString(
        snapshot.selected_board,
        "selected leaderboard",
      ),
      boards: array(snapshot.boards, "leaderboards").map((value) => {
        const board = record(value, "leaderboard");
        if (typeof board.key !== "string")
          throw new Error("Invalid leaderboard");
        return {
          key: board.key,
          name: nullableString(board.name, "leaderboard name"),
          description: nullableString(
            board.description,
            "leaderboard description",
          ),
          board_type: nullableString(board.board_type, "leaderboard type"),
        };
      }),
      entries: array(snapshot.entries, "leaderboard entries").map((value) => {
        const entry = record(value, "leaderboard entry");
        return {
          rank: optionalFiniteNumber(entry.rank, "leaderboard rank"),
          replicant: entry.replicant === null ? null : entity(entry.replicant),
          name: nullableString(entry.name, "leaderboard name"),
          designation: nullableString(
            entry.designation,
            "leaderboard designation",
          ),
          value: optionalFiniteNumber(entry.value, "leaderboard value"),
          contribution_count: optionalFiniteNumber(
            entry.contribution_count,
            "leaderboard contributions",
          ),
        };
      }),
    };
  });
}

export function parseSettingsResponse(
  value: unknown,
): Versioned<SettingsSnapshot> {
  return envelope(value, (payload) => {
    const snapshot = record(payload, "settings snapshot");
    if (
      typeof snapshot.profile !== "string" ||
      typeof snapshot.bind_address !== "string" ||
      typeof snapshot.managed_database_path !== "string" ||
      typeof snapshot.history_database_path !== "string" ||
      typeof snapshot.telemetry_database_path !== "string" ||
      typeof snapshot.runtime_database_path !== "string" ||
      typeof snapshot.log_filter !== "string"
    )
      throw new Error("Invalid settings snapshot");
    return {
      metadata: metadata(snapshot.metadata),
      profile: snapshot.profile,
      bind_address: snapshot.bind_address,
      managed_database_path: snapshot.managed_database_path,
      history_database_path: snapshot.history_database_path,
      telemetry_database_path: snapshot.telemetry_database_path,
      runtime_database_path: snapshot.runtime_database_path,
      log_filter: snapshot.log_filter,
      docker: boolean(snapshot.docker, "docker environment"),
      api_token_source: oneOf(
        snapshot.api_token_source,
        apiTokenSources,
        "API token source",
      ),
      daemon_settings_require_restart: boolean(
        snapshot.daemon_settings_require_restart,
        "daemon settings restart flag",
      ),
    };
  });
}

function parseFactoryJob(value: unknown): FactoryJobSummary {
  const job = record(value, "factory job");
  if (typeof job.device_type !== "string")
    throw new Error("Invalid factory job type");
  return {
    device_type: job.device_type,
    quantity: number(job.quantity, "factory job quantity"),
    eta_seconds: optionalFiniteNumber(job.eta_seconds, "factory job ETA"),
    tags: stringArray(job.tags, "factory job tags"),
  };
}

export function parseAutofactoryResponse(
  value: unknown,
): Versioned<AutofactorySnapshot> {
  return envelope(value, (payload) => {
    const snapshot = record(payload, "autofactory snapshot");
    const utilization = record(snapshot.utilization, "autofactory utilization");
    return {
      metadata: metadata(snapshot.metadata),
      utilization: {
        total: number(utilization.total, "factory total"),
        busy: number(utilization.busy, "busy factories"),
        available: number(utilization.available, "available factories"),
        unavailable: number(utilization.unavailable, "unavailable factories"),
        queued_units: number(utilization.queued_units, "queued units"),
        utilization_percent: finiteNumber(
          utilization.utilization_percent,
          "factory utilization",
        ),
      },
      factories: array(snapshot.factories, "factories").map((value) => {
        const factory = record(value, "autofactory");
        return {
          device: parseDeviceSummary(factory.device),
          availability: oneOf(
            factory.availability,
            ["available", "busy", "unavailable"] as const,
            "factory availability",
          ),
          queue_capacity:
            factory.queue_capacity === null
              ? null
              : number(factory.queue_capacity, "queue capacity"),
          queued_units: number(factory.queued_units, "queued units"),
          current_job:
            factory.current_job === null
              ? null
              : parseFactoryJob(factory.current_job),
          queued_jobs: array(factory.queued_jobs, "queued jobs").map(
            parseFactoryJob,
          ),
        };
      }),
    };
  });
}

export function parseCargoResponse(value: unknown): Versioned<CargoSnapshot> {
  return envelope(value, (payload) => {
    const snapshot = record(payload, "cargo snapshot");
    return {
      metadata: metadata(snapshot.metadata),
      cargo_used: number(snapshot.cargo_used, "cargo used"),
      cargo_capacity: number(snapshot.cargo_capacity, "cargo capacity"),
      attachment_used: number(snapshot.attachment_used, "attachment used"),
      attachment_capacity: number(
        snapshot.attachment_capacity,
        "attachment capacity",
      ),
      carriers: array(snapshot.carriers, "cargo carriers").map((value) => {
        const carrier = record(value, "cargo carrier");
        return {
          device: parseDeviceSummary(carrier.device),
          attachment_used: number(carrier.attachment_used, "attachment used"),
          resources: array(carrier.resources, "cargo resources").map(
            (value) => {
              const resource = record(value, "cargo resource");
              if (typeof resource.resource !== "string")
                throw new Error("Invalid cargo resource");
              return {
                resource: resource.resource,
                quantity: number(resource.quantity, "cargo resource quantity"),
              };
            },
          ),
        };
      }),
    };
  });
}

export function parseInventoryResponse(
  value: unknown,
): Versioned<InventorySnapshot> {
  const owner = (value: unknown, name: string) => {
    const item = record(value, name);
    if (typeof item.owner !== "string") throw new Error(`Invalid ${name}`);
    return {
      owner_kind: oneOf(
        item.owner_kind,
        inventoryOwnerKinds,
        "inventory owner",
      ),
      owner: item.owner,
      system: nullableString(item.system, "inventory system"),
      location: nullableString(item.location, "inventory location"),
    };
  };
  return envelope(value, (payload) => {
    const snapshot = record(payload, "inventory snapshot");
    return {
      metadata: metadata(snapshot.metadata),
      total_quantity: number(snapshot.total_quantity, "inventory total"),
      locations: array(snapshot.locations, "inventory locations").map(
        (value) => {
          const item = record(value, "inventory location");
          return {
            ...owner(item, "inventory location"),
            total_quantity: number(item.total_quantity, "location total"),
            resources: array(item.resources, "location resources").map(
              (value) => {
                const resource = record(value, "inventory quantity");
                if (typeof resource.resource !== "string")
                  throw new Error("Invalid inventory resource");
                return {
                  resource: resource.resource,
                  quantity: number(resource.quantity, "resource quantity"),
                };
              },
            ),
          };
        },
      ),
      resources: array(snapshot.resources, "inventory resources").map(
        (value) => {
          const item = record(value, "inventory resource");
          if (typeof item.resource !== "string")
            throw new Error("Invalid inventory resource");
          return {
            resource: item.resource,
            total_quantity: number(item.total_quantity, "resource total"),
            distribution: array(item.distribution, "resource distribution").map(
              (value) => {
                const distribution = record(value, "inventory distribution");
                return {
                  ...owner(distribution, "inventory distribution"),
                  quantity: number(
                    distribution.quantity,
                    "distribution quantity",
                  ),
                };
              },
            ),
          };
        },
      ),
    };
  });
}

export function parseSnapshotResponse(
  value: unknown,
): Versioned<RuntimeSnapshot> {
  return envelope(value, (payload) => {
    const item = record(payload, "runtime snapshot");
    if (!Array.isArray(item.workflows))
      throw new Error("Invalid snapshot workflows");
    const requirements = item.requirements ?? [];
    if (!Array.isArray(requirements))
      throw new Error("Invalid snapshot requirements");
    const notifications = item.notifications ?? [];
    if (!Array.isArray(notifications))
      throw new Error("Invalid snapshot notifications");
    const refreshes = item.refreshes ?? [];
    if (!Array.isArray(refreshes))
      throw new Error("Invalid snapshot refreshes");
    return {
      metadata: metadata(item.metadata),
      sync: sync(item.sync),
      automation: automation(item.automation),
      workflows: item.workflows.map(workflow),
      refreshes: refreshes.map(refreshRunSummary),
      notifications: notifications.map(notification),
      slice_revisions: Object.fromEntries(
        Object.entries(
          item.slice_revisions === undefined
            ? {}
            : record(item.slice_revisions, "slice revisions"),
        ).map(([slice, revision]) => [
          oneOf(slice, domainSlices, "domain slice"),
          number(revision, "slice revision"),
        ]),
      ),
      requirements: requirements.map((value) => {
        const requirement = record(value, "requirement");
        if (
          typeof requirement.id !== "string" ||
          typeof requirement.name !== "string" ||
          typeof requirement.target !== "string" ||
          typeof requirement.scope !== "string" ||
          typeof requirement.workflow_id !== "string"
        )
          throw new Error("Invalid requirement");
        return {
          id: requirement.id,
          name: requirement.name,
          target: requirement.target,
          scope: requirement.scope,
          desired: number(requirement.desired, "requirement desired"),
          actual: number(requirement.actual, "requirement actual"),
          in_progress: number(
            requirement.in_progress,
            "requirement in progress",
          ),
          missing: number(requirement.missing, "requirement missing"),
          workflow_id: requirement.workflow_id,
          status: oneOf(
            requirement.status,
            [
              "queued",
              "running",
              "waiting",
              "paused",
              "reconciling",
              "succeeded",
              "failed",
              "cancelled",
            ] as const,
            "requirement status",
          ),
        };
      }),
    };
  });
}

export function parseEntityIndexResponse(
  value: unknown,
): Versioned<EntityIndexSnapshot> {
  return envelope(value, (payload) => {
    const item = record(payload, "entity index");
    return {
      metadata: metadata(item.metadata),
      entities: array(item.entities, "entity index entries").map(entitySummary),
    };
  });
}

export function parseGalaxySceneResponse(
  value: unknown,
): Versioned<GalaxySceneSnapshot> {
  return envelope(value, (payload) => {
    const item = record(payload, "galaxy scene");
    return {
      revision: number(item.revision, "galaxy scene revision"),
      generated_at_ms: number(item.generated_at_ms, "galaxy scene time"),
      stars: array(item.stars, "galaxy scene stars").map((value) => {
        const star = record(value, "galaxy star");
        if (
          typeof star.id !== "string" ||
          typeof star.current !== "boolean" ||
          typeof star.has_hub !== "boolean" ||
          typeof star.has_life !== "boolean" ||
          typeof star.has_relay !== "boolean"
        )
          throw new Error("Invalid galaxy star");
        return {
          id: star.id,
          name: nullableString(star.name, "galaxy star name"),
          spectral_type: nullableString(
            star.spectral_type,
            "galaxy spectral type",
          ),
          region:
            star.region === undefined
              ? null
              : nullableString(star.region, "galaxy region"),
          position: point(star.position),
          exploration: oneOf(
            star.exploration,
            ["undiscovered", "partial", "explored"] as const,
            "galaxy exploration",
          ),
          current: star.current,
          has_hub: star.has_hub,
          has_life: star.has_life,
          has_relay: star.has_relay,
          has_megastructure: star.has_megastructure === true,
        };
      }),
      relay_edges: array(item.relay_edges, "galaxy relay edges").map((value) =>
        stringPair(value, "relay edge"),
      ),
      active_travel: array(item.active_travel, "galaxy travel").map((value) => {
        const travel = record(value, "active travel");
        return {
          entity: entity(travel.entity),
          ...stringPair(value, "active travel"),
          started_at: nullableString(travel.started_at, "travel start"),
          arrives_at: nullableString(travel.arrives_at, "travel arrival"),
        };
      }),
      signals: array(item.signals, "galaxy signals").map((value) => {
        const signal = record(value, "galaxy signal");
        if (typeof signal.id !== "string")
          throw new Error("Invalid galaxy signal");
        return {
          id: signal.id,
          label: nullableString(signal.label, "signal label"),
          position: point(signal.position),
        };
      }),
      highlights: array(item.highlights, "galaxy highlights").map((value) => {
        const highlight = record(value, "galaxy highlight");
        if (typeof highlight.workflow_id !== "string")
          throw new Error("Invalid galaxy highlight");
        return {
          workflow_id: highlight.workflow_id,
          ...stringPair(value, "galaxy highlight"),
        };
      }),
      overlays: array(item.overlays, "galaxy overlays").map((value) => {
        const overlay = record(value, "galaxy overlay");
        if (typeof overlay.system !== "string")
          throw new Error("Invalid galaxy overlay");
        return {
          kind: oneOf(
            overlay.kind,
            ["life", "device", "influence"] as const,
            "galaxy overlay kind",
          ),
          system: overlay.system,
          position: point(overlay.position),
          count: number(overlay.count, "galaxy overlay count"),
        };
      }),
      workflow_targets: array(item.workflow_targets, "workflow targets").map(
        (value) => {
          const target = record(value, "workflow target");
          if (
            typeof target.workflow_id !== "string" ||
            typeof target.workflow_kind !== "string" ||
            typeof target.system !== "string"
          )
            throw new Error("Invalid workflow target");
          return {
            workflow_id: target.workflow_id,
            workflow_kind: target.workflow_kind,
            system: target.system,
          };
        },
      ),
    };
  });
}

export function parseSystemSceneResponse(
  value: unknown,
): Versioned<SystemSceneSnapshot> {
  return envelope(value, (payload) => {
    const item = record(payload, "system scene");
    if (typeof item.system !== "string")
      throw new Error("Invalid system scene");
    return {
      system: item.system,
      revision: number(item.revision, "system scene revision"),
      generated_at_ms: number(item.generated_at_ms, "system scene time"),
      markers: array(item.markers, "system markers").map((value) => {
        const marker = record(value, "system marker");
        if (
          typeof marker.id !== "string" ||
          typeof marker.label !== "string" ||
          typeof marker.location !== "string" ||
          (marker.in_habitable_zone !== null &&
            typeof marker.in_habitable_zone !== "boolean")
        )
          throw new Error("Invalid system marker");
        const position = record(marker.position, "system marker position");
        return {
          id: marker.id,
          label: marker.label,
          kind: oneOf(
            marker.kind,
            [
              "star",
              "planet",
              "moon",
              "belt",
              "lagrange",
              "location",
              "vessel",
              "device",
              "factory",
              "relay",
              "event",
              "resource_site",
              "megastructure",
            ] as const,
            "system marker kind",
          ),
          entity: entity(marker.entity),
          location: marker.location,
          parent: nullableString(marker.parent, "system marker parent"),
          in_habitable_zone: marker.in_habitable_zone,
          position: {
            x: finiteNumber(position.x, "system marker x"),
            y: finiteNumber(position.y, "system marker y"),
          },
          count: number(marker.count, "system marker count"),
        };
      }),
      active_travel: array(item.active_travel, "system travel").map((value) => {
        const travel = record(value, "system travel");
        return {
          entity: entity(travel.entity),
          ...stringPair(value, "system travel"),
          started_at: nullableString(travel.started_at, "travel start"),
          arrives_at: nullableString(travel.arrives_at, "travel arrival"),
        };
      }),
      workflow_markers: array(
        item.workflow_markers,
        "system workflow markers",
      ).map((value) => {
        const marker = record(value, "system workflow marker");
        if (
          typeof marker.workflow_id !== "string" ||
          typeof marker.workflow_kind !== "string" ||
          typeof marker.location !== "string"
        )
          throw new Error("Invalid system workflow marker");
        return {
          workflow_id: marker.workflow_id,
          workflow_kind: marker.workflow_kind,
          location: marker.location,
        };
      }),
    };
  });
}

export function parseDescriptorsResponse(
  value: unknown,
): Versioned<DescriptorCatalog> {
  return envelope(value, (payload) => {
    const item = record(payload, "descriptor catalog");
    if (
      !Array.isArray(item.reports) ||
      !Array.isArray(item.actions) ||
      !Array.isArray(item.workflows)
    )
      throw new Error("Invalid descriptors");
    return {
      reports: item.reports.map(reportDescriptor),
      actions: item.actions.map((value) => {
        const parsed = descriptor(value, "action descriptor");
        const item = record(value, "action descriptor");
        return {
          ...parsed,
          operation_class: oneOf(
            item.operation_class,
            ["action"] as const,
            "action operation class",
          ),
          risk: oneOf(item.risk, ["none", "low", "elevated"] as const, "risk"),
          device_commands:
            item.device_commands === undefined
              ? []
              : array(item.device_commands, "device command bindings").map(
                  (value) => {
                    const binding = record(value, "device command binding");
                    if (typeof binding.command !== "string")
                      throw new Error("Invalid device command binding");
                    return {
                      command: binding.command,
                      parameters:
                        binding.parameters === undefined
                          ? {}
                          : record(
                              binding.parameters,
                              "device command parameters",
                            ),
                    };
                  },
                ),
        };
      }),
      workflows: item.workflows.map(workflowDescriptor),
    };
  });
}

export function parseOperationResponse(
  value: unknown,
): Versioned<FiniteExecution> {
  return envelope(value, (payload) =>
    finiteExecution(record(payload, "operation response").execution),
  );
}

export function parseFiniteExecutionHistoryResponse(
  value: unknown,
): Versioned<FiniteExecution[]> {
  return envelope(value, (payload) => {
    const item = record(payload, "finite execution history response");
    if (!Array.isArray(item.executions))
      throw new Error("Invalid finite execution history");
    return item.executions.map(finiteExecution);
  });
}

export function parseTriggerListResponse(
  value: unknown,
): Versioned<AutomationTrigger[]> {
  return envelope(value, (payload) => {
    const item = record(payload, "trigger list response");
    if (!Array.isArray(item.triggers)) throw new Error("Invalid trigger list");
    return item.triggers.map(trigger);
  });
}

export function parseTriggerResponse(
  value: unknown,
): Versioned<AutomationTrigger> {
  return envelope(value, trigger);
}

export function parseWorkflowResponse(
  value: unknown,
): Versioned<WorkflowSummary> {
  return envelope(value, (payload) => {
    const item = record(payload, "workflow response");
    return workflow(item.workflow);
  });
}

export function parseWorkflowDetailResponse(
  value: unknown,
): Versioned<WorkflowDetail> {
  return envelope(value, workflowDetail);
}

export function parseWorkflowActivityResponse(
  value: unknown,
): Versioned<WorkflowActivity[]> {
  return envelope(value, (payload) => {
    const item = record(payload, "workflow activity response");
    if (!Array.isArray(item.activity)) throw new Error("Invalid activity list");
    return item.activity.map(activity);
  });
}

export function parseLiveMessage(value: unknown): LiveMessage {
  const item = record(value, "live message");
  if (item.protocol_version !== PROTOCOL_VERSION)
    throw new Error("Unsupported daemon protocol version");
  const revision = number(item.revision, "live revision");
  const raw = record(item.delta, "live delta");
  const type = raw.type;
  if (typeof type !== "string") throw new Error("Invalid live delta type");
  const data = raw.data;
  let delta: LiveDelta;
  switch (type) {
    case "snapshot":
      delta = { type, data: metadata(data) };
      break;
    case "entity_upsert": {
      const value = record(data, "entity upsert");
      const parsedEntity = entity(value.entity);
      const summary = entitySummary(value.value);
      if (
        parsedEntity.kind !== summary.entity.kind ||
        parsedEntity.id !== summary.entity.id
      )
        throw new Error("Mismatched entity summary");
      delta = {
        type,
        data: {
          entity: parsedEntity,
          value: summary,
        },
      };
      break;
    }
    case "entity_remove": {
      const value = record(data, "entity removal");
      delta = { type, data: { entity: entity(value.entity) } };
      break;
    }
    case "domain_invalidated": {
      const value = record(data, "domain invalidation");
      delta = {
        type,
        data: { slice: oneOf(value.slice, domainSlices, "domain slice") },
      };
      break;
    }
    case "domains_invalidated": {
      const value = record(data, "domain invalidation");
      const slices = record(value.slices, "invalidated slices");
      delta = {
        type,
        data: {
          slices: Object.fromEntries(
            Object.entries(slices).map(([slice, revision]) => [
              oneOf(slice, domainSlices, "domain slice"),
              number(revision, "slice revision"),
            ]),
          ),
        },
      };
      break;
    }
    case "workflow_created":
    case "workflow_updated":
      delta = { type, data: workflow(data) };
      break;
    case "workflow_activity":
      delta = { type, data: activity(data) };
      break;
    case "operation_updated":
      delta = { type, data: operation(data) };
      break;
    case "notification":
      delta = { type, data: notification(data) };
      break;
    case "automation_changed":
      delta = { type, data: automation(data) };
      break;
    case "daemon_status_changed": {
      const value = record(data, "daemon status");
      delta = {
        type,
        data: { health: health(value.health), sync: sync(value.sync) },
      };
      break;
    }
    default:
      throw new Error(`Unsupported live delta: ${type}`);
  }
  return { protocol_version: PROTOCOL_VERSION, revision, delta };
}

const directorModes = ["off", "advisory", "automatic"] as const;
const directorGoalKinds = [
  "establish_regions",
  "expand_star_catalogue",
  "enhance_star_catalogue",
  "discover_belts",
  "expand_mining_ops",
  "salvage_recovery",
  "event_completion",
  "asteroid_diversion",
  "blueprint_acquisition",
  "maintain_system_hubs",
  "stranded_device_recovery",
  "unserviced_resources",
  "expand_ftl_network",
  "establish_beacons",
] as const;
const directorGoalStatuses = [
  "satisfied",
  "active",
  "blocked",
  "waiting",
] as const;
const directorRegionStatuses = [
  "discovered",
  "establishing",
  "established",
] as const;
const directorRequirementKinds = [
  "blueprint",
  "logistics",
  "worker_capacity",
  "connectivity",
] as const;
const directorRequirementStatuses = [
  "pending",
  "active",
  "blocked",
  "satisfied",
  "unavailable",
] as const;

export function parseDirectorResponse(
  value: unknown,
): Versioned<DirectorSnapshot> {
  return envelope(value, (payload) => {
    const item = record(payload, "Director snapshot");
    if (
      !Array.isArray(item.regions) ||
      !Array.isArray(item.goals) ||
      !Array.isArray(item.replicants)
    )
      throw new Error("Invalid Director collections");
    const workforce = record(item.workforce, "Director workforce");
    return {
      metadata: metadata(item.metadata),
      mode: oneOf(item.mode, directorModes, "Director mode"),
      regions: item.regions.map((value) => {
        const region = record(value, "Director region");
        return {
          region: requiredString(region.region, "region"),
          status: oneOf(region.status, directorRegionStatuses, "region status"),
          hub_system: nullableString(region.hub_system, "hub system"),
          hub_location: nullableString(region.hub_location, "hub location"),
          replicants: stringArray(region.replicants, "regional replicants"),
          known_systems: number(region.known_systems, "known systems"),
        };
      }),
      goals: item.goals.map((value) => {
        const goal = record(value, "Director goal");
        return {
          id: requiredString(goal.id, "goal id"),
          kind: oneOf(goal.kind, directorGoalKinds, "goal kind"),
          region: nullableString(goal.region, "goal region"),
          status: oneOf(goal.status, directorGoalStatuses, "goal status"),
          objective: requiredString(goal.objective, "goal objective"),
          blocker: nullableString(goal.blocker, "goal blocker"),
          next_action: nullableString(goal.next_action, "goal next action"),
          progress_current: number(goal.progress_current, "goal progress"),
          progress_total: number(goal.progress_total, "goal total"),
          active_workflows: stringArray(
            goal.active_workflows,
            "goal workflows",
          ),
          enabled: boolean(goal.enabled, "goal enabled"),
        };
      }),
      mining_policies: array(
        item.mining_policies ?? [],
        "Director mining policies",
      ).map((value) => {
        const policy = record(value, "Director mining policy");
        return {
          region: requiredString(policy.region, "mining policy region"),
          expand_moderate: boolean(
            policy.expand_moderate,
            "expand moderate belts",
          ),
          expand_sparse: boolean(policy.expand_sparse, "expand sparse belts"),
        };
      }),
      replicants: item.replicants.map((value) => {
        const replicant = record(value, "Director Replicant");
        return {
          code: requiredString(replicant.code, "Replicant code"),
          name: nullableString(replicant.name, "Replicant name"),
          region: nullableString(replicant.region, "Replicant region"),
          busy: boolean(replicant.busy, "Replicant busy"),
          workflow_id: nullableString(
            replicant.workflow_id,
            "Replicant workflow",
          ),
          role_affinity: nullableString(
            replicant.role_affinity,
            "role affinity",
          ),
        };
      }),
      requirements: array(item.requirements ?? [], "Director requirements").map(
        (value) => {
          const requirement = record(value, "Director requirement");
          return {
            id: requiredString(requirement.id, "requirement id"),
            kind: oneOf(
              requirement.kind,
              directorRequirementKinds,
              "requirement kind",
            ),
            status: oneOf(
              requirement.status,
              directorRequirementStatuses,
              "requirement status",
            ),
            region: nullableString(requirement.region, "requirement region"),
            target: requiredString(requirement.target, "requirement target"),
            priority: number(requirement.priority, "requirement priority"),
            requesters: array(
              requirement.requesters,
              "requirement requesters",
            ).map((value) => {
              const requester = record(value, "requirement requester");
              return {
                goal_id: requiredString(requester.goal_id, "requester goal"),
                reason: requiredString(requester.reason, "requester reason"),
                priority: number(requester.priority, "requester priority"),
              };
            }),
            active_workflows: stringArray(
              requirement.active_workflows ?? [],
              "requirement workflows",
            ),
          };
        },
      ),
      workforce: {
        total: number(workforce.total, "workforce total"),
        busy: number(workforce.busy, "workforce busy"),
        idle: number(workforce.idle, "workforce idle"),
        idle_ratio: finiteNumber(workforce.idle_ratio, "workforce idle ratio"),
        pending_worker_demand: number(
          workforce.pending_worker_demand,
          "worker demand",
        ),
        scale_up_recommended: boolean(
          workforce.scale_up_recommended,
          "scale recommendation",
        ),
        scale_reason: nullableString(workforce.scale_reason, "scale reason"),
      },
    };
  });
}

export function parseAutomationControlResponse(
  value: unknown,
): Versioned<AutomationControlResponse> {
  return envelope(value, (payload) => {
    const item = record(payload, "automation control response");
    return {
      automation: automation(item.automation),
      affected_workflows: number(
        item.affected_workflows,
        "affected workflow count",
      ),
    };
  });
}
