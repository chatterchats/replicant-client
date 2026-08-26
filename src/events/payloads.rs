//! Additional typed Replicant Space event payloads.

use serde::Deserialize;

use crate::raw::JsonObject;

/// Typed payload for documented events whose current payload is empty.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct EmptyEventPayload {
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `ami.adopted`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct AmiAdoptedPayload {
    /// Documented `devices` field.
    #[serde(default)]
    pub devices: Vec<JsonObject>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `ami.assembled`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct AmiAssembledPayload {
    /// Documented `destination` field.
    pub destination: Option<String>,
    /// Documented `assembled_count` field.
    pub assembled_count: Option<i64>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `ami.launched`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct AmiLaunchedPayload {
    /// Documented `directive_status` field.
    pub directive_status: Option<String>,
    /// Documented `evaluated` field.
    pub evaluated: Option<bool>,
    /// Documented `devices_deployed` field.
    pub devices_deployed: Option<i64>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `ami.released`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct AmiReleasedPayload {
    /// Documented `devices` field.
    #[serde(default)]
    pub devices: Vec<JsonObject>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `ami.withdrawn`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct AmiWithdrawnPayload {
    /// Documented `directive_paused` field.
    pub directive_paused: Option<bool>,
    /// Documented `devices_recalled` field.
    pub devices_recalled: Option<i64>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `bobnet.new`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct BobnetNewPayload {
    /// Documented `id` field.
    pub id: Option<i64>,
    /// Documented `replicant_name` field.
    pub replicant_name: Option<String>,
    /// Documented `replicant_code` field.
    pub replicant_code: Option<String>,
    /// Documented `current_star` field.
    pub current_star: Option<String>,
    /// Documented `channel` field.
    pub channel: Option<String>,
    /// Documented `message` field.
    pub message: Option<String>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `device.attached`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct DeviceAttachedPayload {
    /// Documented `target_code` field.
    pub target_code: Option<String>,
    /// Documented `target_type` field.
    pub target_type: Option<String>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `device.changed_owner`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct DeviceChangedOwnerPayload {
    /// Documented `from_replicant` field.
    pub from_replicant: Option<String>,
    /// Documented `to_replicant` field.
    pub to_replicant: Option<String>,
    /// Documented `direction` field.
    pub direction: Option<String>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `device.decommissioned`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct DeviceDecommissionedPayload {
    /// Documented `resources_recovered` field.
    #[serde(default)]
    pub resources_recovered: JsonObject,
    /// Documented `blueprint_discovered` field.
    pub blueprint_discovered: Option<serde_json::Value>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `device.deployed`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct DeviceDeployedPayload {
    /// Documented `deployed_from_device_code` field.
    pub deployed_from_device_code: Option<String>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `device.detached`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct DeviceDetachedPayload {
    /// Documented `target_code` field.
    pub target_code: Option<String>,
    /// Documented `target_type` field.
    pub target_type: Option<String>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `device.stowed`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct DeviceStowedPayload {
    /// Documented `stowed_in_device_code` field.
    pub stowed_in_device_code: Option<String>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `directive.cleared`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct DirectiveClearedPayload {
    /// Documented `previous_directive` field.
    pub previous_directive: Option<String>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `directive.completed`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct DirectiveCompletedPayload {
    /// Documented `directive` field.
    pub directive: Option<String>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `directive.paused`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct DirectivePausedPayload {
    /// Documented `directive` field.
    pub directive: Option<String>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `directive.resumed`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct DirectiveResumedPayload {
    /// Documented `directive` field.
    pub directive: Option<String>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `directive.set`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct DirectiveSetPayload {
    /// Documented `directive` field.
    pub directive: Option<String>,
    /// Documented `configuration` field.
    #[serde(default)]
    pub configuration: JsonObject,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `diversion.activated`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct DiversionActivatedPayload {
    /// Documented `object_designation` field.
    pub object_designation: Option<String>,
    /// Documented `size_class` field.
    pub size_class: Option<String>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `diversion.deactivated`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct DiversionDeactivatedPayload {
    /// Documented `device_code` field.
    pub device_code: Option<String>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `diversion.diverted`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct DiversionDivertedPayload {
    /// Documented `object_designation` field.
    pub object_designation: Option<String>,
    /// Documented `outcome` field.
    pub outcome: Option<String>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `diversion.impacted`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct DiversionImpactedPayload {
    /// Documented `object_designation` field.
    pub object_designation: Option<String>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `diversion.partial`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct DiversionPartialPayload {
    /// Documented `object_designation` field.
    pub object_designation: Option<String>,
    /// Documented `outcome` field.
    pub outcome: Option<String>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `event.completed`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct EventCompletedPayload {
    /// Documented `designation` field.
    pub designation: Option<String>,
    /// Documented `location` field.
    pub location: Option<String>,
    /// Documented `event_type` field.
    pub event_type: Option<String>,
    /// Documented `tier` field.
    pub tier: Option<i64>,
    /// Documented `rewards` field.
    #[serde(default)]
    pub rewards: JsonObject,
    /// Documented `consumed` field.
    #[serde(default)]
    pub consumed: JsonObject,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `event.discovered`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct EventDiscoveredPayload {
    /// Documented `designation` field.
    pub designation: Option<String>,
    /// Documented `location` field.
    pub location: Option<String>,
    /// Documented `event_type` field.
    pub event_type: Option<String>,
    /// Documented `tier` field.
    pub tier: Option<i64>,
    /// Documented `title` field.
    pub title: Option<String>,
    /// Documented `description` field.
    pub description: Option<String>,
    /// Documented `criteria` field.
    #[serde(default)]
    pub criteria: Vec<JsonObject>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `experience.gained`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct ExperienceGainedPayload {
    /// Documented `source` field.
    pub source: Option<String>,
    /// Documented `amount` field.
    pub amount: Option<i64>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `hub.activated`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct HubActivatedPayload {
    /// Documented `star` field.
    pub star: Option<String>,
    /// Documented `location` field.
    pub location: Option<String>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `hub.destroyed`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct HubDestroyedPayload {
    /// Documented `star` field.
    pub star: Option<String>,
    /// Documented `location` field.
    pub location: Option<String>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `megastructure.contributed`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct MegastructureContributedPayload {
    /// Documented `megastructure_designation` field.
    pub megastructure_designation: Option<String>,
    /// Documented `accepted_count` field.
    pub accepted_count: Option<i64>,
    /// Documented `contributed_devices` field.
    #[serde(default)]
    pub contributed_devices: Vec<JsonObject>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `message.new`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct MessageNewPayload {
    /// Documented `message_id` field.
    pub message_id: Option<i64>,
    /// Documented `message_type` field.
    pub message_type: Option<String>,
    /// Documented `title` field.
    pub title: Option<String>,
    /// Documented `body` field.
    pub body: Option<String>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `mining.retargeted`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct MiningRetargetedPayload {
    /// Documented `location_type` field.
    pub location_type: Option<String>,
    /// Documented `location` field.
    pub location: Option<String>,
    /// Documented `site` field.
    pub site: Option<String>,
    /// Documented `old_resource` field.
    pub old_resource: Option<String>,
    /// Documented `new_resource` field.
    pub new_resource: Option<String>,
    /// Documented `availability` field.
    pub availability: Option<String>,
    /// Documented `density` field.
    pub density: Option<String>,
    /// Documented `cycle_time_seconds` field.
    pub cycle_time_seconds: Option<i64>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `mining.started`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct MiningStartedPayload {
    /// Documented `location_type` field.
    pub location_type: Option<String>,
    /// Documented `location` field.
    pub location: Option<String>,
    /// Documented `site` field.
    pub site: Option<String>,
    /// Documented `resource_type` field.
    pub resource_type: Option<String>,
    /// Documented `availability` field.
    pub availability: Option<String>,
    /// Documented `density` field.
    pub density: Option<String>,
    /// Documented `cycle_time_seconds` field.
    pub cycle_time_seconds: Option<i64>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `mining.stopped`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct MiningStoppedPayload {
    /// Documented `location` field.
    pub location: Option<String>,
    /// Documented `resource_type` field.
    pub resource_type: Option<String>,
    /// Documented `quantity_mined` field.
    pub quantity_mined: Option<i64>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `prospect.completed`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct ProspectCompletedPayload {
    /// Documented `origin` field.
    pub origin: Option<String>,
    /// Documented `stars_generated` field.
    pub stars_generated: Option<i64>,
    /// Documented `stars` field.
    #[serde(default)]
    pub stars: Vec<String>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `relay.activated`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct RelayActivatedPayload {
    /// Documented `star` field.
    pub star: Option<String>,
    /// Documented `location` field.
    pub location: Option<String>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `replicant.transferred`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct ReplicantTransferredPayload {
    /// Documented `old_host` field.
    pub old_host: Option<String>,
    /// Documented `new_host` field.
    pub new_host: Option<String>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `salvage.depleted`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct SalvageDepletedPayload {
    /// Documented `site` field.
    pub site: Option<String>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `salvage.discovered`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct SalvageDiscoveredPayload {
    /// Documented `designation` field.
    pub designation: Option<String>,
    /// Documented `location` field.
    pub location: Option<String>,
    /// Documented `salvage_type` field.
    pub salvage_type: Option<String>,
    /// Documented `name` field.
    pub name: Option<String>,
    /// Documented `resources` field.
    #[serde(default)]
    pub resources: JsonObject,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `scan.completed`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct ScanCompletedPayload {
    /// Documented `scan_target` field.
    pub scan_target: Option<String>,
    /// Documented `scan_type` field.
    pub scan_type: Option<String>,
    /// Documented `report` field.
    #[serde(default)]
    pub report: JsonObject,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `scan.started`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct ScanStartedPayload {
    /// Documented `scan_target` field.
    pub scan_target: Option<String>,
    /// Documented `scan_type` field.
    pub scan_type: Option<String>,
    /// Documented `eta_seconds` field.
    pub eta_seconds: Option<i64>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `search.completed`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct SearchCompletedPayload {
    /// Documented `search_target` field.
    pub search_target: Option<String>,
    /// Documented `search_type` field.
    pub search_type: Option<String>,
    /// Documented `report` field.
    #[serde(default)]
    pub report: JsonObject,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `search.started`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct SearchStartedPayload {
    /// Documented `search_target` field.
    pub search_target: Option<String>,
    /// Documented `search_type` field.
    pub search_type: Option<String>,
    /// Documented `eta_seconds` field.
    pub eta_seconds: Option<i64>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `simulation.abandoned`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct SimulationAbandonedPayload {
    /// Documented `simulation_id` field.
    pub simulation_id: Option<i64>,
    /// Documented `scenario_code` field.
    pub scenario_code: Option<String>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `simulation.completed`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct SimulationCompletedPayload {
    /// Documented `simulation_id` field.
    pub simulation_id: Option<i64>,
    /// Documented `scenario_code` field.
    pub scenario_code: Option<String>,
    /// Documented `score_seconds` field.
    pub score_seconds: Option<i64>,
    /// Documented `resources_mined` field.
    pub resources_mined: Option<i64>,
    /// Documented `devices_printed` field.
    pub devices_printed: Option<i64>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `simulation.expired`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct SimulationExpiredPayload {
    /// Documented `simulation_id` field.
    pub simulation_id: Option<i64>,
    /// Documented `scenario_code` field.
    pub scenario_code: Option<String>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `simulation.started`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct SimulationStartedPayload {
    /// Documented `simulation_id` field.
    pub simulation_id: Option<i64>,
    /// Documented `scenario_code` field.
    pub scenario_code: Option<String>,
    /// Documented `starting_star` field.
    pub starting_star: Option<String>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `site.depleted`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct SiteDepletedPayload {
    /// Documented `site` field.
    pub site: Option<String>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `story.awakened`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct StoryAwakenedPayload {
    /// Documented `new_replicant_code` field.
    pub new_replicant_code: Option<String>,
    /// Documented `new_replicant_name` field.
    pub new_replicant_name: Option<String>,
    /// Documented `host_device_code` field.
    pub host_device_code: Option<String>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `story.hint`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct StoryHintPayload {
    /// Documented `hint` field.
    pub hint: Option<String>,
    /// Documented `planet` field.
    pub planet: Option<String>,
    /// Documented `designation` field.
    pub designation: Option<String>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `system.body_renamed`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct SystemBodyRenamedPayload {
    /// Documented `body_type` field.
    pub body_type: Option<String>,
    /// Documented `designation` field.
    pub designation: Option<String>,
    /// Documented `new_name` field.
    pub new_name: Option<String>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `system.devices_halted`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct SystemDevicesHaltedPayload {
    /// Documented `star` field.
    pub star: Option<String>,
    /// Documented `devices_halted` field.
    pub devices_halted: Option<i64>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `system.entry_point_set`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct SystemEntryPointSetPayload {
    /// Documented `star` field.
    pub star: Option<String>,
    /// Documented `entry_point` field.
    pub entry_point: Option<String>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `teleport.completed`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct TeleportCompletedPayload {
    /// Documented `destination_star` field.
    pub destination_star: Option<String>,
    /// Documented `new_host_code` field.
    pub new_host_code: Option<String>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `teleport.failed`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct TeleportFailedPayload {
    /// Documented `reason` field.
    pub reason: Option<String>,
    /// Documented `target_matrix_code` field.
    pub target_matrix_code: Option<String>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `teleport.started`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct TeleportStartedPayload {
    /// Documented `source_star` field.
    pub source_star: Option<String>,
    /// Documented `destination_star` field.
    pub destination_star: Option<String>,
    /// Documented `target_matrix_code` field.
    pub target_matrix_code: Option<String>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `trade.created`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct TradeCreatedPayload {
    /// Documented `trade_code` field.
    pub trade_code: Option<String>,
    /// Documented `name` field.
    pub name: Option<String>,
    /// Documented `stock` field.
    pub stock: Option<i64>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `trade.deleted`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct TradeDeletedPayload {
    /// Documented `trade_code` field.
    pub trade_code: Option<String>,
    /// Documented `name` field.
    pub name: Option<String>,
    /// Documented `remaining_stock` field.
    pub remaining_stock: Option<i64>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `transport.collected`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct TransportCollectedPayload {
    /// Documented `resources` field.
    #[serde(default)]
    pub resources: JsonObject,
    /// Documented `total` field.
    pub total: Option<i64>,
    /// Documented `cargo_after` field.
    pub cargo_after: Option<i64>,
    /// Documented `cargo_capacity` field.
    pub cargo_capacity: Option<i64>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `transport.delivered`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct TransportDeliveredPayload {
    /// Documented `resources` field.
    #[serde(default)]
    pub resources: JsonObject,
    /// Documented `total` field.
    pub total: Option<i64>,
    /// Documented `cargo_after` field.
    pub cargo_after: Option<i64>,
    /// Documented `cargo_capacity` field.
    pub cargo_capacity: Option<i64>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `travel.arrived`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct TravelArrivedPayload {
    /// Documented `attached_devices` field.
    #[serde(default)]
    pub attached_devices: Vec<String>,
    /// Documented `destination` field.
    pub destination: Option<String>,
    /// Documented `origin` field.
    pub origin: Option<String>,
    /// Documented `recalling` field.
    pub recalling: Option<bool>,
    /// Documented `travel_type` field.
    pub travel_type: Option<String>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `travel.cancelled`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct TravelCancelledPayload {
    /// Documented `travel_type` field.
    pub travel_type: Option<String>,
    /// Documented `origin` field.
    pub origin: Option<String>,
    /// Documented `destination` field.
    pub destination: Option<String>,
    /// Documented `return_time_seconds` field.
    pub return_time_seconds: Option<i64>,
    /// Documented `attached_devices` field.
    #[serde(default)]
    pub attached_devices: Vec<String>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `travel.departed`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct TravelDepartedPayload {
    /// Documented `travel_type` field.
    pub travel_type: Option<String>,
    /// Documented `origin` field.
    pub origin: Option<String>,
    /// Documented `destination` field.
    pub destination: Option<String>,
    /// Documented `distance_au` field.
    pub distance_au: Option<f64>,
    /// Documented `distance_ly` field.
    pub distance_ly: Option<f64>,
    /// Documented `travel_time_seconds` field.
    pub travel_time_seconds: Option<i64>,
    /// Documented `arrives_at` field.
    pub arrives_at: Option<String>,
    /// Documented `attached_devices` field.
    #[serde(default)]
    pub attached_devices: Vec<String>,
    /// Future payload fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Typed payload for `ami.transport.digest`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct AmiTransportDigestPayload {
    /// Active controller directive.
    pub directive: Option<String>,
    /// Buffered activity summary.
    pub activity: Option<super::AmiDigestActivity>,
    /// Current state of each managed device.
    #[serde(default)]
    pub devices: Vec<super::AmiDigestDevice>,
    /// Directive-specific transport report.
    #[serde(default)]
    pub report: JsonObject,
    /// Future digest fields.
    #[serde(flatten)]
    pub extra: JsonObject,
}
