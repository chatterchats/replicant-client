//! Device fleet operations (`/v1/devices*`).
//!
//! This is the largest raw domain: listing/filtering owned devices, reading
//! full device status, configuring tags, dispatching the unified device
//! command endpoint (40 command names over 24 distinct payload shapes),
//! device event logs, network topology, and device permission grants
//! (undocumented/opaque, like the trading endpoints).
//!
//! Device commands are unsafe mutations: the raw client never retries them
//! automatically after a request may have reached the server, since a lost
//! response cannot prove whether the command executed.

use std::collections::HashMap;

use reqwest::Method;
use serde::{Deserialize, Deserializer, Serialize, de::Error as _};

use crate::error::Error;
use crate::raw::common::{encode_path_segment, with_query};
use crate::raw::{Client, JsonObject, RawResponse, RequestSafety};

fn deserialize_optional_reference<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    let Some(value) = value else {
        return Ok(None);
    };

    match value {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::String(value) => Ok(Some(value)),
        serde_json::Value::Object(object) => Ok([
            "replicant_code",
            "designation",
            "device_code",
            "location_code",
            "star_code",
            "code",
            "id",
            "name",
            "status",
            "type",
        ]
        .into_iter()
        .find_map(|key| object.get(key).and_then(serde_json::Value::as_str))
        .map(str::to_owned)),
        other => Err(D::Error::custom(format!(
            "expected a string, object reference, or null; got {}",
            match other {
                serde_json::Value::Bool(_) => "boolean",
                serde_json::Value::Number(_) => "number",
                serde_json::Value::Array(_) => "array",
                serde_json::Value::Null
                | serde_json::Value::String(_)
                | serde_json::Value::Object(_) => unreachable!(),
            }
        ))),
    }
}

fn deserialize_references<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let values = Vec::<serde_json::Value>::deserialize(deserializer)?;
    values
        .into_iter()
        .filter_map(|value| match value {
            serde_json::Value::String(value) => Some(Ok(value)),
            serde_json::Value::Object(object) => [
                "replicant_code",
                "device_code",
                "code",
                "id",
            ]
            .into_iter()
            .find_map(|key| object.get(key).and_then(serde_json::Value::as_str))
            .map(str::to_owned)
            .map(Ok),
            serde_json::Value::Null => None,
            other => Some(Err(D::Error::custom(format!(
                "expected a string or reference object, got {other}"
            )))),
        })
        .collect()
}

fn validate_page_limit(limit: Option<i64>) -> Result<(), Error> {
    if limit.is_some_and(|value| !(1..=100).contains(&value)) {
        return Err(Error::Configuration {
            message: "page limit must be between 1 and 100".into(),
        });
    }
    Ok(())
}

fn validate_device_list_limit(limit: Option<i64>) -> Result<(), Error> {
    if limit.is_some_and(|value| !(1..=50).contains(&value)) {
        return Err(Error::Configuration {
            message: "device list limit must be between 1 and 50".into(),
        });
    }
    Ok(())
}

/// One resource stack carried by a device.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct CargoItem {
    /// Stack quantity.
    pub quantity: Option<i64>,
    /// Resource type key.
    pub resource_type: Option<String>,
}

/// A device's in-progress mining operation.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct MiningInfo {
    /// Current resource availability at the mining site.
    pub availability: Option<String>,
    /// The asteroid belt being mined. Carries only the current field name;
    /// the contract defines no legacy alias for this embedded status block
    /// (unlike the replicant mine-response, see
    /// [`crate::raw::replicants::MineResponse`]).
    pub belt: Option<String>,
    /// Seconds per mining cycle.
    pub cycle_time_seconds: Option<f64>,
    /// Resource density at the site.
    pub density: Option<String>,
    /// Mining cycles completed but not yet collected.
    pub pending_cycles: Option<i64>,
    /// Quantity mined but not yet collected.
    pub pending_quantity: Option<f64>,
    /// Total quantity mined so far.
    pub quantity_mined: Option<i64>,
    /// Resource type being mined.
    pub resource_type: Option<String>,
    /// When mining started, RFC3339.
    pub started_at: Option<String>,
}

/// A device's in-progress print job.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct PrintingInfo {
    /// When the current print completes, RFC3339.
    pub completes_at: Option<String>,
    /// Device type being printed.
    pub device_type: Option<String>,
    /// Estimated seconds remaining. Replicant Space 2.3.3 emits whole seconds;
    /// `f64` is retained for source compatibility and accepts integer JSON.
    pub eta_seconds: Option<f64>,
    /// Completion percentage.
    pub progress_percent: Option<f64>,
    /// When the current print started, RFC3339.
    pub started_at: Option<String>,
    /// Tags to apply to the printed device.
    #[serde(default)]
    pub tags: Vec<String>,
}

/// A device's in-progress prospecting scan.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct ProspectInfo {
    /// When prospecting completes, RFC3339.
    pub completes_at: Option<String>,
    /// Prospecting direction vector.
    pub direction: Option<Vec<f64>>,
    /// Estimated seconds remaining. Replicant Space 2.3.3 emits whole seconds;
    /// `f64` is retained for source compatibility and accepts integer JSON.
    pub eta_seconds: Option<f64>,
    /// Origin location designation.
    pub origin: Option<String>,
    /// Completion percentage.
    pub progress_percent: Option<f64>,
    /// When prospecting started, RFC3339.
    pub started_at: Option<String>,
}

/// A device's in-progress repair.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct RepairInfo {
    /// Estimated seconds remaining. Replicant Space 2.3.3 emits whole seconds;
    /// `f64` is retained for source compatibility and accepts integer JSON.
    pub eta_seconds: Option<f64>,
    /// Completion percentage.
    pub progress_percent: Option<f64>,
    /// When the repair started, RFC3339.
    pub started_at: Option<String>,
    /// The device being repaired.
    pub target_device_code: Option<String>,
}

/// A device's in-progress scan.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct ScanInfo {
    /// When the scan completes, RFC3339.
    pub completes_at: Option<String>,
    /// Estimated seconds remaining. Replicant Space 2.3.3 emits whole seconds;
    /// `f64` is retained for source compatibility and accepts integer JSON.
    pub eta_seconds: Option<f64>,
    /// Completion percentage.
    pub progress_percent: Option<f64>,
    /// When the scan started, RFC3339.
    pub started_at: Option<String>,
    /// Scan target designation.
    pub target: Option<String>,
}

/// A device's in-progress travel.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct TravelInfo {
    /// When this leg arrives, RFC3339.
    pub arrives_at: Option<String>,
    /// When travel departed, RFC3339.
    pub departed_at: Option<String>,
    /// This leg's destination.
    pub destination: Option<String>,
    /// Distance of this leg, in AU.
    pub distance_au: Option<f64>,
    /// Distance of this leg, in light-years.
    pub distance_ly: Option<f64>,
    /// Estimated seconds remaining for this leg. Replicant Space 2.3.3 emits
    /// whole seconds; `f64` is retained for source compatibility.
    pub eta_seconds: Option<f64>,
    /// When the final destination is reached, RFC3339.
    pub final_arrives_at: Option<String>,
    /// The overall route's final destination.
    pub final_destination: Option<String>,
    /// Human-readable final destination name.
    pub final_destination_name: Option<String>,
    /// This leg's origin.
    pub origin: Option<String>,
    /// Completion percentage of this leg.
    pub progress_percent: Option<f64>,
    /// Remaining route legs, open-shaped.
    #[serde(default)]
    pub route: Vec<JsonObject>,
    /// Estimated seconds remaining for the whole route. Replicant Space 2.3.3
    /// emits whole seconds; `f64` is retained for source compatibility.
    pub route_eta_seconds: Option<f64>,
    /// Completion percentage of the whole route.
    pub route_progress_percent: Option<f64>,
    /// Current travel stage.
    pub stage: Option<String>,
    /// Total route distance, in light-years.
    pub total_distance_ly: Option<f64>,
    /// Total route time, in seconds.
    pub total_time_seconds: Option<f64>,
    /// Travel type, e.g. `"direct"`, `"relay"`.
    pub r#type: Option<String>,
}

/// Full status of a single device.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct DeviceStatus {
    /// The device's current AMI (autonomous management instruction)
    /// directive, open-shaped.
    pub ami_directive: Option<JsonObject>,
    /// Status of the current AMI directive.
    pub ami_directive_status: Option<String>,
    /// Maximum number of devices this device can hold attached.
    pub attach_capacity: Option<i64>,
    /// Devices currently attached, open-shaped.
    #[serde(default)]
    pub attached_devices: Vec<JsonObject>,
    /// The device this device is attached to, if any.
    pub attached_to_device_code: Option<String>,
    /// Commands currently valid for this device.
    #[serde(default)]
    pub available_commands: Vec<String>,
    /// AMI directives currently valid for this device.
    #[serde(default)]
    pub available_directives: Vec<String>,
    /// Whether this device is beacon-only (relay-incapable).
    pub beacon_only: Option<bool>,
    /// Carried resource stacks.
    #[serde(default)]
    pub cargo: Vec<CargoItem>,
    /// Maximum cargo capacity.
    pub cargo_capacity: Option<i64>,
    /// Cargo capacity currently used.
    pub cargo_used: Option<f64>,
    /// Deprecated command list; prefer `available_commands`.
    #[serde(default)]
    pub commands: Vec<String>,
    /// Devices this device controls, open-shaped.
    #[serde(default)]
    pub controlled_devices: Vec<JsonObject>,
    /// The device controlling this device, if any.
    pub controller_device_code: Option<String>,
    /// Creation timestamp, RFC3339.
    pub created_at: Option<String>,
    /// Deployment timestamp, RFC3339.
    pub deployed_at: Option<String>,
    /// Stable device code.
    pub device_code: Option<String>,
    /// Device type.
    pub device_type: Option<String>,
    /// Feature flags this device has.
    #[serde(default)]
    pub features: Vec<String>,
    /// Remaining grace period, in seconds, before upkeep is enforced.
    pub grace_period_remaining: Option<i64>,
    /// Whether this device is currently in its controller's control range.
    pub in_control_range: Option<bool>,
    /// Whether this device is currently prospecting.
    pub is_prospecting: Option<bool>,
    /// Current location designation.
    pub location: Option<String>,
    /// Human-readable current location name.
    pub location_name: Option<String>,
    /// In-progress mining operation, if any.
    pub mining: Option<MiningInfo>,
    /// Current operational capacity. Most device endpoints report percentage
    /// points (`0.0..=100.0`), while some legacy replicant-device responses use
    /// a `0.0..=1.0` fraction.
    pub operational_capacity: Option<f64>,
    /// Queued print jobs, open-shaped.
    #[serde(default)]
    pub print_queue: Vec<JsonObject>,
    /// In-progress print job, if any.
    pub printing: Option<PrintingInfo>,
    /// In-progress prospecting scan, if any.
    pub prospect: Option<ProspectInfo>,
    /// When the current prospect completes, RFC3339.
    pub prospect_completes_at: Option<String>,
    /// Current prospecting direction vector.
    pub prospect_direction: Option<Vec<f64>>,
    /// When the current prospect started, RFC3339.
    pub prospect_started_at: Option<String>,
    /// Print queue size limit.
    pub queue_size: Option<i64>,
    /// In-progress repair, if any.
    pub repair: Option<RepairInfo>,
    /// Percentage of repair cost already paid.
    pub repair_paid_pct: Option<f64>,
    /// Replicant currently assigned as this device's owner/operator.
    pub replicant_code: Option<String>,
    /// Replicant matrix physically hosted by this device, when this is a vessel.
    #[serde(default, deserialize_with = "deserialize_optional_reference")]
    pub hosting_replicant: Option<String>,
    /// In-progress scan, if any.
    pub scan: Option<ScanInfo>,
    /// Device status, e.g. `"active"`, `"deactivated"`.
    pub status: Option<String>,
    /// Maximum stow capacity.
    pub stow_capacity: Option<i64>,
    /// Stow capacity currently used.
    pub stow_used: Option<i64>,
    /// Devices currently stowed inside this device, open-shaped.
    #[serde(default)]
    pub stowed_devices: Vec<JsonObject>,
    /// The device this device is stowed inside, if any.
    pub stowed_in_device_code: Option<String>,
    /// System-specific status detail, open-shaped.
    pub system_status: Option<JsonObject>,
    /// User-assigned tags.
    #[serde(default)]
    pub tags: Vec<String>,
    /// Taxi mode setting, for passenger-capable devices.
    pub taxi_mode: Option<String>,
    /// Tracking site ID, for tracking-capable devices.
    pub tracking_site_id: Option<i64>,
    /// In-progress travel, if any.
    pub travel: Option<TravelInfo>,
    /// Upkeep requirements not currently met, open-shaped.
    #[serde(default)]
    pub upkeep_requirements: Vec<JsonObject>,
    /// What this device is currently blocked waiting for, open-shaped.
    pub waiting_for: Option<JsonObject>,
    /// Configured welcome message, for hub-capable devices.
    pub welcome_message: Option<String>,
    /// Any other fields the server returns.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Response body shared by `GET /v1/devices` and `GET /v1/devices/tags/{tag}`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct DeviceListResponse {
    /// Devices on this page.
    #[serde(default)]
    pub devices: Vec<DeviceStatus>,
    /// Cursor for the next page, or `None` if this is the last page.
    pub next_cursor: Option<i64>,
}

/// Query parameters for `GET /v1/devices`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DeviceListQuery {
    /// Restrict to devices assigned to this replicant.
    pub replicant_code: Option<String>,
    /// Restrict to a single device type.
    pub device_type: Option<String>,
    /// Restrict to devices carrying this tag.
    pub tag: Option<String>,
    /// Restrict to devices with no tags. Incompatible with `tag`.
    pub untagged: Option<bool>,
    /// Restrict to a single location designation.
    pub location: Option<String>,
    /// Opaque integer cursor from a previous page's `next_cursor`.
    pub cursor: Option<i64>,
    /// Maximum number of devices to return.
    pub limit: Option<i64>,
}

/// Query parameters for `GET /v1/devices/tags/{tag}`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DeviceTagListQuery {
    /// Opaque integer cursor from a previous page's `next_cursor`.
    pub cursor: Option<i64>,
    /// Maximum number of devices to return.
    pub limit: Option<i64>,
}

/// Request body for `PATCH /v1/devices/{device_code}`.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct DeviceConfigurationRequest {
    /// The tag changes to apply.
    pub configuration: DeviceConfiguration,
}

/// Tag changes to apply to a device.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct DeviceConfiguration {
    /// Tags to add.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub add_tags: Option<Vec<String>>,
    /// Tags to remove.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remove_tags: Option<Vec<String>>,
    /// Tags to set outright, replacing the current set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<String>>,
}

/// Response body for `PATCH /v1/devices/{device_code}`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct DeviceConfigurationResponse {
    /// The configured device's code.
    pub device_code: Option<String>,
    /// Tags after the update.
    #[serde(default)]
    pub tags: Vec<String>,
}

/// Shared payload for the `adopt`, `attach`, `detach`, and `release`
/// commands, all of which accept the same single-or-batch target shape.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct TargetsCommand {
    /// A single device code to target.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device: Option<String>,
    /// Multiple device codes to target (server-defined batch shape).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub devices: Option<serde_json::Value>,
    /// A single target code (device or replicant, depending on command).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    /// Multiple target codes (server-defined batch shape).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub targets: Option<serde_json::Value>,
}

/// A forward-compatible device command payload. Use this when the server
/// advertises a command name newer than this crate's typed command enum.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct DynamicDeviceCommand {
    /// Server-defined command name, preserved verbatim.
    #[serde(rename = "command")]
    pub name: String,
    /// Server-defined command parameters.
    #[serde(flatten)]
    pub parameters: JsonObject,
}

/// A device command, dispatched via `POST /v1/devices/{device_code}`.
/// Internally tagged on the wire by a `command` field; each variant carries
/// exactly the parameters its command accepts.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(tag = "command", rename_all = "snake_case")]
pub enum DeviceCommand {
    /// Activates a dormant device.
    Activate,
    /// Assembles a device from its component parts.
    Assemble,
    /// Adopts one or more ownerless devices.
    Adopt(TargetsCommand),
    /// Attaches one or more devices to this device.
    Attach(TargetsCommand),
    /// Cancels the device's current interruptible operation, such as an
    /// Autofactory print.
    Cancel,
    /// Transfers device ownership to another account.
    ChangeOwner {
        /// The receiving account or replicant code.
        target: String,
    },
    /// Clears this device's current AMI directive.
    ClearDirective,
    /// Clears this device's print queue.
    ClearQueue,
    /// Collects resources into this device's cargo.
    CollectResources {
        /// Resource type to quantity to collect.
        resources: JsonObject,
    },
    /// Compacts this device's stowed contents.
    Compact,
    /// Changes this device's operating mode.
    Configure {
        /// The new mode.
        #[serde(skip_serializing_if = "Option::is_none")]
        mode: Option<String>,
    },
    /// Deactivates an active device, cancelling any active prospecting or
    /// triangulation action.
    Deactivate,
    /// Permanently decommissions a device.
    Decommission,
    /// Deploys a device into the field.
    Deploy,
    /// Deposits resources from this device's cargo.
    DepositResources {
        /// Resource type to quantity to deposit.
        #[serde(skip_serializing_if = "Option::is_none")]
        resources: Option<JsonObject>,
    },
    /// Removes a queued print job.
    DequeuePrint {
        /// Queue index to remove.
        #[serde(skip_serializing_if = "Option::is_none")]
        index: Option<i64>,
    },
    /// Detaches one or more attached devices.
    Detach(TargetsCommand),
    /// Detonates a device.
    Detonate,
    /// Queues a new device to be printed.
    EnqueuePrint {
        /// The device type to print.
        device_type: String,
        /// Number of identical devices to enqueue.
        #[serde(skip_serializing_if = "Option::is_none")]
        quantity: Option<i64>,
        /// The controller device to route this print through, if not this
        /// device itself.
        #[serde(skip_serializing_if = "Option::is_none")]
        controller: Option<String>,
        /// Server-defined follow-up action once printing completes.
        #[serde(skip_serializing_if = "Option::is_none")]
        oncomplete: Option<JsonObject>,
        /// Tags to apply to the printed device.
        #[serde(skip_serializing_if = "Option::is_none")]
        tags: Option<Vec<String>>,
        /// Print a modular device in its compacted transport state.
        #[serde(skip_serializing_if = "Option::is_none")]
        flatpack: Option<bool>,
    },
    /// Launches a deployed device.
    Launch,
    /// Broadcasts a BobNet message from this device.
    Message {
        /// The channel to broadcast on.
        channel: String,
        /// The message body.
        text: String,
    },
    /// Begins a prospecting scan in a direction.
    Prospect {
        /// The prospecting direction vector.
        #[serde(skip_serializing_if = "Option::is_none")]
        direction: Option<Vec<f64>>,
    },
    /// Recalls a deployed device.
    Recall,
    /// Releases one or more adopted or attached devices.
    Release(TargetsCommand),
    /// Renames a device.
    Rename {
        /// The new designation.
        designation: String,
        /// The new display name.
        name: String,
    },
    /// Begins repairing a device.
    Repair {
        /// The device performing the repair, if not this device.
        #[serde(skip_serializing_if = "Option::is_none")]
        device: Option<String>,
        /// The device being repaired, if not this device.
        #[serde(skip_serializing_if = "Option::is_none")]
        target: Option<String>,
    },
    /// Replicates this device into a new replicant.
    Replicate {
        /// The new replicant's target location or identity, per the
        /// contract's own semantics for this command.
        target: String,
        /// The new replicant's display name.
        #[serde(skip_serializing_if = "Option::is_none")]
        name: Option<String>,
    },
    /// Changes the resource type this device targets.
    Retarget {
        /// The new resource type.
        resource_type: String,
    },
    /// Begins a full-planet or full-system scan.
    Scan,
    /// Searches the current location.
    Search,
    /// Sets this device's AMI directive.
    SetDirective {
        /// The directive to set.
        directive: String,
        /// Directive-specific configuration, open-shaped.
        #[serde(skip_serializing_if = "Option::is_none")]
        configuration: Option<JsonObject>,
        /// Notification preferences for this directive, open-shaped.
        #[serde(skip_serializing_if = "Option::is_none")]
        notify: Option<JsonObject>,
    },
    /// Designates this device as an interstellar entry point.
    SetEntryPoint,
    /// Sets this device's hub welcome message.
    SetWelcomeMessage {
        /// The new welcome message.
        #[serde(skip_serializing_if = "Option::is_none")]
        message: Option<String>,
    },
    /// Begins mining a resource.
    StartMining {
        /// The resource type to mine.
        resource_type: String,
        /// The specific site to mine, if not the current location.
        #[serde(skip_serializing_if = "Option::is_none")]
        target: Option<String>,
    },
    /// Runs a stellar census.
    StellarCensus {
        /// Page number to fetch.
        #[serde(skip_serializing_if = "Option::is_none")]
        page: Option<i64>,
        /// Page size.
        #[serde(skip_serializing_if = "Option::is_none")]
        per_page: Option<i64>,
    },
    /// Stows this device inside another.
    Stow {
        /// The device to stow inside, if not implied by context.
        #[serde(skip_serializing_if = "Option::is_none")]
        target: Option<String>,
    },
    /// Scans the entire current star system.
    SystemScan,
    /// Begins travel to a destination.
    Travel {
        /// The destination location or star designation.
        destination: String,
        /// If `true`, computes and returns the route without departing.
        #[serde(skip_serializing_if = "Option::is_none")]
        dry_run: Option<bool>,
        /// Server-defined routing hint (e.g. a specific relay chain).
        #[serde(skip_serializing_if = "Option::is_none")]
        via: Option<serde_json::Value>,
    },
    /// Triangulates a spectral signature from a deep-space reference point.
    Triangulate {
        /// Spectral signature hash to locate.
        signature: String,
        /// Reference point coordinates `[x, y, z]` for the observation.
        target: Vec<f64>,
    },
    /// Unfurls a deployed structure.
    Unfurl,
    /// Withdraws a device from active service.
    Withdraw,
}

/// Response body for `POST /v1/devices/{device_code}`. Every device command
/// shares this single flat response shape; only a subset of fields is
/// populated depending on which command was dispatched.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct DeviceCommandResponse {
    /// Devices adopted by an `adopt` command, open-shaped.
    #[serde(default)]
    pub adopted: Vec<JsonObject>,
    /// AMI directive set by a `set_directive` command, open-shaped.
    pub ami_directive: Option<JsonObject>,
    /// AMI directive status after a `set_directive` command.
    pub ami_directive_status: Option<String>,
    /// Arrival time after a `travel` command, RFC3339.
    pub arrives_at: Option<String>,
    /// Devices attached by an `attach` command.
    #[serde(default, deserialize_with = "deserialize_references")]
    pub attached_devices: Vec<String>,
    /// Resource availability after a `start_mining` command.
    pub availability: Option<String>,
    /// Asteroid belt targeted by a `start_mining` command.
    pub belt: Option<String>,
    /// Operational capacity restored by a `repair` command.
    pub capacity_restored: Option<f64>,
    /// Cargo contents after a `collect_resources` or `deposit_resources`
    /// command.
    #[serde(default)]
    pub cargo: Vec<CargoItem>,
    /// Cargo capacity after the command.
    pub cargo_capacity: Option<i64>,
    /// Cargo used after the command.
    pub cargo_used: Option<f64>,
    /// Carrier device code after an `enqueue_print` command.
    pub carrier_code: Option<String>,
    /// Channel broadcast on by a `message` command.
    pub channel: Option<String>,
    /// Resources collected by a `collect_resources` command.
    #[serde(default)]
    pub collected: HashMap<String, f64>,
    /// Completion time for the triggered operation, RFC3339.
    pub completes_at: Option<String>,
    /// Devices controlled after the command, open-shaped.
    #[serde(default)]
    pub controlled_devices: Vec<JsonObject>,
    /// Controller device code after the command.
    pub controller_code: Option<String>,
    /// Controller device code after the command (alternate key).
    pub controller_device_code: Option<String>,
    /// Generic count, meaning depends on the dispatched command.
    pub count: Option<i64>,
    /// Mining cycle time after a `start_mining` command.
    pub cycle_time_seconds: Option<i64>,
    /// Resource density after a `start_mining` command.
    pub density: Option<String>,
    /// Departure time after a `travel` command, RFC3339.
    pub departed_at: Option<String>,
    /// Origin device code after a `deploy`-style command.
    pub deployed_from: Option<String>,
    /// Resources deposited by a `deposit_resources` command.
    #[serde(default)]
    pub deposited: HashMap<String, f64>,
    /// Destination after a `travel` command.
    pub destination: Option<String>,
    /// Human-readable destination name after a `travel` command.
    pub destination_name: Option<String>,
    /// Destination type after a `travel` command.
    pub destination_type: Option<String>,
    /// Devices detached by a `detach` command.
    #[serde(default, deserialize_with = "deserialize_references")]
    pub detached: Vec<String>,
    /// The command's target device code.
    pub device_code: Option<String>,
    /// Direction returned by a `prospect` or completed triangulation.
    pub direction: Option<Vec<f64>>,
    /// Distance in AU after a `travel` command.
    pub distance_au: Option<f64>,
    /// Distance in light-years after a `travel` command.
    pub distance_ly: Option<f64>,
    /// Per-target errors, for batch commands, open-shaped.
    #[serde(default)]
    pub errors: Vec<JsonObject>,
    /// Estimated seconds remaining for the triggered operation. Replicant Space
    /// 2.3.3 emits whole seconds; `f64` is retained for source compatibility.
    pub eta_seconds: Option<f64>,
    /// Final arrival time after a multi-leg `travel` command, RFC3339.
    pub final_arrives_at: Option<String>,
    /// Final destination after a multi-leg `travel` command.
    pub final_destination: Option<String>,
    /// Human-readable final destination name.
    pub final_destination_name: Option<String>,
    /// Host device code after a `replicate` command.
    pub host_device_code: Option<String>,
    /// Location after the command.
    #[serde(default, deserialize_with = "deserialize_optional_reference")]
    pub location: Option<String>,
    /// Matrix code assigned by a `replicate` command.
    pub matrix_code: Option<String>,
    /// ID of the message sent by a `message` command.
    pub message_id: Option<i64>,
    /// Mode after a `configure` command.
    pub mode: Option<String>,
    /// Replicant code created by a `replicate` command.
    pub new_replicant_code: Option<String>,
    /// Replicant name created by a `replicate` command.
    pub new_replicant_name: Option<String>,
    /// Origin after a `travel` command.
    pub origin: Option<String>,
    /// Print time in seconds after an `enqueue_print` command.
    pub print_time_seconds: Option<i64>,
    /// Completion percentage for the triggered operation.
    pub progress_percent: Option<f64>,
    /// Print queue after a print-related command, open-shaped.
    #[serde(default)]
    pub queue: Vec<JsonObject>,
    /// Print queue length after a print-related command.
    pub queue_length: Option<i64>,
    /// Devices released by a `release` command, open-shaped.
    #[serde(default)]
    pub released: Vec<JsonObject>,
    /// Repair time in seconds after a `repair` command.
    pub repair_time_seconds: Option<f64>,
    /// Resource type after a resource-related command.
    pub resource_type: Option<String>,
    /// Resources mined, cumulative, after a mining-related command.
    pub resources_mined: Option<i64>,
    /// Resources recovered by a `decommission` command.
    #[serde(default)]
    pub resources_recovered: HashMap<String, i64>,
    /// Route legs after a `travel` command, open-shaped.
    #[serde(default)]
    pub route: Vec<JsonObject>,
    /// Scan target after a scan-related command.
    #[serde(default, deserialize_with = "deserialize_optional_reference")]
    pub scan_target: Option<String>,
    /// Scan type after a scan-related command.
    #[serde(default, deserialize_with = "deserialize_optional_reference")]
    pub scan_type: Option<String>,
    /// Spectral signature accepted by a `triangulate` command.
    pub signature: Option<String>,
    /// Star after the command.
    #[serde(default, deserialize_with = "deserialize_optional_reference")]
    pub star: Option<String>,
    /// Start time for the triggered operation, RFC3339.
    pub started_at: Option<String>,
    /// Device status after the command.
    #[serde(default, deserialize_with = "deserialize_optional_reference")]
    pub status: Option<String>,
    /// Device this device was stowed inside by a `stow` command.
    pub stowed_in: Option<String>,
    /// Tags after a print- or configuration-related command.
    #[serde(default)]
    pub tags: Vec<String>,
    /// Reference coordinates accepted by a `triangulate` command.
    pub target: Option<Vec<f64>>,
    /// Target device code, meaning depends on the dispatched command.
    pub target_device_code: Option<String>,
    /// Total route distance in light-years after a `travel` command.
    pub total_distance_ly: Option<f64>,
    /// Total route time in seconds after a `travel` command.
    pub total_time_seconds: Option<f64>,
    /// Travel time in seconds after a `travel` command.
    pub travel_time_seconds: Option<f64>,
    /// What the device is now blocked waiting for, open-shaped.
    pub waiting_for: Option<JsonObject>,
    /// Any other fields the server returns.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Query parameters for `GET /v1/devices/{device_code}/audit`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DeviceAuditQuery {
    /// Restrict to a single device type.
    pub device_type: Option<String>,
    /// Restrict to actions taken by a single replicant.
    pub replicant_code: Option<String>,
    /// Opaque integer cursor from a previous page's `next_cursor`.
    pub cursor: Option<i64>,
    /// Maximum number of entries to return.
    pub limit: Option<i64>,
    /// Return only the single latest entry.
    pub latest: Option<bool>,
}

/// Query parameters for `GET /v1/devices/{device_code}/logs`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DeviceLogsQuery {
    /// Opaque integer cursor from a previous page's `next_cursor`.
    pub cursor: Option<i64>,
    /// Maximum number of entries to return.
    pub limit: Option<i64>,
    /// Return only the single latest entry.
    pub latest: Option<bool>,
}

/// One entry in a device's event log.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct DeviceLogEvent {
    /// When this event occurred, RFC3339.
    pub created_at: Option<String>,
    /// The device this event concerns.
    pub device_code: Option<String>,
    /// The device's type.
    pub device_type: Option<String>,
    /// Event type key.
    pub event_type: Option<String>,
    /// Event ID.
    pub id: Option<i64>,
    /// Human-readable event message.
    pub message: Option<String>,
    /// Event-specific payload, open-shaped.
    pub payload: Option<JsonObject>,
}

/// Response body for `GET /v1/devices/{device_code}/logs`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct DeviceLogsResponse {
    /// Events on this page.
    #[serde(default)]
    pub events: Vec<DeviceLogEvent>,
    /// Cursor for the next page, or `None` if this is the last page.
    pub next_cursor: Option<i64>,
}

/// One relay-network connection from a device.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct NetworkConnection {
    /// The connected device's code.
    pub device_code: Option<String>,
    /// Distance to the connected device, in light-years.
    pub distance_ly: Option<f64>,
    /// The connected device's star.
    pub star: Option<String>,
}

/// Response body for `GET /v1/devices/{device_code}/network`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct DeviceNetwork {
    /// Devices this device can currently relay to.
    #[serde(default)]
    pub connections: Vec<NetworkConnection>,
    /// This device's relay range, in light-years.
    pub range_ly: Option<f64>,
    /// Network status, e.g. `"connected"`, `"isolated"`.
    pub status: Option<String>,
}

/// Typed client for `/v1/devices*`.
#[derive(Clone, Debug)]
pub struct DevicesClient {
    client: Client,
}

impl DevicesClient {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    /// Lists devices owned by the authenticated account.
    pub async fn list(
        &self,
        query: &DeviceListQuery,
    ) -> Result<RawResponse<DeviceListResponse>, Error> {
        validate_device_list_limit(query.limit)?;
        if query.tag.is_some() && query.untagged.is_some() {
            return Err(Error::Configuration {
                message: "device list tag and untagged filters are incompatible".into(),
            });
        }
        let path = with_query(
            "v1/devices",
            &[
                ("replicant_code", query.replicant_code.clone()),
                ("device_type", query.device_type.clone()),
                ("tag", query.tag.clone()),
                ("untagged", query.untagged.map(|value| value.to_string())),
                ("location", query.location.clone()),
                ("cursor", query.cursor.map(|value| value.to_string())),
                ("limit", query.limit.map(|value| value.to_string())),
            ],
        );
        self.client
            .execute(Method::GET, &path, true, RequestSafety::SafeRead)
            .await
    }

    /// Lists devices carrying a given tag.
    pub async fn list_by_tag(
        &self,
        tag: &str,
        query: &DeviceTagListQuery,
    ) -> Result<RawResponse<DeviceListResponse>, Error> {
        validate_device_list_limit(query.limit)?;
        let base = format!("v1/devices/tags/{}", encode_path_segment(tag));
        let path = with_query(
            &base,
            &[
                ("cursor", query.cursor.map(|value| value.to_string())),
                ("limit", query.limit.map(|value| value.to_string())),
            ],
        );
        self.client
            .execute(Method::GET, &path, true, RequestSafety::SafeRead)
            .await
    }

    /// Fetches a single device's full status.
    pub async fn get(&self, device_code: &str) -> Result<RawResponse<DeviceStatus>, Error> {
        let path = format!("v1/devices/{}", encode_path_segment(device_code));
        self.client
            .execute(Method::GET, &path, true, RequestSafety::SafeRead)
            .await
    }

    /// Updates a device's tags.
    pub async fn configure(
        &self,
        device_code: &str,
        request: &DeviceConfigurationRequest,
    ) -> Result<RawResponse<DeviceConfigurationResponse>, Error> {
        let path = format!("v1/devices/{}", encode_path_segment(device_code));
        self.client
            .execute_json(Method::PATCH, &path, true, RequestSafety::Mutating, request)
            .await
    }

    /// Dispatches a command to a device. This is an unsafe mutation: if the
    /// response is lost after transmission, the command may already have
    /// executed. The raw client never retries it automatically.
    pub async fn command<T: Serialize>(
        &self,
        device_code: &str,
        command: &T,
    ) -> Result<RawResponse<DeviceCommandResponse>, Error> {
        let path = format!("v1/devices/{}", encode_path_segment(device_code));
        self.client
            .execute_json(Method::POST, &path, true, RequestSafety::Mutating, command)
            .await
    }

    /// Lists ownership/configuration audit entries for a device. The
    /// contract documents no response schema for this endpoint, so the raw
    /// body is returned as-is.
    pub async fn audit(
        &self,
        device_code: &str,
        query: &DeviceAuditQuery,
    ) -> Result<RawResponse<serde_json::Value>, Error> {
        validate_page_limit(query.limit)?;
        let base = format!("v1/devices/{}/audit", encode_path_segment(device_code));
        let path = with_query(
            &base,
            &[
                ("device_type", query.device_type.clone()),
                ("replicant_code", query.replicant_code.clone()),
                ("cursor", query.cursor.map(|value| value.to_string())),
                ("limit", query.limit.map(|value| value.to_string())),
                ("latest", query.latest.map(|value| value.to_string())),
            ],
        );
        self.client
            .execute(Method::GET, &path, true, RequestSafety::SafeRead)
            .await
    }

    /// Lists event log entries for an owned device.
    pub async fn logs(
        &self,
        device_code: &str,
        query: &DeviceLogsQuery,
    ) -> Result<RawResponse<DeviceLogsResponse>, Error> {
        validate_page_limit(query.limit)?;
        let base = format!("v1/devices/{}/logs", encode_path_segment(device_code));
        let path = with_query(
            &base,
            &[
                ("cursor", query.cursor.map(|value| value.to_string())),
                ("limit", query.limit.map(|value| value.to_string())),
                ("latest", query.latest.map(|value| value.to_string())),
            ],
        );
        self.client
            .execute(Method::GET, &path, true, RequestSafety::SafeRead)
            .await
    }

    /// Fetches a device's relay network topology.
    pub async fn network(&self, device_code: &str) -> Result<RawResponse<DeviceNetwork>, Error> {
        let path = format!("v1/devices/{}/network", encode_path_segment(device_code));
        self.client
            .execute(Method::GET, &path, true, RequestSafety::SafeRead)
            .await
    }

    /// Lists permissions granted on a device. The contract documents no
    /// response schema for this endpoint, so the raw body is returned as-is.
    pub async fn list_permissions(
        &self,
        device_code: &str,
    ) -> Result<RawResponse<serde_json::Value>, Error> {
        let path = format!(
            "v1/devices/{}/permissions",
            encode_path_segment(device_code)
        );
        self.client
            .execute(Method::GET, &path, true, RequestSafety::SafeRead)
            .await
    }

    /// Grants a permission on a device to another account. The contract
    /// documents no request or response schema for this endpoint, so both
    /// are caller-supplied/raw JSON.
    pub async fn grant_permission(
        &self,
        device_code: &str,
        request: &JsonObject,
    ) -> Result<RawResponse<serde_json::Value>, Error> {
        let path = format!(
            "v1/devices/{}/permissions",
            encode_path_segment(device_code)
        );
        self.client
            .execute_json(Method::POST, &path, true, RequestSafety::Mutating, request)
            .await
    }

    /// Revokes a permission on a device. The contract documents no request
    /// or response schema for this endpoint.
    pub async fn revoke_permission(
        &self,
        device_code: &str,
    ) -> Result<RawResponse<serde_json::Value>, Error> {
        let path = format!(
            "v1/devices/{}/permissions",
            encode_path_segment(device_code)
        );
        self.client
            .execute(Method::DELETE, &path, true, RequestSafety::Mutating)
            .await
    }
}

#[cfg(test)]
mod command_response_tests {
    use super::{DeviceCommand, DeviceCommandResponse};

    #[test]
    fn cancel_serializes_as_typed_device_command() {
        let payload = serde_json::to_value(DeviceCommand::Cancel).expect("serialize cancel");
        assert_eq!(payload, serde_json::json!({"command": "cancel"}));
    }

    #[test]
    fn triangulate_serializes_signature_and_reference_point() {
        let command = DeviceCommand::Triangulate {
            signature: "a3f7c2e8b1d94f06".to_owned(),
            target: vec![5000.0, 14_000.0, 100.0],
        };
        let payload = serde_json::to_value(command).expect("serialize triangulate");
        assert_eq!(payload["command"], "triangulate");
        assert_eq!(payload["signature"], "a3f7c2e8b1d94f06");
        assert_eq!(payload["target"], serde_json::json!([5000.0, 14_000.0, 100.0]));
    }

    #[test]
    fn triangulate_response_preserves_documented_fields() {
        let response: DeviceCommandResponse = serde_json::from_value(serde_json::json!({
            "status": "triangulating",
            "signature": "a3f7c2e8b1d94f06",
            "target": [5000, 14000, 100],
            "started_at": "2026-08-05T10:30:00Z",
            "completes_at": "2026-08-05T11:30:00Z"
        }))
        .expect("deserialize triangulate response");

        assert_eq!(response.status.as_deref(), Some("triangulating"));
        assert_eq!(response.signature.as_deref(), Some("a3f7c2e8b1d94f06"));
        assert_eq!(response.target, Some(vec![5000.0, 14_000.0, 100.0]));
        assert_eq!(
            response.completes_at.as_deref(),
            Some("2026-08-05T11:30:00Z")
        );
    }

    #[test]
    fn enqueue_print_serializes_optional_quantity() {
        let command = DeviceCommand::EnqueuePrint {
            device_type: "survey_drone".to_owned(),
            quantity: Some(4),
            controller: None,
            oncomplete: None,
            tags: None,
            flatpack: Some(true),
        };
        let payload = serde_json::to_value(command).expect("serialize enqueue_print");
        assert_eq!(payload["command"], "enqueue_print");
        assert_eq!(payload["device_type"], "survey_drone");
        assert_eq!(payload["quantity"], 4);
        assert_eq!(payload["flatpack"], true);
    }

    #[test]
    fn command_references_accept_strings_and_objects() {
        let response: DeviceCommandResponse = serde_json::from_value(serde_json::json!({
            "star": {"designation": "KRAKXIKHU", "spectral_type": "M"},
            "location": {"code": "KRAKXIKHU-1"},
            "scan_target": "KRAKXIKHU"
        }))
        .expect("object-shaped command references should deserialize");

        assert_eq!(response.star.as_deref(), Some("KRAKXIKHU"));
        assert_eq!(response.location.as_deref(), Some("KRAKXIKHU-1"));
        assert_eq!(response.scan_target.as_deref(), Some("KRAKXIKHU"));
    }

    #[test]
    fn unrecognized_reference_objects_remain_non_fatal() {
        let response: DeviceCommandResponse = serde_json::from_value(serde_json::json!({
            "star": {"spectral_type": "M"}
        }))
        .expect("unknown object details should not fail the entire command response");

        assert_eq!(response.star, None);
    }

    #[test]
    fn command_reference_lists_accept_strings_and_objects() {
        let response: DeviceCommandResponse = serde_json::from_value(serde_json::json!({
            "attached_devices": ["D1", {"device_code": "D2", "device_type": "survey_drone"}],
            "detached": [{"code": "D3"}]
        }))
        .expect("mixed command reference lists should deserialize");

        assert_eq!(response.attached_devices, ["D1".to_owned(), "D2".to_owned()]);
        assert_eq!(response.detached, ["D3".to_owned()]);
    }
}
