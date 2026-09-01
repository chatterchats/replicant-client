//! Reusable point-to-point logistics for Replicant Space.
//!
//! The crate deliberately separates *what to move* from event, mining, relay,
//! or bootstrap-specific completion logic. [`plan_delivery`] resolves a
//! location/system origin into concrete resource pickup locations, payload
//! device codes, and transport devices. [`execute_delivery`] then performs the
//! trips using the managed client for mutations and state, with raw device
//! detail used only for cargo fields that the normalized device model does not
//! currently expose.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Mutex, MutexGuard, PoisonError},
    time::Duration,
};

use futures::future;
use replicant_client::{Client, Device, Operation, OperationId, OperationStatus, raw};
// Workflow device claims are shared vocabulary; the prefix list lives in one
// place so transport can never disagree with a workflow about what is claimed.
use replicant_protocol::workflow_reserved;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;
use tokio::time::{Instant, sleep};
use tracing::info;
use uuid::Uuid;

/// Resource quantities keyed by Replicant Space resource type.
pub type ResourceMap = BTreeMap<String, i64>;

/// Maximum number of transports auto-selected when the caller does not pin an
/// explicit [`CarrierPreference`]. Trips run concurrently per transport, so
/// this bounds how much of the free fleet one delivery may claim.
const DEFAULT_TRANSPORT_LIMIT: usize = 3;

/// One requested device type and quantity.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DeviceRequest {
    /// Number of devices to move.
    pub quantity: i64,
    /// Open device type key.
    pub device_type: String,
}

/// Optional transport-type restriction.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CarrierPreference {
    /// Number of transports of this type to use.
    pub quantity: usize,
    /// Required transport device type.
    pub device_type: String,
}

/// High-level delivery request suitable for a CLI or another automation.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct DeliveryRequest {
    /// Origin location or system designation.
    pub origin: String,
    /// Exact destination location designation.
    pub destination: String,
    /// Resource quantities to move.
    #[serde(default)]
    pub resources: ResourceMap,
    /// Device quantities to move.
    #[serde(default)]
    pub devices: Vec<DeviceRequest>,
    /// Exact device codes to move. This is used by restart-safe coordinators
    /// that must preserve the identity of an already-selected physical device.
    #[serde(default)]
    pub device_codes: Vec<String>,
    /// Move every eligible device in the origin scope carrying any of these tags.
    #[serde(default)]
    pub device_tags: Vec<String>,
    /// Optional transport type/count restriction.
    #[serde(default)]
    pub carrier: Option<CarrierPreference>,
    /// Allow an otherwise-free transport outside the origin system to self-stage
    /// to the pickup location when no suitable local transport is available.
    #[serde(default)]
    pub allow_transport_staging: bool,
}

/// One concrete resource pickup location.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ResourcePickup {
    /// Exact source location.
    pub location: String,
    /// Resource manifest available from this source.
    pub resources: ResourceMap,
}

/// One concrete payload device selected for delivery.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PayloadDevice {
    /// Stable device code.
    pub code: String,
    /// Open device type key.
    pub device_type: String,
    /// Exact source location.
    pub origin: String,
}

/// Concrete, executable delivery plan.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct DeliveryPlan {
    /// User-supplied origin location/system scope.
    pub origin: String,
    /// Exact destination location.
    pub destination: String,
    /// Concrete resource pickup manifests.
    #[serde(default)]
    pub resource_pickups: Vec<ResourcePickup>,
    /// Concrete payload device codes.
    #[serde(default)]
    pub payload_devices: Vec<PayloadDevice>,
    /// Resource-capable transports.
    #[serde(default)]
    pub cargo_transports: Vec<String>,
    /// Attachment-capable transports.
    #[serde(default)]
    pub device_carriers: Vec<String>,
    /// Exact pre-delivery location for every selected transport.
    ///
    /// Plans persisted before this field was introduced deserialize with an
    /// empty map and use the exact `origin` compatibility fallback.
    #[serde(default)]
    pub transport_origins: BTreeMap<String, String>,
}

/// Runtime behavior for delivery execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeliveryOptions {
    /// Maximum wait for one travel/state transition.
    pub wait_timeout: Duration,
    /// Poll interval for state the normalized event stream cannot fully prove.
    pub poll_interval: Duration,
    /// Unfurl compacted modular payload after it is detached at destination.
    pub unfurl_modular_payload: bool,
    /// Return each selected transport to its recorded pre-delivery location.
    pub return_transports: bool,
    /// Maximum transports auto-selected for one delivery when the caller does
    /// not pin an explicit [`CarrierPreference`]. Trips run concurrently per
    /// transport, so this bounds how much of the free fleet one delivery may
    /// claim from other workflows.
    pub transport_limit: usize,
    /// Stable UUID namespace for restart-safe managed transport mutations.
    ///
    /// When set, travel, compaction, unfurling, attachment, and detachment
    /// commands use deterministic operation identities derived from their
    /// complete target and action. `None` preserves the ordinary random-ID
    /// behavior for standalone callers.
    pub operation_namespace: Option<Uuid>,
}

impl Default for DeliveryOptions {
    fn default() -> Self {
        Self {
            wait_timeout: Duration::from_secs(21_600),
            poll_interval: Duration::from_secs(5),
            unfurl_modular_payload: true,
            return_transports: false,
            transport_limit: DEFAULT_TRANSPORT_LIMIT,
            operation_namespace: None,
        }
    }
}

/// Delivery execution summary.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct DeliveryReport {
    /// Resources deposited at the destination.
    #[serde(default)]
    pub resources_delivered: ResourceMap,
    /// Payload device codes detached at the destination.
    #[serde(default)]
    pub devices_delivered: Vec<String>,
    /// Resource transport device codes used.
    #[serde(default)]
    pub cargo_transports: Vec<String>,
    /// Device carrier codes used.
    #[serde(default)]
    pub device_carriers: Vec<String>,
}

/// Transport planning or execution failure.
#[derive(Debug, Error)]
pub enum TransportError {
    /// Managed/raw client failure.
    #[error(transparent)]
    Client(#[from] replicant_client::Error),
    /// Invalid request or current state.
    #[error("{0}")]
    Invalid(String),
    /// Required stock or transport could not be found.
    #[error("{0}")]
    NotFound(String),
    /// A selected payload became unavailable while managed state was changing.
    #[error("{0}")]
    PayloadUnavailable(String),
    /// A planned resource pickup no longer has the required stock.
    #[error("{0}")]
    StaleResourcePickup(String),
    /// A state transition exceeded the configured wait bound.
    #[error("{0}")]
    TimedOut(String),
    /// A durable operation ended unsuccessfully.
    #[error("{0}")]
    Operation(String),
}

/// Result type used by this crate.
pub type Result<T> = std::result::Result<T, TransportError>;

#[derive(Clone, Debug)]
struct TransportCandidate {
    code: String,
    device_type: String,
    location: String,
    origin_rank: u8,
    cargo_capacity: i64,
    attach_capacity: i64,
}

/// Resolves a high-level request into concrete payload and transport devices.
///
/// Uses [`DeliveryOptions::default`] for selection policy; call
/// [`plan_delivery_with`] to tune how many transports one delivery may claim.
pub async fn plan_delivery(client: &Client, request: &DeliveryRequest) -> Result<DeliveryPlan> {
    plan_delivery_with(client, request, DeliveryOptions::default()).await
}

/// Resolves a high-level request using explicit selection options.
pub async fn plan_delivery_with(
    client: &Client,
    request: &DeliveryRequest,
    options: DeliveryOptions,
) -> Result<DeliveryPlan> {
    validate_request(request)?;

    let inventories = fetch_inventories(client).await?;
    let resource_pickups = allocate_resources(
        &request.origin,
        &request.destination,
        &request.resources,
        &inventories,
    )?;

    let blueprints = fetch_transport_capacities(client).await?;
    // Normal logistics planning consumes the managed projection maintained by
    // SSE and targeted reads. A previous implementation performed an
    // unfiltered remote refresh here, turning every delivery plan into a full
    // account traversal (dozens of pages on large fleets). Exact devices are
    // still authoritatively checked by the execution path before mutation.
    let handles = client.devices().find().owned().collect().await?;
    let mut devices = Vec::with_capacity(handles.len());
    for handle in handles {
        let device = match handle.snapshot().await {
            Ok(device) => device,
            Err(_) => handle.refresh().await?.snapshot().await?,
        };
        devices.push(device);
    }

    let payload_devices = select_payload_devices(
        &request.origin,
        &request.device_codes,
        &request.devices,
        &request.device_tags,
        &devices,
    )?;
    let payload_codes = payload_devices
        .iter()
        .map(|payload| payload.code.clone())
        .collect::<BTreeSet<_>>();
    let candidates = transport_candidates(
        &request.origin,
        &devices,
        &blueprints,
        &payload_codes,
        request.allow_transport_staging,
    );

    let resource_demand = resource_pickups
        .iter()
        .map(|pickup| pickup.resources.values().copied().sum::<i64>())
        .max()
        .unwrap_or(0);
    let device_demand = payload_devices
        .iter()
        .fold(BTreeMap::<&str, i64>::new(), |mut by_location, payload| {
            *by_location.entry(payload.origin.as_str()).or_default() += 1;
            by_location
        })
        .values()
        .copied()
        .max()
        .unwrap_or(0);

    let cargo_transports = if request.resources.is_empty() {
        Vec::new()
    } else {
        select_transports(
            client,
            &candidates,
            resource_demand,
            CapacityKind::Cargo,
            request.carrier.as_ref(),
            options,
        )
        .await?
    };
    let device_carriers = if payload_devices.is_empty() {
        Vec::new()
    } else {
        select_transports(
            client,
            &candidates,
            device_demand,
            CapacityKind::Attachment,
            request.carrier.as_ref(),
            options,
        )
        .await?
    };
    let transport_origins =
        selected_transport_origins(&candidates, &cargo_transports, &device_carriers);

    Ok(DeliveryPlan {
        origin: request.origin.trim().to_ascii_uppercase(),
        destination: request.destination.trim().to_ascii_uppercase(),
        resource_pickups,
        payload_devices,
        cargo_transports,
        device_carriers,
        transport_origins,
    })
}
fn selected_transport_origins(
    candidates: &[TransportCandidate],
    cargo_transports: &[String],
    device_carriers: &[String],
) -> BTreeMap<String, String> {
    let mut selected = cargo_transports.iter().cloned().collect::<BTreeSet<_>>();
    selected.extend(device_carriers.iter().cloned());
    candidates
        .iter()
        .filter(|candidate| selected.contains(&candidate.code))
        .map(|candidate| (candidate.code.clone(), candidate.location.clone()))
        .collect()
}

/// Revalidates every planned resource pickup against a fresh account inventory
/// snapshot before any delivery mutation begins.
///
/// A durable workflow can hold a plan across scheduler turns while other
/// automations move stock. Returning [`TransportError::StaleResourcePickup`]
/// here lets the coordinator discard that stale plan and replan without
/// partially executing an obsolete manifest.
pub async fn validate_resource_pickups(client: &Client, plan: &DeliveryPlan) -> Result<()> {
    if plan.resource_pickups.is_empty() {
        return Ok(());
    }
    let inventories = fetch_inventories(client).await?;
    let mut by_location = BTreeMap::<String, ResourceMap>::new();
    for inventory in inventories {
        let replicant_client::domain::InventoryOwner::Location(location) = inventory.owner else {
            continue;
        };
        let resources = by_location
            .entry(location.id.as_str().to_ascii_uppercase())
            .or_default();
        for item in inventory.items {
            *resources
                .entry(item.resource.to_ascii_lowercase())
                .or_default() += item.quantity.max(0);
        }
    }
    for pickup in &plan.resource_pickups {
        let available = by_location.get(&pickup.location.to_ascii_uppercase());
        for (resource, required) in &pickup.resources {
            let present = available
                .and_then(|resources| resources.get(&resource.to_ascii_lowercase()))
                .copied()
                .unwrap_or_default();
            if present < *required {
                return Err(TransportError::StaleResourcePickup(format!(
                    "planned resource pickup at {} is stale: need {} {}, have {}",
                    pickup.location, required, resource, present
                )));
            }
        }
    }
    Ok(())
}

async fn validate_resource_manifest_at_location(
    client: &Client,
    location: &str,
    manifest: &ResourceMap,
) -> Result<()> {
    if manifest.is_empty() {
        return Ok(());
    }
    let (inventories, _) = client
        .inventory()
        .list(&raw::inventory::AccountInventoryQuery {
            location: Some(location.to_owned()),
            cursor: None,
            limit: Some(50),
        })
        .await?;
    let mut available = ResourceMap::new();
    for inventory in inventories {
        let replicant_client::domain::InventoryOwner::Location(owner) = inventory.owner else {
            continue;
        };
        if !owner.id.as_str().eq_ignore_ascii_case(location) {
            continue;
        }
        for item in inventory.items {
            *available
                .entry(item.resource.to_ascii_lowercase())
                .or_default() += item.quantity.max(0);
        }
    }
    for (resource, required) in manifest {
        let present = available
            .get(&resource.to_ascii_lowercase())
            .copied()
            .unwrap_or_default();
        if present < *required {
            return Err(TransportError::StaleResourcePickup(format!(
                "planned resource pickup at {location} is stale: need {required} {resource}, have {present}"
            )));
        }
    }
    Ok(())
}

/// Executes a concrete plan and optionally returns the transport devices.
pub async fn execute_delivery(
    client: &Client,
    plan: &DeliveryPlan,
    options: DeliveryOptions,
) -> Result<DeliveryReport> {
    let mut transports = plan.cargo_transports.clone();
    transports.extend(plan.device_carriers.iter().cloned());
    transports.sort();
    transports.dedup();
    // Validate legacy fallback targets before any delivery mutation. A
    // hyphenated system scope must never become a carrier travel destination.
    let return_locations = if options.return_transports {
        let mut locations = BTreeMap::new();
        for code in &transports {
            locations.insert(
                code.clone(),
                return_location_at_execution(client, plan, code).await?,
            );
        }
        locations
    } else {
        BTreeMap::new()
    };
    let resources_delivered = deliver_resource_pickups(
        client,
        &plan.destination,
        &plan.resource_pickups,
        &plan.cargo_transports,
        options,
    )
    .await?;
    let devices_delivered = deliver_payload_devices(
        client,
        &plan.destination,
        &plan.payload_devices,
        &plan.device_carriers,
        options,
    )
    .await?;

    if options.return_transports {
        for code in &transports {
            let return_location = return_locations.get(code).ok_or_else(|| {
                TransportError::Invalid(format!(
                    "missing validated return location for transport {code}"
                ))
            })?;
            ensure_device_at(client, code, return_location, options, "return").await?;
        }
    }

    Ok(DeliveryReport {
        resources_delivered,
        devices_delivered,
        cargo_transports: plan.cargo_transports.clone(),
        device_carriers: plan.device_carriers.clone(),
    })
}

/// Delivers a fixed resource manifest using already-selected cargo transports.
///
/// This is the event-automation integration point: event code can determine
/// its live remaining requirements and keep event-specific completion checks,
/// while this crate owns the actual collection, trips, and deposits.
pub async fn deliver_resources_with(
    client: &Client,
    origin: &str,
    destination: &str,
    resources: &ResourceMap,
    cargo_transports: &[String],
    options: DeliveryOptions,
) -> Result<ResourceMap> {
    if resources.is_empty() {
        return Ok(ResourceMap::new());
    }
    if cargo_transports.is_empty() {
        return Err(TransportError::NotFound(
            "resources were requested but no cargo transport was supplied".into(),
        ));
    }
    deliver_resource_pickups(
        client,
        destination,
        &[ResourcePickup {
            location: origin.to_owned(),
            resources: resources.clone(),
        }],
        cargo_transports,
        options,
    )
    .await
}

/// Delivers fixed device codes using already-selected attachment carriers.
///
/// Payload devices already standing at the destination are treated as
/// delivered, which makes event resume safe for the device half of a mission.
pub async fn deliver_devices_with(
    client: &Client,
    destination: &str,
    payload_devices: &[PayloadDevice],
    carriers: &[String],
    options: DeliveryOptions,
) -> Result<Vec<String>> {
    deliver_payload_devices(client, destination, payload_devices, carriers, options).await
}

fn validate_request(request: &DeliveryRequest) -> Result<()> {
    if request.origin.trim().is_empty() {
        return Err(TransportError::Invalid("origin cannot be empty".into()));
    }
    if request.destination.trim().is_empty() {
        return Err(TransportError::Invalid(
            "destination cannot be empty".into(),
        ));
    }
    if request.resources.is_empty()
        && request.devices.is_empty()
        && request.device_codes.is_empty()
        && request.device_tags.is_empty()
    {
        return Err(TransportError::Invalid(
            "delivery requires at least one resource, device code, device type, or device tag payload".into(),
        ));
    }
    if request.resources.values().any(|quantity| *quantity <= 0) {
        return Err(TransportError::Invalid(
            "resource quantities must be positive".into(),
        ));
    }
    if request.devices.iter().any(|request| request.quantity <= 0) {
        return Err(TransportError::Invalid(
            "device quantities must be positive".into(),
        ));
    }
    if request
        .device_codes
        .iter()
        .any(|code| code.trim().is_empty())
    {
        return Err(TransportError::Invalid(
            "device codes cannot be empty".into(),
        ));
    }
    let unique_codes = request
        .device_codes
        .iter()
        .map(|code| code.trim().to_ascii_uppercase())
        .collect::<BTreeSet<_>>();
    if unique_codes.len() != request.device_codes.len() {
        return Err(TransportError::Invalid(
            "device codes cannot contain duplicates".into(),
        ));
    }
    if request.device_tags.iter().any(|tag| tag.trim().is_empty()) {
        return Err(TransportError::Invalid(
            "device tags cannot be empty".into(),
        ));
    }
    if request
        .carrier
        .as_ref()
        .is_some_and(|carrier| carrier.quantity == 0 || carrier.device_type.trim().is_empty())
    {
        return Err(TransportError::Invalid(
            "carrier quantity must be positive and carrier type cannot be empty".into(),
        ));
    }
    Ok(())
}

async fn fetch_inventories(client: &Client) -> Result<Vec<replicant_client::domain::Inventory>> {
    let mut cursor = None;
    let mut inventories = Vec::new();
    for _ in 0..100 {
        let (mut page, next_cursor) = client
            .inventory()
            .list(&raw::inventory::AccountInventoryQuery {
                location: None,
                cursor,
                limit: Some(50),
            })
            .await?;
        inventories.append(&mut page);
        let Some(next) = next_cursor else {
            return Ok(inventories);
        };
        cursor = Some(next);
    }
    Err(TransportError::Invalid(
        "inventory listing exceeded the 100-page safety bound".into(),
    ))
}

fn allocate_resources(
    origin: &str,
    destination: &str,
    requested: &ResourceMap,
    inventories: &[replicant_client::domain::Inventory],
) -> Result<Vec<ResourcePickup>> {
    if requested.is_empty() {
        return Ok(Vec::new());
    }

    let mut candidates = inventories
        .iter()
        .filter_map(|inventory| {
            let replicant_client::domain::InventoryOwner::Location(location) = &inventory.owner
            else {
                return None;
            };
            let location = location.id.as_str();
            (scope_matches(origin, location) && !location.eq_ignore_ascii_case(destination))
                .then_some((location.to_owned(), &inventory.items))
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        origin_location_rank(origin, &left.0)
            .cmp(&origin_location_rank(origin, &right.0))
            .then_with(|| left.0.cmp(&right.0))
    });

    let mut pickups = BTreeMap::<String, ResourceMap>::new();
    for (resource, quantity) in requested {
        let mut remaining = *quantity;
        let mut available_total = 0i64;
        for (location, items) in &candidates {
            let available = items
                .iter()
                .find(|item| item.resource.eq_ignore_ascii_case(resource))
                .map(|item| item.quantity)
                .unwrap_or(0)
                .max(0);
            available_total = available_total.saturating_add(available);
            if remaining == 0 || available == 0 {
                continue;
            }
            let take = remaining.min(available);
            pickups
                .entry(location.clone())
                .or_default()
                .insert(resource.clone(), take);
            remaining -= take;
        }
        if remaining > 0 {
            return Err(TransportError::NotFound(format!(
                "origin {origin} has only {available_total} {resource}; {quantity} requested"
            )));
        }
    }

    Ok(pickups
        .into_iter()
        .map(|(location, resources)| ResourcePickup {
            location,
            resources,
        })
        .collect())
}

async fn fetch_transport_capacities(client: &Client) -> Result<BTreeMap<String, (i64, i64)>> {
    Ok(client
        .raw()
        .blueprints()
        .list()
        .await?
        .value
        .blueprints
        .into_iter()
        .filter_map(|blueprint| {
            Some((
                blueprint.device_type?,
                (
                    blueprint.cargo_capacity.unwrap_or(0),
                    blueprint.attach_capacity.unwrap_or(0),
                ),
            ))
        })
        .collect())
}

fn select_payload_devices(
    origin: &str,
    codes: &[String],
    requests: &[DeviceRequest],
    tags: &[String],
    devices: &[Device],
) -> Result<Vec<PayloadDevice>> {
    let mut selected = Vec::new();
    let mut used = BTreeSet::new();

    for requested_code in codes {
        let requested_code = requested_code.trim();
        let device = devices
            .iter()
            .find(|device| device.key.id.as_str().eq_ignore_ascii_case(requested_code))
            .ok_or_else(|| {
                TransportError::NotFound(format!(
                    "requested payload device {requested_code} is not owned"
                ))
            })?;
        let code = device.key.id.as_str().to_owned();
        let location = device
            .location
            .as_ref()
            .map(|item| item.id.as_str().to_owned())
            .ok_or_else(|| {
                TransportError::Invalid(format!("payload device {code} has no location"))
            })?;
        if !scope_matches(origin, &location) {
            return Err(TransportError::Invalid(format!(
                "payload device {code} is at {location}, outside origin scope {origin}"
            )));
        }
        if !eligible_payload(device) {
            return Err(TransportError::PayloadUnavailable(format!(
                "payload device {code} is not a free inactive payload"
            )));
        }
        if workflow_reserved(&device.tags) {
            return Err(TransportError::PayloadUnavailable(format!(
                "payload device {code} is reserved by another workflow"
            )));
        }
        let device_type = device
            .device_type
            .as_ref()
            .map(|device_type| device_type.as_str().to_owned())
            .ok_or_else(|| {
                TransportError::Invalid(format!("payload device {code} has no device type"))
            })?;
        used.insert(code.clone());
        selected.push(PayloadDevice {
            code,
            device_type,
            origin: location,
        });
    }

    for tag in tags {
        let tag = tag.trim();
        let mut matching = devices
            .iter()
            .filter(|device| {
                device.tags.iter().any(|existing| existing == tag)
                    && device
                        .location
                        .as_ref()
                        .is_some_and(|location| scope_matches(origin, location.id.as_str()))
            })
            .collect::<Vec<_>>();
        matching.sort_by(|left, right| {
            let left_location = left
                .location
                .as_ref()
                .map(|item| item.id.as_str())
                .unwrap_or("");
            let right_location = right
                .location
                .as_ref()
                .map(|item| item.id.as_str())
                .unwrap_or("");
            origin_location_rank(origin, left_location)
                .cmp(&origin_location_rank(origin, right_location))
                .then_with(|| left.key.id.as_str().cmp(right.key.id.as_str()))
        });

        if matching.is_empty() {
            return Err(TransportError::NotFound(format!(
                "origin {origin} has no devices tagged {tag}"
            )));
        }

        for device in matching {
            let code = device.key.id.as_str().to_owned();
            if used.contains(&code) {
                continue;
            }
            if !eligible_payload(device) {
                return Err(TransportError::PayloadUnavailable(format!(
                    "device {code} tagged {tag} is in the origin scope but is not a free inactive payload"
                )));
            }
            if workflow_reserved(&device.tags) {
                return Err(TransportError::PayloadUnavailable(format!(
                    "device {code} tagged {tag} is reserved by another workflow"
                )));
            }
            let device_type = device
                .device_type
                .as_ref()
                .map(|device_type| device_type.as_str().to_owned())
                .ok_or_else(|| {
                    TransportError::Invalid(format!(
                        "payload device {code} tagged {tag} has no device type"
                    ))
                })?;
            let location = device
                .location
                .as_ref()
                .map(|item| item.id.as_str().to_owned())
                .ok_or_else(|| {
                    TransportError::Invalid(format!("payload device {code} has no location"))
                })?;
            used.insert(code.clone());
            selected.push(PayloadDevice {
                code,
                device_type,
                origin: location,
            });
        }
    }

    for request in requests {
        let mut candidates = devices
            .iter()
            .filter(|device| {
                device.device_type.as_ref().is_some_and(|device_type| {
                    device_type
                        .as_str()
                        .eq_ignore_ascii_case(&request.device_type)
                }) && device
                    .location
                    .as_ref()
                    .is_some_and(|location| scope_matches(origin, location.id.as_str()))
                    && eligible_payload(device)
                    && !workflow_reserved(&device.tags)
                    && !used.contains(device.key.id.as_str())
            })
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| {
            let left_location = left
                .location
                .as_ref()
                .map(|item| item.id.as_str())
                .unwrap_or("");
            let right_location = right
                .location
                .as_ref()
                .map(|item| item.id.as_str())
                .unwrap_or("");
            origin_location_rank(origin, left_location)
                .cmp(&origin_location_rank(origin, right_location))
                .then_with(|| left.key.id.as_str().cmp(right.key.id.as_str()))
        });

        let needed = usize::try_from(request.quantity).map_err(|_| {
            TransportError::Invalid(format!(
                "device quantity for {} is too large",
                request.device_type
            ))
        })?;
        if candidates.len() < needed {
            return Err(TransportError::NotFound(format!(
                "origin {origin} has {} free inactive {}; {} requested",
                candidates.len(),
                request.device_type,
                request.quantity
            )));
        }
        for device in candidates.into_iter().take(needed) {
            let code = device.key.id.as_str().to_owned();
            let location = device
                .location
                .as_ref()
                .map(|item| item.id.as_str().to_owned())
                .ok_or_else(|| {
                    TransportError::Invalid(format!("payload device {code} has no location"))
                })?;
            used.insert(code.clone());
            selected.push(PayloadDevice {
                code,
                device_type: request.device_type.clone(),
                origin: location,
            });
        }
    }
    // Keep the origin-preference order (exact match, then belts, then the
    // rest) established by the per-selector sorts above; a plain lexicographic
    // sort on the location string would silently undo it.
    selected.sort_by(|left, right| {
        origin_location_rank(origin, &left.origin)
            .cmp(&origin_location_rank(origin, &right.origin))
            .then_with(|| left.origin.cmp(&right.origin))
            .then_with(|| left.device_type.cmp(&right.device_type))
            .then_with(|| left.code.cmp(&right.code))
    });
    Ok(selected)
}

fn transport_candidates(
    origin: &str,
    devices: &[Device],
    blueprints: &BTreeMap<String, (i64, i64)>,
    payload_codes: &BTreeSet<String>,
    allow_transport_staging: bool,
) -> Vec<TransportCandidate> {
    let mut candidates = devices
        .iter()
        .filter(|device| {
            device.location.as_ref().is_some_and(|location| {
                allow_transport_staging || transport_scope_matches(origin, location.id.as_str())
            }) && device.travel.is_none()
                && device.relationships.attached_to.is_none()
                && device.relationships.stowed_in.is_none()
                && device.relationships.controller.is_none()
                && !workflow_reserved(&device.tags)
                && !payload_codes.contains(device.key.id.as_str())
        })
        .filter_map(|device| {
            let device_type = device.device_type.as_ref()?.as_str().to_owned();
            let location = device.location.as_ref()?.id.as_str().to_owned();
            let (blueprint_cargo, blueprint_attach) =
                blueprints.get(&device_type).copied().unwrap_or_default();
            if !device.relationships.attached_devices.is_empty() {
                return None;
            }
            let attach_capacity = device.attach_capacity.unwrap_or(blueprint_attach).max(0);
            let origin_rank = transport_origin_rank(origin, &location);
            Some(TransportCandidate {
                code: device.key.id.as_str().to_owned(),
                device_type,
                location,
                origin_rank,
                cargo_capacity: blueprint_cargo.max(0),
                attach_capacity,
            })
        })
        .filter(|candidate| candidate.cargo_capacity > 0 || candidate.attach_capacity > 0)
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| left.code.cmp(&right.code));
    candidates
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CapacityKind {
    Cargo,
    Attachment,
}

impl CapacityKind {
    fn label(self) -> &'static str {
        match self {
            Self::Cargo => "cargo",
            Self::Attachment => "attachment",
        }
    }

    fn capacity(self, candidate: &TransportCandidate) -> i64 {
        match self {
            Self::Cargo => candidate.cargo_capacity,
            Self::Attachment => candidate.attach_capacity,
        }
    }
}

async fn select_transports(
    client: &Client,
    candidates: &[TransportCandidate],
    demand: i64,
    kind: CapacityKind,
    preference: Option<&CarrierPreference>,
    options: DeliveryOptions,
) -> Result<Vec<String>> {
    let quantity = preference.map_or(1, |preference| preference.quantity);
    let mut eligible = candidates
        .iter()
        .filter(|candidate| {
            kind.capacity(candidate) > 0
                && preference.is_none_or(|preference| {
                    candidate
                        .device_type
                        .eq_ignore_ascii_case(&preference.device_type)
                })
        })
        .collect::<Vec<_>>();
    eligible.sort_by(|left, right| {
        (left.origin_rank >= REMOTE_TRANSPORT_RANK)
            .cmp(&(right.origin_rank >= REMOTE_TRANSPORT_RANK))
            .then_with(|| {
                capacity_rank(kind.capacity(left), demand)
                    .cmp(&capacity_rank(kind.capacity(right), demand))
            })
            .then_with(|| left.origin_rank.cmp(&right.origin_rank))
            .then_with(|| left.code.cmp(&right.code))
    });

    let mut selected = Vec::new();
    let mut selected_capacity = 0i64;
    for candidate in eligible {
        if preference.is_none()
            && !selected.is_empty()
            && candidate.origin_rank >= REMOTE_TRANSPORT_RANK
        {
            // One usable local transport can make repeated trips. Remote
            // staging is a fallback for a missing local carrier, not a way to
            // shorten a delivery by borrowing additional cross-system hulls.
            break;
        }
        let enough = match preference {
            // An explicit preference pins the exact transport count.
            Some(preference) => selected.len() >= preference.quantity,
            // Otherwise take just enough hulls to cover the demand in one
            // concurrent wave, bounded so a single delivery cannot claim the
            // whole free fleet.
            None => {
                !selected.is_empty()
                    && (selected_capacity >= demand.max(1)
                        || selected.len() >= options.transport_limit.max(1))
            }
        };
        if enough {
            break;
        }
        if kind == CapacityKind::Cargo {
            let detail = client.raw().devices().get(&candidate.code).await?.value;
            if detail.controller_device_code.is_some() || !cargo_map(&detail).is_empty() {
                continue;
            }
        }
        selected_capacity = selected_capacity.saturating_add(kind.capacity(candidate));
        selected.push(candidate.code.clone());
    }

    if selected.len() < quantity {
        let constraint = preference
            .map(|preference| format!(" of type {}", preference.device_type))
            .unwrap_or_default();
        return Err(TransportError::NotFound(format!(
            "need {quantity} free {} transport(s){constraint} in the origin scope, found {}",
            kind.label(),
            selected.len()
        )));
    }
    Ok(selected)
}

fn capacity_rank(capacity: i64, demand: i64) -> (u8, i64) {
    if capacity >= demand.max(1) {
        (0, capacity - demand.max(1))
    } else {
        (1, -capacity)
    }
}

async fn deliver_resource_pickups(
    client: &Client,
    destination: &str,
    pickups: &[ResourcePickup],
    transports: &[String],
    options: DeliveryOptions,
) -> Result<ResourceMap> {
    if pickups.is_empty() {
        return Ok(ResourceMap::new());
    }
    if transports.is_empty() {
        return Err(TransportError::NotFound(
            "resource delivery has no cargo transports".into(),
        ));
    }

    let mut delivered = ResourceMap::new();
    for pickup in pickups {
        // Every selected transport runs its own trip loop concurrently,
        // reserving manifest slices under the lock so no unit is carried
        // twice. Any worker error aborts the whole pickup, matching the
        // previous sequential semantics.
        let remaining = Mutex::new(pickup.resources.clone());
        let pickup_delivered = Mutex::new(ResourceMap::new());
        future::try_join_all(transports.iter().map(|code| {
            run_cargo_trips(
                client,
                code,
                &pickup.location,
                destination,
                &remaining,
                &pickup_delivered,
                options,
            )
        }))
        .await?;
        let remaining = into_inner(remaining);
        if !remaining.is_empty() {
            return Err(TransportError::Invalid(format!(
                "no cargo transport made progress on remaining manifest {}",
                format_resources(&remaining)
            )));
        }
        merge_resources(&mut delivered, &into_inner(pickup_delivered));
    }
    Ok(delivered)
}

/// One cargo transport's trip loop between a pickup location and the
/// destination. Runs until the shared remaining manifest is drained.
async fn run_cargo_trips(
    client: &Client,
    code: &str,
    origin: &str,
    destination: &str,
    remaining: &Mutex<ResourceMap>,
    delivered: &Mutex<ResourceMap>,
    options: DeliveryOptions,
) -> Result<()> {
    loop {
        if lock(remaining).is_empty() {
            return Ok(());
        }
        let leg = format_resources(&lock(remaining));
        settle_transport_between(client, code, origin, destination, options, &leg).await?;
        ensure_device_at(client, code, origin, options, &leg).await?;
        let mut detail = client.raw().devices().get(code).await?.value;
        ensure_uncontrolled(&detail, code)?;
        if !cargo_map(&detail).is_empty() {
            deposit_all(client, code, options).await?;
            detail = client.raw().devices().get(code).await?.value;
        }
        let capacity = detail.cargo_capacity.unwrap_or(0);
        if capacity <= 0 {
            return Err(TransportError::Invalid(format!(
                "transport {code} has no usable cargo capacity"
            )));
        }
        // Reserve while holding the lock so concurrent transports never take
        // the same units. A failed trip aborts the whole delivery, so a
        // reservation is never silently dropped.
        let manifest = {
            let mut remaining = lock(remaining);
            let manifest = take_manifest(&remaining, capacity);
            subtract_resources(&mut remaining, &manifest);
            manifest
        };
        if manifest.is_empty() {
            return Ok(());
        }
        info!(
            transport = %code,
            origin = %origin,
            destination = %destination,
            manifest = %format_resources(&manifest),
            "delivering resource manifest"
        );
        let leg = format_resources(&manifest);
        collect_resources(client, code, origin, &manifest, options).await?;
        ensure_device_at(client, code, destination, options, &leg).await?;
        deposit_resources(client, code, Some(&manifest), options).await?;
        merge_resources(&mut lock(delivered), &manifest);
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn into_inner<T>(mutex: Mutex<T>) -> T {
    mutex.into_inner().unwrap_or_else(PoisonError::into_inner)
}

async fn deliver_payload_devices(
    client: &Client,
    destination: &str,
    payload_devices: &[PayloadDevice],
    carriers: &[String],
    options: DeliveryOptions,
) -> Result<Vec<String>> {
    if payload_devices.is_empty() {
        return Ok(Vec::new());
    }
    if carriers.is_empty() {
        return Err(TransportError::NotFound(
            "device delivery has no attachment carriers".into(),
        ));
    }

    let payload_codes = payload_devices
        .iter()
        .map(|payload| payload.code.clone())
        .collect::<BTreeSet<_>>();
    let mut by_origin = BTreeMap::<String, Vec<PayloadDevice>>::new();
    for payload in payload_devices {
        by_origin
            .entry(payload.origin.clone())
            .or_default()
            .push(payload.clone());
    }

    let mut delivered = Vec::new();
    for (origin, remaining_payloads) in by_origin {
        // Resume-safe cleanup comes first, sequentially. A previous run may
        // have (a) already delivered payloads now standing free at the
        // destination, (b) reached the destination with payloads still
        // attached, or (c) attached payloads at the origin before travelling.
        // The last two states continue the achieved attachment to the
        // destination; they must never detach at the origin and retry attach.
        for payload in &remaining_payloads {
            if device_already_delivered(client, &payload.code, destination).await? {
                if options.unfurl_modular_payload {
                    unfurl_modular_devices(client, std::slice::from_ref(&payload.code), options)
                        .await?;
                }
                delivered.push(payload.code.clone());
            }
        }
        let pending = remaining_payloads
            .iter()
            .filter(|payload| payload_is_pending(&payload.code, &delivered))
            .collect::<Vec<_>>();
        // A carrier may already be on its own return leg after the last
        // payload was detached. Do not run delivery preflight in that case:
        // return reconciliation below owns that state.
        if pending.is_empty() {
            continue;
        }

        let preflight_leg = canonical_payload_key(
            &pending
                .iter()
                .map(|payload| payload.code.clone())
                .collect::<Vec<_>>(),
        );
        for carrier in carriers {
            let detail = client.raw().devices().get(carrier).await?.value;
            let attached = detail
                .attached_devices
                .iter()
                .filter_map(reference_code)
                .collect::<Vec<_>>();
            if attached.is_empty() {
                settle_transport_between(
                    client,
                    carrier,
                    &origin,
                    destination,
                    options,
                    &preflight_leg,
                )
                .await?;
                continue;
            }
            let continued = continue_attached_delivery(
                client,
                carrier,
                &origin,
                destination,
                &payload_codes,
                options,
            )
            .await?;
            delivered.extend(continued);
        }
        delivered.sort();
        delivered.dedup();

        // Every selected carrier runs its own trip loop concurrently,
        // reserving payload devices under the lock so no device is attached
        // by two carriers.
        let remaining = remaining_payloads
            .into_iter()
            .filter(|payload| !delivered.contains(&payload.code))
            .collect::<Vec<_>>();
        let remaining = Mutex::new(remaining);
        let origin_delivered = Mutex::new(Vec::<String>::new());
        future::try_join_all(carriers.iter().map(|carrier| {
            run_carrier_trips(
                client,
                carrier,
                &origin,
                destination,
                &payload_codes,
                &remaining,
                &origin_delivered,
                options,
            )
        }))
        .await?;
        let leftover = into_inner(remaining);
        if !leftover.is_empty() {
            return Err(TransportError::Invalid(format!(
                "no carrier made progress on {} remaining payload device(s) from {origin}",
                leftover.len()
            )));
        }
        delivered.extend(into_inner(origin_delivered));
        delivered.sort();
        delivered.dedup();
    }
    delivered.sort();
    delivered.dedup();
    Ok(delivered)
}

/// Continues an already-attached delivery without undoing the attachment.
///
/// This is the restart path after attach has been accepted but before the
/// carrier has reached the destination. The carrier is allowed to be
/// stationary at the origin, already travelling to either leg endpoint, or
/// stationary at the destination. Attachments are detached only at the exact
/// destination.
async fn continue_attached_delivery(
    client: &Client,
    carrier: &str,
    origin: &str,
    destination: &str,
    payload_codes: &BTreeSet<String>,
    options: DeliveryOptions,
) -> Result<Vec<String>> {
    let detail = client.raw().devices().get(carrier).await?.value;
    let attached = detail
        .attached_devices
        .iter()
        .filter_map(reference_code)
        .collect::<Vec<_>>();
    ensure_only_delivery_attachments(carrier, &attached, payload_codes)?;
    if attached.is_empty() {
        return Ok(Vec::new());
    }
    let attached_leg = canonical_payload_key(&attached);

    if let Some(travel) = &detail.travel {
        let planned = travel
            .final_destination
            .as_ref()
            .or(travel.destination.as_ref())
            .map(String::as_str)
            .ok_or_else(|| {
                TransportError::Invalid(format!(
                    "carrier {carrier} is travelling with attached payloads but has no destination"
                ))
            })?;
        if attached_delivery_state(
            detail.location.as_deref(),
            Some(planned),
            origin,
            destination,
        )
        .is_none()
        {
            return Err(TransportError::Invalid(format!(
                "carrier {carrier} is already travelling to {planned}, outside this delivery"
            )));
        }
        settle_transport_between(client, carrier, origin, destination, options, &attached_leg)
            .await?;
    }

    let current = client.raw().devices().get(carrier).await?.value;
    match attached_delivery_state(current.location.as_deref(), None, origin, destination) {
        Some(AttachedDeliveryState::AtDestination) => {}
        Some(AttachedDeliveryState::AtOrigin) => {
            ensure_device_at(client, carrier, destination, options, &attached_leg).await?;
        }
        _ => {
            return Err(TransportError::Invalid(format!(
                "carrier {carrier} with attached payloads is not at the delivery origin or destination"
            )));
        }
    }

    let final_detail = client.raw().devices().get(carrier).await?.value;
    if final_detail.travel.is_some()
        || !final_detail
            .location
            .as_deref()
            .is_some_and(|location| same_location(location, destination))
    {
        return Err(TransportError::Invalid(format!(
            "carrier {carrier} did not reach exact delivery destination {destination}"
        )));
    }
    let attached = final_detail
        .attached_devices
        .iter()
        .filter_map(reference_code)
        .collect::<Vec<_>>();
    ensure_only_delivery_attachments(carrier, &attached, payload_codes)?;
    if attached.is_empty() {
        return Ok(Vec::new());
    }
    detach_devices(client, carrier, &attached, options).await?;
    if options.unfurl_modular_payload {
        unfurl_modular_devices(client, &attached, options).await?;
    }
    Ok(attached)
}

/// One attachment carrier's trip loop between an origin and the destination.
/// Runs until the shared payload pool is drained.
#[expect(
    clippy::too_many_arguments,
    reason = "internal worker sharing per-origin delivery state; a struct would only relocate the noise"
)]
async fn run_carrier_trips(
    client: &Client,
    carrier: &str,
    origin: &str,
    destination: &str,
    payload_codes: &BTreeSet<String>,
    remaining: &Mutex<Vec<PayloadDevice>>,
    delivered: &Mutex<Vec<String>>,
    options: DeliveryOptions,
) -> Result<()> {
    loop {
        if lock(remaining).is_empty() {
            return Ok(());
        }
        let leg = {
            let pending = lock(remaining)
                .iter()
                .map(|payload| payload.code.clone())
                .collect::<Vec<_>>();
            canonical_payload_key(&pending)
        };

        // Inspect attachments before preflighting the carrier to origin.
        // An accepted attach is an achieved physical state, so taking this
        // carrier back to origin (or detaching there) would make a restart
        // issue the same completed attach ID again.
        let detail = client.raw().devices().get(carrier).await?.value;
        let existing = detail
            .attached_devices
            .iter()
            .filter_map(reference_code)
            .collect::<Vec<_>>();
        if !existing.is_empty() {
            let continued = continue_attached_delivery(
                client,
                carrier,
                origin,
                destination,
                payload_codes,
                options,
            )
            .await?;
            {
                let mut pending = lock(remaining);
                pending.retain(|payload| !continued.contains(&payload.code));
            }
            lock(delivered).extend(continued);
            continue;
        }

        settle_transport_between(client, carrier, origin, destination, options, &leg).await?;
        ensure_device_at(client, carrier, origin, options, &leg).await?;
        let detail = client.raw().devices().get(carrier).await?.value;
        let existing = detail
            .attached_devices
            .iter()
            .filter_map(reference_code)
            .collect::<Vec<_>>();
        if !existing.is_empty() {
            let continued = continue_attached_delivery(
                client,
                carrier,
                origin,
                destination,
                payload_codes,
                options,
            )
            .await?;
            {
                let mut pending = lock(remaining);
                pending.retain(|payload| !continued.contains(&payload.code));
            }
            lock(delivered).extend(continued);
            continue;
        }

        let capacity = detail.attach_capacity.unwrap_or(0).max(0);
        if capacity == 0 {
            return Ok(());
        }
        // Reserve while holding the lock so concurrent carriers never claim
        // the same payload device. A failed trip aborts the whole delivery,
        // so a reservation is never silently dropped.
        let selected = {
            let mut remaining = lock(remaining);
            let take = usize::try_from(capacity)
                .unwrap_or(usize::MAX)
                .min(remaining.len());
            remaining.drain(..take).collect::<Vec<_>>()
        };
        if selected.is_empty() {
            return Ok(());
        }
        let selected = selected
            .iter()
            .map(|payload| payload.code.clone())
            .collect::<Vec<_>>();
        for code in &selected {
            ensure_attachable_device(client, code, options).await?;
        }
        let leg = canonical_payload_key(&selected);
        info!(
            carrier = %carrier,
            origin = %origin,
            destination = %destination,
            payload = %selected.join(","),
            "delivering device payload"
        );
        attach_devices(client, carrier, &selected, options).await?;
        ensure_device_at(client, carrier, destination, options, &leg).await?;
        detach_devices(client, carrier, &selected, options).await?;
        if options.unfurl_modular_payload {
            unfurl_modular_devices(client, &selected, options).await?;
        }
        lock(delivered).extend(selected);
    }
}

fn ensure_only_delivery_attachments(
    carrier: &str,
    attached: &[String],
    payload_codes: &BTreeSet<String>,
) -> Result<()> {
    let unexpected = attached
        .iter()
        .filter(|code| !payload_codes.contains(code.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    if unexpected.is_empty() {
        Ok(())
    } else {
        Err(TransportError::Invalid(format!(
            "carrier {carrier} contains non-delivery attachments: {}",
            unexpected.join(", ")
        )))
    }
}
async fn device_already_delivered(client: &Client, code: &str, destination: &str) -> Result<bool> {
    let detail = client.raw().devices().get(code).await?.value;
    Ok(detail.travel.is_none()
        && detail
            .location
            .as_deref()
            .is_some_and(|location| same_location(location, destination))
        && detail.attached_to_device_code.is_none()
        && detail.stowed_in_device_code.is_none())
}
fn same_location(left: &str, right: &str) -> bool {
    left.trim().eq_ignore_ascii_case(right.trim())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AttachedDeliveryState {
    AtOrigin,
    AtDestination,
    TravelingToOrigin,
    TravelingToDestination,
}

fn attached_delivery_state(
    location: Option<&str>,
    travel_destination: Option<&str>,
    origin: &str,
    destination: &str,
) -> Option<AttachedDeliveryState> {
    if let Some(travel_destination) = travel_destination {
        if same_location(travel_destination, origin) {
            Some(AttachedDeliveryState::TravelingToOrigin)
        } else if same_location(travel_destination, destination) {
            Some(AttachedDeliveryState::TravelingToDestination)
        } else {
            None
        }
    } else if location.is_some_and(|location| same_location(location, origin)) {
        Some(AttachedDeliveryState::AtOrigin)
    } else if location.is_some_and(|location| same_location(location, destination)) {
        Some(AttachedDeliveryState::AtDestination)
    } else {
        None
    }
}

fn payload_is_pending(code: &str, delivered: &[String]) -> bool {
    !delivered.iter().any(|delivered| delivered == code)
}

async fn settle_transport_between(
    client: &Client,
    code: &str,
    origin: &str,
    destination: &str,
    options: DeliveryOptions,
    leg: &str,
) -> Result<()> {
    let detail = client.raw().devices().get(code).await?.value;
    if detail.travel.is_none() {
        return Ok(());
    }
    let planned = detail
        .travel
        .as_ref()
        .and_then(|travel| {
            travel
                .final_destination
                .as_ref()
                .or(travel.destination.as_ref())
        })
        .cloned()
        .ok_or_else(|| {
            TransportError::Invalid(format!(
                "transport {code} is travelling without a destination"
            ))
        })?;

    if !same_location(&planned, origin) && !same_location(&planned, destination) {
        return Err(TransportError::Invalid(format!(
            "transport {code} is already travelling to {planned}, outside this delivery"
        )));
    }
    ensure_device_at(client, code, &planned, options, leg).await
}
/// Builds a deterministic operation identity for one restart-safe transport
/// mutation. Length-prefixing every component prevents delimiter ambiguity and
/// keeps action, target, destination, and payload-set changes in distinct
/// namespaces.
fn managed_operation_id(
    options: DeliveryOptions,
    action: &str,
    target: &str,
    destination: &str,
    payload: &str,
) -> Option<OperationId> {
    let namespace = options.operation_namespace?;
    let canonical_action = action.trim().to_ascii_lowercase();
    let canonical_target = canonical_device_code(target);
    let canonical_destination = canonical_device_code(destination);
    let canonical_payload = if payload.trim().is_empty() {
        String::new()
    } else {
        canonical_payload_codes(&payload.split(',').map(str::to_owned).collect::<Vec<_>>())
            .join(",")
    };
    let mut name = Vec::new();
    for component in [
        &canonical_action,
        &canonical_target,
        &canonical_destination,
        &canonical_payload,
    ] {
        name.extend_from_slice(&(component.len() as u64).to_le_bytes());
        name.extend_from_slice(component.as_bytes());
    }
    Some(OperationId::new(
        Uuid::new_v5(&namespace, &name).to_string(),
    ))
}

fn canonical_device_code(code: &str) -> String {
    code.trim().to_ascii_uppercase()
}

fn canonical_payload_codes(codes: &[String]) -> Vec<String> {
    codes
        .iter()
        .map(|code| canonical_device_code(code))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn canonical_payload_key(codes: &[String]) -> String {
    canonical_payload_codes(codes).join(",")
}

async fn ensure_device_at(
    client: &Client,
    code: &str,
    destination: &str,
    options: DeliveryOptions,
    leg: &str,
) -> Result<()> {
    let code = canonical_device_code(code);
    let destination = canonical_device_code(destination);
    let handle = client.devices().get(&code).await?;
    let snapshot = handle.snapshot().await?;
    if snapshot.travel.is_none()
        && snapshot
            .location
            .as_ref()
            .is_some_and(|location| same_location(location.id.as_str(), &destination))
    {
        return Ok(());
    }
    let operation = if let Some(travel) = &snapshot.travel {
        let planned = travel
            .final_destination
            .as_ref()
            .or(travel.destination.as_ref())
            .map(|location| location.id.as_str());
        if !planned.is_some_and(|planned| same_location(planned, &destination)) {
            return Err(TransportError::Invalid(format!(
                "device {code} is already travelling to {:?}, not {destination}",
                planned
            )));
        }
        None
    } else {
        info!(device = %code, destination = %destination, "dispatching transport travel");
        let via = client
            .smart_travel()
            .route_for_device(&code, &destination)
            .await?
            .filter(|plan| !plan.is_direct && !plan.intermediate_systems.is_empty())
            .map(|plan| {
                Value::Array(
                    plan.explicit_waypoints_for(&destination)
                        .into_iter()
                        .map(Value::String)
                        .collect(),
                )
            });
        let command = raw::devices::DeviceCommand::Travel {
            destination: destination.clone(),
            dry_run: None,
            via,
        };
        let operation = if let Some(operation_id) =
            managed_operation_id(options, "travel", &code, &destination, leg)
        {
            handle.command_with_id(operation_id, command).await?
        } else {
            handle.command(command).await?
        };
        ensure_operation_accepted(&operation).await?;
        Some(operation)
    };

    wait_for_device(client, &code, operation.as_ref(), options, |device| {
        device.travel.is_none()
            && device
                .location
                .as_ref()
                .is_some_and(|location| same_location(location.id.as_str(), &destination))
    })
    .await
}

/// Polls the managed device state until `predicate` holds.
///
/// When the wait was triggered by a durable operation, passing it here aborts
/// the poll as soon as that operation is classified as rejected, instead of
/// running out the full `wait_timeout` against a state change that will never
/// happen.
///
/// While a device is in transit the poll interval scales with its reported
/// ETA: a long freighter leg no longer costs one remote read every
/// `poll_interval` for its whole duration, while short hops keep reacting
/// promptly.
async fn wait_for_device(
    client: &Client,
    code: &str,
    operation: Option<&Operation>,
    options: DeliveryOptions,
    predicate: impl Fn(&Device) -> bool,
) -> Result<()> {
    let deadline = Instant::now() + options.wait_timeout;
    loop {
        let snapshot = client.devices().get(code).await?.snapshot().await?;
        if predicate(&snapshot) {
            return Ok(());
        }
        ensure_operation_not_rejected(operation, code).await?;
        if Instant::now() >= deadline {
            return Err(TransportError::TimedOut(format!(
                "timed out waiting for device {code} state"
            )));
        }
        let eta_seconds = snapshot
            .travel
            .as_ref()
            .and_then(|travel| travel.eta_seconds);
        sleep(travel_poll_interval(eta_seconds, options.poll_interval)).await;
    }
}

/// Scales the poll interval to a device's reported travel ETA.
///
/// A device that will not arrive for another ten minutes does not need to be
/// re-read every few seconds; one that is nearly there, or that reports no
/// ETA, keeps the caller's configured interval.
fn travel_poll_interval(eta_seconds: Option<i64>, configured: Duration) -> Duration {
    let scaled = match eta_seconds.unwrap_or(0) {
        eta if eta >= 300 => Duration::from_secs(60),
        eta if eta >= 60 => Duration::from_secs(30),
        eta if eta > 0 => Duration::from_secs(10),
        _ => configured,
    };
    scaled.max(configured)
}

/// Errors when a watched durable operation has been definitively rejected.
async fn ensure_operation_not_rejected(operation: Option<&Operation>, code: &str) -> Result<()> {
    let Some(operation) = operation else {
        return Ok(());
    };
    let outcome = operation.outcome().await?;
    if operation_rejected(outcome.status) {
        return Err(TransportError::Operation(format!(
            "operation {} ended as {:?} while waiting for device {code}: {:?}",
            operation.id().as_str(),
            outcome.status,
            outcome.response
        )));
    }
    Ok(())
}

async fn ensure_attachable_device(
    client: &Client,
    code: &str,
    options: DeliveryOptions,
) -> Result<()> {
    let mut detail = client.raw().devices().get(code).await?.value;
    if let Some(carrier) = &detail.attached_to_device_code {
        return Err(TransportError::Invalid(format!(
            "payload device {code} is already attached to {carrier}"
        )));
    }
    if detail.stowed_in_device_code.is_some() {
        let operation = client.devices().get(code).await?.deploy().await?;
        ensure_operation_accepted(&operation).await?;
        wait_for_raw_device(client, code, Some(&operation), options, |device| {
            device.stowed_in_device_code.is_none()
        })
        .await?;
        detail = client.raw().devices().get(code).await?.value;
    }

    if status_is(&detail, "active") && command_available(&detail, "deactivate") {
        info!(device = %code, "deactivating active payload before transport");
        let operation = client.devices().get(code).await?.deactivate().await?;
        ensure_operation_accepted(&operation).await?;
        wait_for_raw_device(client, code, Some(&operation), options, |device| {
            !status_is(device, "active")
        })
        .await?;
        detail = client.raw().devices().get(code).await?.value;
    }

    if !is_modular_device(&detail) || status_is(&detail, "compacted") {
        return Ok(());
    }
    if status_is(&detail, "compacting") {
        return wait_for_raw_device(client, code, None, options, |device| {
            status_is(device, "compacted")
        })
        .await;
    }
    if status_is(&detail, "unfurling") {
        wait_for_raw_device(client, code, None, options, |device| {
            !status_is(device, "unfurling")
        })
        .await?;
        detail = client.raw().devices().get(code).await?.value;
        if status_is(&detail, "compacted") {
            return Ok(());
        }
    }
    if detail.printing.is_some() || !detail.print_queue.is_empty() {
        return Err(TransportError::Invalid(format!(
            "modular payload {code} must finish its Autofactory work before transport"
        )));
    }
    if !command_available(&detail, "compact") {
        return Err(TransportError::Invalid(format!(
            "modular payload {code} is {:?} and cannot currently be compacted for attachment",
            detail.status
        )));
    }

    info!(device = %code, "compacting modular payload for transport");
    let handle = client.devices().get(code).await?;
    let command = raw::devices::DeviceCommand::Compact;
    let operation = if let Some(operation_id) =
        managed_operation_id(options, "compact", &canonical_device_code(code), "", "")
    {
        handle.command_with_id(operation_id, command).await?
    } else {
        handle.command(command).await?
    };
    wait_for_raw_device(client, code, Some(&operation), options, |device| {
        status_is(device, "compacted")
            && device.attached_to_device_code.is_none()
            && device.stowed_in_device_code.is_none()
    })
    .await
}
async fn attach_devices(
    client: &Client,
    carrier: &str,
    devices: &[String],
    options: DeliveryOptions,
) -> Result<()> {
    if devices.is_empty() {
        return Ok(());
    }
    let carrier = canonical_device_code(carrier);
    let devices = canonical_payload_codes(devices);
    let handle = client.devices().get(&carrier).await?;
    let command = raw::devices::DeviceCommand::Attach(raw::devices::TargetsCommand {
        device: None,
        devices: Some(Value::Array(
            devices.iter().cloned().map(Value::String).collect(),
        )),
        target: None,
        targets: None,
    });
    let operation = if let Some(operation_id) = managed_operation_id(
        options,
        "attach",
        &carrier,
        "",
        &canonical_payload_key(&devices),
    ) {
        handle.command_with_id(operation_id, command).await?
    } else {
        handle.command(command).await?
    };
    ensure_operation_accepted(&operation).await?;
    for code in &devices {
        wait_for_device(client, code, Some(&operation), options, |device| {
            device
                .relationships
                .attached_to
                .as_ref()
                .is_some_and(|attached| attached.id.as_str() == carrier)
        })
        .await?;
    }
    Ok(())
}

async fn detach_devices(
    client: &Client,
    carrier: &str,
    devices: &[String],
    options: DeliveryOptions,
) -> Result<()> {
    if devices.is_empty() {
        return Ok(());
    }
    let carrier = canonical_device_code(carrier);
    let devices = canonical_payload_codes(devices);
    ensure_cached_command_available(client, &carrier, "detach").await?;
    let handle = client.devices().get(&carrier).await?;
    let command = raw::devices::DeviceCommand::Detach(raw::devices::TargetsCommand {
        device: None,
        devices: Some(Value::Array(
            devices.iter().cloned().map(Value::String).collect(),
        )),
        target: None,
        targets: None,
    });
    let operation = if let Some(operation_id) = managed_operation_id(
        options,
        "detach",
        &carrier,
        "",
        &canonical_payload_key(&devices),
    ) {
        handle.command_with_id(operation_id, command).await?
    } else {
        handle.command(command).await?
    };
    ensure_operation_accepted(&operation).await?;
    for code in &devices {
        wait_for_device(client, code, Some(&operation), options, |device| {
            device.relationships.attached_to.is_none()
        })
        .await?;
    }
    Ok(())
}

async fn ensure_cached_command_available(client: &Client, code: &str, command: &str) -> Result<()> {
    let Some(handle) = client.devices().cached(code) else {
        return Ok(());
    };
    let snapshot = handle.snapshot().await?;
    if snapshot.available_commands.is_empty()
        || snapshot
            .available_commands
            .iter()
            .any(|available| available.as_str() == command)
    {
        return Ok(());
    }
    Err(TransportError::Invalid(format!(
        "device {code} is not currently commandable for {command}; it may be out of control range"
    )))
}

async fn unfurl_modular_devices(
    client: &Client,
    devices: &[String],
    options: DeliveryOptions,
) -> Result<()> {
    for code in devices {
        let mut detail = client.raw().devices().get(code).await?.value;
        if !is_modular_device(&detail) {
            continue;
        }
        if status_is(&detail, "unfurling") {
            wait_for_raw_device(client, code, None, options, |device| {
                !status_is(device, "unfurling")
            })
            .await?;
            detail = client.raw().devices().get(code).await?.value;
        }
        if !status_is(&detail, "compacted") && !command_available(&detail, "unfurl") {
            continue;
        }
        if !command_available(&detail, "unfurl") {
            return Err(TransportError::Invalid(format!(
                "modular payload {code} is compacted but does not advertise unfurl"
            )));
        }
        info!(device = %code, "unfurling modular payload after delivery");
        let handle = client.devices().get(code).await?;
        let command = raw::devices::DeviceCommand::Unfurl;
        let operation = if let Some(operation_id) =
            managed_operation_id(options, "unfurl", &canonical_device_code(code), "", "")
        {
            handle.command_with_id(operation_id, command).await?
        } else {
            handle.command(command).await?
        };
        wait_for_raw_device(client, code, Some(&operation), options, |device| {
            !status_is(device, "compacted")
                && !status_is(device, "compacting")
                && !status_is(device, "unfurling")
                && device.attached_to_device_code.is_none()
                && device.stowed_in_device_code.is_none()
        })
        .await?;
    }
    Ok(())
}
async fn collect_resources(
    client: &Client,
    code: &str,
    location: &str,
    resources: &ResourceMap,
    options: DeliveryOptions,
) -> Result<()> {
    if resources.is_empty() {
        return Ok(());
    }
    validate_resource_manifest_at_location(client, location, resources).await?;
    let before = cargo_map(&client.raw().devices().get(code).await?.value);
    let operation = client
        .devices()
        .get(code)
        .await?
        .command(raw::devices::DeviceCommand::CollectResources {
            resources: resource_json(resources),
        })
        .await?;
    ensure_resource_collect_accepted(&operation, location).await?;
    wait_for_raw_device(client, code, Some(&operation), options, |device| {
        let cargo = cargo_map(device);
        resources.iter().all(|(resource, quantity)| {
            cargo.get(resource).copied().unwrap_or(0)
                >= before
                    .get(resource)
                    .copied()
                    .unwrap_or(0)
                    .saturating_add(*quantity)
        })
    })
    .await
}

async fn deposit_all(client: &Client, code: &str, options: DeliveryOptions) -> Result<()> {
    deposit_resources(client, code, None, options).await
}

async fn deposit_resources(
    client: &Client,
    code: &str,
    resources: Option<&ResourceMap>,
    options: DeliveryOptions,
) -> Result<()> {
    let before = cargo_map(&client.raw().devices().get(code).await?.value);
    if before.is_empty() {
        return Ok(());
    }
    let requested = resources.cloned().unwrap_or_else(|| before.clone());
    let operation = client
        .devices()
        .get(code)
        .await?
        .command(raw::devices::DeviceCommand::DepositResources {
            resources: resources.map(resource_json),
        })
        .await?;
    ensure_operation_accepted(&operation).await?;
    wait_for_raw_device(client, code, Some(&operation), options, |device| {
        let cargo = cargo_map(device);
        requested.iter().all(|(resource, quantity)| {
            cargo.get(resource).copied().unwrap_or(0)
                <= before
                    .get(resource)
                    .copied()
                    .unwrap_or(0)
                    .saturating_sub(*quantity)
        })
    })
    .await
}

async fn wait_for_raw_device(
    client: &Client,
    code: &str,
    operation: Option<&Operation>,
    options: DeliveryOptions,
    predicate: impl Fn(&raw::devices::DeviceStatus) -> bool,
) -> Result<()> {
    let deadline = Instant::now() + options.wait_timeout;
    loop {
        let detail = client.raw().devices().get(code).await?.value;
        if predicate(&detail) {
            return Ok(());
        }
        ensure_operation_not_rejected(operation, code).await?;
        if Instant::now() >= deadline {
            return Err(TransportError::TimedOut(format!(
                "timed out waiting for device {code} cargo/state"
            )));
        }
        let eta_seconds = detail
            .travel
            .as_ref()
            .and_then(|travel| travel.eta_seconds)
            .map(|eta| eta as i64);
        sleep(travel_poll_interval(eta_seconds, options.poll_interval)).await;
    }
}

/// Verifies the immediate durable classification of a submitted command.
///
/// The submission call has already registered and classified the operation;
/// non-terminal states such as `reconciliation_required` or
/// `awaiting_evidence` resolve through the event stream or a later reconcile,
/// not by blocking here. (Previously this waited up to 30 seconds for a
/// terminal status that rarely arrives that early, stalling every device
/// command for the full timeout.) Physical effects are verified separately by
/// the device-state waits, which also abort early if the operation is
/// rejected after this check.
async fn ensure_resource_collect_accepted(operation: &Operation, location: &str) -> Result<()> {
    let outcome = operation.outcome().await?;
    if !operation_rejected(outcome.status) {
        return Ok(());
    }
    let error = format!(
        "operation {} ended as {:?}: {:?}",
        operation.id().as_str(),
        outcome.status,
        outcome.response
    );
    if error.contains("Insufficient ") && error.contains(" at location: need ") {
        return Err(TransportError::StaleResourcePickup(format!(
            "planned resource pickup at {location} became stale during collection: {error}"
        )));
    }
    Err(TransportError::Operation(error))
}

async fn ensure_operation_accepted(operation: &Operation) -> Result<()> {
    let outcome = operation.outcome().await?;
    if operation_rejected(outcome.status) {
        return Err(TransportError::Operation(format!(
            "operation {} ended as {:?}: {:?}",
            operation.id().as_str(),
            outcome.status,
            outcome.response
        )));
    }
    Ok(())
}

fn operation_rejected(status: OperationStatus) -> bool {
    matches!(
        status,
        OperationStatus::Cancelled | OperationStatus::Rejected | OperationStatus::Failed
    )
}

fn ensure_uncontrolled(device: &raw::devices::DeviceStatus, code: &str) -> Result<()> {
    if device.controller_device_code.is_some() {
        Err(TransportError::Invalid(format!(
            "cargo transport {code} is controlled by an AMI"
        )))
    } else {
        Ok(())
    }
}

fn cargo_map(device: &raw::devices::DeviceStatus) -> ResourceMap {
    device
        .cargo
        .iter()
        .filter_map(|item| {
            let resource = item.resource_type.clone()?;
            let quantity = item.quantity.unwrap_or(0);
            (quantity > 0).then_some((resource, quantity))
        })
        .collect()
}

fn resource_json(resources: &ResourceMap) -> BTreeMap<String, f64> {
    resources
        .iter()
        .map(|(resource, quantity)| (resource.clone(), *quantity as f64))
        .collect()
}

fn reference_code(value: &Map<String, Value>) -> Option<String> {
    ["device_code", "code", "device"]
        .into_iter()
        .find_map(|key| value.get(key).and_then(Value::as_str))
        .map(str::to_owned)
}

fn is_modular_device(device: &raw::devices::DeviceStatus) -> bool {
    device
        .features
        .iter()
        .any(|feature| feature.eq_ignore_ascii_case("modular"))
        || device
            .available_commands
            .iter()
            .chain(device.commands.iter())
            .any(|command| matches!(command.as_str(), "compact" | "unfurl"))
        || device.status.as_deref().is_some_and(|status| {
            matches!(
                status.to_ascii_lowercase().as_str(),
                "compacting" | "compacted" | "unfurling"
            )
        })
}

fn status_is(device: &raw::devices::DeviceStatus, expected: &str) -> bool {
    device
        .status
        .as_deref()
        .is_some_and(|status| status.eq_ignore_ascii_case(expected))
}

fn command_available(device: &raw::devices::DeviceStatus, expected: &str) -> bool {
    device
        .available_commands
        .iter()
        .chain(device.commands.iter())
        .any(|command| command.eq_ignore_ascii_case(expected))
}

fn inactive_payload(device: &Device) -> bool {
    device.status.as_ref().is_some_and(|status| {
        matches!(
            status.as_str().to_ascii_lowercase().as_str(),
            "inactive"
                | "deactivated"
                | "idle"
                | "stowed"
                | "recalled"
                | "compacted"
                | "out_of_range"
                | "monitoring"
        )
    }) && device.travel.is_none()
}

fn eligible_payload(device: &Device) -> bool {
    inactive_payload(device)
        && device.relationships.attached_to.is_none()
        && device.relationships.stowed_in.is_none()
        && device.relationships.controller.is_none()
}
fn return_location_for_transport<'a>(plan: &'a DeliveryPlan, code: &str) -> Option<&'a str> {
    if let Some(location) = plan.transport_origins.get(code) {
        return is_exact_location(location).then_some(location.as_str());
    }
    let origin = plan.origin.trim();
    is_exact_location(origin).then_some(origin)
}

/// Resolves a return target with an authoritative exact-location check for
/// plans persisted before `transport_origins` existed. A hyphen is not enough
/// to distinguish a location from a system scope; only the location endpoint's
/// canonical designation can prove that legacy fallback is safe.
async fn return_location_at_execution(
    client: &Client,
    plan: &DeliveryPlan,
    code: &str,
) -> Result<String> {
    let return_location = return_location_for_transport(plan, code).ok_or_else(|| {
        TransportError::Invalid(
            "--return-carriers requires an exact recorded transport location \
             (legacy plans may use an exact origin, not only a system)"
                .into(),
        )
    })?;
    if plan.transport_origins.contains_key(code) {
        return Ok(return_location.to_owned());
    }

    let response = client.raw().locations().get(return_location, None).await?;
    let authoritative = response.value.location.as_deref();
    if authoritative_exact_location(authoritative, return_location) {
        Ok(return_location.to_owned())
    } else {
        Err(TransportError::Invalid(format!(
            "legacy return origin {return_location} is not an authoritative exact location for transport {code}"
        )))
    }
}
fn authoritative_exact_location(authoritative: Option<&str>, requested: &str) -> bool {
    authoritative.is_some_and(|location| same_location(location, requested))
}

fn is_exact_location(location: &str) -> bool {
    let location = location.trim();
    !location.is_empty() && !location.eq_ignore_ascii_case("account") && location.contains('-')
}

fn scope_matches(origin: &str, location: &str) -> bool {
    let origin = origin.trim();
    if origin.eq_ignore_ascii_case("account") {
        true
    } else if origin.contains('-') {
        location.eq_ignore_ascii_case(origin)
    } else {
        system_designation(location).eq_ignore_ascii_case(origin)
    }
}

const REMOTE_TRANSPORT_RANK: u8 = 3;

fn transport_scope_matches(origin: &str, location: &str) -> bool {
    let origin = origin.trim();
    origin.eq_ignore_ascii_case("account")
        || system_designation(location).eq_ignore_ascii_case(system_designation(origin))
}

fn transport_origin_rank(origin: &str, location: &str) -> u8 {
    if transport_scope_matches(origin, location) {
        origin_location_rank(origin, location)
    } else {
        REMOTE_TRANSPORT_RANK
    }
}

fn origin_location_rank(origin: &str, location: &str) -> u8 {
    if origin.eq_ignore_ascii_case(location) {
        0
    } else if location
        .split('-')
        .any(|component| component.eq_ignore_ascii_case("belt"))
    {
        1
    } else {
        2
    }
}

fn system_designation(location: &str) -> &str {
    location
        .split('-')
        .next()
        .filter(|system| !system.is_empty())
        .unwrap_or(location)
}

fn take_manifest(resources: &ResourceMap, capacity: i64) -> ResourceMap {
    let mut free = capacity.max(0);
    let mut result = ResourceMap::new();
    for (resource, quantity) in resources {
        if free == 0 {
            break;
        }
        let amount = (*quantity).min(free);
        if amount > 0 {
            result.insert(resource.clone(), amount);
            free -= amount;
        }
    }
    result
}

fn subtract_resources(target: &mut ResourceMap, subtraction: &ResourceMap) {
    for (resource, amount) in subtraction {
        let remove = if let Some(quantity) = target.get_mut(resource) {
            *quantity = quantity.saturating_sub(*amount);
            *quantity == 0
        } else {
            false
        };
        if remove {
            target.remove(resource);
        }
    }
}

fn merge_resources(target: &mut ResourceMap, addition: &ResourceMap) {
    for (resource, amount) in addition {
        *target.entry(resource.clone()).or_default() += amount;
    }
}

fn format_resources(resources: &ResourceMap) -> String {
    resources
        .iter()
        .map(|(resource, quantity)| format!("{quantity} {resource}"))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload_device(code: &str, location: &str, tags: &[&str]) -> Device {
        Device {
            key: replicant_client::DeviceKey::live(code.into()),
            device_type: Some(replicant_client::DeviceType::from("exotic_matter_injector")),
            status: Some(replicant_client::DeviceStatus::from("inactive")),
            location: Some(replicant_client::LocationKey::live(location.into())),
            deployed_at: None,
            in_control_range: None,
            features: Vec::new(),
            available_commands: Vec::new(),
            available_directives: Vec::new(),
            tags: tags.iter().map(|tag| (*tag).to_owned()).collect(),
            relationships: replicant_client::DeviceRelationships::default(),
            cargo: Default::default(),
            cargo_capacity: None,
            attach_capacity: None,
            stow_capacity: None,
            stow_used: None,
            operational_capacity: None,
            grace_period_remaining: None,
            upkeep_requirements: Vec::new(),
            system_status: None,
            active_directive: None,
            travel: None,
            access: replicant_client::domain::AccessScope::Owned,
        }
    }

    #[test]
    fn out_of_range_device_can_be_selected_as_transport_payload() {
        let mut device = payload_device("WARD-1", "RHYVENAI", &[]);
        device.status = Some(replicant_client::DeviceStatus::from("out_of_range"));

        assert!(eligible_payload(&device));
    }

    fn attachment_transport(code: &str, location: &str) -> Device {
        Device {
            key: replicant_client::DeviceKey::live(code.into()),
            device_type: Some(replicant_client::DeviceType::from("mobile_fleet")),
            status: Some(replicant_client::DeviceStatus::from("inactive")),
            location: Some(replicant_client::LocationKey::live(location.into())),
            deployed_at: None,
            in_control_range: None,
            features: Vec::new(),
            available_commands: Vec::new(),
            available_directives: Vec::new(),
            tags: Vec::new(),
            relationships: replicant_client::DeviceRelationships::default(),
            cargo: Default::default(),
            cargo_capacity: None,
            attach_capacity: Some(1),
            stow_capacity: None,
            stow_used: None,
            operational_capacity: None,
            grace_period_remaining: None,
            upkeep_requirements: Vec::new(),
            system_status: None,
            active_directive: None,
            travel: None,
            access: replicant_client::domain::AccessScope::Owned,
        }
    }

    #[test]
    fn resource_allocation_never_uses_destination_stock_as_a_pickup() {
        let destination = replicant_client::LocationKey::live("SCEPTURUM-7-L4".into());
        let belt = replicant_client::LocationKey::live("SCEPTURUM-BELT-1".into());
        let inventories = vec![
            replicant_client::domain::Inventory {
                owner: replicant_client::domain::InventoryOwner::Location(destination.clone()),
                location: Some(destination),
                items: vec![replicant_client::domain::InventoryItem {
                    resource: "structural".to_owned(),
                    quantity: 500,
                }],
            },
            replicant_client::domain::Inventory {
                owner: replicant_client::domain::InventoryOwner::Location(belt.clone()),
                location: Some(belt),
                items: vec![replicant_client::domain::InventoryItem {
                    resource: "structural".to_owned(),
                    quantity: 400,
                }],
            },
        ];
        let pickups = allocate_resources(
            "SCEPTURUM",
            "SCEPTURUM-7-L4",
            &BTreeMap::from([("structural".to_owned(), 400)]),
            &inventories,
        )
        .expect("allocate source stock");

        assert_eq!(pickups.len(), 1);
        assert_eq!(pickups[0].location, "SCEPTURUM-BELT-1");
    }

    #[test]
    fn system_origin_matches_all_locations_in_system() {
        assert!(scope_matches("SCEPTURUM", "SCEPTURUM-BELT-1"));
        assert!(scope_matches("SCEPTURUM", "SCEPTURUM-7-L4"));
        assert!(!scope_matches("SCEPTURUM", "TWAFFY-OBJ-1"));
    }

    #[test]
    fn exact_origin_matches_only_that_location() {
        assert!(scope_matches("SCEPTURUM-BELT-1", "SCEPTURUM-BELT-1"));
        assert!(!scope_matches("SCEPTURUM-BELT-1", "SCEPTURUM-7-L4"));
    }

    #[test]
    fn account_origin_matches_inventory_and_transports_everywhere() {
        assert!(scope_matches("account", "SCEPTURUM-BELT-1"));
        assert!(scope_matches("ACCOUNT", "TWAFFY-OBJ-1"));
        assert!(transport_scope_matches("account", "DELTA-3-L4"));
    }

    #[test]
    fn transport_staging_is_opt_in_and_keeps_remote_carriers_as_fallbacks() {
        let devices = vec![
            attachment_transport("LOCAL", "SCEPTURUM-7-L4"),
            attachment_transport("REMOTE", "TWAFFY-OBJ-1"),
        ];
        let blueprints = BTreeMap::new();
        let payload_codes = BTreeSet::new();

        let local_only = transport_candidates(
            "SCEPTURUM-BELT-1",
            &devices,
            &blueprints,
            &payload_codes,
            false,
        );
        assert_eq!(local_only.len(), 1);
        assert_eq!(local_only[0].code, "LOCAL");
        assert!(local_only[0].origin_rank < REMOTE_TRANSPORT_RANK);

        let with_staging = transport_candidates(
            "SCEPTURUM-BELT-1",
            &devices,
            &blueprints,
            &payload_codes,
            true,
        );
        assert_eq!(with_staging.len(), 2);
        assert_eq!(
            with_staging
                .iter()
                .find(|candidate| candidate.code == "REMOTE")
                .map(|candidate| candidate.origin_rank),
            Some(REMOTE_TRANSPORT_RANK)
        );
    }
    #[test]
    fn selected_transport_origins_capture_every_selected_carrier() {
        let candidates = vec![
            TransportCandidate {
                code: "LOCAL".into(),
                device_type: "mobile_fleet".into(),
                location: "ALPHA-7-L4".into(),
                origin_rank: 0,
                cargo_capacity: 100,
                attach_capacity: 1,
            },
            TransportCandidate {
                code: "REMOTE".into(),
                device_type: "mobile_fleet".into(),
                location: "BETA-HUB".into(),
                origin_rank: REMOTE_TRANSPORT_RANK,
                cargo_capacity: 100,
                attach_capacity: 1,
            },
        ];

        let origins =
            selected_transport_origins(&candidates, &["LOCAL".into()], &["REMOTE".into()]);
        assert_eq!(
            origins,
            BTreeMap::from([
                ("LOCAL".into(), "ALPHA-7-L4".into()),
                ("REMOTE".into(), "BETA-HUB".into()),
            ])
        );
    }

    #[test]
    fn remote_carrier_returns_to_recorded_home_not_payload_origin() {
        let plan = DeliveryPlan {
            origin: "ALPHA-BELT-1".into(),
            destination: "ALPHA-HUB".into(),
            device_carriers: vec!["REMOTE".into()],
            transport_origins: BTreeMap::from([("REMOTE".into(), "BETA-HUB".into())]),
            ..DeliveryPlan::default()
        };

        assert_eq!(
            return_location_for_transport(&plan, "REMOTE"),
            Some("BETA-HUB")
        );
        assert_ne!(
            return_location_for_transport(&plan, "REMOTE"),
            Some(plan.origin.as_str())
        );
    }

    #[test]
    fn managed_operation_ids_are_stable_and_action_target_payload_specific() {
        let options = DeliveryOptions {
            operation_namespace: Some(Uuid::nil()),
            ..DeliveryOptions::default()
        };
        let attach = managed_operation_id(options, "attach", "CARRIER", "", "DEVICE-1,DEVICE-2")
            .expect("namespace produces an ID");
        let reordered = managed_operation_id(
            options,
            "attach",
            "carrier",
            "",
            &canonical_payload_key(&[" device-2 ".into(), "DEVICE-1".into()]),
        )
        .expect("namespace produces an ID");
        assert_eq!(attach, reordered);
        assert_ne!(
            attach,
            managed_operation_id(options, "detach", "CARRIER", "", "DEVICE-1,DEVICE-2")
                .expect("namespace produces an ID")
        );
        assert_ne!(
            attach,
            managed_operation_id(options, "attach", "OTHER-CARRIER", "", "DEVICE-1,DEVICE-2",)
                .expect("namespace produces an ID")
        );
        assert_ne!(
            attach,
            managed_operation_id(options, "attach", "CARRIER", "", "DEVICE-1")
                .expect("namespace produces an ID")
        );
        let first_leg = managed_operation_id(options, "travel", "carrier", "ALPHA-HUB", "device-1")
            .expect("namespace produces an ID");
        let second_leg =
            managed_operation_id(options, "travel", "CARRIER", "ALPHA-HUB", "DEVICE-2")
                .expect("namespace produces an ID");
        let first_retry =
            managed_operation_id(options, "TRAVEL", "CARRIER", " alpha-hub ", " DEVICE-1 ")
                .expect("namespace produces an ID");
        assert_ne!(first_leg, second_leg);
        assert_eq!(first_leg, first_retry);
        assert_eq!(
            managed_operation_id(DeliveryOptions::default(), "travel", "D1", "HUB", "leg",),
            None
        );
    }

    #[test]
    fn legacy_exact_origin_is_return_compatibility_fallback() {
        let plan = DeliveryPlan {
            origin: "ALPHA-BELT-1".into(),
            ..DeliveryPlan::default()
        };

        assert_eq!(
            return_location_for_transport(&plan, "LEGACY-CARRIER"),
            Some("ALPHA-BELT-1")
        );
    }

    #[test]
    fn legacy_system_origin_is_rejected_for_return() {
        let plan = DeliveryPlan {
            origin: "ALPHA".into(),
            ..DeliveryPlan::default()
        };

        assert_eq!(return_location_for_transport(&plan, "LEGACY-CARRIER"), None);
    }
    #[test]
    fn attached_at_origin_continues_to_destination_without_detach() {
        assert_eq!(
            attached_delivery_state(Some("ALPHA-BELT-1"), None, "ALPHA-BELT-1", "ALPHA-HUB",),
            Some(AttachedDeliveryState::AtOrigin)
        );
        assert_eq!(
            attached_delivery_state(
                Some("ALPHA-BELT-1"),
                Some("ALPHA-HUB"),
                "ALPHA-BELT-1",
                "ALPHA-HUB",
            ),
            Some(AttachedDeliveryState::TravelingToDestination)
        );
    }

    #[test]
    fn all_delivered_payloads_skip_delivery_preflight() {
        assert!(!payload_is_pending("DEVICE-1", &["DEVICE-1".to_owned()]));
        assert!(payload_is_pending("DEVICE-2", &["DEVICE-1".to_owned()]));
    }

    #[test]
    fn active_own_home_return_is_an_exact_target() {
        assert!(same_location("BETA-HUB", "beta-hub"));
        assert!(!same_location("BETA-HUB", "ALPHA-HUB"));
    }

    #[test]
    fn hyphenated_legacy_system_requires_authoritative_location() {
        let requested = "ALPHA-SECTOR";
        assert!(!authoritative_exact_location(None, requested));
        assert!(authoritative_exact_location(
            Some("alpha-sector"),
            requested
        ));
    }

    #[test]
    fn legacy_plan_deserializes_with_empty_transport_origins() {
        let plan: DeliveryPlan =
            serde_json::from_str(r#"{"origin":"ALPHA-BELT-1","destination":"ALPHA-HUB"}"#)
                .expect("legacy delivery plan");

        assert!(plan.transport_origins.is_empty());
        assert_eq!(
            return_location_for_transport(&plan, "LEGACY-CARRIER"),
            Some("ALPHA-BELT-1")
        );
    }

    #[test]
    fn transport_origin_rank_marks_other_systems_as_remote() {
        assert_eq!(
            transport_origin_rank("SCEPTURUM-BELT-1", "SCEPTURUM-7-L4"),
            2
        );
        assert_eq!(
            transport_origin_rank("SCEPTURUM-BELT-1", "TWAFFY-BELT-1"),
            REMOTE_TRANSPORT_RANK
        );
    }

    #[test]
    fn transport_rank_prefers_smallest_single_trip_fit() {
        assert!(capacity_rank(400, 350) < capacity_rank(900, 350));
        assert!(capacity_rank(400, 350) < capacity_rank(300, 350));
        assert!(capacity_rank(300, 350) < capacity_rank(100, 350));
    }

    #[test]
    fn travel_poll_interval_scales_with_eta_but_never_below_configured() {
        let configured = Duration::from_secs(5);
        assert_eq!(
            travel_poll_interval(Some(600), configured),
            Duration::from_secs(60)
        );
        assert_eq!(
            travel_poll_interval(Some(90), configured),
            Duration::from_secs(30)
        );
        assert_eq!(
            travel_poll_interval(Some(5), configured),
            Duration::from_secs(10)
        );
        assert_eq!(travel_poll_interval(None, configured), configured);
        // A caller asking for slower polling than the ETA tier is honored.
        let slow = Duration::from_secs(120);
        assert_eq!(travel_poll_interval(Some(600), slow), slow);
    }

    #[test]
    fn manifest_respects_capacity() {
        let resources = [("rares".into(), 400), ("volatiles".into(), 100)]
            .into_iter()
            .collect();
        let manifest = take_manifest(&resources, 450);
        assert_eq!(manifest.values().sum::<i64>(), 450);
    }

    #[test]
    fn tag_selector_selects_every_matching_device_in_origin_scope() {
        let devices = vec![
            payload_device("A", "SCEPTURUM-BELT-1", &["twaffy-obj-1"]),
            payload_device("B", "SCEPTURUM-7-L4", &["twaffy-obj-1"]),
            payload_device("C", "TWAFFY-OBJ-1", &["twaffy-obj-1"]),
            payload_device("D", "SCEPTURUM-BELT-1", &["other"]),
        ];

        let selected =
            select_payload_devices("SCEPTURUM", &[], &[], &["twaffy-obj-1".into()], &devices)
                .expect("tag selection");
        let codes = selected
            .iter()
            .map(|device| device.code.as_str())
            .collect::<Vec<_>>();

        assert_eq!(codes, ["A", "B"]);
    }

    #[test]
    fn exact_device_selector_preserves_selected_physical_device() {
        let devices = vec![
            payload_device("A", "SCEPTURUM-BELT-1", &[]),
            payload_device("B", "SCEPTURUM-BELT-1", &[]),
        ];

        let selected =
            select_payload_devices("SCEPTURUM-BELT-1", &["B".into()], &[], &[], &devices)
                .expect("exact device selection");

        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].code, "B");
    }

    #[test]
    fn repeated_tag_selectors_do_not_duplicate_payload_codes() {
        let devices = vec![payload_device(
            "A",
            "SCEPTURUM-BELT-1",
            &["twaffy-obj-1", "ring-payload"],
        )];

        let selected = select_payload_devices(
            "SCEPTURUM",
            &[],
            &[],
            &["twaffy-obj-1".into(), "ring-payload".into()],
            &devices,
        )
        .expect("tag selection");

        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].code, "A");
    }
}
