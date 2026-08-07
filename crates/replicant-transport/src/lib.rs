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
    time::Duration,
};

use replicant_client::{Client, Device, Operation, OperationStatus, raw};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;
use tokio::time::{Instant, sleep};
use tracing::info;

/// Resource quantities keyed by Replicant Space resource type.
pub type ResourceMap = BTreeMap<String, i64>;

const RESERVED_TAG_PREFIXES: &[&str] = &[
    "evt-m:", "evt-r:", "boot-m:", "boot-r:", "region:", "mine-m:", "mine-b:", "mine-r:",
    "mine-s:", "relay-m:", "relay-b:", "relay-s:", "infra-r:", "infra-s:",
];

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
    /// Move every eligible device in the origin scope carrying any of these tags.
    #[serde(default)]
    pub device_tags: Vec<String>,
    /// Optional transport type/count restriction.
    #[serde(default)]
    pub carrier: Option<CarrierPreference>,
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
    /// Return selected transport devices after delivery.
    pub return_transports: bool,
}

impl Default for DeliveryOptions {
    fn default() -> Self {
        Self {
            wait_timeout: Duration::from_secs(21_600),
            poll_interval: Duration::from_secs(5),
            unfurl_modular_payload: true,
            return_transports: false,
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
    origin_rank: u8,
    cargo_capacity: i64,
    attach_capacity: i64,
}

/// Resolves a high-level request into concrete payload and transport devices.
pub async fn plan_delivery(client: &Client, request: &DeliveryRequest) -> Result<DeliveryPlan> {
    validate_request(request)?;

    let inventories = fetch_inventories(client).await?;
    let resource_pickups = allocate_resources(&request.origin, &request.resources, &inventories)?;

    let blueprints = fetch_transport_capacities(client).await?;
    let handles = client.devices().refresh_many().collect().await?;
    let mut devices = Vec::with_capacity(handles.len());
    for handle in handles {
        devices.push(handle.snapshot().await?);
    }

    let payload_devices = select_payload_devices(
        &request.origin,
        &request.devices,
        &request.device_tags,
        &devices,
    )?;
    let payload_codes = payload_devices
        .iter()
        .map(|payload| payload.code.clone())
        .collect::<BTreeSet<_>>();
    let candidates = transport_candidates(&request.origin, &devices, &blueprints, &payload_codes);

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
        )
        .await?
    };

    Ok(DeliveryPlan {
        origin: request.origin.trim().to_ascii_uppercase(),
        destination: request.destination.trim().to_ascii_uppercase(),
        resource_pickups,
        payload_devices,
        cargo_transports,
        device_carriers,
    })
}

/// Executes a concrete plan and optionally returns the transport devices.
pub async fn execute_delivery(
    client: &Client,
    plan: &DeliveryPlan,
    options: DeliveryOptions,
) -> Result<DeliveryReport> {
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
        let return_location = exact_return_location(plan).ok_or_else(|| {
            TransportError::Invalid(
                "--return-carriers requires an exact origin location, not only a system".into(),
            )
        })?;
        let mut transports = plan.cargo_transports.clone();
        transports.extend(plan.device_carriers.iter().cloned());
        transports.sort();
        transports.dedup();
        for code in &transports {
            ensure_device_at(client, code, return_location, options).await?;
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
    if request.resources.is_empty() && request.devices.is_empty() && request.device_tags.is_empty()
    {
        return Err(TransportError::Invalid(
            "delivery requires at least one resource, device type, or device tag payload".into(),
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
            scope_matches(origin, location).then_some((location.to_owned(), &inventory.items))
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
    requests: &[DeviceRequest],
    tags: &[String],
    devices: &[Device],
) -> Result<Vec<PayloadDevice>> {
    let mut selected = Vec::new();
    let mut used = BTreeSet::new();

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
                return Err(TransportError::Invalid(format!(
                    "device {code} tagged {tag} is in the origin scope but is not a free inactive payload"
                )));
            }
            if workflow_reserved(&device.tags) {
                return Err(TransportError::Invalid(format!(
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
    selected.sort_by(|left, right| {
        left.origin
            .cmp(&right.origin)
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
) -> Vec<TransportCandidate> {
    let mut candidates = devices
        .iter()
        .filter(|device| {
            device
                .location
                .as_ref()
                .is_some_and(|location| transport_scope_matches(origin, location.id.as_str()))
                && device.travel.is_none()
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
            Some(TransportCandidate {
                code: device.key.id.as_str().to_owned(),
                device_type,
                origin_rank: origin_location_rank(origin, &location),
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
        capacity_rank(kind.capacity(left), demand)
            .cmp(&capacity_rank(kind.capacity(right), demand))
            .then_with(|| left.origin_rank.cmp(&right.origin_rank))
            .then_with(|| left.code.cmp(&right.code))
    });

    let mut selected = Vec::new();
    for candidate in eligible {
        if kind == CapacityKind::Cargo {
            let detail = client.raw().devices().get(&candidate.code).await?.value;
            if detail.controller_device_code.is_some() || !cargo_map(&detail).is_empty() {
                continue;
            }
        }
        selected.push(candidate.code.clone());
        if selected.len() == quantity {
            break;
        }
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
        let mut remaining = pickup.resources.clone();
        while !remaining.is_empty() {
            let before = sum_resources(&remaining);
            for code in transports {
                settle_transport_between(client, code, &pickup.location, destination, options)
                    .await?;
                ensure_device_at(client, code, &pickup.location, options).await?;
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
                let manifest = take_manifest(&remaining, capacity);
                if manifest.is_empty() {
                    continue;
                }
                info!(
                    transport = %code,
                    origin = %pickup.location,
                    destination = %destination,
                    manifest = %format_resources(&manifest),
                    "delivering resource manifest"
                );
                collect_resources(client, code, &manifest, options).await?;
                ensure_device_at(client, code, destination, options).await?;
                deposit_resources(client, code, Some(&manifest), options).await?;
                subtract_resources(&mut remaining, &manifest);
                merge_resources(&mut delivered, &manifest);
                if remaining.is_empty() {
                    break;
                }
                ensure_device_at(client, code, &pickup.location, options).await?;
            }
            if sum_resources(&remaining) >= before {
                return Err(TransportError::Invalid(format!(
                    "no cargo transport made progress on remaining manifest {}",
                    format_resources(&remaining)
                )));
            }
        }
    }
    Ok(delivered)
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
        // Resume-safe cleanup comes first. A previous run may have reached the
        // destination and detached the payload, or arrived with it still
        // attached to one of the selected carriers.
        for payload in &remaining_payloads {
            if device_already_delivered(client, &payload.code, destination).await? {
                if options.unfurl_modular_payload {
                    unfurl_modular_devices(client, std::slice::from_ref(&payload.code), options)
                        .await?;
                }
                delivered.push(payload.code.clone());
            }
        }
        for carrier in carriers {
            settle_transport_between(client, carrier, &origin, destination, options).await?;
            let detail = client.raw().devices().get(carrier).await?.value;
            let attached = detail
                .attached_devices
                .iter()
                .filter_map(reference_code)
                .collect::<Vec<_>>();
            let unexpected = attached
                .iter()
                .filter(|code| !payload_codes.contains(code.as_str()))
                .cloned()
                .collect::<Vec<_>>();
            if !unexpected.is_empty() {
                return Err(TransportError::Invalid(format!(
                    "carrier {carrier} contains non-delivery attachments: {}",
                    unexpected.join(", ")
                )));
            }
            if !attached.is_empty() && detail.location.as_deref() == Some(destination) {
                detach_devices(client, carrier, &attached, options).await?;
                if options.unfurl_modular_payload {
                    unfurl_modular_devices(client, &attached, options).await?;
                }
                delivered.extend(attached);
            }
        }
        delivered.sort();
        delivered.dedup();

        let mut remaining = remaining_payloads
            .into_iter()
            .filter(|payload| !delivered.contains(&payload.code))
            .collect::<Vec<_>>();
        while !remaining.is_empty() {
            let before = remaining.len();
            for carrier in carriers {
                settle_transport_between(client, carrier, &origin, destination, options).await?;
                ensure_device_at(client, carrier, &origin, options).await?;
                let detail = client.raw().devices().get(carrier).await?.value;
                let existing = detail
                    .attached_devices
                    .iter()
                    .filter_map(reference_code)
                    .collect::<Vec<_>>();
                if !existing.is_empty() {
                    let unexpected = existing
                        .iter()
                        .filter(|code| !payload_codes.contains(code.as_str()))
                        .cloned()
                        .collect::<Vec<_>>();
                    if !unexpected.is_empty() {
                        return Err(TransportError::Invalid(format!(
                            "carrier {carrier} contains non-delivery attachments: {}",
                            unexpected.join(", ")
                        )));
                    }
                    // An interrupted run may have attached the planned payload
                    // at the origin before travelling. Detach it so the trip can
                    // be rebuilt from the still-needed set below.
                    detach_devices(client, carrier, &existing, options).await?;
                }

                let capacity = detail.attach_capacity.unwrap_or(0).max(0);
                if capacity == 0 {
                    continue;
                }
                let trip_count = usize::try_from(capacity)
                    .unwrap_or(usize::MAX)
                    .min(remaining.len());
                let selected = remaining
                    .iter()
                    .take(trip_count)
                    .map(|payload| payload.code.clone())
                    .collect::<Vec<_>>();
                if selected.is_empty() {
                    continue;
                }
                for code in &selected {
                    ensure_attachable_device(client, code, options).await?;
                }
                info!(
                    carrier = %carrier,
                    origin = %origin,
                    destination = %destination,
                    payload = %selected.join(","),
                    "delivering device payload"
                );
                attach_devices(client, carrier, &selected, options).await?;
                ensure_device_at(client, carrier, destination, options).await?;
                detach_devices(client, carrier, &selected, options).await?;
                if options.unfurl_modular_payload {
                    unfurl_modular_devices(client, &selected, options).await?;
                }
                delivered.extend(selected.iter().cloned());
                delivered.sort();
                delivered.dedup();
                remaining.retain(|payload| !selected.contains(&payload.code));
                if remaining.is_empty() {
                    break;
                }
                ensure_device_at(client, carrier, &origin, options).await?;
            }
            if remaining.len() >= before {
                return Err(TransportError::Invalid(format!(
                    "no carrier made progress on {} remaining payload device(s) from {origin}",
                    remaining.len()
                )));
            }
        }
    }
    delivered.sort();
    delivered.dedup();
    Ok(delivered)
}
async fn device_already_delivered(client: &Client, code: &str, destination: &str) -> Result<bool> {
    let detail = client.raw().devices().get(code).await?.value;
    Ok(detail.travel.is_none()
        && detail.location.as_deref() == Some(destination)
        && detail.attached_to_device_code.is_none()
        && detail.stowed_in_device_code.is_none())
}

async fn settle_transport_between(
    client: &Client,
    code: &str,
    origin: &str,
    destination: &str,
    options: DeliveryOptions,
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
    if planned != origin && planned != destination {
        return Err(TransportError::Invalid(format!(
            "transport {code} is already travelling to {planned}, outside this delivery"
        )));
    }
    ensure_device_at(client, code, &planned, options).await
}

async fn ensure_device_at(
    client: &Client,
    code: &str,
    destination: &str,
    options: DeliveryOptions,
) -> Result<()> {
    let handle = client.devices().get(code).await?;
    let snapshot = handle.snapshot().await?;
    if snapshot.travel.is_none()
        && snapshot
            .location
            .as_ref()
            .is_some_and(|location| location.id.as_str() == destination)
    {
        return Ok(());
    }
    if let Some(travel) = &snapshot.travel {
        let planned = travel
            .final_destination
            .as_ref()
            .or(travel.destination.as_ref())
            .map(|location| location.id.as_str());
        if planned != Some(destination) {
            return Err(TransportError::Invalid(format!(
                "device {code} is already travelling to {:?}, not {destination}",
                planned
            )));
        }
    } else {
        info!(device = %code, destination = %destination, "dispatching transport travel");
        let operation = handle
            .command(raw::devices::DeviceCommand::Travel {
                destination: destination.to_owned(),
                dry_run: None,
                via: None,
            })
            .await?;
        ensure_operation_accepted(&operation).await?;
    }

    wait_for_device(client, code, options, |device| {
        device.travel.is_none()
            && device
                .location
                .as_ref()
                .is_some_and(|location| location.id.as_str() == destination)
    })
    .await
}

async fn wait_for_device(
    client: &Client,
    code: &str,
    options: DeliveryOptions,
    predicate: impl Fn(&Device) -> bool,
) -> Result<()> {
    let deadline = Instant::now() + options.wait_timeout;
    loop {
        let snapshot = client.devices().get(code).await?.snapshot().await?;
        if predicate(&snapshot) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(TransportError::TimedOut(format!(
                "timed out waiting for device {code} state"
            )));
        }
        sleep(options.poll_interval).await;
    }
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
        wait_for_raw_device(client, code, options, |device| {
            device.stowed_in_device_code.is_none()
        })
        .await?;
        detail = client.raw().devices().get(code).await?.value;
    }

    if !is_modular_device(&detail) || status_is(&detail, "compacted") {
        return Ok(());
    }
    if status_is(&detail, "compacting") {
        return wait_for_raw_device(client, code, options, |device| {
            status_is(device, "compacted")
        })
        .await;
    }
    if status_is(&detail, "unfurling") {
        wait_for_raw_device(client, code, options, |device| {
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
    let operation = client.devices().get(code).await?.compact().await?;
    ensure_operation_accepted(&operation).await?;
    wait_for_raw_device(client, code, options, |device| {
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
    let operation = client
        .devices()
        .get(carrier)
        .await?
        .attach(raw::devices::TargetsCommand {
            device: None,
            devices: Some(Value::Array(
                devices.iter().cloned().map(Value::String).collect(),
            )),
            target: None,
            targets: None,
        })
        .await?;
    ensure_operation_accepted(&operation).await?;
    for code in devices {
        wait_for_device(client, code, options, |device| {
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
    let operation = client
        .devices()
        .get(carrier)
        .await?
        .command(raw::devices::DeviceCommand::Detach(
            raw::devices::TargetsCommand {
                device: None,
                devices: Some(Value::Array(
                    devices.iter().cloned().map(Value::String).collect(),
                )),
                target: None,
                targets: None,
            },
        ))
        .await?;
    ensure_operation_accepted(&operation).await?;
    for code in devices {
        wait_for_device(client, code, options, |device| {
            device.relationships.attached_to.is_none()
        })
        .await?;
    }
    Ok(())
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
            wait_for_raw_device(client, code, options, |device| {
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
        let operation = client.devices().get(code).await?.unfurl().await?;
        ensure_operation_accepted(&operation).await?;
        wait_for_raw_device(client, code, options, |device| {
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
    resources: &ResourceMap,
    options: DeliveryOptions,
) -> Result<()> {
    if resources.is_empty() {
        return Ok(());
    }
    let before = cargo_map(&client.raw().devices().get(code).await?.value);
    let operation = client
        .devices()
        .get(code)
        .await?
        .command(raw::devices::DeviceCommand::CollectResources {
            resources: resource_json(resources),
        })
        .await?;
    ensure_operation_accepted(&operation).await?;
    wait_for_raw_device(client, code, options, |device| {
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
    wait_for_raw_device(client, code, options, |device| {
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
    options: DeliveryOptions,
    predicate: impl Fn(&raw::devices::DeviceStatus) -> bool,
) -> Result<()> {
    let deadline = Instant::now() + options.wait_timeout;
    loop {
        let detail = client.raw().devices().get(code).await?.value;
        if predicate(&detail) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(TransportError::TimedOut(format!(
                "timed out waiting for device {code} cargo/state"
            )));
        }
        sleep(options.poll_interval).await;
    }
}

async fn ensure_operation_accepted(operation: &Operation) -> Result<()> {
    let outcome = operation.wait_timeout(Duration::from_secs(30)).await?;
    if matches!(
        outcome.status,
        OperationStatus::Cancelled | OperationStatus::Rejected | OperationStatus::Failed
    ) {
        return Err(TransportError::Operation(format!(
            "operation {} ended as {:?}: {:?}",
            operation.id().as_str(),
            outcome.status,
            outcome.response
        )));
    }
    Ok(())
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

fn resource_json(resources: &ResourceMap) -> raw::JsonObject {
    resources
        .iter()
        .map(|(resource, quantity)| (resource.clone(), Value::from(*quantity)))
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
            "inactive" | "deactivated" | "idle" | "stowed" | "recalled" | "compacted"
        )
    }) && device.travel.is_none()
}

fn eligible_payload(device: &Device) -> bool {
    inactive_payload(device)
        && device.relationships.attached_to.is_none()
        && device.relationships.stowed_in.is_none()
        && device.relationships.controller.is_none()
}

fn workflow_reserved(tags: &[String]) -> bool {
    tags.iter().any(|tag| {
        RESERVED_TAG_PREFIXES
            .iter()
            .any(|prefix| tag.starts_with(prefix))
    })
}

fn scope_matches(origin: &str, location: &str) -> bool {
    let origin = origin.trim();
    if origin.contains('-') {
        location.eq_ignore_ascii_case(origin)
    } else {
        system_designation(location).eq_ignore_ascii_case(origin)
    }
}

fn transport_scope_matches(origin: &str, location: &str) -> bool {
    system_designation(location).eq_ignore_ascii_case(system_designation(origin.trim()))
}

fn origin_location_rank(origin: &str, location: &str) -> u8 {
    if location.eq_ignore_ascii_case(origin) {
        0
    } else if location.to_ascii_uppercase().contains("-BELT-") {
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

fn exact_return_location(plan: &DeliveryPlan) -> Option<&str> {
    plan.origin.contains('-').then_some(plan.origin.as_str())
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

fn sum_resources(resources: &ResourceMap) -> i64 {
    resources.values().copied().sum()
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
            features: Vec::new(),
            available_commands: Vec::new(),
            available_directives: Vec::new(),
            tags: tags.iter().map(|tag| (*tag).to_owned()).collect(),
            relationships: replicant_client::DeviceRelationships::default(),
            attach_capacity: None,
            stow_capacity: None,
            stow_used: None,
            operational_capacity: None,
            active_directive: None,
            travel: None,
            access: replicant_client::domain::AccessScope::Owned,
        }
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
    fn transport_rank_prefers_smallest_single_trip_fit() {
        assert!(capacity_rank(400, 350) < capacity_rank(900, 350));
        assert!(capacity_rank(400, 350) < capacity_rank(300, 350));
        assert!(capacity_rank(300, 350) < capacity_rank(100, 350));
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

        let selected = select_payload_devices("SCEPTURUM", &[], &["twaffy-obj-1".into()], &devices)
            .expect("tag selection");
        let codes = selected
            .iter()
            .map(|device| device.code.as_str())
            .collect::<Vec<_>>();

        assert_eq!(codes, ["A", "B"]);
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
            &["twaffy-obj-1".into(), "ring-payload".into()],
            &devices,
        )
        .expect("tag selection");

        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].code, "A");
    }
}
