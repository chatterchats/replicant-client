//! Managed-client Autofactory discovery and durable queue submission.

use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use replicant_client::{Client, Operation, OperationStatus, domain::Device};
use serde::Serialize;
use serde_json::{Map, Value};
use thiserror::Error;
use tokio::time::sleep;
use tracing::info;

use crate::{
    Blueprint, FactoryWorkload, PrintRequest, PrintTime, QuantityMap, ScheduleError,
    normalize_requests, schedule_prints,
};

const AUTOFACTORY: &str = "autofactory";

/// One live Autofactory and its current queue capacity.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct FactoryState {
    /// Autofactory device code.
    pub code: String,
    /// Maximum number of queued print units.
    pub queue_size: usize,
    /// Number of queued print units currently consuming queue capacity.
    pub queued_units: usize,
    /// Whether one print is actively running.
    pub printing: bool,
    /// Estimated seconds until work newly appended now would finish waiting.
    pub remaining_seconds: f64,
}

impl FactoryState {
    /// Number of print units that can be submitted immediately.
    #[must_use]
    pub fn available_slots(&self) -> usize {
        self.queue_size.saturating_sub(self.queued_units)
    }

    /// Converts this live state into pure scheduler input.
    #[must_use]
    pub fn workload(&self) -> FactoryWorkload {
        FactoryWorkload {
            code: self.code.clone(),
            remaining_seconds: self.remaining_seconds,
        }
    }
}

/// Options for continuously filling live Autofactory queues.
#[derive(Clone, Debug)]
pub struct QueueOptions {
    /// Location containing eligible Autofactories.
    pub hub: String,
    /// Tags applied to every printed device.
    pub tags: Vec<String>,
    /// Delay between queue-capacity checks when more work remains.
    pub poll_interval: Duration,
    /// Maximum time allowed to queue every requested unit.
    pub wait_timeout: Duration,
}

impl QueueOptions {
    /// Creates queue options for one manufacturing hub.
    #[must_use]
    pub fn at(hub: impl Into<String>) -> Self {
        Self {
            hub: hub.into(),
            tags: Vec::new(),
            poll_interval: Duration::from_secs(5),
            wait_timeout: Duration::from_secs(21_600),
        }
    }
}

/// Summary of successfully queued manufacturing work.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct QueueReport {
    /// Canonical combined request quantities.
    pub requested: QuantityMap,
    /// Quantities accepted by Autofactories during this call.
    pub queued: QuantityMap,
    /// Accepted quantities keyed by Autofactory device code.
    pub by_factory: BTreeMap<String, i64>,
    /// Durable operation identifiers for each accepted unit submission.
    pub operation_ids: Vec<String>,
}

/// Live discovery or durable queueing failure.
#[derive(Debug, Error)]
pub enum PrintingError {
    /// Pure schedule validation failed.
    #[error(transparent)]
    Schedule(#[from] ScheduleError),
    /// The managed Replicant client failed.
    #[error(transparent)]
    Client(#[from] replicant_client::Error),
    /// No owned Autofactory was found at the requested hub.
    #[error("no account-owned Autofactory is available at `{0}`")]
    NoFactoryAtHub(String),
    /// A queue payload contained an invalid quantity.
    #[error("Autofactory {factory_code} reported invalid queued quantity {quantity}")]
    InvalidQueuedQuantity {
        /// Autofactory reporting the invalid quantity.
        factory_code: String,
        /// Invalid queued quantity.
        quantity: i64,
    },
    /// The server definitively rejected a print submission.
    #[error("print operation {operation_id} ended as {status:?}: {response:?}")]
    SubmissionRejected {
        /// Durable managed operation identifier.
        operation_id: String,
        /// Terminal rejection status.
        status: OperationStatus,
        /// Sanitized server response or error.
        response: Option<Value>,
    },
    /// A print submission may have reached the server but is not classified.
    #[error("print operation {operation_id} has unresolved status {status:?}; it was not retried")]
    SubmissionUnresolved {
        /// Durable managed operation identifier.
        operation_id: String,
        /// Current unresolved operation status.
        status: OperationStatus,
    },
    /// Queue capacity did not become available before the configured timeout.
    #[error("timed out waiting for Autofactory queue capacity; remaining: {remaining:?}")]
    TimedOut {
        /// Quantities not yet queued.
        remaining: QuantityMap,
    },
}

/// Fetches unlocked blueprints and their print durations.
pub async fn fetch_blueprints(client: &Client) -> Result<BTreeMap<String, Blueprint>, PrintingError> {
    Ok(client
        .raw()
        .blueprints()
        .list()
        .await?
        .value
        .blueprints
        .into_iter()
        .filter_map(|blueprint| {
            let device_type = blueprint.device_type?;
            Some((
                device_type.clone(),
                Blueprint {
                    device_type,
                    print_time_seconds: blueprint.print_time.unwrap_or(0.0),
                },
            ))
        })
        .collect())
}

/// Discovers account-owned Autofactories at `hub` and reads their live queues.
pub async fn discover_factories<B: PrintTime>(
    client: &Client,
    hub: &str,
    blueprints: &BTreeMap<String, B>,
) -> Result<Vec<FactoryState>, PrintingError> {
    let handles = client
        .devices()
        .refresh_many()
        .page_size(50)
        .collect()
        .await?;
    let mut factory_codes = Vec::new();
    for handle in handles {
        let snapshot = handle.snapshot().await?;
        if device_type(&snapshot) == Some(AUTOFACTORY)
            && device_location(&snapshot) == Some(hub)
        {
            factory_codes.push(handle.id().as_str().to_owned());
        }
    }
    factory_codes.sort();

    let mut factories = Vec::with_capacity(factory_codes.len());
    for code in factory_codes {
        factories.push(inspect_factory(client, &code, blueprints).await?);
    }
    Ok(factories)
}

/// Reads one Autofactory's queue capacity and projected workload.
pub async fn inspect_factory<B: PrintTime>(
    client: &Client,
    factory_code: &str,
    blueprints: &BTreeMap<String, B>,
) -> Result<FactoryState, PrintingError> {
    let detail = client.raw().devices().get(factory_code).await?.value;
    let queue_size = usize::try_from(detail.queue_size.unwrap_or(1).max(1)).map_err(|_| {
        PrintingError::InvalidQueuedQuantity {
            factory_code: factory_code.to_owned(),
            quantity: detail.queue_size.unwrap_or(1),
        }
    })?;
    let queued_units = queued_print_units(factory_code, &detail.print_queue)?;
    let active_seconds = detail
        .printing
        .as_ref()
        .and_then(|printing| printing.eta_seconds)
        .unwrap_or(0.0)
        .max(0.0);
    let queued_seconds = detail
        .print_queue
        .iter()
        .map(|job| {
            let quantity = integer_field(job, &["quantity", "count"])
                .unwrap_or(1)
                .max(1);
            string_field(job, &["device_type", "type"])
                .and_then(|device_type| blueprints.get(device_type))
                .map_or(0.0, |blueprint| {
                    blueprint.print_time_seconds().max(0.0) * quantity as f64
                })
        })
        .sum::<f64>();
    Ok(FactoryState {
        code: factory_code.to_owned(),
        queue_size,
        queued_units,
        printing: detail.printing.is_some(),
        remaining_seconds: active_seconds + queued_seconds,
    })
}

/// Returns the live number of available queue slots on one Autofactory.
pub async fn factory_queue_slots(
    client: &Client,
    factory_code: &str,
) -> Result<usize, PrintingError> {
    let detail = client.raw().devices().get(factory_code).await?.value;
    let queue_size = usize::try_from(detail.queue_size.unwrap_or(1).max(1)).map_err(|_| {
        PrintingError::InvalidQueuedQuantity {
            factory_code: factory_code.to_owned(),
            quantity: detail.queue_size.unwrap_or(1),
        }
    })?;
    Ok(queue_size.saturating_sub(queued_print_units(
        factory_code,
        &detail.print_queue,
    )?))
}

/// Submits one durable print operation and verifies its immediate outcome.
///
/// This waits for the HTTP submission classification, not for the physical
/// print to finish. Ambiguous operations are returned as errors and are never
/// automatically retried.
pub async fn enqueue_print(
    client: &Client,
    factory_code: &str,
    device_type: &str,
    quantity: i64,
    tags: &[String],
) -> Result<Operation, PrintingError> {
    let handle = client.devices().get(factory_code).await?;
    let operation = if tags.is_empty() {
        handle.enqueue_print(device_type, quantity).await?
    } else {
        handle
            .enqueue_print_with_tags(device_type, quantity, tags.iter().cloned())
            .await?
    };
    ensure_submission_accepted(&operation).await?;
    Ok(operation)
}

/// Verifies the immediate server classification of a durable operation.
pub async fn ensure_submission_accepted(operation: &Operation) -> Result<(), PrintingError> {
    let outcome = operation.outcome().await?;
    let operation_id = operation.id().as_str().to_owned();
    if matches!(
        outcome.status,
        OperationStatus::Cancelled | OperationStatus::Rejected | OperationStatus::Failed
    ) {
        return Err(PrintingError::SubmissionRejected {
            operation_id,
            status: outcome.status,
            response: outcome.response,
        });
    }
    if matches!(
        outcome.status,
        OperationStatus::Prepared | OperationStatus::Submitted | OperationStatus::Ambiguous
    ) {
        return Err(PrintingError::SubmissionUnresolved {
            operation_id,
            status: outcome.status,
        });
    }
    Ok(())
}

/// Continuously fills all eligible Autofactory queues until every request has
/// been accepted.
///
/// Submissions use quantity one so the server's queue-unit limit is never
/// exceeded by a grouped request. The function returns after queueing all work;
/// it does not wait for the physical devices to finish printing.
pub async fn queue_prints(
    client: &Client,
    requests: &[PrintRequest],
    options: &QueueOptions,
) -> Result<QueueReport, PrintingError> {
    let requested = normalize_requests(requests)?;
    let mut remaining = requested.clone();
    let mut report = QueueReport {
        requested,
        ..QueueReport::default()
    };
    if remaining.is_empty() {
        return Ok(report);
    }

    let blueprints = fetch_blueprints(client).await?;
    for device_type in remaining.keys() {
        if !blueprints.contains_key(device_type) {
            return Err(ScheduleError::MissingBlueprint(device_type.clone()).into());
        }
    }

    let deadline = Instant::now() + options.wait_timeout;
    loop {
        remaining.retain(|_, quantity| *quantity > 0);
        if remaining.is_empty() {
            return Ok(report);
        }

        let factories = discover_factories(client, &options.hub, &blueprints).await?;
        if factories.is_empty() {
            return Err(PrintingError::NoFactoryAtHub(options.hub.clone()));
        }
        let workloads = factories
            .iter()
            .map(FactoryState::workload)
            .collect::<Vec<_>>();
        let schedule = schedule_prints(&remaining, &blueprints, &workloads)?;
        let mut slots = factories
            .iter()
            .map(|factory| (factory.code.clone(), factory.available_slots()))
            .collect::<BTreeMap<_, _>>();
        let mut submitted = 0usize;

        for batch in schedule.batches {
            let available = slots.get(&batch.factory_code).copied().unwrap_or(0);
            let quantity = usize::try_from(batch.quantity).map_err(|_| {
                ScheduleError::InvalidQuantity {
                    device_type: batch.device_type.clone(),
                    quantity: batch.quantity,
                }
            })?;
            let to_submit = available.min(quantity);
            for _ in 0..to_submit {
                let operation = enqueue_print(
                    client,
                    &batch.factory_code,
                    &batch.device_type,
                    1,
                    &options.tags,
                )
                .await?;
                *remaining.entry(batch.device_type.clone()).or_default() -= 1;
                *report.queued.entry(batch.device_type.clone()).or_default() += 1;
                *report
                    .by_factory
                    .entry(batch.factory_code.clone())
                    .or_default() += 1;
                report
                    .operation_ids
                    .push(operation.id().as_str().to_owned());
                submitted += 1;
                info!(
                    factory = %batch.factory_code,
                    device_type = %batch.device_type,
                    "queued distributed print unit"
                );
            }
            slots.insert(batch.factory_code, available.saturating_sub(to_submit));
        }

        remaining.retain(|_, quantity| *quantity > 0);
        if remaining.is_empty() {
            return Ok(report);
        }
        if Instant::now() >= deadline {
            return Err(PrintingError::TimedOut { remaining });
        }
        if submitted == 0 {
            info!("waiting for Autofactory queue capacity");
        }
        sleep(options.poll_interval).await;
    }
}

fn queued_print_units(
    factory_code: &str,
    jobs: &[Map<String, Value>],
) -> Result<usize, PrintingError> {
    let mut queued_units = 0usize;
    for job in jobs {
        let quantity = integer_field(job, &["quantity", "count"])
            .unwrap_or(1)
            .max(1);
        queued_units = queued_units.saturating_add(usize::try_from(quantity).map_err(|_| {
            PrintingError::InvalidQueuedQuantity {
                factory_code: factory_code.to_owned(),
                quantity,
            }
        })?);
    }
    Ok(queued_units)
}

fn string_field<'a>(object: &'a Map<String, Value>, names: &[&str]) -> Option<&'a str> {
    names
        .iter()
        .find_map(|name| object.get(*name).and_then(Value::as_str))
}

fn integer_field(object: &Map<String, Value>, names: &[&str]) -> Option<i64> {
    names.iter().find_map(|name| {
        object.get(*name).and_then(|value| {
            value
                .as_i64()
                .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
                .or_else(|| value.as_f64().map(|value| value.ceil() as i64))
        })
    })
}

fn device_type(device: &Device) -> Option<&str> {
    device.device_type.as_ref().map(|value| value.as_str())
}

fn device_location(device: &Device) -> Option<&str> {
    device
        .location
        .as_ref()
        .map(|location| location.id.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_occupancy_counts_device_quantities() {
        let jobs = vec![
            [("quantity".into(), Value::from(2))]
                .into_iter()
                .collect(),
            [("count".into(), Value::from(3))]
                .into_iter()
                .collect(),
            Map::new(),
        ];
        assert_eq!(queued_print_units("AF1", &jobs).unwrap(), 6);
    }

    #[test]
    fn available_slots_saturate_at_zero() {
        let factory = FactoryState {
            code: "AF1".into(),
            queue_size: 3,
            queued_units: 5,
            printing: true,
            remaining_seconds: 100.0,
        };
        assert_eq!(factory.available_slots(), 0);
    }
}
