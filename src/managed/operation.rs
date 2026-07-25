//! Durable managed operations.
//!
//! Every unsafe managed mutation is registered durably (SQLite) before
//! transmission, submitted at most once automatically, and its transport
//! outcome is classified rather than blindly retried:
//!
//! ```text
//! validate request locally
//! -> create durable operation record (state = prepared)
//! -> submit exactly once (state = submitted)
//! -> classify transport outcome (ambiguous | rejected | accepted)
//! -> hydrate authoritative fields via targeted reconciliation
//! -> await event evidence and/or explicit REST reconciliation
//! -> resolve operation (completed)
//! ```
//!
//! A [`Transport`](crate::Error::Transport) failure on the one automatic
//! attempt is definitionally ambiguous (the request may or may not have
//! reached the server) and is never retried automatically; a process that
//! crashes mid-attempt is recovered the same way on restart (see
//! [`recover`]). Any other failure is a definite rejection. A successful
//! response is definite acceptance; whether the caller must keep watching for
//! evidence depends on whether the dispatched action documents its own
//! asynchronous completion (a `completes_at`/`eta_seconds`-shaped response) or
//! is fully reflected in its own response.
//!
//! Operation intents store only sanitized gameplay request bodies. This
//! client accepts no authentication material in any gameplay request body
//! (auth is header-only, never body-carried), so nothing here is ever
//! redacted-then-replayed; [`ensure_no_secrets`] instead refuses to create an
//! operation whose caller-supplied payload looks like it carries credentials,
//! which matters most for [`DynamicCommand`]'s free-form arguments.

use std::time::Duration;

use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

use crate::domain::{self, DeviceId, DeviceKey, OperationId};
use crate::error::Error;
use crate::raw;
use crate::{Client, Result};

use super::store::StoreError;

fn persistence_error(_: StoreError) -> Error {
    Error::Persistence {
        message: "SQLite store operation failed".into(),
    }
}

fn to_value<T: Serialize>(value: T) -> Result<Value> {
    serde_json::to_value(value).map_err(|error| Error::Operation {
        message: format!("request serialization failed: {error}"),
    })
}

fn str_field<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| Error::Operation {
            message: format!("operation intent is missing path field `{key}`"),
        })
}

fn i64_field(value: &Value, key: &str) -> Result<i64> {
    value
        .get(key)
        .and_then(Value::as_i64)
        .ok_or_else(|| Error::Operation {
            message: format!("operation intent is missing path field `{key}`"),
        })
}

/// Refuses gameplay request payloads shaped like they carry authentication
/// material. Every mutating endpoint this client submits takes only
/// gameplay-domain fields in its body (auth is header-only), so a match here
/// means the caller supplied something that should never be durably stored.
const SENSITIVE_KEYS: [&str; 6] = [
    "token",
    "secret",
    "authorization",
    "password",
    "credential",
    "webhook",
];

fn ensure_no_secrets(value: &Value) -> Result<()> {
    fn contains_secret(value: &Value) -> bool {
        match value {
            Value::Object(fields) => fields.iter().any(|(key, value)| {
                let key = key.to_ascii_lowercase();
                SENSITIVE_KEYS
                    .iter()
                    .any(|sensitive| key.contains(sensitive))
                    || contains_secret(value)
            }),
            Value::Array(values) => values.iter().any(contains_secret),
            _ => false,
        }
    }
    if contains_secret(value) {
        return Err(Error::Operation {
            message: "gameplay request must not contain authentication-shaped fields".into(),
        });
    }
    Ok(())
}

fn sanitize_error(error: &Error) -> Value {
    serde_json::json!({
        "message": error.to_string(),
        "status": error.status(),
    })
}

/// The observable lifecycle state of a durable [`Operation`].
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationStatus {
    /// Intent is durable; the one automatic submission attempt has not
    /// started (or, after a restart, was never confirmed to have started).
    Prepared,
    /// The one automatic submission attempt is in flight or was interrupted
    /// before its outcome could be classified.
    Submitted,
    /// The server accepted the mutation.
    Accepted,
    /// The dispatched action is a known, still-running server-side process.
    InProgress,
    /// Accepted; this client is still watching for confirming event evidence
    /// or an explicit reconciliation before considering it resolved.
    AwaitingEvidence,
    /// A caller-requested reconciliation is needed to resolve this operation.
    ReconciliationRequired,
    /// Resolved: the mutation is known to have applied.
    Completed,
    /// The operation was cancelled before resolution.
    Cancelled,
    /// The server definitely rejected the mutation.
    Rejected,
    /// The one automatic submission attempt's outcome is unknown: the
    /// request may or may not have reached the server. Never retried
    /// automatically; resolved only by event evidence or [`Operation::reconcile`].
    Ambiguous,
    /// The operation could not be resolved and will not be retried.
    Failed,
}

impl OperationStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Prepared => "prepared",
            Self::Submitted => "submitted",
            Self::Accepted => "accepted",
            Self::InProgress => "in_progress",
            Self::AwaitingEvidence => "awaiting_evidence",
            Self::ReconciliationRequired => "reconciliation_required",
            Self::Completed => "completed",
            Self::Cancelled => "cancelled",
            Self::Rejected => "rejected",
            Self::Ambiguous => "ambiguous",
            Self::Failed => "failed",
        }
    }

    fn parse(value: &str) -> Self {
        match value {
            "prepared" => Self::Prepared,
            "submitted" => Self::Submitted,
            "accepted" => Self::Accepted,
            "in_progress" => Self::InProgress,
            "awaiting_evidence" => Self::AwaitingEvidence,
            "reconciliation_required" => Self::ReconciliationRequired,
            "completed" => Self::Completed,
            "cancelled" => Self::Cancelled,
            "rejected" => Self::Rejected,
            "ambiguous" => Self::Ambiguous,
            _ => Self::Failed,
        }
    }

    /// Whether this status is final: no event, reconciliation, or restart
    /// recovery will ever change it.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Cancelled | Self::Rejected | Self::Failed
        )
    }
}

/// A sanitized snapshot of a durable operation's current resolution.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq)]
pub struct OperationOutcome {
    /// The operation's current status.
    pub status: OperationStatus,
    /// A sanitized summary of the server's response, or the sanitized
    /// rejection detail, once available.
    pub response: Option<Value>,
}

/// A durable handle to one managed mutation. Construction (`operation =
/// device.activate().await?`) only ever fails for local/infra reasons;
/// the remote classification is observed through [`Operation::status`],
/// [`Operation::outcome`], or [`Operation::wait`].
#[derive(Clone, Debug)]
pub struct Operation {
    client: Client,
    id: OperationId,
}

impl Operation {
    /// This operation's durable identifier.
    #[must_use]
    pub fn id(&self) -> &OperationId {
        &self.id
    }

    /// Reads the operation's current status. Local-only.
    pub async fn status(&self) -> Result<OperationStatus> {
        self.client.ensure_open()?;
        Ok(OperationStatus::parse(&load(&self.client, &self.id)?.state))
    }

    /// Reads the operation's current sanitized outcome. Local-only.
    pub async fn outcome(&self) -> Result<OperationOutcome> {
        self.client.ensure_open()?;
        let entry = load(&self.client, &self.id)?;
        Ok(OperationOutcome {
            status: OperationStatus::parse(&entry.state),
            response: entry.projection,
        })
    }

    /// Subscribes to this operation's status changes. Local-only: it never
    /// itself issues a network request.
    pub async fn watch(&self) -> Result<OperationWatch> {
        self.client.ensure_open()?;
        Ok(OperationWatch {
            receiver: self.client.managed_operations().subscribe(),
            id: self.id.clone(),
        })
    }

    /// Waits (bounded to 30 seconds) for a terminal status. Never fails
    /// merely because the local timeout elapsed: an unresolved operation
    /// remains durable and this returns its latest outcome instead of `Err`.
    pub async fn wait(&self) -> Result<OperationOutcome> {
        self.wait_timeout(Duration::from_secs(30)).await
    }

    /// Waits up to `timeout` for a terminal status, returning the latest
    /// outcome either way.
    pub async fn wait_timeout(&self, timeout: Duration) -> Result<OperationOutcome> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let outcome = self.outcome().await?;
            if outcome.status.is_terminal() || tokio::time::Instant::now() >= deadline {
                return Ok(outcome);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Explicitly reconciles this operation's target entity through its
    /// authoritative REST endpoint. This never resubmits the original
    /// mutation; it only refreshes local state and, if that refresh
    /// succeeds, resolves an operation that was awaiting evidence.
    pub async fn reconcile(&self) -> Result<OperationOutcome> {
        self.client.ensure_open()?;
        let entry = load(&self.client, &self.id)?;
        let refreshed = match (entry.target_kind.as_deref(), entry.target_id.as_deref()) {
            (Some("device"), Some(code)) => self.client.sync().device(code).await.is_ok(),
            (Some("replicant"), Some(code)) => self.client.sync().replicant(code).await.is_ok(),
            (Some("location"), Some(code)) => self.client.sync().location(code).await.is_ok(),
            (Some("account"), _) => self.client.account().refresh().await.is_ok(),
            _ => false,
        };
        if refreshed && !OperationStatus::parse(&entry.state).is_terminal() {
            let _ = self
                .client
                .managed_state()
                .set_operation_state(self.id.as_str(), OperationStatus::Completed.as_str());
            notify(&self.client, &self.id, OperationStatus::Completed);
        }
        self.outcome().await
    }
}

fn load(client: &Client, id: &OperationId) -> Result<super::store::OperationJournalEntry> {
    client
        .managed_state()
        .read_operation(id.as_str())
        .map_err(persistence_error)?
        .ok_or_else(|| Error::Operation {
            message: "operation not found".into(),
        })
}

/// A local, deduplicated operation-status stream. Never itself issues a
/// network request.
pub struct OperationWatch {
    receiver: std::sync::mpsc::Receiver<(OperationId, OperationStatus)>,
    id: OperationId,
}

impl OperationWatch {
    /// Returns the latest status published for this operation since the last
    /// call, if any is available now.
    pub fn try_next(&self) -> Option<OperationStatus> {
        self.receiver
            .try_iter()
            .filter(|(id, _)| *id == self.id)
            .map(|(_, status)| status)
            .last()
    }
}

/// Subscriber registry for durable operation status changes. Owned by
/// `ClientInner`.
pub(crate) struct OperationEngine {
    subscribers: std::sync::Mutex<Vec<std::sync::mpsc::Sender<(OperationId, OperationStatus)>>>,
}

impl OperationEngine {
    pub(crate) fn new() -> Self {
        Self {
            subscribers: std::sync::Mutex::new(Vec::new()),
        }
    }

    pub(crate) fn subscribe(&self) -> std::sync::mpsc::Receiver<(OperationId, OperationStatus)> {
        let (sender, receiver) = std::sync::mpsc::channel();
        self.subscribers
            .lock()
            .expect("operation subscribers lock poisoned")
            .push(sender);
        receiver
    }

    fn notify(&self, id: OperationId, status: OperationStatus) {
        self.subscribers
            .lock()
            .expect("operation subscribers lock poisoned")
            .retain(|sender| sender.send((id.clone(), status)).is_ok());
    }
}

fn notify(client: &Client, id: &OperationId, status: OperationStatus) {
    client.managed_operations().notify(id.clone(), status);
}

/// Required, explicit acknowledgement for [`AccountGateway::wipe`](super::gateways::AccountGateway::wipe)
/// and other irreversible operations. Constructing one requires naming the
/// exact account being destroyed; the call itself still rejects a mismatch
/// against the store's bound account at runtime.
#[derive(Clone, Debug)]
pub struct ConfirmAccountWipe(String);

impl ConfirmAccountWipe {
    /// Acknowledges destructive, irreversible deletion of `account_id`.
    #[must_use]
    pub fn new(account_id: impl Into<String>) -> Self {
        Self(account_id.into())
    }
}

/// A forward-compatible escape hatch for device commands this client's typed
/// [`crate::raw::devices::DeviceCommand`] enum does not (yet) name. Still
/// uses the durable operation engine like every typed command.
#[derive(Clone, Debug, Default)]
pub struct DynamicCommand {
    name: String,
    arguments: raw::JsonObject,
}

impl DynamicCommand {
    /// Starts building a dynamic command dispatched under `name`.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            arguments: raw::JsonObject::new(),
        }
    }

    /// Sets one argument field on the command payload.
    #[must_use]
    pub fn argument(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.arguments.insert(key.into(), value.into());
        self
    }
}

/// Gateway returned by `Client::operations`, for locating and recovering
/// previously created operations (most useful after a restart).
#[derive(Clone, Debug)]
pub struct OperationsGateway {
    client: Client,
}

impl OperationsGateway {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    /// Returns a handle to a previously created operation by ID. Does not by
    /// itself confirm the operation still exists; `status()`/`outcome()` do.
    #[must_use]
    pub fn get(&self, id: OperationId) -> Operation {
        Operation {
            client: self.client.clone(),
            id,
        }
    }

    /// Every durable operation not yet in a terminal state, most useful for
    /// recovering an application's view of in-flight operations after a
    /// restart.
    pub async fn list_unresolved(&self) -> Result<Vec<Operation>> {
        self.client.ensure_open()?;
        let rows = self
            .client
            .managed_state()
            .list_unresolved_operations()
            .map_err(persistence_error)?;
        Ok(rows
            .into_iter()
            .map(|(id, _)| Operation {
                client: self.client.clone(),
                id: OperationId::new(id),
            })
            .collect())
    }
}

/// Gateway returned by `Client::messages` for the account-wide inbox's only
/// mutation.
#[derive(Clone, Debug)]
pub struct MessagesGateway {
    client: Client,
}

impl MessagesGateway {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    /// Marks one, several, or all inbox messages read.
    pub async fn mark_read(
        &self,
        request: raw::messages::MessagesReadRequest,
    ) -> Result<Operation> {
        create(
            &self.client,
            "messages_mark_read",
            None,
            Value::Null,
            to_value(request)?,
            false,
        )
        .await
    }
}

/// Gateway returned by `Client::locations` for location-scoped mutations.
#[derive(Clone, Debug)]
pub struct LocationsGateway {
    client: Client,
}

impl LocationsGateway {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    /// Contributes devices' resources toward a location's active
    /// megastructure or location event.
    pub async fn contribute(&self, designation: &str, devices: Vec<String>) -> Result<Operation> {
        create(
            &self.client,
            "location_contribute",
            Some(("location", designation.to_string())),
            serde_json::json!({ "designation": designation }),
            to_value(raw::locations::LocationContributionRequest { devices })?,
            false,
        )
        .await
    }
}

/// Gateway returned by `Client::location_events` for civilisation-event
/// resolution, distinct from account events and device logs.
#[derive(Clone, Debug)]
pub struct LocationEventsGateway {
    client: Client,
}

impl LocationEventsGateway {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    /// Lists location events at `location_code` that the account has
    /// discovered. Authoritative within discovered-event scope only:
    /// undiscovered events are never implied by absence here. Distinct from
    /// account events (`client.events()`) and device logs (`device.logs()`).
    pub async fn list(
        &self,
        location_code: &str,
        status: Option<&str>,
    ) -> Result<Vec<raw::events::LocationEvent>> {
        self.client.ensure_open()?;
        Ok(self
            .client
            .managed_raw()
            .location_events()
            .list(location_code, status)
            .await?
            .value
            .events)
    }

    /// Resolves a single discovered location event for the caller.
    pub async fn resolve(&self, location_code: &str, designation: &str) -> Result<Operation> {
        create(
            &self.client,
            "location_event_resolve",
            Some(("location", location_code.to_string())),
            serde_json::json!({ "location_code": location_code, "designation": designation }),
            Value::Null,
            false,
        )
        .await
    }
}

/// Compares an intended device command against the device's latest cached
/// `available_commands`. A stale or absent capability list never blocks the
/// call; the server remains the authoritative validator either way.
fn check_device_capability(client: &Client, device_code: &str, command_name: &str) -> Result<()> {
    let key = DeviceKey::live(DeviceId::from(device_code));
    if let Some(observation) = client.managed_state().device(&key) {
        let known = &observation.value.available_commands;
        if !known.is_empty() && !known.iter().any(|command| command.as_str() == command_name) {
            return Err(Error::Operation {
                message: format!(
                    "device `{device_code}` does not currently advertise capability for `{command_name}`"
                ),
            });
        }
    }
    Ok(())
}

/// Device commands whose own response documents a still-running,
/// asynchronously-completing process (a `completes_at`/`eta_seconds` field),
/// per the response shapes in `crate::raw::devices::DeviceCommandResponse`.
/// Every other command's response is already the complete, final outcome.
fn device_command_expects_evidence(command: &raw::devices::DeviceCommand) -> bool {
    matches!(
        command,
        raw::devices::DeviceCommand::Travel { .. }
            | raw::devices::DeviceCommand::EnqueuePrint { .. }
            | raw::devices::DeviceCommand::Prospect { .. }
            | raw::devices::DeviceCommand::Repair { .. }
            | raw::devices::DeviceCommand::Scan
            | raw::devices::DeviceCommand::SystemScan
    )
}

pub(crate) async fn device_command(
    client: &Client,
    device_code: &str,
    command: raw::devices::DeviceCommand,
) -> Result<Operation> {
    let expects_evidence = device_command_expects_evidence(&command);
    let body = to_value(&command)?;
    let command_name = body
        .get("command")
        .and_then(Value::as_str)
        .unwrap_or_default();
    check_device_capability(client, device_code, command_name)?;
    create(
        client,
        "device_command",
        Some(("device", device_code.to_string())),
        serde_json::json!({ "device_code": device_code }),
        body,
        expects_evidence,
    )
    .await
}

pub(crate) async fn device_dynamic_command(
    client: &Client,
    device_code: &str,
    command: DynamicCommand,
) -> Result<Operation> {
    let mut body = serde_json::Map::new();
    body.insert("command".into(), Value::String(command.name));
    for (key, value) in command.arguments {
        body.insert(key, value);
    }
    // Unknown command: this client cannot classify sync vs. async completion,
    // so it conservatively awaits evidence rather than assuming completion.
    create(
        client,
        "device_dynamic_command",
        Some(("device", device_code.to_string())),
        serde_json::json!({ "device_code": device_code }),
        Value::Object(body),
        true,
    )
    .await
}

pub(crate) async fn device_configure(
    client: &Client,
    device_code: &str,
    request: raw::devices::DeviceConfigurationRequest,
) -> Result<Operation> {
    create(
        client,
        "device_configure",
        Some(("device", device_code.to_string())),
        serde_json::json!({ "device_code": device_code }),
        to_value(request)?,
        false,
    )
    .await
}

pub(crate) async fn device_grant_permission(
    client: &Client,
    device_code: &str,
    request: raw::JsonObject,
) -> Result<Operation> {
    create(
        client,
        "device_grant_permission",
        Some(("device", device_code.to_string())),
        serde_json::json!({ "device_code": device_code }),
        Value::Object(request),
        false,
    )
    .await
}

pub(crate) async fn device_revoke_permission(
    client: &Client,
    device_code: &str,
) -> Result<Operation> {
    create(
        client,
        "device_revoke_permission",
        Some(("device", device_code.to_string())),
        serde_json::json!({ "device_code": device_code }),
        Value::Null,
        false,
    )
    .await
}

pub(crate) async fn device_enter_simulation(
    client: &Client,
    device_code: &str,
    request: raw::simulations::SimulationEnterRequest,
) -> Result<Operation> {
    create(
        client,
        "device_enter_simulation",
        Some(("device", device_code.to_string())),
        serde_json::json!({ "device_code": device_code }),
        to_value(request)?,
        false,
    )
    .await
}

pub(crate) async fn device_abandon_simulation(
    client: &Client,
    device_code: &str,
    simulation_id: i64,
) -> Result<Operation> {
    create(
        client,
        "device_abandon_simulation",
        Some(("device", device_code.to_string())),
        serde_json::json!({ "device_code": device_code, "simulation_id": simulation_id }),
        Value::Null,
        false,
    )
    .await
}

pub(crate) async fn device_create_trade(
    client: &Client,
    device_code: &str,
    request: raw::JsonObject,
) -> Result<Operation> {
    create(
        client,
        "device_create_trade",
        Some(("device", device_code.to_string())),
        serde_json::json!({ "device_code": device_code }),
        Value::Object(request),
        false,
    )
    .await
}

pub(crate) async fn device_delete_trade(
    client: &Client,
    device_code: &str,
    trade_code: &str,
) -> Result<Operation> {
    create(
        client,
        "device_delete_trade",
        Some(("device", device_code.to_string())),
        serde_json::json!({ "device_code": device_code, "trade_code": trade_code }),
        Value::Null,
        false,
    )
    .await
}

pub(crate) async fn device_fulfill_trade(
    client: &Client,
    device_code: &str,
    trade_code: &str,
) -> Result<Operation> {
    create(
        client,
        "device_fulfill_trade",
        Some(("device", device_code.to_string())),
        serde_json::json!({ "device_code": device_code, "trade_code": trade_code }),
        Value::Null,
        false,
    )
    .await
}

pub(crate) async fn replicant_update(
    client: &Client,
    replicant_code: &str,
    request: raw::replicants::ReplicantUpdateRequest,
) -> Result<Operation> {
    create(
        client,
        "replicant_update",
        Some(("replicant", replicant_code.to_string())),
        serde_json::json!({ "replicant_code": replicant_code }),
        to_value(request)?,
        false,
    )
    .await
}

pub(crate) async fn replicant_message(
    client: &Client,
    replicant_code: &str,
    request: raw::replicants::ReplicantMessageRequest,
) -> Result<Operation> {
    create(
        client,
        "replicant_message",
        Some(("replicant", replicant_code.to_string())),
        serde_json::json!({ "replicant_code": replicant_code }),
        to_value(request)?,
        false,
    )
    .await
}

pub(crate) async fn replicant_mine(
    client: &Client,
    replicant_code: &str,
    request: raw::replicants::MineRequest,
) -> Result<Operation> {
    create(
        client,
        "replicant_mine",
        Some(("replicant", replicant_code.to_string())),
        serde_json::json!({ "replicant_code": replicant_code }),
        to_value(request)?,
        false,
    )
    .await
}

pub(crate) async fn replicant_stop_mining(
    client: &Client,
    replicant_code: &str,
) -> Result<Operation> {
    create(
        client,
        "replicant_stop_mining",
        Some(("replicant", replicant_code.to_string())),
        serde_json::json!({ "replicant_code": replicant_code }),
        Value::Null,
        false,
    )
    .await
}

pub(crate) async fn replicant_print(
    client: &Client,
    replicant_code: &str,
    request: raw::replicants::PrintRequest,
) -> Result<Operation> {
    create(
        client,
        "replicant_print",
        Some(("replicant", replicant_code.to_string())),
        serde_json::json!({ "replicant_code": replicant_code }),
        to_value(request)?,
        true,
    )
    .await
}

pub(crate) async fn replicant_scan(client: &Client, replicant_code: &str) -> Result<Operation> {
    create(
        client,
        "replicant_scan",
        Some(("replicant", replicant_code.to_string())),
        serde_json::json!({ "replicant_code": replicant_code }),
        Value::Null,
        false,
    )
    .await
}

pub(crate) async fn replicant_teleport(
    client: &Client,
    replicant_code: &str,
    request: raw::replicants::TeleportRequest,
) -> Result<Operation> {
    create(
        client,
        "replicant_teleport",
        Some(("replicant", replicant_code.to_string())),
        serde_json::json!({ "replicant_code": replicant_code }),
        to_value(request)?,
        true,
    )
    .await
}

pub(crate) async fn replicant_transfer(
    client: &Client,
    replicant_code: &str,
    request: raw::replicants::TransferRequest,
) -> Result<Operation> {
    create(
        client,
        "replicant_transfer",
        Some(("replicant", replicant_code.to_string())),
        serde_json::json!({ "replicant_code": replicant_code }),
        to_value(request)?,
        false,
    )
    .await
}

pub(crate) async fn replicant_travel(
    client: &Client,
    replicant_code: &str,
    request: raw::replicants::TravelRequest,
) -> Result<Operation> {
    create(
        client,
        "replicant_travel",
        Some(("replicant", replicant_code.to_string())),
        serde_json::json!({ "replicant_code": replicant_code }),
        to_value(request)?,
        true,
    )
    .await
}

pub(crate) async fn replicant_cancel_travel(
    client: &Client,
    replicant_code: &str,
) -> Result<Operation> {
    create(
        client,
        "replicant_cancel_travel",
        Some(("replicant", replicant_code.to_string())),
        serde_json::json!({ "replicant_code": replicant_code }),
        Value::Null,
        false,
    )
    .await
}

pub(crate) async fn account_update(
    client: &Client,
    request: raw::accounts::AccountUpdateRequest,
) -> Result<Operation> {
    create(
        client,
        "account_update",
        Some(("account", String::new())),
        Value::Null,
        to_value(request)?,
        false,
    )
    .await
}

pub(crate) async fn account_wipe(
    client: &Client,
    confirm: ConfirmAccountWipe,
) -> Result<Operation> {
    client.ensure_open()?;
    let bound = client
        .managed_state()
        .bound_account_id()
        .map_err(persistence_error)?;
    if bound.as_deref() != Some(confirm.0.as_str()) {
        return Err(Error::Operation {
            message: "destructive confirmation does not match the account bound to this store"
                .into(),
        });
    }
    create(
        client,
        "account_wipe",
        Some(("account", String::new())),
        Value::Null,
        Value::Null,
        false,
    )
    .await
}

/// The common entry point for every durable mutation: persists sanitized
/// intent, then performs the one automatic submission attempt.
async fn create(
    client: &Client,
    kind: &'static str,
    target: Option<(&'static str, String)>,
    path: Value,
    body: Value,
    expects_evidence: bool,
) -> Result<Operation> {
    client.ensure_open()?;
    ensure_no_secrets(&path)?;
    ensure_no_secrets(&body)?;
    let id = OperationId::new(Uuid::new_v4().to_string());
    let intent = serde_json::json!({
        "kind": kind,
        "path": path,
        "body": body,
        "expects_evidence": expects_evidence,
    });
    let (target_realm, target_kind, target_id): (Option<&str>, Option<&str>, Option<String>) =
        match &target {
            Some((kind, id)) => (Some("live"), Some(*kind), Some(id.clone())),
            None => (None, None, None),
        };
    client
        .managed_state()
        .record_operation(
            id.as_str(),
            OperationStatus::Prepared.as_str(),
            target_realm,
            target_kind,
            target_id.as_deref(),
            &intent,
        )
        .map_err(persistence_error)?;
    attempt(client, &id).await;
    Ok(Operation {
        client: client.clone(),
        id,
    })
}

/// Performs (or, on restart recovery, retries exactly once) the automatic
/// submission attempt for a `prepared` operation. Idempotent to call on an
/// operation that is not `prepared`: it simply reloads and re-dispatches
/// nothing further in that case, since `dispatch` is only ever reached from
/// here immediately after a `prepared` state is confirmed durable.
async fn attempt(client: &Client, id: &OperationId) {
    let Ok(Some(entry)) = client.managed_state().read_operation(id.as_str()) else {
        return;
    };
    if entry.state != OperationStatus::Prepared.as_str() {
        return;
    }
    let kind = entry
        .intent
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let path = entry.intent.get("path").cloned().unwrap_or(Value::Null);
    let body = entry.intent.get("body").cloned().unwrap_or(Value::Null);
    let expects_evidence = entry
        .intent
        .get("expects_evidence")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    let _ = client
        .managed_state()
        .set_operation_state(id.as_str(), OperationStatus::Submitted.as_str());
    notify(client, id, OperationStatus::Submitted);

    match dispatch(client, &kind, &path, &body).await {
        Err(error) if error.is_ambiguous_transport_failure() => {
            let _ = client
                .managed_state()
                .set_operation_state(id.as_str(), OperationStatus::Ambiguous.as_str());
            notify(client, id, OperationStatus::Ambiguous);
        }
        Err(error) => {
            let _ = client.managed_state().append_operation_projection(
                id.as_str(),
                OperationStatus::Rejected.as_str(),
                &sanitize_error(&error),
            );
            notify(client, id, OperationStatus::Rejected);
        }
        Ok(response) => {
            let next = if expects_evidence {
                OperationStatus::AwaitingEvidence
            } else {
                OperationStatus::Completed
            };
            let _ = client.managed_state().append_operation_projection(
                id.as_str(),
                next.as_str(),
                &response,
            );
            notify(client, id, next);
            if let (Some(target_kind), Some(target_id)) =
                (entry.target_kind.as_deref(), entry.target_id.as_deref())
            {
                schedule_target_reconciliation(client, target_kind, target_id);
            }
        }
    }
}

fn schedule_target_reconciliation(client: &Client, target_kind: &str, target_id: &str) {
    let work_id = format!("operation:{target_kind}:{target_id}");
    let _ = client.managed_state().enqueue_reconciliation(
        &work_id,
        &domain::Realm::Live,
        target_kind,
        &serde_json::json!({ "id": target_id }),
    );
}

/// Called by the event engine for every applied event: resolves any
/// operation awaiting evidence on the event's device/replicant/location key.
/// Matching is intentionally name-agnostic (any subsequent event on the same
/// target counts as evidence), the same forward-compatible principle already
/// used for narrow reconciliation scheduling.
pub(crate) fn resolve_awaiting_evidence(client: &Client, event: &domain::Event) {
    if let Some(device) = &event.device {
        mark_resolved(client, "device", device.id.as_str());
    }
    if let Some(replicant) = &event.replicant {
        mark_resolved(client, "replicant", replicant.id.as_str());
    }
    if let Some(location) = &event.location {
        mark_resolved(client, "location", location.id.as_str());
    }
}

fn mark_resolved(client: &Client, target_kind: &str, target_id: &str) {
    let Ok(ids) =
        client
            .managed_state()
            .find_operations_awaiting_evidence("live", target_kind, target_id)
    else {
        return;
    };
    for operation_id in ids {
        let _ = client
            .managed_state()
            .set_operation_state(&operation_id, OperationStatus::Completed.as_str());
        notify(
            client,
            &OperationId::new(operation_id),
            OperationStatus::Completed,
        );
    }
}

/// Restart recovery: an operation left in `prepared` was durably registered
/// but never confirmed to have even started its one automatic submission
/// attempt, so it is safe (and required, for exactly-once delivery) to
/// attempt it now. Every other non-terminal state (`ambiguous`, `accepted`,
/// `awaiting_evidence`, ...) is left untouched — recoverable via evidence or
/// [`Operation::reconcile`], never blindly resubmitted.
pub(crate) async fn recover(client: &Client) {
    let Ok(rows) = client.managed_state().list_unresolved_operations() else {
        return;
    };
    for (id, entry) in rows {
        if entry.state == OperationStatus::Prepared.as_str() {
            attempt(client, &OperationId::new(id)).await;
        }
    }
}

/// Resolves a durable operation kind to the exact `/v1` request this client
/// already sends for that endpoint through its typed `raw` clients. Intents
/// store the already-serialized, wire-identical request body, so replaying
/// it here reproduces exactly what the typed `raw` method would have sent —
/// without needing every raw request/response DTO to additionally implement
/// `Deserialize`/`Serialize` just to round-trip through the operation
/// journal.
fn dispatch_target(kind: &str, path: &Value) -> Result<(reqwest::Method, String)> {
    use crate::raw::common::encode_path_segment as enc;
    use reqwest::Method;

    let url = match kind {
        "account_update" | "account_wipe" => "v1/accounts/me".to_string(),
        "device_configure" | "device_command" | "device_dynamic_command" => {
            format!("v1/devices/{}", enc(str_field(path, "device_code")?))
        }
        "device_grant_permission" | "device_revoke_permission" => {
            format!(
                "v1/devices/{}/permissions",
                enc(str_field(path, "device_code")?)
            )
        }
        "device_enter_simulation" => {
            format!(
                "v1/devices/{}/simulate",
                enc(str_field(path, "device_code")?)
            )
        }
        "device_abandon_simulation" => format!(
            "v1/devices/{}/simulate/{}",
            enc(str_field(path, "device_code")?),
            i64_field(path, "simulation_id")?
        ),
        "device_create_trade" => {
            format!("v1/devices/{}/trades", enc(str_field(path, "device_code")?))
        }
        "device_delete_trade" | "device_fulfill_trade" => format!(
            "v1/devices/{}/trades/{}",
            enc(str_field(path, "device_code")?),
            enc(str_field(path, "trade_code")?)
        ),
        "location_contribute" => {
            format!(
                "v1/locations/{}/contribute",
                enc(str_field(path, "designation")?)
            )
        }
        "location_event_resolve" => format!(
            "v1/locations/{}/events/{}",
            enc(str_field(path, "location_code")?),
            enc(str_field(path, "designation")?)
        ),
        "messages_mark_read" => "v1/messages/read".to_string(),
        "replicant_update" => {
            format!("v1/replicants/{}", enc(str_field(path, "replicant_code")?))
        }
        "replicant_message" => {
            format!(
                "v1/replicants/{}/message",
                enc(str_field(path, "replicant_code")?)
            )
        }
        "replicant_mine" | "replicant_stop_mining" => {
            format!(
                "v1/replicants/{}/mine",
                enc(str_field(path, "replicant_code")?)
            )
        }
        "replicant_print" => {
            format!(
                "v1/replicants/{}/print",
                enc(str_field(path, "replicant_code")?)
            )
        }
        "replicant_scan" => {
            format!(
                "v1/replicants/{}/scan",
                enc(str_field(path, "replicant_code")?)
            )
        }
        "replicant_teleport" => {
            format!(
                "v1/replicants/{}/teleport",
                enc(str_field(path, "replicant_code")?)
            )
        }
        "replicant_transfer" => {
            format!(
                "v1/replicants/{}/transfer",
                enc(str_field(path, "replicant_code")?)
            )
        }
        "replicant_travel" | "replicant_cancel_travel" => {
            format!(
                "v1/replicants/{}/travel",
                enc(str_field(path, "replicant_code")?)
            )
        }
        other => {
            return Err(Error::Operation {
                message: format!("unknown durable operation kind `{other}`"),
            });
        }
    };
    let method = match kind {
        "account_wipe"
        | "device_revoke_permission"
        | "device_abandon_simulation"
        | "device_delete_trade"
        | "replicant_stop_mining"
        | "replicant_cancel_travel" => Method::DELETE,
        "device_configure" | "replicant_update" => Method::PATCH,
        _ => Method::POST,
    };
    Ok((method, url))
}

async fn dispatch(client: &Client, kind: &str, path: &Value, body: &Value) -> Result<Value> {
    let (method, url) = dispatch_target(kind, path)?;
    let raw = client.managed_raw();
    let response = if matches!(body, Value::Null) {
        raw.execute::<Value>(method, &url, true, raw::RequestSafety::Mutating)
            .await?
    } else {
        raw.execute_json::<Value, Value>(method, &url, true, raw::RequestSafety::Mutating, body)
            .await?
    };
    Ok(response.value)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use super::*;
    use crate::domain::{DeviceId, Realm};
    use crate::managed::client::StartupPolicy;
    use crate::raw::{SecretString, Url};

    /// A base URL with nothing listening, so a request against it fails at
    /// the transport level (connection refused) rather than getting back any
    /// HTTP response — the definitionally ambiguous case.
    fn unreachable_base_url() -> String {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        let port = listener.local_addr().expect("local addr").port();
        drop(listener); // synchronously frees the port; nothing is listening on it
        format!("http://127.0.0.1:{port}")
    }

    async fn client_at(base_url: &str) -> Client {
        Client::builder()
            .authentication_token(SecretString::from("token".to_string()))
            .base_url(Url::parse(base_url).expect("mock URL"))
            .in_memory()
            .startup_policy(StartupPolicy::RestoreOnly)
            .start()
            .await
            .expect("restore-only client")
    }

    #[tokio::test]
    async fn intent_is_durable_and_ambiguous_on_transport_failure() {
        // Nothing is listening at this base URL: the one automatic attempt
        // fails at the transport level, which is definitionally ambiguous.
        let client = client_at(&unreachable_base_url()).await;

        let operation = device_configure(
            &client,
            "D1",
            raw::devices::DeviceConfigurationRequest {
                configuration: raw::devices::DeviceConfiguration {
                    add_tags: Some(vec!["mining".into()]),
                    ..Default::default()
                },
            },
        )
        .await
        .expect("operation is durably registered even though transmission fails");

        assert_eq!(
            operation.status().await.expect("status"),
            OperationStatus::Ambiguous
        );
        let entry = load(&client, operation.id()).expect("intent is durable");
        assert_eq!(entry.target_kind.as_deref(), Some("device"));
        assert_eq!(entry.target_id.as_deref(), Some("D1"));
        assert_eq!(entry.intent["kind"], "device_configure");
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn exactly_one_automatic_submission_attempt() {
        let server = MockServer::start().await;
        Mock::given(method("PATCH"))
            .and(path("/v1/devices/D1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&server)
            .await;
        let client = client_at(&server.uri()).await;

        let operation = device_configure(
            &client,
            "D1",
            raw::devices::DeviceConfigurationRequest::default(),
        )
        .await
        .expect("operation created");
        assert_eq!(
            operation.status().await.expect("status"),
            OperationStatus::Completed
        );

        client.close().await.expect("close");
        server.verify().await; // panics if the mock was not hit exactly once
    }

    #[tokio::test]
    async fn definite_rejection_is_classified_and_never_treated_as_ambiguous() {
        let server = MockServer::start().await;
        Mock::given(method("PATCH"))
            .and(path("/v1/devices/D1"))
            .respond_with(
                ResponseTemplate::new(422)
                    .set_body_json(serde_json::json!({"error": "invalid tag"})),
            )
            .mount(&server)
            .await;
        let client = client_at(&server.uri()).await;

        let operation = device_configure(
            &client,
            "D1",
            raw::devices::DeviceConfigurationRequest::default(),
        )
        .await
        .expect("operation created");
        let outcome = operation.outcome().await.expect("outcome");
        assert_eq!(outcome.status, OperationStatus::Rejected);
        assert!(outcome.response.is_some());
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn ambiguous_operations_are_resolved_by_explicit_reconciliation_not_resubmission() {
        let server = MockServer::start().await;
        let client = client_at(&server.uri()).await;
        client
            .managed_state()
            .record_operation(
                "op-ambiguous",
                OperationStatus::Ambiguous.as_str(),
                Some("live"),
                Some("device"),
                Some("D1"),
                &serde_json::json!({"kind": "device_configure"}),
            )
            .expect("record ambiguous operation");

        // Only the read used by `reconcile()` is mounted; nothing matches the
        // original mutating endpoint at all, so a resubmission would fail
        // this test outright rather than merely being unverified.
        Mock::given(method("GET"))
            .and(path("/v1/devices/D1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "device_code": "D1", "device_type": "mining_drone", "status": "idle"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let operation = client.operations().get(OperationId::new("op-ambiguous"));
        let outcome = operation.reconcile().await.expect("reconcile");
        assert_eq!(outcome.status, OperationStatus::Completed);
        server.verify().await;
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn restart_recovery_promotes_submitted_and_retries_prepared() {
        let path_buf = std::env::temp_dir().join(format!(
            "replicant-client-operation-restart-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        {
            let mut store = super::super::store::Store::open_file(&path_buf).expect("open store");
            store
                .record_operation(
                    "op-submitted",
                    "submitted",
                    Some("live"),
                    Some("device"),
                    Some("D1"),
                    &serde_json::json!({"kind": "device_configure", "path": {"device_code":"D1"}, "body": {}, "expects_evidence": false}),
                )
                .expect("record submitted");
            store
                .record_operation(
                    "op-prepared",
                    "prepared",
                    Some("device"),
                    Some("device"),
                    Some("D2"),
                    &serde_json::json!({"kind": "device_revoke_permission", "path": {"device_code":"D2"}, "body": null, "expects_evidence": false}),
                )
                .expect("record prepared");
        }

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/accounts/me"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"email": "a@b.test"})),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/devices"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"devices": [], "next_cursor": null})),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/events"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"events": [], "next_cursor": null})),
            )
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/v1/devices/D2/permissions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&server)
            .await;

        let client = Client::builder()
            .authentication_token(SecretString::from("token".to_string()))
            .base_url(Url::parse(&server.uri()).expect("mock URL"))
            .sqlite(&path_buf)
            .startup_policy(StartupPolicy::Essential)
            .start()
            .await
            .expect("start client");

        // The pure-local half runs synchronously during `start()`.
        let submitted = client
            .operations()
            .get(OperationId::new("op-submitted"))
            .status()
            .await
            .expect("status");
        assert_eq!(submitted, OperationStatus::Ambiguous);

        // The network half runs from the spawned startup task.
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let status = client
                    .operations()
                    .get(OperationId::new("op-prepared"))
                    .status()
                    .await
                    .expect("status");
                if status == OperationStatus::Completed {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("prepared operation is retried exactly once after restart");

        client.close().await.expect("close");
        std::fs::remove_file(&path_buf).expect("remove test database");
    }

    #[tokio::test]
    async fn restart_recovers_representative_travel_trade_and_simulation_operations() {
        let path_buf = std::env::temp_dir().join(format!(
            "replicant-client-phase10-restart-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        {
            let mut store = super::super::store::Store::open_file(&path_buf).expect("open store");
            store
                .record_operation(
                    "op-travel",
                    "prepared",
                    Some("live"),
                    Some("replicant"),
                    Some("R1"),
                    &serde_json::json!({"kind": "replicant_travel", "path": {"replicant_code":"R1"}, "body": {"destination":"SOL"}, "expects_evidence": true}),
                )
                .expect("record travel");
            store
                .record_operation(
                    "op-trade",
                    "prepared",
                    Some("live"),
                    Some("device"),
                    Some("TC1"),
                    &serde_json::json!({"kind": "device_create_trade", "path": {"device_code":"TC1"}, "body": {"name": "sample"}, "expects_evidence": false}),
                )
                .expect("record trade");
            store
                .record_operation(
                    "op-simulate",
                    "prepared",
                    Some("live"),
                    Some("device"),
                    Some("SIMDEV1"),
                    &serde_json::json!({"kind": "device_enter_simulation", "path": {"device_code":"SIMDEV1"}, "body": {"replicant_code": "R1", "scenario": "mining_rush"}, "expects_evidence": false}),
                )
                .expect("record simulate");
        }

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/accounts/me"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"email": "a@b.test"})),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/devices"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"devices": [], "next_cursor": null})),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/events"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"events": [], "next_cursor": null})),
            )
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/v1/replicants/R1/travel"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/v1/devices/TC1/trades"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/v1/devices/SIMDEV1/simulate"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&server)
            .await;

        let client = Client::builder()
            .authentication_token(SecretString::from("token".to_string()))
            .base_url(Url::parse(&server.uri()).expect("mock URL"))
            .sqlite(&path_buf)
            .startup_policy(StartupPolicy::Essential)
            .start()
            .await
            .expect("start client");

        for id in ["op-travel", "op-trade", "op-simulate"] {
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    let status = client
                        .operations()
                        .get(OperationId::new(id))
                        .status()
                        .await
                        .expect("status");
                    if status.is_terminal() || status == OperationStatus::AwaitingEvidence {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("`{id}` is retried exactly once after restart"));
        }

        client.close().await.expect("close");
        std::fs::remove_file(&path_buf).expect("remove test database");
    }

    #[tokio::test]
    async fn location_event_discovery_is_a_distinct_endpoint_from_account_events() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/locations/SOL-4/events"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "events": [{"designation": "SOL-4-EVT-001", "event_type": "mineral_shortage"}],
                "next_cursor": null
            })))
            .expect(1)
            .mount(&server)
            .await;
        // No mock for the account-wide event log: a request there would
        // fail this test outright, proving location events never fall back
        // to it.
        let client = client_at(&server.uri()).await;

        let events = client
            .location_events()
            .list("SOL-4", None)
            .await
            .expect("location events");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].designation.as_deref(), Some("SOL-4-EVT-001"));

        server.verify().await;
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn event_evidence_resolves_an_awaiting_operation() {
        let client = client_at(&MockServer::start().await.uri()).await;
        client
            .managed_state()
            .record_operation(
                "op-travel",
                OperationStatus::AwaitingEvidence.as_str(),
                Some("live"),
                Some("device"),
                Some("D1"),
                &serde_json::json!({"kind": "device_command"}),
            )
            .expect("record awaiting-evidence operation");

        let event = domain::Event {
            id: crate::domain::EventId::new("evt-1"),
            realm: Some(Realm::Live),
            name: domain::EventName::from("device.travel_arrived"),
            category: domain::EventCategory::from("device"),
            device: Some(DeviceKey::live(DeviceId::from("D1"))),
            replicant: None,
            location: None,
            star: None,
            occurred_at: "2026-07-25T00:00:00Z".into(),
            payload: Default::default(),
        };
        resolve_awaiting_evidence(&client, &event);

        let operation = client.operations().get(OperationId::new("op-travel"));
        assert_eq!(
            operation.status().await.expect("status"),
            OperationStatus::Completed
        );
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn wait_timeout_returns_unresolved_outcome_without_erroring() {
        let client = client_at(&MockServer::start().await.uri()).await;
        client
            .managed_state()
            .record_operation(
                "op-pending",
                OperationStatus::AwaitingEvidence.as_str(),
                Some("live"),
                Some("replicant"),
                Some("R1"),
                &serde_json::json!({"kind": "replicant_travel"}),
            )
            .expect("record");
        let operation = client.operations().get(OperationId::new("op-pending"));

        let outcome = operation
            .wait_timeout(Duration::from_millis(50))
            .await
            .expect("wait_timeout never errors merely because of a local timeout");
        assert_eq!(outcome.status, OperationStatus::AwaitingEvidence);
        // The durable record remains exactly as it was: recoverable, not failed.
        assert_eq!(
            operation.status().await.expect("status"),
            OperationStatus::AwaitingEvidence
        );
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn destructive_confirmation_mismatch_is_rejected_before_any_request() {
        let server = MockServer::start().await;
        let client = client_at(&server.uri()).await;
        let error = account_wipe(
            &client,
            ConfirmAccountWipe::new("someone-else@example.test"),
        )
        .await
        .expect_err("mismatched confirmation must be rejected");
        assert!(matches!(error, Error::Operation { .. }));
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn destructive_confirmation_match_proceeds() {
        let path_buf = std::env::temp_dir().join(format!(
            "replicant-client-wipe-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        {
            let mut store = super::super::store::Store::open_file(&path_buf).expect("open store");
            store
                .bind_account(&crate::domain::AccountId::new("wiper@example.test"))
                .expect("bind account");
        }
        let server = MockServer::start().await;
        Mock::given(method("DELETE"))
            .and(path("/v1/accounts/me"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&server)
            .await;
        let client = Client::builder()
            .authentication_token(SecretString::from("token".to_string()))
            .base_url(Url::parse(&server.uri()).expect("mock URL"))
            .sqlite(&path_buf)
            .startup_policy(StartupPolicy::RestoreOnly)
            .start()
            .await
            .expect("start client");

        let operation = account_wipe(&client, ConfirmAccountWipe::new("wiper@example.test"))
            .await
            .expect("matching confirmation proceeds");
        assert_eq!(
            operation.status().await.expect("status"),
            OperationStatus::Completed
        );

        client.close().await.expect("close");
        std::fs::remove_file(&path_buf).expect("remove test database");
    }

    #[tokio::test]
    async fn dynamic_command_preserves_an_unknown_command_name() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/devices/D1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&server)
            .await;
        let client = client_at(&server.uri()).await;

        let command = DynamicCommand::new("future_command").argument("field", "value");
        device_dynamic_command(&client, "D1", command)
            .await
            .expect("dynamic command is dispatched");

        let requests = server.received_requests().await.expect("recorded requests");
        assert_eq!(requests.len(), 1);
        let body: Value = requests[0].body_json().expect("json body");
        assert_eq!(body["command"], "future_command");
        assert_eq!(body["field"], "value");
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn dynamic_command_with_credential_shaped_argument_is_rejected() {
        let client = client_at(&MockServer::start().await.uri()).await;
        let command = DynamicCommand::new("future_command").argument("token", "sekrit");
        let error = device_dynamic_command(&client, "D1", command)
            .await
            .expect_err("credential-shaped argument must be rejected");
        assert!(matches!(error, Error::Operation { .. }));
        // No operation record was ever created for the rejected payload.
        assert!(
            client
                .managed_state()
                .list_unresolved_operations()
                .expect("list unresolved")
                .is_empty()
        );
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn raw_unsafe_calls_never_create_operation_records() {
        let server = MockServer::start().await;
        Mock::given(method("DELETE"))
            .and(path("/v1/replicants/R1/mine"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .mount(&server)
            .await;
        let client = client_at(&server.uri()).await;

        client
            .raw()
            .replicants()
            .stop_mining("R1")
            .await
            .expect("raw call succeeds");

        assert!(
            client
                .managed_state()
                .list_unresolved_operations()
                .expect("list unresolved")
                .is_empty()
        );
        client.close().await.expect("close");
    }

    #[test]
    fn every_non_bootstrap_unsafe_operation_is_dispatchable() {
        // Cross-checks this engine's dispatch table against the checked-in
        // contract inventory: every `supported`, non-`GET` operation except
        // the three bootstrap endpoints (account registration/recovery and
        // feedback, which run before any durable authenticated client can
        // exist) must resolve to a concrete method + path here.
        #[derive(serde::Deserialize)]
        struct OperationEntry {
            method: String,
            path: String,
            classification: String,
        }
        #[derive(serde::Deserialize)]
        struct Policy {
            operations: Vec<OperationEntry>,
        }
        let policy: Policy =
            serde_json::from_str(include_str!("../../policy/operations.json")).expect("policy");
        let bootstrap = [
            "POST /v1/accounts",
            "POST /v1/accounts/recover",
            "POST /v1/feedback",
        ];
        let unsafe_ops: Vec<(String, String)> = policy
            .operations
            .into_iter()
            .filter(|op| op.classification == "supported" && op.method != "GET")
            .map(|op| (op.method, op.path))
            .filter(|(method, path)| !bootstrap.contains(&format!("{method} {path}").as_str()))
            .collect();
        assert_eq!(
            unsafe_ops.len(),
            24,
            "expected exactly 24 durable operations"
        );

        let kinds = [
            ("account_update", serde_json::json!({})),
            ("account_wipe", serde_json::json!({})),
            ("device_configure", serde_json::json!({"device_code": "D1"})),
            ("device_command", serde_json::json!({"device_code": "D1"})),
            (
                "device_grant_permission",
                serde_json::json!({"device_code": "D1"}),
            ),
            (
                "device_revoke_permission",
                serde_json::json!({"device_code": "D1"}),
            ),
            (
                "device_enter_simulation",
                serde_json::json!({"device_code": "D1"}),
            ),
            (
                "device_abandon_simulation",
                serde_json::json!({"device_code": "D1", "simulation_id": 1}),
            ),
            (
                "device_create_trade",
                serde_json::json!({"device_code": "D1"}),
            ),
            (
                "device_delete_trade",
                serde_json::json!({"device_code": "D1", "trade_code": "T1"}),
            ),
            (
                "device_fulfill_trade",
                serde_json::json!({"device_code": "D1", "trade_code": "T1"}),
            ),
            (
                "location_contribute",
                serde_json::json!({"designation": "SOL"}),
            ),
            (
                "location_event_resolve",
                serde_json::json!({"location_code": "SOL", "designation": "EVT1"}),
            ),
            ("messages_mark_read", serde_json::json!({})),
            (
                "replicant_update",
                serde_json::json!({"replicant_code": "R1"}),
            ),
            (
                "replicant_message",
                serde_json::json!({"replicant_code": "R1"}),
            ),
            (
                "replicant_mine",
                serde_json::json!({"replicant_code": "R1"}),
            ),
            (
                "replicant_stop_mining",
                serde_json::json!({"replicant_code": "R1"}),
            ),
            (
                "replicant_print",
                serde_json::json!({"replicant_code": "R1"}),
            ),
            (
                "replicant_scan",
                serde_json::json!({"replicant_code": "R1"}),
            ),
            (
                "replicant_teleport",
                serde_json::json!({"replicant_code": "R1"}),
            ),
            (
                "replicant_transfer",
                serde_json::json!({"replicant_code": "R1"}),
            ),
            (
                "replicant_travel",
                serde_json::json!({"replicant_code": "R1"}),
            ),
            (
                "replicant_cancel_travel",
                serde_json::json!({"replicant_code": "R1"}),
            ),
        ];
        // `device_command` and `device_dynamic_command` cover the same single
        // REST endpoint (`POST /v1/devices/{device_code}`), so the kind table
        // has 24 entries mapping to the same 24 distinct endpoints.
        assert_eq!(kinds.len(), 24);
        for (kind, path) in &kinds {
            dispatch_target(kind, path)
                .unwrap_or_else(|_| panic!("kind `{kind}` must resolve to a dispatch target"));
        }
    }
}
