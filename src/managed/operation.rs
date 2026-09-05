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
//! [`recover`]). A `2xx` response whose success body cannot be decoded is also
//! ambiguous because the mutation may already have executed. Explicit
//! non-success responses are definite rejections. A successfully decoded
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

use std::{
    collections::{BTreeSet, VecDeque},
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::domain::{
    self, DeviceId, DeviceKey, LocationId, LocationKey, Message, ObservationTime, OperationId,
    Realm,
};
use crate::error::Error;
use crate::raw;
use crate::{Client, Result};

use super::store::StoreError;

fn persistence_error(error: StoreError) -> Error {
    Error::Persistence {
        message: error.to_string(),
    }
}

fn to_value<T: Serialize>(value: T) -> Result<Value> {
    serde_json::to_value(value).map_err(|error| Error::Operation {
        message: format!("request serialization failed: {error}"),
    })
}

/// The single internal mutation contract.  Its variants are deliberately
/// request-shaped: replay decodes the durable intent back into the exact
/// request type accepted by the corresponding raw endpoint.
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum MutationAdapter {
    AccountUpdate {
        request: raw::accounts::AccountUpdateRequest,
    },
    AccountWipe,
    DeviceConfigure {
        device_code: String,
        request: raw::devices::DeviceConfigurationRequest,
    },
    DeviceRetrieve {
        device_code: String,
    },
    DeviceCommand {
        device_code: String,
        command: raw::devices::DeviceCommand,
    },
    DeviceDynamicCommand {
        device_code: String,
        command: raw::JsonObject,
    },
    DeviceGrantPermission {
        device_code: String,
        request: raw::JsonObject,
    },
    DeviceRevokePermission {
        device_code: String,
    },
    DeviceEnterSimulation {
        device_code: String,
        request: raw::simulations::SimulationEnterRequest,
    },
    DeviceAbandonSimulation {
        device_code: String,
        simulation_id: i64,
    },
    DeviceCreateTrade {
        device_code: String,
        request: raw::JsonObject,
    },
    DeviceDeleteTrade {
        device_code: String,
        trade_code: String,
    },
    DeviceFulfillTrade {
        device_code: String,
        trade_code: String,
    },
    LocationContribute {
        designation: String,
        request: raw::locations::LocationContributionRequest,
    },
    LocationEventResolve {
        location_code: String,
        designation: String,
    },
    MessagesMarkRead {
        request: raw::messages::MessagesReadRequest,
    },
    ReplicantUpdate {
        replicant_code: String,
        request: raw::replicants::ReplicantUpdateRequest,
    },
    ReplicantMessage {
        replicant_code: String,
        request: raw::replicants::ReplicantMessageRequest,
    },
    ReplicantMine {
        replicant_code: String,
        request: raw::replicants::MineRequest,
    },
    ReplicantStopMining {
        replicant_code: String,
    },
    ReplicantPrint {
        replicant_code: String,
        request: raw::replicants::PrintRequest,
    },
    ReplicantScan {
        replicant_code: String,
    },
    ReplicantTeleport {
        replicant_code: String,
        request: raw::replicants::TeleportRequest,
    },
    ReplicantTransfer {
        replicant_code: String,
        request: raw::replicants::TransferRequest,
    },
    ReplicantTravel {
        replicant_code: String,
        request: raw::replicants::TravelRequest,
    },
    ReplicantCancelTravel {
        replicant_code: String,
    },
}

/// Durable managed-mutation behavior. There is intentionally no generic
/// method/path/body escape hatch: adding a mutation requires one explicit enum variant.
impl MutationAdapter {
    fn operation_id(&self) -> &'static str {
        match self {
            Self::AccountUpdate { .. } => "account_update",
            Self::AccountWipe => "account_wipe",
            Self::DeviceConfigure { .. } => "device_configure",
            Self::DeviceRetrieve { .. } => "device_retrieve",
            Self::DeviceCommand { .. } => "device_command",
            Self::DeviceDynamicCommand { .. } => "device_dynamic_command",
            Self::DeviceGrantPermission { .. } => "device_grant_permission",
            Self::DeviceRevokePermission { .. } => "device_revoke_permission",
            Self::DeviceEnterSimulation { .. } => "device_enter_simulation",
            Self::DeviceAbandonSimulation { .. } => "device_abandon_simulation",
            Self::DeviceCreateTrade { .. } => "device_create_trade",
            Self::DeviceDeleteTrade { .. } => "device_delete_trade",
            Self::DeviceFulfillTrade { .. } => "device_fulfill_trade",
            Self::LocationContribute { .. } => "location_contribute",
            Self::LocationEventResolve { .. } => "location_event_resolve",
            Self::MessagesMarkRead { .. } => "messages_mark_read",
            Self::ReplicantUpdate { .. } => "replicant_update",
            Self::ReplicantMessage { .. } => "replicant_message",
            Self::ReplicantMine { .. } => "replicant_mine",
            Self::ReplicantStopMining { .. } => "replicant_stop_mining",
            Self::ReplicantPrint { .. } => "replicant_print",
            Self::ReplicantScan { .. } => "replicant_scan",
            Self::ReplicantTeleport { .. } => "replicant_teleport",
            Self::ReplicantTransfer { .. } => "replicant_transfer",
            Self::ReplicantTravel { .. } => "replicant_travel",
            Self::ReplicantCancelTravel { .. } => "replicant_cancel_travel",
        }
    }

    fn target(&self) -> Option<(&'static str, String)> {
        match self {
            Self::AccountUpdate { .. } | Self::AccountWipe => Some(("account", String::new())),
            Self::DeviceRetrieve { .. } => None,
            Self::DeviceConfigure { device_code, .. }
            | Self::DeviceCommand { device_code, .. }
            | Self::DeviceDynamicCommand { device_code, .. }
            | Self::DeviceGrantPermission { device_code, .. }
            | Self::DeviceRevokePermission { device_code }
            | Self::DeviceEnterSimulation { device_code, .. }
            | Self::DeviceCreateTrade { device_code, .. }
            | Self::DeviceDeleteTrade { device_code, .. }
            | Self::DeviceFulfillTrade { device_code, .. } => Some(("device", device_code.clone())),
            Self::DeviceAbandonSimulation { simulation_id, .. } => {
                Some(("simulation", simulation_id.to_string()))
            }
            Self::LocationContribute { designation, .. } => Some(("location", designation.clone())),
            Self::LocationEventResolve { location_code, .. } => {
                Some(("location", location_code.clone()))
            }
            Self::MessagesMarkRead { .. } => None,
            Self::ReplicantUpdate { replicant_code, .. }
            | Self::ReplicantMessage { replicant_code, .. }
            | Self::ReplicantMine { replicant_code, .. }
            | Self::ReplicantStopMining { replicant_code }
            | Self::ReplicantPrint { replicant_code, .. }
            | Self::ReplicantScan { replicant_code }
            | Self::ReplicantTeleport { replicant_code, .. }
            | Self::ReplicantTransfer { replicant_code, .. }
            | Self::ReplicantTravel { replicant_code, .. }
            | Self::ReplicantCancelTravel { replicant_code } => {
                Some(("replicant", replicant_code.clone()))
            }
        }
    }

    fn expects_evidence(&self) -> bool {
        if operation_evidence(self)["event_names"]
            .as_array()
            .is_some_and(|names| !names.is_empty())
        {
            return true;
        }
        match self {
            Self::DeviceCommand { command, .. } => device_command_expects_evidence(command),
            Self::DeviceDynamicCommand { .. } | Self::ReplicantTeleport { .. } => true,
            Self::ReplicantPrint { request, .. } => {
                request.command.is_none() && request.device_type.is_some()
            }
            Self::ReplicantTravel { request, .. } => {
                request.dry_run != Some(true) && request.destination.is_some()
            }
            _ => false,
        }
    }

    fn durable_intent(&self) -> Result<Value> {
        let intent = to_value(self)?;
        ensure_no_secrets(&intent)?;
        Ok(intent)
    }

    async fn submit(&self, raw: &raw::Client) -> Result<Value> {
        macro_rules! response {
            ($call:expr) => {{
                // Calling the raw method performs the endpoint-specific
                // typed response decode before this operation advances.
                // Endpoints without a snapshot response reconcile before
                // terminal completion below.
                let decoded = $call.await?.value;
                drop(decoded);
                Ok(Value::Null)
            }};
        }
        match self {
            Self::AccountUpdate { request } => response!(raw.accounts().update(request)),
            Self::AccountWipe => response!(raw.accounts().request_destructive_wipe()),
            Self::DeviceConfigure {
                device_code,
                request,
            } => response!(raw.devices().configure(device_code, request)),
            Self::DeviceRetrieve { device_code } => response!(raw.devices().retrieve(device_code)),
            Self::DeviceCommand {
                device_code,
                command,
            } => response!(raw.devices().command(device_code, command)),
            Self::DeviceDynamicCommand {
                device_code,
                command,
            } => response!(raw.devices().command(device_code, command)),
            Self::DeviceGrantPermission {
                device_code,
                request,
            } => response!(raw.devices().grant_permission(device_code, request)),
            Self::DeviceRevokePermission { device_code } => {
                response!(raw.devices().revoke_permission(device_code))
            }
            Self::DeviceEnterSimulation {
                device_code,
                request,
            } => to_value(raw.simulations().enter(device_code, request).await?.value),
            Self::DeviceAbandonSimulation {
                device_code,
                simulation_id,
            } => response!(raw.simulations().cancel(device_code, *simulation_id)),
            Self::DeviceCreateTrade {
                device_code,
                request,
            } => response!(raw.trading().create(device_code, request)),
            Self::DeviceDeleteTrade {
                device_code,
                trade_code,
            } => response!(raw.trading().delete(device_code, trade_code)),
            Self::DeviceFulfillTrade {
                device_code,
                trade_code,
            } => response!(raw.trading().fulfill(device_code, trade_code)),
            Self::LocationContribute {
                designation,
                request,
            } => response!(raw.locations().contribute(designation, request)),
            Self::LocationEventResolve {
                location_code,
                designation,
            } => response!(raw.location_events().resolve(location_code, designation)),
            Self::MessagesMarkRead { request } => response!(raw.messages().mark_read(request)),
            Self::ReplicantUpdate {
                replicant_code,
                request,
            } => response!(raw.replicants().update(replicant_code, request)),
            Self::ReplicantMessage {
                replicant_code,
                request,
            } => response!(raw.replicants().message(replicant_code, request)),
            Self::ReplicantMine {
                replicant_code,
                request,
            } => response!(raw.replicants().mine(replicant_code, request)),
            Self::ReplicantStopMining { replicant_code } => {
                response!(raw.replicants().stop_mining(replicant_code))
            }
            Self::ReplicantPrint {
                replicant_code,
                request,
            } => response!(raw.replicants().print(replicant_code, request)),
            Self::ReplicantScan { replicant_code } => {
                response!(raw.replicants().scan(replicant_code))
            }
            Self::ReplicantTeleport {
                replicant_code,
                request,
            } => response!(raw.replicants().teleport(replicant_code, request)),
            Self::ReplicantTransfer {
                replicant_code,
                request,
            } => response!(raw.replicants().transfer(replicant_code, request)),
            Self::ReplicantTravel {
                replicant_code,
                request,
            } => response!(raw.replicants().travel(replicant_code, request)),
            Self::ReplicantCancelTravel { replicant_code } => {
                response!(raw.replicants().cancel_travel(replicant_code))
            }
        }
    }
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
    let server = error
        .details()
        .and_then(|details| details.body_excerpt.as_deref())
        .and_then(|body| serde_json::from_str::<Value>(body).ok());
    serde_json::json!({
        "message": error.to_string(),
        "status": error.status(),
        "server": server,
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

impl OperationOutcome {
    /// Returns the structured HTTP status from a sanitized rejection response.
    #[must_use]
    pub fn http_status(&self) -> Option<u16> {
        self.response
            .as_ref()?
            .get("status")?
            .as_u64()
            .and_then(|status| u16::try_from(status).ok())
    }

    /// Returns the structured server error from a sanitized rejection response.
    #[must_use]
    pub fn server_error(&self) -> Option<&str> {
        self.response
            .as_ref()?
            .get("server")?
            .get("error")?
            .as_str()
    }
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
        // Subscribe before the durable read so a transition between the two
        // cannot be missed. The read is still authoritative after reconnect.
        let mut watch = self.watch().await?;
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let outcome = self.outcome().await?;
            if outcome.status.is_terminal() || tokio::time::Instant::now() >= deadline {
                return Ok(outcome);
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            watch.wait_for_change(remaining).await;
        }
    }

    /// Explicitly reconciles this operation's target entity through its
    /// authoritative REST endpoint. This never resubmits the original
    /// mutation; it only refreshes local state and, if that refresh
    /// succeeds, resolves an operation that was awaiting evidence.
    pub async fn reconcile(&self) -> Result<OperationOutcome> {
        self.client.ensure_open()?;
        let entry = load(&self.client, &self.id)?;
        let current_status = OperationStatus::parse(&entry.state);
        if current_status == OperationStatus::ReconciliationRequired
            && entry.intent.get("kind").and_then(Value::as_str) == Some("device_retrieve")
        {
            if self.client.devices().refresh_many().collect().await.is_ok() {
                self.client
                    .managed_state()
                    .set_operation_state(self.id.as_str(), OperationStatus::Completed.as_str())
                    .map_err(persistence_error)?;
                notify(&self.client, &self.id, OperationStatus::Completed);
            }
            return self.outcome().await;
        }

        let snapshot = match (entry.target_kind.as_deref(), entry.target_id.as_deref()) {
            (Some("device"), Some(code)) if self.client.sync().device(code).await.is_ok() => self
                .client
                .managed_state()
                .device(&DeviceKey::live(DeviceId::from(code)))
                .and_then(|observation| to_value(observation.value).ok()),
            (Some("replicant"), Some(code)) if self.client.sync().replicant(code).await.is_ok() => {
                self.client
                    .managed_state()
                    .replicant(&domain::ReplicantKey::live(domain::ReplicantId::from(code)))
                    .and_then(|observation| to_value(observation.value).ok())
            }
            (Some("location"), Some(code)) if self.client.sync().location(code).await.is_ok() => {
                None
            }
            (Some("account"), _) if self.client.account().refresh().await.is_ok() => None,
            _ => None,
        };
        if !OperationStatus::parse(&entry.state).is_terminal() {
            let matches = snapshot.as_ref().is_some_and(|snapshot| {
                if entry.intent.get("kind").and_then(Value::as_str) == Some("device_configure") {
                    device_configuration_applied(&entry, snapshot)
                } else {
                    entry
                        .intent
                        .get("evidence")
                        .and_then(|evidence| evidence.get("expected_state"))
                        .is_some_and(|expected| {
                            !expected.is_null() && value_contains(snapshot, expected)
                        })
                }
            });
            let state = if matches {
                OperationStatus::Completed
            } else {
                OperationStatus::ReconciliationRequired
            };
            self.client
                .managed_state()
                .set_operation_state(self.id.as_str(), state.as_str())
                .map_err(persistence_error)?;
            notify(&self.client, &self.id, state);
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
    receiver: tokio::sync::broadcast::Receiver<(OperationId, OperationStatus)>,
    id: OperationId,
}

/// Local stream of status changes for every durable managed operation.
pub struct OperationsWatch {
    receiver: tokio::sync::broadcast::Receiver<(OperationId, OperationStatus)>,
}

impl OperationsWatch {
    /// Waits for the next managed operation status change.
    pub async fn next(&mut self) -> Result<(OperationId, OperationStatus)> {
        self.receiver.recv().await.map_err(|error| match error {
            tokio::sync::broadcast::error::RecvError::Closed => Error::Closed,
            tokio::sync::broadcast::error::RecvError::Lagged(skipped) => Error::Transport {
                message: format!("managed operation watcher lagged by {skipped} updates"),
                source: None,
            },
        })
    }
}

impl OperationWatch {
    /// Returns the latest status published for this operation since the last
    /// call, if any is available now.
    pub fn try_next(&mut self) -> Result<Option<OperationStatus>> {
        let mut latest = None;
        loop {
            match self.receiver.try_recv() {
                Ok((id, status)) if id == self.id => latest = Some(status),
                Ok(_) | Err(tokio::sync::broadcast::error::TryRecvError::Empty) => {
                    return Ok(latest);
                }
                Err(tokio::sync::broadcast::error::TryRecvError::Closed) => {
                    return Err(Error::Closed);
                }
                Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => return Ok(latest),
            }
        }
    }

    async fn wait_for_change(&mut self, timeout: Duration) {
        let _ = tokio::time::timeout(timeout, async {
            loop {
                match self.receiver.recv().await {
                    Ok((id, _)) if id == self.id => return,
                    Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                }
            }
        })
        .await;
    }
}

/// Subscriber registry for durable operation status changes. Owned by
/// `ClientInner`.
pub(crate) struct OperationEngine {
    subscribers: tokio::sync::broadcast::Sender<(OperationId, OperationStatus)>,
}

impl OperationEngine {
    pub(crate) fn new() -> Self {
        Self {
            subscribers: tokio::sync::broadcast::channel(256).0,
        }
    }

    pub(crate) fn subscribe(
        &self,
    ) -> tokio::sync::broadcast::Receiver<(OperationId, OperationStatus)> {
        self.subscribers.subscribe()
    }

    fn notify(&self, id: OperationId, status: OperationStatus) {
        let _ = self.subscribers.send((id, status));
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

    /// Watches local status changes for all durable managed operations.
    pub fn watch(&self) -> Result<OperationsWatch> {
        self.client.ensure_open()?;
        Ok(OperationsWatch {
            receiver: self.client.managed_operations().subscribe(),
        })
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

/// Durable account inbox projection.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MessageInbox {
    /// Messages ordered newest first.
    pub messages: Vec<Message>,
    /// Upstream incremental-sync cursor.
    pub last_cursor: Option<i64>,
    /// Account-wide unread count when supplied upstream.
    pub unread_count: Option<i64>,
    /// Last successful full pagination pass.
    pub refreshed_at: Option<ObservationTime>,
    /// Monotonically increasing durable inbox revision.
    pub revision: u64,
    /// Last bounded refresh failure, if any.
    pub last_error: Option<String>,
}

const MESSAGE_PAGE_SIZE: i64 = 100;
const MESSAGE_REFRESH_INTERVAL: Duration = Duration::from_secs(30);

impl MessagesGateway {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    /// Returns the durable inbox projection without network I/O.
    pub fn cached(&self) -> Result<MessageInbox> {
        self.client.ensure_open()?;
        let (messages, metadata) = self
            .client
            .managed_state()
            .messages()
            .map_err(persistence_error)?;
        Ok(MessageInbox {
            messages: messages
                .into_iter()
                .map(|observation| observation.value)
                .collect(),
            last_cursor: metadata.last_cursor,
            unread_count: metadata.unread_count,
            refreshed_at: metadata.refreshed_at,
            revision: metadata.revision,
            last_error: metadata.last_error,
        })
    }

    /// Returns the durable projection, refreshing it when its short freshness
    /// window has expired.
    pub async fn list(&self) -> Result<MessageInbox> {
        let cached = self.cached()?;
        let now = ObservationTime::now();
        if cached.refreshed_at.is_some_and(|refreshed_at| {
            now.unix_millis().saturating_sub(refreshed_at.unix_millis())
                < MESSAGE_REFRESH_INTERVAL.as_millis() as i64
        }) {
            return Ok(cached);
        }
        self.refresh().await
    }

    /// Explicitly refreshes every new inbox page and commits the complete
    /// staged result only after pagination succeeds.
    pub async fn refresh(&self) -> Result<MessageInbox> {
        self.client.ensure_open()?;
        let mut metadata = self
            .client
            .managed_state()
            .messages()
            .map_err(persistence_error)?
            .1;
        let mut cursor = metadata.last_cursor;
        let mut staged_messages = Vec::new();
        loop {
            let response = self
                .client
                .managed_raw()
                .messages()
                .list(&raw::messages::MessageListQuery {
                    cursor,
                    limit: Some(MESSAGE_PAGE_SIZE),
                    latest: None,
                    unread_only: None,
                })
                .await?;
            let next_cursor = response.value.next_cursor;
            metadata.unread_count = response
                .value
                .unread_message_count
                .or(metadata.unread_count);
            let observed_at = ObservationTime::now();
            let messages = response
                .value
                .messages
                .into_iter()
                .map(|message| domain::message(message, observed_at))
                .collect::<Vec<_>>();
            let page_last_id = messages.last().and_then(|message| message.value.id);
            staged_messages.extend(messages);
            metadata.last_cursor = next_cursor.or(page_last_id).or(metadata.last_cursor);

            let Some(next) = next_cursor else {
                break;
            };
            if cursor == Some(next) {
                warn!(
                    cursor = next,
                    "account message pagination cursor did not advance"
                );
                break;
            }
            cursor = Some(next);
        }
        metadata.refreshed_at = Some(ObservationTime::now());
        metadata.last_error = None;
        self.client
            .managed_state()
            .commit_messages_and_metadata(&staged_messages, metadata)
            .map_err(persistence_error)?;
        self.cached()
    }

    /// Records a bounded refresh error without changing committed messages or
    /// their cursor, unread count, refresh timestamp, or revision.
    pub fn record_refresh_failure(&self, message: &str) -> Result<MessageInbox> {
        self.client.ensure_open()?;
        self.client
            .managed_state()
            .persist_message_error(message)
            .map_err(persistence_error)?;
        self.cached()
    }

    /// Applies a confirmed read mutation to the durable projection.
    pub fn mark_cached_read(&self, ids: &[i64], mark_all: bool) -> Result<MessageInbox> {
        self.client.ensure_open()?;
        let (messages, mut metadata) = self
            .client
            .managed_state()
            .messages()
            .map_err(persistence_error)?;
        let mut changed = Vec::new();
        let mut newly_read = 0_i64;
        for mut message in messages {
            if mark_all || message.value.id.is_some_and(|id| ids.contains(&id)) {
                if message.value.is_read == Some(false) {
                    newly_read += 1;
                }
                if message.value.is_read != Some(true) {
                    message.value.is_read = Some(true);
                    changed.push(message);
                }
            }
        }
        metadata.unread_count = if mark_all {
            Some(0)
        } else {
            metadata
                .unread_count
                .map(|count| count.saturating_sub(newly_read))
        };
        self.client
            .managed_state()
            .commit_messages_and_metadata(&changed, metadata)
            .map_err(persistence_error)?;
        self.cached()
    }

    /// Marks one, several, or all inbox messages read.
    pub async fn mark_read(
        &self,
        request: raw::messages::MessagesReadRequest,
    ) -> Result<Operation> {
        create(&self.client, MutationAdapter::MessagesMarkRead { request }).await
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

    /// Returns a committed live-realm location without network I/O.
    #[must_use]
    pub fn cached(&self, designation: &str) -> Option<domain::Location> {
        self.client
            .managed_state()
            .location(&LocationKey::live(LocationId::from(designation)))
            .map(|observation| observation.value)
    }

    /// Starts a local-only query over committed location state.
    #[must_use]
    pub fn find(&self) -> super::gateways::LocationQuery {
        super::gateways::LocationQuery::new(self.client.clone())
    }

    /// Returns every retained resource-site projection without network I/O.
    pub fn resource_sites(&self) -> Result<Vec<domain::ResourceSite>> {
        self.client.ensure_open()?;
        let mut sites = self
            .client
            .managed_state()
            .resource_sites()
            .map_err(persistence_error)?
            .into_iter()
            .map(|observation| observation.value)
            .collect::<Vec<_>>();
        sites.sort_by(|left, right| left.key.cmp(&right.key));
        Ok(sites)
    }

    /// Returns every retained location-event projection without network I/O.
    pub fn location_events(&self) -> Result<Vec<domain::LocationEvent>> {
        self.client.ensure_open()?;
        let mut events = self
            .client
            .managed_state()
            .location_events()
            .map_err(persistence_error)?
            .into_iter()
            .map(|observation| observation.value)
            .collect::<Vec<_>>();
        events.sort_by(|left, right| left.key.cmp(&right.key));
        Ok(events)
    }

    /// Returns every retained incoming-object projection without network I/O.
    pub fn incoming_objects(&self) -> Result<Vec<domain::IncomingObject>> {
        self.client.ensure_open()?;
        let mut objects = self
            .client
            .managed_state()
            .incoming_objects()
            .map_err(persistence_error)?
            .into_iter()
            .map(|observation| observation.value)
            .collect::<Vec<_>>();
        objects.sort_by(|left, right| left.key.cmp(&right.key));
        Ok(objects)
    }

    async fn get_scoped(
        &self,
        designation: &str,
        replicant_code: Option<&str>,
    ) -> Result<domain::Location> {
        self.client.ensure_open()?;
        let response = self
            .client
            .managed_raw()
            .locations()
            .get(designation, replicant_code)
            .await?;
        let observation = domain::location_detail(
            &response.value,
            Realm::Live,
            crate::domain::ObservationTime::now(),
        )
        .map_err(|error| Error::Decode {
            message: error.to_string(),
            status: None,
            source: None,
        })?;
        let value = observation.value.clone();
        self.client
            .managed_state()
            .persist_location(observation)
            .map_err(persistence_error)?;
        Ok(self.cached(designation).unwrap_or(value))
    }

    /// Fetches, normalizes, commits, and publishes one unscoped location.
    pub async fn get(&self, designation: &str) -> Result<domain::Location> {
        self.get_scoped(designation, None).await
    }

    /// Fetches one location with replicant-relative fields such as travel
    /// estimates and account-specific system survey progress, then commits the
    /// normalized location before returning it.
    pub async fn get_for_replicant(
        &self,
        designation: &str,
        replicant_code: &str,
    ) -> Result<domain::Location> {
        self.get_scoped(designation, Some(replicant_code)).await
    }

    /// Alias for [`Self::get`]; remote I/O remains explicit.
    pub async fn refresh(&self, designation: &str) -> Result<domain::Location> {
        self.get(designation).await
    }

    /// Contributes devices' resources toward a location's active
    /// megastructure or location event.
    pub async fn contribute(&self, designation: &str, devices: Vec<String>) -> Result<Operation> {
        create(
            &self.client,
            MutationAdapter::LocationContribute {
                designation: designation.to_owned(),
                request: raw::locations::LocationContributionRequest { devices },
            },
        )
        .await
    }

    /// Builds an explicit, safe-read-only traversal of one explored system.
    #[must_use]
    pub fn hydrate_system(&self, star_designation: impl Into<String>) -> LocationHydration {
        LocationHydration {
            client: self.client.clone(),
            root: star_designation.into(),
            scope: LocationHydrationScope::AllKnownObjects,
            max_locations: 4096,
            max_depth: 64,
            concurrency: 1,
        }
    }
}

/// One location that could not be fetched during a hydration run.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocationHydrationFailure {
    /// The requested location designation.
    pub designation: String,
    /// Sanitized request or decoding failure.
    pub message: String,
}

/// Durable progress from a safe system-hydration traversal.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LocationHydrationReport {
    locations_committed: usize,
    maximum_reached: bool,
    failures: Vec<LocationHydrationFailure>,
    unknown_designations: BTreeSet<String>,
}
impl LocationHydrationReport {
    /// Successful location details committed before completion.
    #[must_use]
    pub fn locations_committed(&self) -> usize {
        self.locations_committed
    }
    /// Whether configured depth or location bounds stopped traversal.
    #[must_use]
    pub fn maximum_reached(&self) -> bool {
        self.maximum_reached
    }
    /// Per-location failures that preserve earlier committed observations.
    #[must_use]
    pub fn failures(&self) -> &[LocationHydrationFailure] {
        &self.failures
    }
    /// Documented child objects that lacked an identity field.
    #[must_use]
    pub fn unknown_designations(&self) -> &BTreeSet<String> {
        &self.unknown_designations
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LocationHydrationScope {
    AllKnownObjects,
    PlanetaryBodiesOnly,
}

/// Configures recursive hydration.  It never constructs a designation from a
/// count or naming convention; only designations present in documented fields
/// are queued.
#[derive(Clone, Debug)]
pub struct LocationHydration {
    client: Client,
    root: String,
    scope: LocationHydrationScope,
    max_locations: usize,
    max_depth: usize,
    concurrency: usize,
}
impl LocationHydration {
    /// Selects every documented designation-bearing child collection.
    #[must_use]
    pub fn all_known_objects(mut self) -> Self {
        self.scope = LocationHydrationScope::AllKnownObjects;
        self
    }
    /// Selects only planets and moons.
    ///
    /// This excludes embedded resource sites, system objects, devices, shops,
    /// belts, and parent references. It is appropriate for workflows that only
    /// need scannable planetary-body knowledge.
    #[must_use]
    pub fn planetary_bodies_only(mut self) -> Self {
        self.scope = LocationHydrationScope::PlanetaryBodiesOnly;
        self
    }
    /// Sets the maximum number of unique location requests.
    #[must_use]
    pub fn max_locations(mut self, value: usize) -> Self {
        self.max_locations = value.max(1);
        self
    }
    /// Sets the maximum parent-to-child traversal depth.
    #[must_use]
    pub fn max_depth(mut self, value: usize) -> Self {
        self.max_depth = value;
        self
    }
    /// Limits in-flight safe reads.  The current commit-before-next pipeline
    /// intentionally uses one request, which is within every requested bound.
    #[must_use]
    pub fn concurrency(mut self, value: usize) -> Self {
        self.concurrency = value.max(1);
        self
    }
    /// Runs the safe-read traversal, committing each successful detail.
    pub async fn run(self) -> Result<LocationHydrationReport> {
        self.client.ensure_open()?;
        let total_started = Instant::now();
        info!(
            target: "replicant_client::locations",
            event = "locations.hydration_started",
            root = %self.root,
            scope = ?self.scope,
            max_locations = self.max_locations,
            max_depth = self.max_depth,
            configured_concurrency = self.concurrency,
            effective_concurrency = 1_u8,
            "starting explored-system location hydration"
        );
        if self.concurrency > 1 {
            warn!(
                target: "replicant_client::locations",
                event = "locations.hydration_concurrency_not_applied",
                configured_concurrency = self.concurrency,
                effective_concurrency = 1_u8,
                "location hydration currently uses a serial commit-before-next pipeline"
            );
        }
        let mut queue = VecDeque::from([(self.root.clone(), 0_usize)]);
        let mut seen = BTreeSet::new();
        let mut report = LocationHydrationReport::default();
        while let Some((designation, depth)) = queue.pop_front() {
            if !seen.insert(designation.clone()) {
                debug!(
                    target: "replicant_client::locations",
                    event = "locations.hydration_duplicate_skipped",
                    designation = %designation,
                    depth,
                    "skipping already-seen location"
                );
                continue;
            }
            if seen.len() > self.max_locations {
                report.maximum_reached = true;
                warn!(
                    target: "replicant_client::locations",
                    event = "locations.hydration_location_bound_reached",
                    root = %self.root,
                    max_locations = self.max_locations,
                    seen = seen.len(),
                    "location hydration reached its configured object bound"
                );
                break;
            }
            if depth > self.max_depth {
                report.maximum_reached = true;
                warn!(
                    target: "replicant_client::locations",
                    event = "locations.hydration_depth_bound_reached",
                    designation = %designation,
                    depth,
                    max_depth = self.max_depth,
                    "location hydration skipped object beyond configured depth"
                );
                continue;
            }

            let item_started = Instant::now();
            debug!(
                target: "replicant_client::locations",
                event = "locations.hydration_location_started",
                designation = %designation,
                depth,
                queue_remaining = queue.len(),
                seen = seen.len(),
                "fetching location detail during system hydration"
            );
            let request_started = Instant::now();
            let response = match self
                .client
                .managed_raw()
                .locations()
                .get(&designation, None)
                .await
            {
                Ok(response) => response,
                Err(error) => {
                    warn!(
                        target: "replicant_client::locations",
                        event = "locations.hydration_location_failed",
                        designation = %designation,
                        depth,
                        elapsed_ms = item_started.elapsed().as_millis() as u64,
                        error = %error,
                        "location detail request failed during system hydration"
                    );
                    report.failures.push(LocationHydrationFailure {
                        designation,
                        message: error.to_string(),
                    });
                    continue;
                }
            };
            let request_elapsed = request_started.elapsed();
            let normalize_started = Instant::now();
            let observation = domain::location_detail(
                &response.value,
                Realm::Live,
                crate::domain::ObservationTime::now(),
            )
            .map_err(|error| Error::Decode {
                message: error.to_string(),
                status: None,
                source: None,
            })?;
            let normalize_elapsed = normalize_started.elapsed();
            let persist_started = Instant::now();
            self.client
                .managed_state()
                .persist_location(observation)
                .map_err(persistence_error)?;
            let persist_elapsed = persist_started.elapsed();
            report.locations_committed += 1;

            let extract_started = Instant::now();
            let children = verified_child_designations(
                &response.value,
                &mut report.unknown_designations,
                self.scope,
            );
            let child_count = children.len();
            for child in children {
                if !seen.contains(&child) {
                    queue.push_back((child, depth + 1));
                }
            }
            info!(
                target: "replicant_client::locations",
                event = "locations.hydration_location_completed",
                designation = %designation,
                depth,
                children = child_count,
                queue_size = queue.len(),
                committed_total = report.locations_committed,
                request_ms = request_elapsed.as_millis() as u64,
                normalize_ms = normalize_elapsed.as_millis() as u64,
                persist_ms = persist_elapsed.as_millis() as u64,
                child_extract_ms = extract_started.elapsed().as_millis() as u64,
                elapsed_ms = item_started.elapsed().as_millis() as u64,
                "location detail committed during system hydration"
            );
        }
        info!(
            target: "replicant_client::locations",
            event = "locations.hydration_completed",
            root = %self.root,
            locations_committed = report.locations_committed,
            failures = report.failures.len(),
            unknown_designations = report.unknown_designations.len(),
            maximum_reached = report.maximum_reached,
            elapsed_ms = total_started.elapsed().as_millis() as u64,
            "explored-system location hydration completed"
        );
        Ok(report)
    }
}

pub(super) fn object_designation(value: &raw::JsonObject) -> Option<String> {
    ["designation", "location", "code"]
        .into_iter()
        .find_map(|key| value.get(key).and_then(serde_json::Value::as_str))
        .map(ToOwned::to_owned)
}

fn verified_child_designations(
    location: &raw::locations::Location,
    unknown: &mut BTreeSet<String>,
    scope: LocationHydrationScope,
) -> Vec<String> {
    let mut result = BTreeSet::new();
    let mut add = |object: &raw::JsonObject| {
        if let Some(designation) = object_designation(object) {
            result.insert(designation);
        } else {
            unknown.insert("documented child without designation".into());
        }
    };

    for items in [&location.planets, &location.moons].into_iter().flatten() {
        for item in items {
            add(item);
        }
    }

    if scope == LocationHydrationScope::AllKnownObjects {
        for items in [
            &location.system_objects,
            &location.devices,
            &location.resource_sites,
            &location.shops,
        ]
        .into_iter()
        .flatten()
        {
            for item in items {
                add(item);
            }
        }
        for object in [
            &location.asteroid_belt,
            &location.kuiper,
            &location.lagrange,
            &location.oort,
            &location.outer_system,
            &location.object,
            &location.star,
            &location.belt,
        ]
        .into_iter()
        .flatten()
        {
            add(object);
        }
        if let Some(parent) = &location.parent {
            result.insert(parent.clone());
        }
    }

    result.into_iter().collect()
}

#[cfg(test)]
mod location_hydration_tests {
    use super::*;

    #[test]
    fn child_extraction_uses_documented_designations_not_counts() {
        let location = raw::locations::Location {
            planets: Some(vec![
                serde_json::json!({"designation": "SOL-2"})
                    .as_object()
                    .unwrap()
                    .clone(),
            ]),
            resource_sites: Some(vec![
                serde_json::json!({"designation": "SOL-2-SAL-1"})
                    .as_object()
                    .unwrap()
                    .clone(),
            ]),
            system_objects: Some(vec![
                serde_json::json!({"designation": "SOL-OBJ-1"})
                    .as_object()
                    .unwrap()
                    .clone(),
            ]),
            moons_total: Some(99),
            ..Default::default()
        };
        let mut unknown = BTreeSet::new();
        assert_eq!(
            verified_child_designations(
                &location,
                &mut unknown,
                LocationHydrationScope::AllKnownObjects,
            ),
            vec!["SOL-2", "SOL-2-SAL-1", "SOL-OBJ-1"]
        );
        assert!(unknown.is_empty());
    }

    #[test]
    fn planetary_body_scope_excludes_unscannable_children() {
        let location = raw::locations::Location {
            planets: Some(vec![
                serde_json::json!({"designation": "XIKKKUX-1"})
                    .as_object()
                    .unwrap()
                    .clone(),
            ]),
            moons: Some(vec![
                serde_json::json!({"designation": "XIKKKUX-1-1"})
                    .as_object()
                    .unwrap()
                    .clone(),
            ]),
            resource_sites: Some(vec![
                serde_json::json!({"designation": "XIKKKUX-1-1-SAL-1"})
                    .as_object()
                    .unwrap()
                    .clone(),
            ]),
            system_objects: Some(vec![
                serde_json::json!({"designation": "XIKKKUX-OBJ-1"})
                    .as_object()
                    .unwrap()
                    .clone(),
            ]),
            ..Default::default()
        };
        let mut unknown = BTreeSet::new();

        assert_eq!(
            verified_child_designations(
                &location,
                &mut unknown,
                LocationHydrationScope::PlanetaryBodiesOnly,
            ),
            vec!["XIKKKUX-1", "XIKKKUX-1-1"]
        );
        assert!(unknown.is_empty());
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
            MutationAdapter::LocationEventResolve {
                location_code: location_code.to_owned(),
                designation: designation.to_owned(),
            },
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
        let device = &observation.value;
        let known = &device.available_commands;
        if !known.is_empty() && !known.iter().any(|command| command.as_str() == command_name) {
            return Err(Error::Operation {
                message: format!(
                    "device `{device_code}` does not currently advertise capability for `{command_name}`"
                ),
            });
        }
        if device_command_requires_star_system(command_name)
            && !device_command_has_stationary_star_system(client, device)
        {
            return Err(Error::Operation {
                message: format!(
                    "device `{device_code}` is not currently projected as stationary in a star system; refusing `{command_name}` until managed state catches up"
                ),
            });
        }
    }
    Ok(())
}

fn device_command_has_stationary_star_system(client: &Client, device: &domain::Device) -> bool {
    if device.location.is_some() && device.travel.is_none() {
        return true;
    }

    // A stowed device intentionally has no direct location of its own. Commands
    // such as AMI `launch` execute in the physical context of the stow
    // container, so a stationary container is authoritative for this preflight.
    let Some(container) = device.relationships.stowed_in.as_ref() else {
        return false;
    };
    let key = DeviceKey::live(DeviceId::from(container.id.as_str()));
    client
        .managed_state()
        .device(&key)
        .is_some_and(|observation| {
            observation.value.location.is_some() && observation.value.travel.is_none()
        })
}

fn device_command_requires_star_system(command_name: &str) -> bool {
    matches!(
        command_name,
        "activate"
            | "collect_resources"
            | "deploy"
            | "deposit_resources"
            | "launch"
            | "prospect"
            | "scan"
            | "search"
            | "set_entry_point"
            | "start_mining"
            | "system_scan"
            | "travel"
            | "triangulate"
            | "unfurl"
    )
}

/// Device commands whose own response documents a still-running,
/// asynchronously-completing process (a `completes_at`/`eta_seconds` field),
/// per the response shapes in `crate::raw::devices::DeviceCommandResponse`.
/// Every other command's response is already the complete, final outcome.
fn device_command_expects_evidence(command: &raw::devices::DeviceCommand) -> bool {
    match command {
        raw::devices::DeviceCommand::Travel { dry_run, .. } => dry_run != &Some(true),
        _ => matches!(
            command,
            raw::devices::DeviceCommand::Compact
                | raw::devices::DeviceCommand::EnqueuePrint { .. }
                | raw::devices::DeviceCommand::Prospect { .. }
                | raw::devices::DeviceCommand::Repair { .. }
                | raw::devices::DeviceCommand::Scan
                | raw::devices::DeviceCommand::SystemScan
                | raw::devices::DeviceCommand::Triangulate { .. }
                | raw::devices::DeviceCommand::Unfurl
        ),
    }
}

fn device_command_response_completes(command: &raw::devices::DeviceCommand) -> bool {
    matches!(
        command,
        raw::devices::DeviceCommand::Deploy | raw::devices::DeviceCommand::Stow { .. }
    )
}

fn operation_evidence(adapter: &MutationAdapter) -> Value {
    fn events(names: &[&str], failures: &[&str], payload: Value) -> Value {
        serde_json::json!({
            "event_names": names,
            "failure_event_names": failures,
            "payload": payload,
            "expected_state": null
        })
    }
    let default = || events(&[], &[], serde_json::json!({}));

    match adapter {
        MutationAdapter::ReplicantUpdate { request, .. } => serde_json::json!({
            "event_names": [],
            "failure_event_names": [],
            "payload": {},
            "expected_state": request.name.as_ref().map_or(Value::Null, |name| {
                serde_json::json!({"name": name})
            })
        }),
        MutationAdapter::DeviceCommand { command, .. } => match command {
            raw::devices::DeviceCommand::Activate => events(
                &[
                    "diversion.activated",
                    "hub.activated",
                    "relay.activated",
                    "ward.activated",
                ],
                &[],
                serde_json::json!({}),
            ),
            raw::devices::DeviceCommand::Assemble => {
                events(&["ami.assembled"], &[], serde_json::json!({}))
            }
            raw::devices::DeviceCommand::Adopt(_) => {
                events(&["ami.adopted"], &[], serde_json::json!({}))
            }
            raw::devices::DeviceCommand::Attach(_) => {
                events(&["device.attached"], &[], serde_json::json!({}))
            }
            raw::devices::DeviceCommand::ChangeOwner { target } => events(
                &["device.changed_owner"],
                &[],
                serde_json::json!({"to_replicant": target}),
            ),
            raw::devices::DeviceCommand::ClearDirective => {
                events(&["directive.cleared"], &[], serde_json::json!({}))
            }
            raw::devices::DeviceCommand::CollectResources { resources } => events(
                &["transport.collected"],
                &[],
                serde_json::json!({"resources": resources}),
            ),
            raw::devices::DeviceCommand::Compact => {
                events(&["device.compacted"], &[], serde_json::json!({}))
            }
            raw::devices::DeviceCommand::Decommission => {
                events(&["device.decommissioned"], &[], serde_json::json!({}))
            }
            raw::devices::DeviceCommand::Deploy => {
                events(&["device.deployed"], &[], serde_json::json!({}))
            }
            raw::devices::DeviceCommand::DepositResources { resources } => events(
                &["transport.delivered"],
                &[],
                resources.as_ref().map_or_else(
                    || serde_json::json!({}),
                    |resources| serde_json::json!({"resources": resources}),
                ),
            ),
            raw::devices::DeviceCommand::Detach(_) => {
                events(&["device.detached"], &[], serde_json::json!({}))
            }
            raw::devices::DeviceCommand::EnqueuePrint { device_type, .. } => events(
                &["print.completed"],
                &[],
                serde_json::json!({"device_type": device_type}),
            ),
            raw::devices::DeviceCommand::Launch => {
                events(&["ami.launched"], &[], serde_json::json!({}))
            }
            raw::devices::DeviceCommand::Message { channel, text } => events(
                &["bobnet.new"],
                &[],
                serde_json::json!({"channel": channel, "message": text}),
            ),
            raw::devices::DeviceCommand::Prospect { .. } => {
                events(&["prospect.completed"], &[], serde_json::json!({}))
            }
            raw::devices::DeviceCommand::Recall | raw::devices::DeviceCommand::Stow { .. } => {
                events(&["device.stowed"], &[], serde_json::json!({}))
            }
            raw::devices::DeviceCommand::Release(_) => {
                events(&["ami.released"], &[], serde_json::json!({}))
            }
            raw::devices::DeviceCommand::Rename { designation, name } => events(
                &["system.body_renamed"],
                &[],
                serde_json::json!({"designation": designation, "new_name": name}),
            ),
            raw::devices::DeviceCommand::Retarget { resource_type } => events(
                &["mining.retargeted"],
                &[],
                serde_json::json!({"new_resource": resource_type}),
            ),
            raw::devices::DeviceCommand::Scan => {
                events(&["scan.completed"], &[], serde_json::json!({}))
            }
            raw::devices::DeviceCommand::Search => {
                events(&["search.completed"], &[], serde_json::json!({}))
            }
            raw::devices::DeviceCommand::SetDirective { directive, .. } => events(
                &["directive.set"],
                &[],
                serde_json::json!({"directive": directive}),
            ),
            raw::devices::DeviceCommand::SetEntryPoint => {
                events(&["system.entry_point_set"], &[], serde_json::json!({}))
            }
            raw::devices::DeviceCommand::StartMining { resource_type, .. } => events(
                &["mining.started"],
                &[],
                serde_json::json!({"resource_type": resource_type}),
            ),
            raw::devices::DeviceCommand::Travel {
                destination,
                dry_run,
                ..
            } if dry_run != &Some(true) => events(
                &["travel.arrived"],
                &[],
                serde_json::json!({"destination": destination}),
            ),
            raw::devices::DeviceCommand::Triangulate { signature, target } => events(
                &["triangulation.complete"],
                &["triangulation.failed"],
                serde_json::json!({"signature": signature, "target": target}),
            ),
            raw::devices::DeviceCommand::Unfurl => {
                events(&["device.unfurled"], &[], serde_json::json!({}))
            }
            raw::devices::DeviceCommand::Withdraw => {
                events(&["ami.withdrawn"], &[], serde_json::json!({}))
            }
            // The 2.5.1 event catalogue documents no unambiguous outcome event
            // or managed target state for the remaining commands.
            _ => default(),
        },
        MutationAdapter::DeviceCreateTrade { .. } => {
            events(&["trade.created"], &[], serde_json::json!({}))
        }
        MutationAdapter::DeviceDeleteTrade { trade_code, .. } => events(
            &["trade.deleted"],
            &[],
            serde_json::json!({"trade_code": trade_code}),
        ),
        MutationAdapter::DeviceFulfillTrade { trade_code, .. } => events(
            &["trade.completed"],
            &[],
            serde_json::json!({"trade_code": trade_code}),
        ),
        MutationAdapter::ReplicantMine { request, .. } => events(
            &["mining.started"],
            &[],
            serde_json::json!({"resource_type": request.resource_type}),
        ),
        MutationAdapter::ReplicantStopMining { .. } => {
            events(&["mining.stopped"], &[], serde_json::json!({}))
        }
        MutationAdapter::ReplicantPrint { request, .. } => {
            match (request.command.as_deref(), request.device_type.as_deref()) {
                (None, Some(device_type)) => events(
                    &["print.completed"],
                    &[],
                    serde_json::json!({"device_type": device_type}),
                ),
                _ => default(),
            }
        }
        MutationAdapter::ReplicantTeleport { request, .. } => events(
            &["teleport.completed"],
            &["teleport.failed"],
            serde_json::json!({"target_matrix_code": request.target}),
        ),
        MutationAdapter::ReplicantTravel { request, .. } => {
            match (request.dry_run, request.destination.as_deref()) {
                (Some(true), _) | (_, None) => default(),
                (_, Some(destination)) => events(
                    &["travel.arrived"],
                    &[],
                    serde_json::json!({"destination": destination}),
                ),
            }
        }
        MutationAdapter::ReplicantCancelTravel { .. } => {
            events(&["travel.cancelled"], &[], serde_json::json!({}))
        }
        // The 2.5.1 event catalogue documents no unambiguous outcome event
        // or managed target state for the remaining adapters.
        _ => default(),
    }
}

pub(crate) async fn device_command(
    client: &Client,
    device_code: &str,
    command: raw::devices::DeviceCommand,
) -> Result<Operation> {
    let body = to_value(&command)?;
    let command_name = body
        .get("command")
        .and_then(Value::as_str)
        .unwrap_or_default();
    check_device_capability(client, device_code, command_name)?;
    create(
        client,
        MutationAdapter::DeviceCommand {
            device_code: device_code.to_owned(),
            command,
        },
    )
    .await
}

/// Dispatches a device command under a caller-supplied durable operation
/// identity. Reusing the identity is safe: the durable store compares the
/// complete target and sanitized command intent before allowing an existing
/// operation to be observed or a prepared operation submitted.
pub(crate) async fn device_command_with_id(
    client: &Client,
    device_code: &str,
    command: raw::devices::DeviceCommand,
    operation_id: OperationId,
) -> Result<Operation> {
    let body = to_value(&command)?;
    let command_name = body
        .get("command")
        .and_then(Value::as_str)
        .unwrap_or_default();
    check_device_capability(client, device_code, command_name)?;
    create_with_id(
        client,
        MutationAdapter::DeviceCommand {
            device_code: device_code.to_owned(),
            command,
        },
        Some(operation_id),
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
        MutationAdapter::DeviceDynamicCommand {
            device_code: device_code.to_owned(),
            command: body,
        },
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
        MutationAdapter::DeviceConfigure {
            device_code: device_code.to_owned(),
            request,
        },
    )
    .await
}

/// Dispatches a device configuration under a caller-supplied durable
/// operation identity. Reusing the identity is safe: the durable store
/// compares the complete target and sanitized request intent before allowing
/// an existing operation to be observed or a prepared operation submitted.
pub(crate) async fn device_configure_with_id(
    client: &Client,
    device_code: &str,
    request: raw::devices::DeviceConfigurationRequest,
    operation_id: OperationId,
) -> Result<Operation> {
    create_with_id(
        client,
        MutationAdapter::DeviceConfigure {
            device_code: device_code.to_owned(),
            request,
        },
        Some(operation_id),
    )
    .await
}

pub(crate) async fn device_retrieve(client: &Client, device_code: &str) -> Result<Operation> {
    create(
        client,
        MutationAdapter::DeviceRetrieve {
            device_code: device_code.to_owned(),
        },
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
        MutationAdapter::DeviceGrantPermission {
            device_code: device_code.to_owned(),
            request,
        },
    )
    .await
}

pub(crate) async fn device_revoke_permission(
    client: &Client,
    device_code: &str,
) -> Result<Operation> {
    create(
        client,
        MutationAdapter::DeviceRevokePermission {
            device_code: device_code.to_owned(),
        },
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
        MutationAdapter::DeviceEnterSimulation {
            device_code: device_code.to_owned(),
            request,
        },
    )
    .await
}

pub(crate) async fn device_abandon_simulation(
    client: &Client,
    device_code: &str,
    simulation_id: i64,
) -> Result<Operation> {
    let operation = create(
        client,
        MutationAdapter::DeviceAbandonSimulation {
            device_code: device_code.to_owned(),
            simulation_id,
        },
    )
    .await?;
    client
        .managed_state()
        .enqueue_reconciliation(
            &format!("simulation:{simulation_id}:active"),
            &domain::Realm::Simulation(crate::domain::SimulationId::new(simulation_id)),
            "simulation",
            &serde_json::json!({ "id": simulation_id, "interface_code": device_code }),
        )
        .map_err(persistence_error)?;
    Ok(operation)
}

pub(crate) async fn device_create_trade(
    client: &Client,
    device_code: &str,
    request: raw::JsonObject,
) -> Result<Operation> {
    create(
        client,
        MutationAdapter::DeviceCreateTrade {
            device_code: device_code.to_owned(),
            request,
        },
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
        MutationAdapter::DeviceDeleteTrade {
            device_code: device_code.to_owned(),
            trade_code: trade_code.to_owned(),
        },
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
        MutationAdapter::DeviceFulfillTrade {
            device_code: device_code.to_owned(),
            trade_code: trade_code.to_owned(),
        },
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
        MutationAdapter::ReplicantUpdate {
            replicant_code: replicant_code.to_owned(),
            request,
        },
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
        MutationAdapter::ReplicantMessage {
            replicant_code: replicant_code.to_owned(),
            request,
        },
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
        MutationAdapter::ReplicantMine {
            replicant_code: replicant_code.to_owned(),
            request,
        },
    )
    .await
}

pub(crate) async fn replicant_stop_mining(
    client: &Client,
    replicant_code: &str,
) -> Result<Operation> {
    create(
        client,
        MutationAdapter::ReplicantStopMining {
            replicant_code: replicant_code.to_owned(),
        },
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
        MutationAdapter::ReplicantPrint {
            replicant_code: replicant_code.to_owned(),
            request,
        },
    )
    .await
}

pub(crate) async fn replicant_scan(client: &Client, replicant_code: &str) -> Result<Operation> {
    create(
        client,
        MutationAdapter::ReplicantScan {
            replicant_code: replicant_code.to_owned(),
        },
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
        MutationAdapter::ReplicantTeleport {
            replicant_code: replicant_code.to_owned(),
            request,
        },
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
        MutationAdapter::ReplicantTransfer {
            replicant_code: replicant_code.to_owned(),
            request,
        },
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
        MutationAdapter::ReplicantTravel {
            replicant_code: replicant_code.to_owned(),
            request,
        },
    )
    .await
}

/// Dispatches Replicant travel under a caller-supplied durable operation
/// identity for restart-safe workflow execution.
pub(crate) async fn replicant_travel_with_id(
    client: &Client,
    replicant_code: &str,
    request: raw::replicants::TravelRequest,
    operation_id: OperationId,
) -> Result<Operation> {
    create_with_id(
        client,
        MutationAdapter::ReplicantTravel {
            replicant_code: replicant_code.to_owned(),
            request,
        },
        Some(operation_id),
    )
    .await
}

pub(crate) async fn replicant_cancel_travel(
    client: &Client,
    replicant_code: &str,
) -> Result<Operation> {
    create(
        client,
        MutationAdapter::ReplicantCancelTravel {
            replicant_code: replicant_code.to_owned(),
        },
    )
    .await
}

pub(crate) async fn account_update(
    client: &Client,
    request: raw::accounts::AccountUpdateRequest,
) -> Result<Operation> {
    create(client, MutationAdapter::AccountUpdate { request }).await
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
    create(client, MutationAdapter::AccountWipe).await
}

/// The common entry point for every durable mutation: persists sanitized
/// intent, then performs the one automatic submission attempt.
/// Uses a fresh random operation identity for ordinary callers.
async fn create(client: &Client, adapter: MutationAdapter) -> Result<Operation> {
    create_with_id(client, adapter, None).await
}

/// The common entry point for every durable mutation: persists sanitized
/// intent, then performs the one automatic submission attempt. A supplied
/// identity is used by restart-safe workflows that must resume the exact
/// mutation after a crash.
async fn create_with_id(
    client: &Client,
    adapter: MutationAdapter,
    supplied_id: Option<OperationId>,
) -> Result<Operation> {
    client.ensure_open()?;
    let total_started = Instant::now();
    let id = supplied_id.unwrap_or_else(|| OperationId::new(Uuid::new_v4().to_string()));
    let operation_kind = adapter.operation_id();
    info!(
        target: "replicant_client::ops",
        event = "operation.create_started",
        operation_id = %id.as_str(),
        operation_kind,
        "creating durable operation"
    );
    let intent_started = Instant::now();
    let mut intent = adapter.durable_intent()?;
    let intent_object = intent.as_object_mut().ok_or_else(|| Error::Operation {
        message: "typed operation intent did not serialize to an object".into(),
    })?;
    intent_object.insert(
        "expects_evidence".into(),
        Value::Bool(adapter.expects_evidence()),
    );
    intent_object.insert("evidence".into(), operation_evidence(&adapter));
    intent_object.insert(
        "rate_limit_bucket".into(),
        Value::String(operation_kind.to_owned()),
    );
    let target = adapter.target();
    let (target_realm, target_kind, target_id): (Option<String>, Option<&str>, Option<String>) =
        match &target {
            Some((kind, id)) => {
                let realm = if kind == &"simulation" {
                    format!("simulation:{id}")
                } else {
                    "live".to_owned()
                };
                (Some(realm), Some(*kind), Some(id.clone()))
            }
            None => (None, None, None),
        };
    let intent_elapsed = intent_started.elapsed();
    let persist_started = Instant::now();
    let existing = client
        .managed_state()
        .record_operation_if_absent(
            id.as_str(),
            OperationStatus::Prepared.as_str(),
            target_realm.as_deref(),
            target_kind,
            target_id.as_deref(),
            &intent,
        )
        .map_err(persistence_error)?;
    if let Some(existing) = existing {
        let identity_matches = existing.target_realm.as_deref() == target_realm.as_deref()
            && existing.target_kind.as_deref() == target_kind
            && existing.target_id.as_deref() == target_id.as_deref()
            && existing.intent == intent;
        if !identity_matches {
            return Err(Error::Operation {
                message: format!(
                    "operation ID collision for {}: existing target or intent differs",
                    id.as_str()
                ),
            });
        }
        info!(
            target: "replicant_client::ops",
            event = "operation.reused",
            operation_id = %id.as_str(),
            operation_kind,
            state = %existing.state,
            "reusing matching durable operation"
        );
    } else {
        info!(
            target: "replicant_client::ops",
            event = "operation.registered",
            operation_id = %id.as_str(),
            operation_kind,
            intent_ms = intent_elapsed.as_millis() as u64,
            persist_ms = persist_started.elapsed().as_millis() as u64,
            "durably registered operation"
        );
    }
    let attempt_started = Instant::now();
    attempt(client, &id).await?;
    info!(
        target: "replicant_client::ops",
        event = "operation.create_completed",
        operation_id = %id.as_str(),
        operation_kind,
        submission_ms = attempt_started.elapsed().as_millis() as u64,
        elapsed_ms = total_started.elapsed().as_millis() as u64,
        "durable operation creation completed"
    );
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
async fn attempt(client: &Client, id: &OperationId) -> Result<()> {
    let total_started = Instant::now();
    let load_started = Instant::now();
    let Some(entry) = client
        .managed_state()
        .read_operation(id.as_str())
        .map_err(persistence_error)?
    else {
        return Err(Error::Operation {
            message: "operation disappeared before submission".into(),
        });
    };
    let load_elapsed = load_started.elapsed();
    if entry.state != OperationStatus::Prepared.as_str() {
        debug!(
            target: "replicant_client::ops",
            event = "operation.submission_skipped",
            operation_id = %id.as_str(),
            state = %entry.state,
            load_ms = load_elapsed.as_millis() as u64,
            "operation was not in prepared state"
        );
        return Ok(());
    }
    let adapter: MutationAdapter =
        serde_json::from_value(entry.intent.clone()).map_err(|error| Error::Operation {
            message: format!("invalid typed durable operation intent: {error}"),
        })?;
    let expects_evidence = entry
        .intent
        .get("expects_evidence")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    let claim_started = Instant::now();
    let claimed = client
        .managed_state()
        .claim_operation_submission(id.as_str(), &Uuid::new_v4().to_string())
        .map_err(persistence_error)?;
    let claim_elapsed = claim_started.elapsed();
    if !claimed {
        warn!(
            target: "replicant_client::ops",
            event = "operation.submission_claim_lost",
            operation_id = %id.as_str(),
            claim_ms = claim_elapsed.as_millis() as u64,
            elapsed_ms = total_started.elapsed().as_millis() as u64,
            "skipping unclaimed durable operation submission"
        );
        return Ok(());
    }
    info!(
        target: "replicant_client::ops",
        event = "operation.submission_started",
        operation_id = %id.as_str(),
        operation_kind = adapter.operation_id(),
        load_ms = load_elapsed.as_millis() as u64,
        claim_ms = claim_elapsed.as_millis() as u64,
        "submitting durable operation"
    );
    notify(client, id, OperationStatus::Submitted);

    let submit_started = Instant::now();
    match adapter.submit(client.managed_raw()).await {
        Err(error) if error.is_ambiguous_mutation_outcome() => {
            warn!(
                target: "replicant_client::ops",
                event = "operation.submission_ambiguous",
                operation_id = %id.as_str(),
                submit_ms = submit_started.elapsed().as_millis() as u64,
                elapsed_ms = total_started.elapsed().as_millis() as u64,
                error = %error,
                "durable operation submission outcome is ambiguous"
            );
            client
                .managed_state()
                .set_operation_state(id.as_str(), OperationStatus::Ambiguous.as_str())
                .map_err(persistence_error)?;
            notify(client, id, OperationStatus::Ambiguous);
        }
        Err(error) => {
            warn!(
                target: "replicant_client::ops",
                event = "operation.submission_rejected",
                operation_id = %id.as_str(),
                submit_ms = submit_started.elapsed().as_millis() as u64,
                elapsed_ms = total_started.elapsed().as_millis() as u64,
                error = %error,
                "durable operation submission was rejected"
            );
            client
                .managed_state()
                .append_operation_projection(
                    id.as_str(),
                    OperationStatus::Rejected.as_str(),
                    &sanitize_error(&error),
                )
                .map_err(persistence_error)?;
            notify(client, id, OperationStatus::Rejected);
        }
        Ok(response) => {
            // Most mutations still require normalized target evidence after a
            // successful HTTP response. Deploy and stow return the complete
            // final placement in their typed command response, so they do not
            // need a second event to become terminal. The account inbox likewise
            // has no managed target projection to reconcile, and
            // POST /v1/messages/read returns the authoritative result of that
            // targetless mutation.
            let response_completes = matches!(&adapter, MutationAdapter::MessagesMarkRead { .. })
                || matches!(
                    &adapter,
                    MutationAdapter::DeviceCommand { command, .. }
                        if device_command_response_completes(command)
                );
            let next = if response_completes {
                OperationStatus::Completed
            } else if expects_evidence {
                OperationStatus::AwaitingEvidence
            } else {
                OperationStatus::ReconciliationRequired
            };
            info!(
                target: "replicant_client::ops",
                event = "operation.submission_accepted",
                operation_id = %id.as_str(),
                next_state = next.as_str(),
                submit_ms = submit_started.elapsed().as_millis() as u64,
                elapsed_ms = total_started.elapsed().as_millis() as u64,
                "durable operation submission accepted"
            );
            client
                .managed_state()
                .append_operation_projection(id.as_str(), next.as_str(), &response)
                .map_err(persistence_error)?;
            notify(client, id, next);

            if matches!(adapter, MutationAdapter::DeviceRetrieve { .. }) {
                // The retrieve response does not identify the newly granted
                // slingshot. A complete, unfiltered device traversal is the
                // authoritative post-mutation observation. This safe read may
                // be retried normally, but the one-time retrieve POST above is
                // never replayed because a refresh fails.
                match client.devices().refresh_many().collect().await {
                    Ok(_) => {
                        client
                            .managed_state()
                            .set_operation_state(id.as_str(), OperationStatus::Completed.as_str())
                            .map_err(persistence_error)?;
                        notify(client, id, OperationStatus::Completed);
                    }
                    Err(error) => {
                        warn!(
                            target: "replicant_client::ops",
                            event = "device.retrieve_refresh_failed",
                            operation_id = %id.as_str(),
                            error = %error,
                            "equipment retrieval was accepted but the authoritative device refresh failed"
                        );
                    }
                }
            } else if let (Some(target_realm), Some(target_kind), Some(target_id)) = (
                entry.target_realm.as_deref(),
                entry.target_kind.as_deref(),
                entry.target_id.as_deref(),
            ) {
                schedule_target_reconciliation(client, target_realm, target_kind, target_id)?;
            }
        }
    }
    Ok(())
}

fn schedule_target_reconciliation(
    client: &Client,
    target_realm: &str,
    target_kind: &str,
    target_id: &str,
) -> Result<()> {
    let realm = target_realm
        .strip_prefix("simulation:")
        .and_then(|id| id.parse().ok())
        .map(crate::domain::SimulationId::new)
        .map(domain::Realm::Simulation)
        .unwrap_or(domain::Realm::Live);
    let work_id = format!("operation:{target_kind}:{target_id}");
    client
        .managed_state()
        .enqueue_reconciliation(
            &work_id,
            &realm,
            target_kind,
            &serde_json::json!({ "id": target_id }),
        )
        .map_err(persistence_error)
}

/// Called by the event engine for every applied event. Target identity only
/// narrows candidates; completion still requires the durable evidence plan.
pub(crate) fn resolve_awaiting_evidence(client: &Client, event: &domain::Event) -> Result<()> {
    let Some(realm) = event.realm.as_ref() else {
        return Ok(());
    };
    let target_realm = match realm {
        domain::Realm::Live => "live".to_owned(),
        domain::Realm::Simulation(id) => format!("simulation:{}", id.get()),
    };
    if let Some(device) = &event.device {
        mark_resolved(client, &target_realm, "device", device.id.as_str(), event)?;
    }
    if let Some(replicant) = &event.replicant {
        mark_resolved(
            client,
            &target_realm,
            "replicant",
            replicant.id.as_str(),
            event,
        )?;
    }
    if let Some(location) = &event.location {
        mark_resolved(
            client,
            &target_realm,
            "location",
            location.id.as_str(),
            event,
        )?;
    }
    if let Some(id) = event.payload.get("simulation_id").and_then(Value::as_i64) {
        mark_resolved(client, &target_realm, "simulation", &id.to_string(), event)?;
    }
    Ok(())
}

fn mark_resolved(
    client: &Client,
    target_realm: &str,
    target_kind: &str,
    target_id: &str,
    event: &domain::Event,
) -> Result<()> {
    let ids = client
        .managed_state()
        .find_operations_awaiting_evidence(target_realm, target_kind, target_id)
        .map_err(persistence_error)?;
    for operation_id in ids {
        let entry = client
            .managed_state()
            .read_operation(&operation_id)
            .map_err(persistence_error)?;
        let Some(entry) = entry else { continue };
        let next = if event_evidence_matches(&entry, event) {
            Some(OperationStatus::Completed)
        } else if event_failure_evidence_matches(&entry, event) {
            Some(OperationStatus::Failed)
        } else {
            None
        };
        let Some(next) = next else {
            continue;
        };
        if next == OperationStatus::Failed {
            client
                .managed_state()
                .append_operation_projection(
                    &operation_id,
                    next.as_str(),
                    &serde_json::json!({
                        "event_id": event.id.as_str(),
                        "event": event.name.as_str(),
                        "payload": event.payload.clone()
                    }),
                )
                .map_err(persistence_error)?;
        } else {
            client
                .managed_state()
                .set_operation_state(&operation_id, next.as_str())
                .map_err(persistence_error)?;
        }
        notify(client, &OperationId::new(operation_id), next);
    }
    Ok(())
}

fn event_evidence_matches(
    entry: &super::store::OperationJournalEntry,
    event: &domain::Event,
) -> bool {
    event_matches_named_evidence(entry, event, "event_names")
}

fn event_failure_evidence_matches(
    entry: &super::store::OperationJournalEntry,
    event: &domain::Event,
) -> bool {
    event_matches_named_evidence(entry, event, "failure_event_names")
}

fn event_matches_named_evidence(
    entry: &super::store::OperationJournalEntry,
    event: &domain::Event,
    names_key: &str,
) -> bool {
    let Some(cursor) = entry.submission_cursor.as_deref() else {
        // Without a durable cursor lower bound, an event cannot prove that it
        // followed this submission.
        return false;
    };
    if event.id.as_str() <= cursor {
        return false;
    }
    let evidence = entry.intent.get("evidence").unwrap_or(&Value::Null);
    let Some(names) = evidence.get(names_key).and_then(Value::as_array) else {
        return false;
    };
    if !names
        .iter()
        .filter_map(Value::as_str)
        .any(|name| name == event.name.as_str())
    {
        return false;
    }
    evidence.get("payload").is_none_or(|predicate| {
        let payload = event
            .payload
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        value_contains(&Value::Object(payload), predicate)
    })
}

fn value_contains(actual: &Value, predicate: &Value) -> bool {
    match (actual, predicate) {
        (_, Value::Null) => true,
        (Value::Object(actual), Value::Object(predicate)) => {
            predicate.iter().all(|(key, value)| {
                actual
                    .get(key)
                    .is_some_and(|actual| value_contains(actual, value))
            })
        }
        (Value::Array(actual), Value::Array(predicate)) => predicate
            .iter()
            .all(|value| actual.iter().any(|actual| value_contains(actual, value))),
        _ => actual == predicate,
    }
}

fn device_configuration_applied(
    entry: &super::store::OperationJournalEntry,
    snapshot: &Value,
) -> bool {
    if entry.intent.get("kind").and_then(Value::as_str) != Some("device_configure") {
        return false;
    }
    let configuration = entry
        .intent
        .get("request")
        .and_then(|request| request.get("configuration"))
        .and_then(Value::as_object);
    let Some(configuration) = configuration else {
        return false;
    };
    // This reconciliation predicate intentionally covers tag-only configure
    // requests, which is the recovery mutation. Linking has its own tri-state
    // semantics and must not be declared complete from a tag snapshot alone.
    if configuration.get("linked_device").is_some() {
        return false;
    }
    let Some(tags) = snapshot.get("tags").and_then(Value::as_array) else {
        return false;
    };
    let has_tag = |tag: &Value| {
        tag.as_str()
            .is_some_and(|tag| tags.iter().any(|present| present.as_str() == Some(tag)))
    };
    if let Some(remove_tags) = configuration.get("remove_tags").and_then(Value::as_array)
        && remove_tags.iter().any(&has_tag)
    {
        return false;
    }
    if let Some(add_tags) = configuration.get("add_tags").and_then(Value::as_array)
        && add_tags.iter().any(|tag| !has_tag(tag))
    {
        return false;
    }
    if let Some(expected_tags) = configuration.get("tags").and_then(Value::as_array) {
        let current = tags
            .iter()
            .filter_map(Value::as_str)
            .collect::<BTreeSet<_>>();
        let expected = expected_tags
            .iter()
            .filter_map(Value::as_str)
            .collect::<BTreeSet<_>>();
        if current != expected {
            return false;
        }
    }
    true
}

/// Restart recovery: an operation left in `prepared` was durably registered
/// but never confirmed to have even started its one automatic submission
/// attempt, so it is safe (and required, for exactly-once delivery) to
/// attempt it now. Every other non-terminal state (`ambiguous`, `accepted`,
/// `awaiting_evidence`, ...) is left untouched — recoverable via evidence or
/// [`Operation::reconcile`], never blindly resubmitted.
pub(crate) async fn recover(client: &Client) -> Result<()> {
    let rows = client
        .managed_state()
        .list_unresolved_operations()
        .map_err(persistence_error)?;
    for (id, entry) in rows {
        if entry.state == OperationStatus::Prepared.as_str() {
            attempt(client, &OperationId::new(id)).await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{body_json, method, path, query_param, query_param_is_missing},
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

    use crate::managed::test_client_at as client_at;

    #[test]
    fn location_bound_device_commands_are_identified_for_stale_state_guard() {
        assert!(device_command_requires_star_system("deploy"));
        assert!(device_command_requires_star_system("system_scan"));
        assert!(device_command_requires_star_system("travel"));
        assert!(!device_command_requires_star_system("cancel"));
        assert!(!device_command_requires_star_system("decommission"));
    }

    #[tokio::test]
    async fn replicant_scoped_location_get_commits_aggregate_survey_progress() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/locations/KRUKKRAK"))
            .and(query_param("replicant", "R1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "location": "KRUKKRAK",
                "location_type": "star",
                "planets_total": 10,
                "planets_scanned": 10,
                "moons_total": 195,
                "moons_scanned": 195,
                "moons_total_estimated": false
            })))
            .expect(1)
            .mount(&server)
            .await;
        let client = client_at(&server.uri()).await;

        let location = client
            .locations()
            .get_for_replicant("KRUKKRAK", "R1")
            .await
            .expect("managed scoped location get");
        assert_eq!(location.survey_progress.planets_total, Some(10));
        assert_eq!(location.survey_progress.planets_scanned, Some(10));
        assert_eq!(location.survey_progress.moons_total, Some(195));
        assert_eq!(location.survey_progress.moons_scanned, Some(195));
        assert_eq!(location.survey_progress.moons_total_estimated, Some(false));

        let cached = client
            .locations()
            .cached("KRUKKRAK")
            .expect("committed location");
        assert_eq!(cached, location);
        server.verify().await;
        client.close().await.expect("close");
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
    async fn successful_mutation_with_undecodable_body_is_ambiguous() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/devices/D1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "count": {"unexpected": true}
            })))
            .expect(1)
            .mount(&server)
            .await;
        let client = client_at(&server.uri()).await;

        let operation = device_command(&client, "D1", raw::devices::DeviceCommand::SystemScan)
            .await
            .expect("operation remains durable when a 2xx success body evolves");

        assert_eq!(
            operation.status().await.expect("status"),
            OperationStatus::Ambiguous
        );
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
            OperationStatus::ReconciliationRequired
        );

        client.close().await.expect("close");
        server.verify().await; // panics if the mock was not hit exactly once
    }

    #[tokio::test]
    async fn synchronous_device_command_completes_from_typed_response() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/devices/D1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "device_code": "D1",
                "status": "idle",
                "location": "SOL-3"
            })))
            .expect(1)
            .mount(&server)
            .await;
        let client = client_at(&server.uri()).await;

        let operation = device_command(&client, "D1", raw::devices::DeviceCommand::Deploy)
            .await
            .expect("synchronous device command");

        assert_eq!(
            operation.status().await.expect("status"),
            OperationStatus::Completed
        );
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn caller_supplied_configure_id_reuses_matching_intent_and_rejects_collision() {
        let server = MockServer::start().await;
        Mock::given(method("PATCH"))
            .and(path("/v1/devices/D1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&server)
            .await;
        let client = client_at(&server.uri()).await;
        let id = OperationId::new("recovery-configure:test");
        let configuration = raw::devices::DeviceConfiguration {
            remove_tags: Some(vec!["WORKFLOW:RECOVERY".into()]),
            ..Default::default()
        };

        let first = device_configure_with_id(
            &client,
            "D1",
            raw::devices::DeviceConfigurationRequest {
                configuration: configuration.clone(),
            },
            id.clone(),
        )
        .await
        .expect("first configure");
        let second = device_configure_with_id(
            &client,
            "D1",
            raw::devices::DeviceConfigurationRequest { configuration },
            id.clone(),
        )
        .await
        .expect("matching configure reuse");
        assert_eq!(first.id(), second.id());

        let collision = device_configure_with_id(
            &client,
            "D1",
            raw::devices::DeviceConfigurationRequest {
                configuration: raw::devices::DeviceConfiguration {
                    add_tags: Some(vec!["different".into()]),
                    ..Default::default()
                },
            },
            id,
        )
        .await
        .expect_err("same ID with different intent must be rejected");
        assert!(matches!(collision, Error::Operation { .. }));
        client.close().await.expect("close");
        server.verify().await;
    }

    #[tokio::test]
    async fn caller_supplied_command_id_reuses_matching_intent_after_database_reopen() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/devices/D1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&server)
            .await;
        let directory = std::env::temp_dir().join(format!(
            "replicant-command-operation-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&directory).expect("operation test directory");
        let database = directory.join("managed.sqlite");
        let base_url = Url::parse(&server.uri()).expect("mock URL");
        let client = Client::builder()
            .authentication_token(SecretString::from("token".to_owned()))
            .base_url(base_url.clone())
            .sqlite(&database)
            .startup_policy(StartupPolicy::RestoreOnly)
            .start()
            .await
            .expect("first managed client");
        let id = OperationId::new("transport-command:test");
        let command = raw::devices::DeviceCommand::Travel {
            destination: "ALPHA-HUB".into(),
            dry_run: None,
            via: None,
        };

        let first = device_command_with_id(&client, "D1", command.clone(), id.clone())
            .await
            .expect("first command");
        client.close().await.expect("close first client");

        let reopened = Client::builder()
            .authentication_token(SecretString::from("token".to_owned()))
            .base_url(base_url)
            .sqlite(&database)
            .startup_policy(StartupPolicy::RestoreOnly)
            .start()
            .await
            .expect("reopened managed client");
        let second = device_command_with_id(&reopened, "D1", command, id.clone())
            .await
            .expect("matching command reuse");
        assert_eq!(first.id(), second.id());

        let collision = device_command_with_id(
            &reopened,
            "D1",
            raw::devices::DeviceCommand::Travel {
                destination: "BETA-HUB".into(),
                dry_run: None,
                via: None,
            },
            id,
        )
        .await
        .expect_err("same ID with different command intent must be rejected");
        assert!(matches!(collision, Error::Operation { .. }));
        reopened.close().await.expect("close reopened client");
        server.verify().await;
        std::fs::remove_dir_all(directory).expect("remove operation test directory");
    }

    #[tokio::test]
    async fn caller_supplied_configure_id_resumes_prepared_operation_once() {
        let server = MockServer::start().await;
        Mock::given(method("PATCH"))
            .and(path("/v1/devices/D1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&server)
            .await;
        let client = client_at(&server.uri()).await;
        let adapter = MutationAdapter::DeviceConfigure {
            device_code: "D1".into(),
            request: raw::devices::DeviceConfigurationRequest {
                configuration: raw::devices::DeviceConfiguration {
                    remove_tags: Some(vec!["WORKFLOW:RECOVERY".into()]),
                    ..Default::default()
                },
            },
        };
        let mut intent = adapter.durable_intent().expect("sanitize intent");
        intent["expects_evidence"] = Value::Bool(adapter.expects_evidence());
        intent["evidence"] = operation_evidence(&adapter);
        intent["rate_limit_bucket"] = Value::String(adapter.operation_id().into());
        client
            .managed_state()
            .record_operation(
                "recovery-configure:prepared",
                OperationStatus::Prepared.as_str(),
                Some("live"),
                Some("device"),
                Some("D1"),
                &intent,
            )
            .expect("persist prepared operation");

        let operation = device_configure_with_id(
            &client,
            "D1",
            raw::devices::DeviceConfigurationRequest {
                configuration: raw::devices::DeviceConfiguration {
                    remove_tags: Some(vec!["WORKFLOW:RECOVERY".into()]),
                    ..Default::default()
                },
            },
            OperationId::new("recovery-configure:prepared"),
        )
        .await
        .expect("resume prepared configure");
        assert_eq!(
            operation.status().await.expect("status"),
            OperationStatus::ReconciliationRequired
        );
        client.close().await.expect("close");
        server.verify().await;
    }
    #[tokio::test]
    async fn message_mark_read_completes_from_its_authoritative_response() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/messages/read"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"status": "ok"})),
            )
            .expect(1)
            .mount(&server)
            .await;
        let client = client_at(&server.uri()).await;

        let operation = client
            .messages()
            .mark_read(raw::messages::MessagesReadRequest {
                ids: Some(vec![1, 2]),
                mark_all: None,
            })
            .await
            .expect("message read operation created");
        assert_eq!(
            operation.status().await.expect("status"),
            OperationStatus::Completed
        );

        client.close().await.expect("close");
        server.verify().await;
    }

    #[tokio::test]
    async fn message_pages_persist_by_row_and_survive_restart_without_refetch() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/messages"))
            .and(query_param("limit", "100"))
            .and(query_param_is_missing("cursor"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "messages": [
                    {"id": 1, "title": "One", "is_read": true},
                    {"id": 2, "title": "Two", "is_read": false}
                ],
                "next_cursor": 2,
                "unread_message_count": 2
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/messages"))
            .and(query_param("cursor", "2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "messages": [{"id": 3, "title": "Three", "is_read": false}],
                "next_cursor": null,
                "unread_message_count": 2
            })))
            .expect(1)
            .mount(&server)
            .await;

        let path =
            std::env::temp_dir().join(format!("replicant-message-{}.sqlite", Uuid::new_v4()));
        let build = || {
            Client::builder()
                .authentication_token(SecretString::from("token".to_owned()))
                .base_url(Url::parse(&server.uri()).expect("mock URL"))
                .sqlite(&path)
                .startup_policy(StartupPolicy::RestoreOnly)
        };
        let client = build().start().await.expect("first client");
        let inbox = client
            .messages()
            .list()
            .await
            .expect("initial message sync");
        assert_eq!(inbox.messages.len(), 3);
        assert_eq!(inbox.last_cursor, Some(3));
        client.close().await.expect("close first client");

        let client = build().start().await.expect("restarted client");
        let restored = client.messages().list().await.expect("restored inbox");
        assert_eq!(restored.messages.len(), 3);
        assert_eq!(restored.unread_count, Some(2));
        client.close().await.expect("close restarted client");
        server.verify().await;

        let _ = std::fs::remove_file(super::super::store::history_database_path(&path));
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn definite_rejection_is_classified_and_never_treated_as_ambiguous() {
        let server = MockServer::start().await;
        Mock::given(method("PATCH"))
            .and(path("/v1/devices/D1"))
            .respond_with(ResponseTemplate::new(422).set_body_json(serde_json::json!({
                "error": "invalid tag",
                "detail": {"field": "tags", "reason": "reserved"}
            })))
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
        assert_eq!(
            outcome
                .response
                .as_ref()
                .and_then(|response| response.get("server"))
                .and_then(|server| server.get("detail"))
                .and_then(|detail| detail.get("reason"))
                .and_then(Value::as_str),
            Some("reserved")
        );
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn failed_submission_claim_never_transmits_the_request() {
        let server = MockServer::start().await;
        Mock::given(method("PATCH"))
            .and(path("/v1/devices/D1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(0)
            .mount(&server)
            .await;
        let client = client_at(&server.uri()).await;
        client.managed_state().fail_next_operation_commit();

        let error = device_configure(
            &client,
            "D1",
            raw::devices::DeviceConfigurationRequest::default(),
        )
        .await
        .expect_err("a failed durable claim prevents transmission");
        assert!(matches!(error, Error::Persistence { .. }));
        server.verify().await;
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
        assert_eq!(outcome.status, OperationStatus::ReconciliationRequired);
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
                    &serde_json::json!({"kind": "device_configure", "device_code":"D1", "request": {"configuration": {}}, "expects_evidence": false}),
                )
                .expect("record submitted");
            store
                .record_operation(
                    "op-prepared",
                    "prepared",
                    Some("device"),
                    Some("device"),
                    Some("D2"),
                    &serde_json::json!({"kind": "device_revoke_permission", "device_code":"D2", "expects_evidence": false}),
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
                if status == OperationStatus::ReconciliationRequired {
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
                    &serde_json::json!({"kind": "replicant_travel", "replicant_code":"R1", "request": {"destination":"SOL", "via":["SCEPTURUM"]}, "expects_evidence": true}),
                )
                .expect("record travel");
            store
                .record_operation(
                    "op-trade",
                    "prepared",
                    Some("live"),
                    Some("device"),
                    Some("TC1"),
                    &serde_json::json!({"kind": "device_create_trade", "device_code":"TC1", "request": {"name": "sample"}, "expects_evidence": false}),
                )
                .expect("record trade");
            store
                .record_operation(
                    "op-simulate",
                    "prepared",
                    Some("live"),
                    Some("device"),
                    Some("SIMDEV1"),
                    &serde_json::json!({"kind": "device_enter_simulation", "device_code":"SIMDEV1", "request": {"replicant_code": "R1", "scenario": "mining_rush"}, "expects_evidence": false}),
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
            .and(body_json(serde_json::json!({
                "destination": "SOL",
                "via": ["SCEPTURUM"]
            })))
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
                    if status == OperationStatus::ReconciliationRequired
                        || status == OperationStatus::AwaitingEvidence
                    {
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

    #[test]
    fn prospect_uses_completed_event_as_typed_evidence() {
        let evidence = operation_evidence(&MutationAdapter::DeviceCommand {
            device_code: "OBS1".into(),
            command: raw::devices::DeviceCommand::Prospect {
                direction: Some(vec![0.0, -1.0, 0.0]),
            },
        });
        assert_eq!(
            evidence["event_names"],
            serde_json::json!(["prospect.completed"])
        );
        assert_eq!(evidence["failure_event_names"], serde_json::json!([]));
    }

    #[test]
    fn triangulate_uses_typed_success_and_failure_evidence() {
        let evidence = operation_evidence(&MutationAdapter::DeviceCommand {
            device_code: "OBS1".into(),
            command: raw::devices::DeviceCommand::Triangulate {
                signature: "a3f7c2e8b1d94f06".into(),
                target: vec![5000.0, 14_000.0, 100.0],
            },
        });
        assert_eq!(
            evidence["event_names"],
            serde_json::json!(["triangulation.complete"])
        );
        assert_eq!(
            evidence["failure_event_names"],
            serde_json::json!(["triangulation.failed"])
        );
        assert_eq!(evidence["payload"]["signature"], "a3f7c2e8b1d94f06");
        assert_eq!(
            evidence["payload"]["target"],
            serde_json::json!([5000.0, 14_000.0, 100.0])
        );
    }

    #[test]
    fn newly_covered_commands_use_documented_event_names() {
        fn assert_events(adapter: MutationAdapter, expected: &[&str]) {
            assert_eq!(
                operation_evidence(&adapter)["event_names"],
                serde_json::json!(expected)
            );
        }
        fn device(command: raw::devices::DeviceCommand) -> MutationAdapter {
            MutationAdapter::DeviceCommand {
                device_code: "D1".into(),
                command,
            }
        }
        let command_resources =
            || std::collections::BTreeMap::from([("structural".to_owned(), 10.0)]);

        assert_events(
            device(raw::devices::DeviceCommand::Activate),
            &[
                "diversion.activated",
                "hub.activated",
                "relay.activated",
                "ward.activated",
            ],
        );
        assert_events(
            device(raw::devices::DeviceCommand::Assemble),
            &["ami.assembled"],
        );
        assert_events(
            device(raw::devices::DeviceCommand::Adopt(Default::default())),
            &["ami.adopted"],
        );
        assert_events(
            device(raw::devices::DeviceCommand::Attach(Default::default())),
            &["device.attached"],
        );
        assert_events(
            device(raw::devices::DeviceCommand::ChangeOwner {
                target: "R2".into(),
            }),
            &["device.changed_owner"],
        );
        assert_events(
            device(raw::devices::DeviceCommand::ClearDirective),
            &["directive.cleared"],
        );
        assert_events(
            device(raw::devices::DeviceCommand::CollectResources {
                resources: command_resources(),
            }),
            &["transport.collected"],
        );
        assert_events(
            device(raw::devices::DeviceCommand::Decommission),
            &["device.decommissioned"],
        );
        assert_events(
            device(raw::devices::DeviceCommand::Deploy),
            &["device.deployed"],
        );
        assert_events(
            device(raw::devices::DeviceCommand::DepositResources {
                resources: Some(command_resources()),
            }),
            &["transport.delivered"],
        );
        assert_events(
            device(raw::devices::DeviceCommand::Detach(Default::default())),
            &["device.detached"],
        );
        assert_events(
            device(raw::devices::DeviceCommand::EnqueuePrint {
                device_type: "mining_drone".into(),
                quantity: None,
                controller: None,
                oncomplete: None,
                tags: None,
                flatpack: None,
            }),
            &["print.completed"],
        );
        assert_events(
            device(raw::devices::DeviceCommand::Launch),
            &["ami.launched"],
        );
        assert_events(
            device(raw::devices::DeviceCommand::Message {
                channel: "ops".into(),
                text: "ready".into(),
            }),
            &["bobnet.new"],
        );
        assert_events(
            device(raw::devices::DeviceCommand::Recall),
            &["device.stowed"],
        );
        assert_events(
            device(raw::devices::DeviceCommand::Release(Default::default())),
            &["ami.released"],
        );
        assert_events(
            device(raw::devices::DeviceCommand::Rename {
                designation: "SOL-4".into(),
                name: "Earth".into(),
            }),
            &["system.body_renamed"],
        );
        assert_events(
            device(raw::devices::DeviceCommand::Retarget {
                resource_type: "carbon".into(),
            }),
            &["mining.retargeted"],
        );
        assert_events(
            device(raw::devices::DeviceCommand::Scan),
            &["scan.completed"],
        );
        assert_events(
            device(raw::devices::DeviceCommand::Search),
            &["search.completed"],
        );
        assert_events(
            device(raw::devices::DeviceCommand::SetDirective {
                directive: "mine_belt".into(),
                configuration: None,
                notify: None,
            }),
            &["directive.set"],
        );
        assert_events(
            device(raw::devices::DeviceCommand::SetEntryPoint),
            &["system.entry_point_set"],
        );
        assert_events(
            device(raw::devices::DeviceCommand::StartMining {
                resource_type: "carbon".into(),
                target: None,
            }),
            &["mining.started"],
        );
        assert_events(
            device(raw::devices::DeviceCommand::Stow {
                target: Some("V1".into()),
            }),
            &["device.stowed"],
        );
        assert_events(
            device(raw::devices::DeviceCommand::Travel {
                destination: "SOL".into(),
                dry_run: None,
                via: None,
            }),
            &["travel.arrived"],
        );
        assert_events(
            device(raw::devices::DeviceCommand::Withdraw),
            &["ami.withdrawn"],
        );
        assert_events(
            MutationAdapter::DeviceCreateTrade {
                device_code: "D1".into(),
                request: serde_json::from_value(serde_json::json!({"structural": 10}))
                    .expect("trade request object"),
            },
            &["trade.created"],
        );
        assert_events(
            MutationAdapter::DeviceDeleteTrade {
                device_code: "D1".into(),
                trade_code: "T1".into(),
            },
            &["trade.deleted"],
        );
        assert_events(
            MutationAdapter::DeviceFulfillTrade {
                device_code: "D1".into(),
                trade_code: "T1".into(),
            },
            &["trade.completed"],
        );
        assert_events(
            MutationAdapter::ReplicantMine {
                replicant_code: "R1".into(),
                request: raw::replicants::MineRequest {
                    notify: None,
                    resource_type: "carbon".into(),
                    target: None,
                },
            },
            &["mining.started"],
        );
        assert_events(
            MutationAdapter::ReplicantStopMining {
                replicant_code: "R1".into(),
            },
            &["mining.stopped"],
        );
        assert_events(
            MutationAdapter::ReplicantPrint {
                replicant_code: "R1".into(),
                request: raw::replicants::PrintRequest {
                    command: None,
                    device_type: Some("mining_drone".into()),
                    flatpack: None,
                    notify: None,
                },
            },
            &["print.completed"],
        );
        assert_events(
            MutationAdapter::ReplicantTeleport {
                replicant_code: "R1".into(),
                request: raw::replicants::TeleportRequest {
                    target: "M1".into(),
                },
            },
            &["teleport.completed"],
        );
        assert_events(
            MutationAdapter::ReplicantTravel {
                replicant_code: "R1".into(),
                request: raw::replicants::TravelRequest {
                    destination: Some("SOL".into()),
                    dry_run: None,
                    notify: None,
                    via: None,
                },
            },
            &["travel.arrived"],
        );
        assert_events(
            MutationAdapter::ReplicantCancelTravel {
                replicant_code: "R1".into(),
            },
            &["travel.cancelled"],
        );
    }

    #[test]
    fn commands_without_documented_evidence_remain_explicitly_empty() {
        let evidence = operation_evidence(&MutationAdapter::DeviceCommand {
            device_code: "D1".into(),
            command: raw::devices::DeviceCommand::SystemScan,
        });
        assert_eq!(evidence["event_names"], serde_json::json!([]));
        assert!(evidence["expected_state"].is_null());
    }

    #[tokio::test]
    async fn triangulation_failed_marks_the_operation_failed() {
        let server = MockServer::start().await;
        let client = client_at(&server.uri()).await;
        client
            .managed_state()
            .set_event_cursor("1-0")
            .expect("set submission cursor");
        client
            .managed_state()
            .record_operation(
                "op-triangulate",
                OperationStatus::Prepared.as_str(),
                Some("live"),
                Some("device"),
                Some("OBS1"),
                &serde_json::json!({
                    "evidence": {
                        "event_names": ["triangulation.complete"],
                        "failure_event_names": ["triangulation.failed"],
                        "payload": {
                            "signature": "a3f7c2e8b1d94f06",
                            "target": [5000.0, 14000.0, 100.0]
                        }
                    }
                }),
            )
            .expect("record operation");
        assert!(
            client
                .managed_state()
                .claim_operation_submission("op-triangulate", "attempt-1")
                .expect("claim submission")
        );
        client
            .managed_state()
            .set_operation_state("op-triangulate", OperationStatus::AwaitingEvidence.as_str())
            .expect("await evidence");

        let event = domain::Event {
            id: crate::domain::EventId::new("2-0"),
            realm: Some(Realm::Live),
            name: domain::EventName::from("triangulation.failed"),
            category: domain::EventCategory::from("device"),
            device: Some(DeviceKey::live(DeviceId::from("OBS1"))),
            replicant: None,
            location: None,
            star: None,
            occurred_at: "2026-08-06T00:00:00Z".into(),
            payload: std::collections::BTreeMap::from([
                ("signature".into(), serde_json::json!("a3f7c2e8b1d94f06")),
                ("target".into(), serde_json::json!([5000.0, 14000.0, 100.0])),
                ("reason".into(), serde_json::json!("signature_not_found")),
            ]),
        };
        resolve_awaiting_evidence(&client, &event).expect("resolve failure evidence");

        let outcome = client
            .operations()
            .get(OperationId::new("op-triangulate"))
            .outcome()
            .await
            .expect("outcome");
        assert_eq!(outcome.status, OperationStatus::Failed);
        let response = outcome.response.as_ref().expect("failure event projection");
        assert_eq!(response["event"], "triangulation.failed");
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn documented_event_completes_an_operation_with_populated_evidence() {
        let client = client_at(&MockServer::start().await.uri()).await;
        client
            .managed_state()
            .set_event_cursor("1-0")
            .expect("set submission cursor");
        let evidence = operation_evidence(&MutationAdapter::DeviceCommand {
            device_code: "D1".into(),
            command: raw::devices::DeviceCommand::Deploy,
        });
        client
            .managed_state()
            .record_operation(
                "op-deploy",
                OperationStatus::Prepared.as_str(),
                Some("live"),
                Some("device"),
                Some("D1"),
                &serde_json::json!({"evidence": evidence}),
            )
            .expect("record operation");
        assert!(
            client
                .managed_state()
                .claim_operation_submission("op-deploy", "attempt-1")
                .expect("claim submission")
        );
        client
            .managed_state()
            .set_operation_state("op-deploy", OperationStatus::AwaitingEvidence.as_str())
            .expect("await evidence");

        resolve_awaiting_evidence(
            &client,
            &domain::Event {
                id: crate::domain::EventId::new("2-0"),
                realm: Some(Realm::Live),
                name: domain::EventName::from("device.deployed"),
                category: domain::EventCategory::from("device"),
                device: Some(DeviceKey::live(DeviceId::from("D1"))),
                replicant: None,
                location: None,
                star: None,
                occurred_at: "2026-08-23T00:00:00Z".into(),
                payload: Default::default(),
            },
        )
        .expect("resolve evidence");

        assert_eq!(
            client
                .operations()
                .get(OperationId::new("op-deploy"))
                .status()
                .await
                .expect("status"),
            OperationStatus::Completed
        );
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn empty_evidence_is_ignored_without_error_or_terminal_transition() {
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
            id: crate::domain::EventId::new("1-0"),
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
        resolve_awaiting_evidence(&client, &event).expect("event application");

        let operation = client.operations().get(OperationId::new("op-travel"));
        assert_eq!(
            operation.status().await.expect("status"),
            OperationStatus::AwaitingEvidence
        );
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn concurrent_operations_on_one_entity_resolve_only_their_own_evidence() {
        let client = client_at(&MockServer::start().await.uri()).await;
        client
            .managed_state()
            .set_event_cursor("1-0")
            .expect("set submission cursor");
        for (id, event_name, payload) in [
            (
                "deploy-a",
                "device.deployed",
                serde_json::json!({"slot": "a"}),
            ),
            (
                "deploy-b",
                "device.deployed",
                serde_json::json!({"slot": "b"}),
            ),
            (
                "travel",
                "device.travel_arrived",
                serde_json::json!({"slot": "travel"}),
            ),
        ] {
            client
                .managed_state()
                .record_operation(
                    id,
                    OperationStatus::Prepared.as_str(),
                    Some("live"),
                    Some("device"),
                    Some("D1"),
                    &serde_json::json!({"evidence": {"event_names": [event_name], "payload": payload}}),
                )
                .expect("record operation");
            assert!(
                client
                    .managed_state()
                    .claim_operation_submission(id, &format!("attempt-{id}"))
                    .expect("claim one automatic submission")
            );
            assert!(
                !client
                    .managed_state()
                    .claim_operation_submission(id, &format!("duplicate-{id}"))
                    .expect("duplicate claim is rejected")
            );
            client
                .managed_state()
                .set_operation_state(id, OperationStatus::AwaitingEvidence.as_str())
                .expect("await evidence");
        }
        let event = |id: &str, name: &str, device: &str, slot: &str| domain::Event {
            id: crate::domain::EventId::new(id),
            realm: Some(Realm::Live),
            name: domain::EventName::from(name),
            category: domain::EventCategory::from("device"),
            device: Some(DeviceKey::live(DeviceId::from(device))),
            replicant: None,
            location: None,
            star: None,
            occurred_at: "2026-07-25T00:00:00Z".into(),
            payload: std::collections::BTreeMap::from([("slot".into(), serde_json::json!(slot))]),
        };

        resolve_awaiting_evidence(&client, &event("2-0", "device.deployed", "D2", "a"))
            .expect("unrelated event");
        resolve_awaiting_evidence(
            &client,
            &event("3-0", "device.travel_arrived", "D1", "travel"),
        )
        .expect("out-of-order travel evidence");
        resolve_awaiting_evidence(&client, &event("4-0", "device.deployed", "D1", "b"))
            .expect("same-kind deploy evidence");
        resolve_awaiting_evidence(&client, &event("5-0", "device.deployed", "D1", "a"))
            .expect("second deploy evidence");

        for id in ["deploy-a", "deploy-b", "travel"] {
            assert_eq!(
                client
                    .operations()
                    .get(OperationId::new(id))
                    .status()
                    .await
                    .expect("operation status"),
                OperationStatus::Completed
            );
        }
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn simulation_evidence_is_never_matched_to_a_same_code_live_operation() {
        let client = client_at(&MockServer::start().await.uri()).await;
        for (id, realm) in [("live", "live"), ("simulation", "simulation:7")] {
            client
                .managed_state()
                .record_operation(
                    id,
                    OperationStatus::AwaitingEvidence.as_str(),
                    Some(realm),
                    Some("device"),
                    Some("D1"),
                    &serde_json::json!({"kind": "device_command"}),
                )
                .expect("record operation");
        }

        let matching = client
            .managed_state()
            .find_operations_awaiting_evidence("simulation:7", "device", "D1")
            .expect("find simulation evidence candidates");
        assert_eq!(matching, vec!["simulation"]);
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
            OperationStatus::ReconciliationRequired
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
    fn mutation_adapter_inventory_covers_every_non_bootstrap_unsafe_operation() {
        // Account registration/recovery and feedback are deliberately raw-only
        // bootstrap calls. Every other unsafe supported endpoint is a typed
        // `MutationAdapter` variant and cannot fall through to generic HTTP.
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
            25,
            "expected exactly 25 durable operations"
        );

        let source = include_str!("operation.rs");
        assert!(!source.contains(&format!("dispatch_{}", "target")));
        assert!(source.matches("Self::").count() >= unsafe_ops.len());
    }

    #[tokio::test]
    async fn equipment_retrieval_is_durable_and_completes_after_device_refresh() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/devices/LOCKER/retrieve"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"status": "retrieved"})),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/devices"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "devices": [{
                    "device_code": "SLING1",
                    "device_type": "ftl_slingshot",
                    "linked_device": "MATRIX1"
                }],
                "next_cursor": null
            })))
            .expect(1)
            .mount(&server)
            .await;
        let client = client_at(&server.uri()).await;

        let operation = device_retrieve(&client, "LOCKER")
            .await
            .expect("retrieval operation");
        assert_eq!(
            operation.status().await.expect("status"),
            OperationStatus::Completed
        );
        let entry = client
            .managed_state()
            .read_operation(operation.id().as_str())
            .expect("read operation")
            .expect("operation row");
        assert_eq!(entry.intent["kind"], "device_retrieve");
        assert_eq!(entry.intent["device_code"], "LOCKER");
        assert!(entry.target_kind.is_none());
        assert!(client.devices().cached("SLING1").is_some());

        client.close().await.expect("close");
        server.verify().await;
    }

    #[tokio::test]
    async fn equipment_retrieval_transport_failure_stays_ambiguous_without_refresh() {
        let client = client_at(&unreachable_base_url()).await;
        let operation = device_retrieve(&client, "LOCKER")
            .await
            .expect("retrieval remains durably inspectable");
        assert_eq!(
            operation.status().await.expect("status"),
            OperationStatus::Ambiguous
        );
        let entry = client
            .managed_state()
            .read_operation(operation.id().as_str())
            .expect("read operation")
            .expect("operation row");
        assert_eq!(entry.intent["kind"], "device_retrieve");
        client.close().await.expect("close");
    }

    #[test]
    fn mutation_adapter_dynamic_command_round_trips_without_a_dispatcher() {
        let mut command = raw::JsonObject::new();
        command.insert(
            "command".into(),
            Value::String("future_server_command".into()),
        );
        command.insert("radius".into(), serde_json::json!(7));
        let adapter = MutationAdapter::DeviceDynamicCommand {
            device_code: "D1".into(),
            command,
        };
        let intent = adapter.durable_intent().expect("serialize dynamic command");
        let replay: MutationAdapter = serde_json::from_value(intent).expect("typed replay");
        assert_eq!(replay.operation_id(), "device_dynamic_command");
        assert!(include_str!("operation.rs").contains("raw.devices().command"));
    }
    #[test]
    fn operation_outcome_exposes_structured_rejection() {
        let status_rejection = OperationOutcome {
            status: OperationStatus::Rejected,
            response: Some(serde_json::json!({
                "status": 404,
                "server": {"error": "Not your device"}
            })),
        };
        assert_eq!(status_rejection.http_status(), Some(404));
        assert_eq!(status_rejection.server_error(), Some("Not your device"));

        let server_rejection = OperationOutcome {
            status: OperationStatus::Rejected,
            response: Some(serde_json::json!({
                "status": 400,
                "server": {"error": "Device not found"}
            })),
        };
        assert_eq!(server_rejection.http_status(), Some(400));
        assert_eq!(server_rejection.server_error(), Some("Device not found"));

        let accepted_missing_payload = OperationOutcome {
            status: OperationStatus::Accepted,
            response: Some(serde_json::json!({
                "status": 404,
                "server": {"error": "Device not found"}
            })),
        };
        assert_eq!(accepted_missing_payload.http_status(), Some(404));
        assert_eq!(
            accepted_missing_payload.server_error(),
            Some("Device not found")
        );

        let prose_only = OperationOutcome {
            status: OperationStatus::Rejected,
            response: Some(serde_json::json!({"message": "Device not found"})),
        };
        assert_eq!(prose_only.http_status(), None);
        assert_eq!(prose_only.server_error(), None);
    }
}
