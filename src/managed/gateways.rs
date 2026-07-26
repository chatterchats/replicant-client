//! Managed read gateways. They normalize the one response they fetch, commit it,
//! publish the resulting revision, and only then return a domain value.

#![allow(missing_docs)] // Gateway module documentation explains the common contract.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, mpsc},
};

use crate::domain::{
    self, AccessScope, Account, AccountId, Device, DeviceCommand, DeviceFeature, DeviceId,
    DeviceKey, DeviceStatus, DeviceType, Realm, Replicant, ReplicantId, ReplicantKey,
    ReplicantStatus,
};
use crate::raw;
use crate::{Client, Error, Result};

use super::ami::{FleetController, MiningController, SurveyController, TransportController};
use super::operation::{self, ConfirmAccountWipe, DynamicCommand, Operation};
use super::travel::TravelBuilder;

/// A local device-update stream. It never polls or otherwise issues network requests.
pub struct DeviceWatch {
    receiver: mpsc::Receiver<std::sync::Arc<super::state::StateSnapshot>>,
    key: DeviceKey,
}

impl DeviceWatch {
    /// Returns the latest published snapshot for this device, if one is available now.
    pub fn try_next(&self) -> Option<Device> {
        self.receiver
            .try_iter()
            .filter_map(|snapshot| snapshot.devices().get(&self.key).cloned())
            .last()
            .map(|observation| observation.value)
    }
}

fn normalization(error: domain::NormalizeError) -> Error {
    Error::Decode {
        message: error.to_string(),
        status: None,
    }
}

fn observed_at() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs().to_string())
        .unwrap_or_default()
}

/// Gateway for the authenticated account. `get` is explicit remote I/O.
#[derive(Clone, Debug)]
pub struct AccountGateway {
    client: Client,
}
impl AccountGateway {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    pub async fn get(&self) -> Result<Account> {
        self.client.ensure_open()?;
        let response = self.client.managed_raw().accounts().me().await?;
        let id = response
            .value
            .email
            .clone()
            .filter(|email| !email.is_empty())
            .map(AccountId::from)
            .ok_or_else(|| Error::Decode {
                message: "response omitted required identity `email`".into(),
                status: None,
            })?;
        let observation = domain::account_me(&response.value, id, observed_at());
        let value = observation.value.clone();
        self.client
            .managed_state()
            .persist_account(observation)
            .map_err(|_| Error::Persistence {
                message: "SQLite store operation failed".into(),
            })?;
        Ok(value)
    }

    pub async fn refresh(&self) -> Result<Account> {
        self.get().await
    }

    /// Updates the authenticated account's profile as a durable operation.
    pub async fn update(&self, request: raw::accounts::AccountUpdateRequest) -> Result<Operation> {
        operation::account_update(&self.client, request).await
    }

    /// Requests destructive, irreversible deletion of the authenticated
    /// account. Requires an explicit [`ConfirmAccountWipe`] naming the exact
    /// account being destroyed, checked against the account bound to this
    /// store before the request is even registered.
    ///
    /// A destructive wipe cannot be requested without naming the account:
    ///
    /// ```compile_fail
    /// # async fn example(client: replicant_client::Client) -> Result<(), replicant_client::Error> {
    /// // Missing the required destructive confirmation: this does not compile.
    /// client.account().wipe().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn wipe(&self, confirm: ConfirmAccountWipe) -> Result<Operation> {
        operation::account_wipe(&self.client, confirm).await
    }
}

/// A durable, local-state handle for a mutable device. It performs no unsafe actions.
#[derive(Clone, Debug)]
pub struct DeviceHandle {
    client: Client,
    key: DeviceKey,
}
impl DeviceHandle {
    fn new(client: Client, key: DeviceKey) -> Self {
        Self { client, key }
    }
    #[cfg(test)]
    pub(crate) fn for_test(client: Client, key: DeviceKey) -> Self {
        Self::new(client, key)
    }
    #[must_use]
    pub fn id(&self) -> &DeviceId {
        &self.key.id
    }
    #[must_use]
    pub fn realm(&self) -> &Realm {
        &self.key.realm
    }
    pub async fn snapshot(&self) -> Result<Device> {
        self.client.ensure_open()?;
        self.client
            .managed_state()
            .device(&self.key)
            .map(|observation| observation.value)
            .ok_or_else(|| Error::Configuration {
                message: "device is not cached".into(),
            })
    }
    pub async fn refresh(&self) -> Result<DeviceHandle> {
        if self.key.realm != Realm::Live {
            return Err(Error::Configuration {
                message:
                    "simulation refresh is not available until simulation reads are implemented"
                        .into(),
            });
        }
        self.client.devices().get(self.id().as_str()).await
    }
    pub async fn watch(&self) -> Result<DeviceWatch> {
        self.client.ensure_open()?;
        Ok(DeviceWatch {
            receiver: self.client.managed_state().subscribe(),
            key: self.key.clone(),
        })
    }

    /// Activates a dormant device.
    pub async fn activate(&self) -> Result<Operation> {
        self.command(raw::devices::DeviceCommand::Activate).await
    }
    /// Deactivates an active device.
    pub async fn deactivate(&self) -> Result<Operation> {
        self.command(raw::devices::DeviceCommand::Deactivate).await
    }
    /// Deploys a device into the field.
    pub async fn deploy(&self) -> Result<Operation> {
        self.command(raw::devices::DeviceCommand::Deploy).await
    }
    /// Stows this device inside another.
    pub async fn stow(&self, target: Option<String>) -> Result<Operation> {
        self.command(raw::devices::DeviceCommand::Stow { target })
            .await
    }
    /// Attaches one or more devices to this device.
    pub async fn attach(&self, targets: raw::devices::TargetsCommand) -> Result<Operation> {
        self.command(raw::devices::DeviceCommand::Attach(targets))
            .await
    }
    /// Compacts this device's stowed contents.
    pub async fn compact(&self) -> Result<Operation> {
        self.command(raw::devices::DeviceCommand::Compact).await
    }
    /// Unfurls a deployed structure.
    pub async fn unfurl(&self) -> Result<Operation> {
        self.command(raw::devices::DeviceCommand::Unfurl).await
    }

    /// Dispatches any known device command as a durable operation. The
    /// intended command is checked against this device's latest cached
    /// `available_commands` first, but the server remains authoritative.
    pub async fn command(&self, command: raw::devices::DeviceCommand) -> Result<Operation> {
        operation::device_command(&self.client, self.id().as_str(), command).await
    }

    /// Dispatches a forward-compatible command this client's typed
    /// [`raw::devices::DeviceCommand`] does not (yet) name.
    pub async fn dynamic_command(&self, command: DynamicCommand) -> Result<Operation> {
        operation::device_dynamic_command(&self.client, self.id().as_str(), command).await
    }

    /// Updates this device's tags as a durable operation.
    pub async fn configure(
        &self,
        configuration: raw::devices::DeviceConfiguration,
    ) -> Result<Operation> {
        operation::device_configure(
            &self.client,
            self.id().as_str(),
            raw::devices::DeviceConfigurationRequest { configuration },
        )
        .await
    }

    /// Grants a permission on this device to another account.
    pub async fn grant_permission(&self, request: raw::JsonObject) -> Result<Operation> {
        operation::device_grant_permission(&self.client, self.id().as_str(), request).await
    }

    /// Revokes a permission on this device.
    pub async fn revoke_permission(&self) -> Result<Operation> {
        operation::device_revoke_permission(&self.client, self.id().as_str()).await
    }

    /// Enters a simulation scenario via this simulator device.
    pub async fn enter_simulation(
        &self,
        request: raw::simulations::SimulationEnterRequest,
    ) -> Result<Operation> {
        operation::device_enter_simulation(&self.client, self.id().as_str(), request).await
    }

    /// Cancels a running simulation on this device.
    pub async fn abandon_simulation(&self, simulation_id: i64) -> Result<Operation> {
        operation::device_abandon_simulation(&self.client, self.id().as_str(), simulation_id).await
    }

    /// Creates a new trade on this trade controller device.
    pub async fn create_trade(&self, request: raw::JsonObject) -> Result<Operation> {
        operation::device_create_trade(&self.client, self.id().as_str(), request).await
    }

    /// Deletes a trade on this device, returning its escrowed rewards.
    pub async fn delete_trade(&self, trade_code: &str) -> Result<Operation> {
        operation::device_delete_trade(&self.client, self.id().as_str(), trade_code).await
    }

    /// Fulfills one unit of a trade on this device as a buyer.
    pub async fn fulfill_trade(&self, trade_code: &str) -> Result<Operation> {
        operation::device_fulfill_trade(&self.client, self.id().as_str(), trade_code).await
    }

    pub(crate) fn client(&self) -> &Client {
        &self.client
    }

    pub(crate) fn key(&self) -> &DeviceKey {
        &self.key
    }

    /// Views this device as an AMI mining controller. Rejected only when the
    /// latest cached snapshot names a different known device type; the
    /// server remains authoritative for command dispatch either way.
    pub fn as_mining_controller(&self) -> Result<MiningController> {
        MiningController::new(self.clone())
    }

    /// Views this device as an AMI survey controller.
    pub fn as_survey_controller(&self) -> Result<SurveyController> {
        SurveyController::new(self.clone())
    }

    /// Views this device as an AMI transport controller.
    pub fn as_transport_controller(&self) -> Result<TransportController> {
        TransportController::new(self.clone())
    }

    /// Views this device as an AMI fleet controller.
    pub fn as_fleet_controller(&self) -> Result<FleetController> {
        FleetController::new(self.clone())
    }

    /// Lists this device's event log. Diagnostic/append-only history,
    /// distinct from account events (`client.events()`) and location events
    /// (`client.location_events()`).
    pub async fn logs(
        &self,
        query: &raw::devices::DeviceLogsQuery,
    ) -> Result<raw::devices::DeviceLogsResponse> {
        self.client.ensure_open()?;
        Ok(self
            .client
            .managed_raw()
            .devices()
            .logs(self.id().as_str(), query)
            .await?
            .value)
    }

    /// Lists ownership/configuration audit entries for this device. The
    /// contract documents no response schema for this endpoint.
    pub async fn audit(&self, query: &raw::devices::DeviceAuditQuery) -> Result<serde_json::Value> {
        self.client.ensure_open()?;
        Ok(self
            .client
            .managed_raw()
            .devices()
            .audit(self.id().as_str(), query)
            .await?
            .value)
    }

    /// Lists permissions granted on this device. Volatile/diagnostic, not a
    /// durably reconciled collection: the contract documents no response
    /// schema for this endpoint.
    pub async fn permissions(&self) -> Result<serde_json::Value> {
        self.client.ensure_open()?;
        Ok(self
            .client
            .managed_raw()
            .devices()
            .list_permissions(self.id().as_str())
            .await?
            .value)
    }

    /// Fetches this device's relay network topology. Volatile: it is never
    /// durably reconciled.
    pub async fn network(&self) -> Result<raw::devices::DeviceNetwork> {
        self.client.ensure_open()?;
        Ok(self
            .client
            .managed_raw()
            .devices()
            .network(self.id().as_str())
            .await?
            .value)
    }

    /// Lists distinct BobNet channels this relay-capable device has
    /// observed. See [`Client::bobnet`](crate::Client::bobnet) for sending
    /// and account-wide BobNet event observation.
    pub async fn channels(&self) -> Result<raw::bobnet::DeviceChannelsResponse> {
        self.client.ensure_open()?;
        Ok(self
            .client
            .managed_raw()
            .bobnet()
            .channels(self.id().as_str())
            .await?
            .value)
    }

    /// Lists recent BobNet messages visible from this relay-capable device.
    /// Bounded diagnostic/catch-up history, distinct from the account-wide
    /// inbox (`client.messages()`).
    pub async fn relay_history(
        &self,
        query: &raw::bobnet::DeviceMessagesQuery,
    ) -> Result<raw::bobnet::DeviceMessagesResponse> {
        self.client.ensure_open()?;
        Ok(self
            .client
            .managed_raw()
            .bobnet()
            .messages(self.id().as_str(), query)
            .await?
            .value)
    }
}

#[derive(Clone, Debug, Default)]
enum DeviceLinkFilter<T> {
    #[default]
    Any,
    Is(T),
    None,
}

/// Local-only device query. It reads one immutable committed snapshot and
/// cannot perform network I/O.
#[derive(Clone, Debug)]
pub struct DeviceQuery {
    client: Client,
    predicate: domain::DevicePredicate,
    tags: Vec<String>,
    system: Option<String>,
    attached_to: DeviceLinkFilter<DeviceKey>,
    controller: DeviceLinkFilter<DeviceKey>,
    hosted_by: DeviceLinkFilter<ReplicantKey>,
    without_adopted_devices: bool,
}

impl DeviceQuery {
    fn new(client: Client) -> Self {
        Self {
            client,
            predicate: domain::DevicePredicate::default(),
            tags: Vec::new(),
            system: None,
            attached_to: DeviceLinkFilter::Any,
            controller: DeviceLinkFilter::Any,
            hosted_by: DeviceLinkFilter::Any,
            without_adopted_devices: false,
        }
    }

    #[must_use]
    pub fn in_realm(mut self, realm: Realm) -> Self {
        self.predicate = self.predicate.in_realm(realm);
        self
    }

    #[must_use]
    pub fn of_type(mut self, value: DeviceType) -> Self {
        self.predicate = self.predicate.of_type(value);
        self
    }

    #[must_use]
    pub fn with_status(mut self, value: DeviceStatus) -> Self {
        self.predicate = self.predicate.with_status(value);
        self
    }

    #[must_use]
    pub fn with_access(mut self, value: AccessScope) -> Self {
        self.predicate = self.predicate.with_access(value);
        self
    }

    #[must_use]
    pub fn owned(self) -> Self {
        self.with_access(AccessScope::Owned)
    }

    #[must_use]
    pub fn with_feature(mut self, value: DeviceFeature) -> Self {
        self.predicate = self.predicate.with_feature(value);
        self
    }

    #[must_use]
    pub fn with_command(mut self, value: DeviceCommand) -> Self {
        self.predicate = self.predicate.with_command(value);
        self
    }

    #[must_use]
    pub fn miners(self) -> Self {
        self.of_type(DeviceType::MiningDrone)
    }

    #[must_use]
    pub fn idle(self) -> Self {
        self.with_status(DeviceStatus::from("idle"))
    }

    /// Matches this exact location ID in every realm. Combine with
    /// [`Self::in_realm`] when a location name is ambiguous.
    #[must_use]
    pub fn at(mut self, location: impl Into<String>) -> Self {
        self.predicate = self.predicate.at(location.into());
        self
    }

    /// Matches locations in a system by its canonical code prefix (for
    /// example, `SOL` matches `SOL-1`).
    #[must_use]
    pub fn in_system(mut self, system: impl Into<String>) -> Self {
        self.system = Some(system.into());
        self
    }

    /// Requires every supplied tag to be present on the device.
    #[must_use]
    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    #[must_use]
    pub fn attached_to(mut self, device: DeviceKey) -> Self {
        self.attached_to = DeviceLinkFilter::Is(device);
        self
    }

    #[must_use]
    pub fn unattached(mut self) -> Self {
        self.attached_to = DeviceLinkFilter::None;
        self
    }

    /// Alias for the attachment relationship used by the stow command.
    #[must_use]
    pub fn stowed_in(self, device: DeviceKey) -> Self {
        self.attached_to(device)
    }

    #[must_use]
    pub fn controlled_by(mut self, controller: DeviceKey) -> Self {
        self.controller = DeviceLinkFilter::Is(controller);
        self
    }

    #[must_use]
    pub fn without_controller(mut self) -> Self {
        self.controller = DeviceLinkFilter::None;
        self
    }

    #[must_use]
    pub fn hosted_by(mut self, replicant: ReplicantKey) -> Self {
        self.hosted_by = DeviceLinkFilter::Is(replicant);
        self
    }

    /// Keeps controller devices which currently control no other cached
    /// device. A device is adopted when its `controller` relationship points
    /// at that controller.
    #[must_use]
    pub fn without_adopted_devices(mut self) -> Self {
        self.without_adopted_devices = true;
        self
    }

    fn matching_entries(
        &self,
        devices: impl IntoIterator<Item = domain::Observation<Device>>,
    ) -> BTreeMap<DeviceKey, Device> {
        let devices: Vec<_> = devices.into_iter().collect();
        devices
            .iter()
            .filter(|entry| self.predicate.matches(&entry.value))
            .filter(|entry| self.tags.iter().all(|tag| entry.value.tags.contains(tag)))
            .filter(|entry| {
                self.system.as_ref().is_none_or(|system| {
                    entry.value.location.as_ref().is_some_and(|location| {
                        let id = location.id.as_str();
                        id == system
                            || id
                                .strip_prefix(system)
                                .is_some_and(|suffix| suffix.starts_with('-'))
                    })
                })
            })
            .filter(|entry| {
                matches_link(
                    &self.attached_to,
                    entry.value.relationships.attached_to.as_ref(),
                )
            })
            .filter(|entry| {
                matches_link(
                    &self.controller,
                    entry.value.relationships.controller.as_ref(),
                )
            })
            .filter(|entry| {
                matches_link(
                    &self.hosted_by,
                    entry.value.relationships.hosted_by.as_ref(),
                )
            })
            .filter(|entry| {
                !self.without_adopted_devices
                    || !devices.iter().any(|other| {
                        other.value.relationships.controller.as_ref() == Some(&entry.value.key)
                    })
            })
            .map(|entry| (entry.value.key.clone(), entry.value.clone()))
            .collect()
    }

    fn handles(&self, entries: &BTreeMap<DeviceKey, Device>) -> Vec<DeviceHandle> {
        entries
            .keys()
            .cloned()
            .map(|key| DeviceHandle::new(self.client.clone(), key))
            .collect()
    }

    /// Collects a stable, key-sorted view from the current committed snapshot.
    pub async fn collect(self) -> Result<Vec<DeviceHandle>> {
        self.client.ensure_open()?;
        let entries = self.matching_entries(self.client.managed_state().devices());
        Ok(self.handles(&entries))
    }

    /// Subscribes to meaningful changes to this local result set. The first
    /// [`DeviceQuerySubscription::try_next`] returns an initial result; later
    /// calls coalesce all pending revisions into their newest distinct result.
    pub async fn subscribe(self) -> Result<DeviceQuerySubscription> {
        self.client.ensure_open()?;
        let receiver = self.client.managed_state().subscribe();
        let initial = self.matching_entries(self.client.managed_state().devices());
        Ok(DeviceQuerySubscription {
            query: self,
            receiver,
            previous: Mutex::new(initial.clone()),
            initial: Mutex::new(Some(initial)),
        })
    }
}

fn matches_link<T: PartialEq>(filter: &DeviceLinkFilter<T>, value: Option<&T>) -> bool {
    match filter {
        DeviceLinkFilter::Any => true,
        DeviceLinkFilter::Is(expected) => value == Some(expected),
        DeviceLinkFilter::None => value.is_none(),
    }
}

/// A coalescing local result-set subscription. It never performs network I/O.
pub struct DeviceQuerySubscription {
    query: DeviceQuery,
    receiver: mpsc::Receiver<Arc<super::state::StateSnapshot>>,
    previous: Mutex<BTreeMap<DeviceKey, Device>>,
    initial: Mutex<Option<BTreeMap<DeviceKey, Device>>>,
}

/// A meaningful change to a device query result set.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum DeviceQueryChange {
    Initial {
        results: Vec<DeviceHandle>,
    },
    Updated {
        revision: u64,
        added: Vec<DeviceHandle>,
        removed: Vec<DeviceHandle>,
        changed: Vec<DeviceHandle>,
        results: Vec<DeviceHandle>,
    },
}

impl DeviceQuerySubscription {
    /// Returns the initial results once, then the newest distinct pending
    /// committed result-set change. This is intentionally non-blocking.
    pub fn try_next(&self) -> Option<DeviceQueryChange> {
        if let Some(initial) = self
            .initial
            .lock()
            .expect("query initial lock poisoned")
            .take()
        {
            return Some(DeviceQueryChange::Initial {
                results: self.query.handles(&initial),
            });
        }
        let snapshot = self.receiver.try_iter().last()?;
        let next = self
            .query
            .matching_entries(snapshot.devices().values().cloned());
        let mut previous = self.previous.lock().expect("query result lock poisoned");
        if *previous == next {
            return None;
        }
        let added = next
            .keys()
            .filter(|key| !previous.contains_key(*key))
            .cloned()
            .map(|key| DeviceHandle::new(self.query.client.clone(), key))
            .collect();
        let removed = previous
            .keys()
            .filter(|key| !next.contains_key(*key))
            .cloned()
            .map(|key| DeviceHandle::new(self.query.client.clone(), key))
            .collect();
        let changed = next
            .iter()
            .filter(|(key, value)| previous.get(*key).is_some_and(|old| old != *value))
            .map(|(key, _)| DeviceHandle::new(self.query.client.clone(), key.clone()))
            .collect();
        *previous = next.clone();
        Some(DeviceQueryChange::Updated {
            revision: snapshot.revision(),
            added,
            removed,
            changed,
            results: self.query.handles(&next),
        })
    }
}

/// Gateway for owned devices. `cached` and `find` are local-only.
#[derive(Clone, Debug)]
pub struct DevicesGateway {
    client: Client,
}
impl DevicesGateway {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }
    #[must_use]
    pub fn cached(&self, code: &str) -> Option<DeviceHandle> {
        let key = DeviceKey::live(DeviceId::from(code));
        self.client
            .managed_state()
            .device(&key)
            .map(|_| DeviceHandle::new(self.client.clone(), key))
    }
    #[must_use]
    pub fn find(&self) -> DeviceQuery {
        DeviceQuery::new(self.client.clone())
    }
    #[must_use]
    pub fn miners(&self) -> DeviceQuery {
        self.find().miners()
    }
    /// Starts a local query for devices of a controller type.
    #[must_use]
    pub fn controllers(&self, controller_type: DeviceType) -> DeviceQuery {
        self.find().of_type(controller_type)
    }
    pub async fn get(&self, code: &str) -> Result<DeviceHandle> {
        self.client.ensure_open()?;
        let response = self.client.managed_raw().devices().get(code).await?;
        let observation = domain::device_detail(
            &response.value,
            Realm::Live,
            AccessScope::Owned,
            observed_at(),
        )
        .map_err(normalization)?;
        let key = observation.value.key.clone();
        self.client
            .managed_state()
            .persist_devices(&[observation])
            .map_err(|_| Error::Persistence {
                message: "SQLite store operation failed".into(),
            })?;
        Ok(DeviceHandle::new(self.client.clone(), key))
    }
    pub async fn refresh(&self, code: &str) -> Result<DeviceHandle> {
        self.get(code).await
    }
    pub async fn list(&self, query: &raw::devices::DeviceListQuery) -> Result<Vec<DeviceHandle>> {
        self.client.ensure_open()?;
        let response = self.client.managed_raw().devices().list(query).await?;
        let collection = domain::device_collection(
            &response.value,
            Realm::Live,
            !query_is_unfiltered(query),
            false,
            observed_at(),
        )
        .map_err(normalization)?;
        let keys = collection
            .members
            .iter()
            .map(|item| item.value.key.clone())
            .collect::<Vec<_>>();
        self.client
            .managed_state()
            .persist_devices(&collection.members)
            .map_err(|_| Error::Persistence {
                message: "SQLite store operation failed".into(),
            })?;
        Ok(keys
            .into_iter()
            .map(|key| DeviceHandle::new(self.client.clone(), key))
            .collect())
    }
}
fn query_is_unfiltered(query: &raw::devices::DeviceListQuery) -> bool {
    query.device_type.is_none() && query.location.is_none() && query.replicant_code.is_none()
}

/// Owned replicant gateway. Owned detail is normalized with private authority.
#[derive(Clone, Debug)]
pub struct ReplicantsGateway {
    client: Client,
}

/// Local-only query over cached replicants.
#[derive(Clone, Debug)]
pub struct ReplicantQuery {
    client: Client,
    realm: Option<Realm>,
    access: Option<AccessScope>,
    status: Option<ReplicantStatus>,
    location: Option<String>,
}

impl ReplicantQuery {
    fn new(client: Client) -> Self {
        Self {
            client,
            realm: None,
            access: None,
            status: None,
            location: None,
        }
    }

    #[must_use]
    pub fn in_realm(mut self, realm: Realm) -> Self {
        self.realm = Some(realm);
        self
    }
    #[must_use]
    pub fn with_access(mut self, access: AccessScope) -> Self {
        self.access = Some(access);
        self
    }
    #[must_use]
    pub fn owned(self) -> Self {
        self.with_access(AccessScope::Owned)
    }
    #[must_use]
    pub fn with_status(mut self, status: ReplicantStatus) -> Self {
        self.status = Some(status);
        self
    }
    #[must_use]
    pub fn at(mut self, location: impl Into<String>) -> Self {
        self.location = Some(location.into());
        self
    }

    /// Collects a stable, key-sorted view from the current committed snapshot.
    pub async fn collect(self) -> Result<Vec<ReplicantHandle>> {
        self.client.ensure_open()?;
        Ok(self
            .client
            .managed_state()
            .replicants()
            .into_iter()
            .filter(|entry| {
                self.realm
                    .as_ref()
                    .is_none_or(|realm| realm == &entry.value.key.realm)
            })
            .filter(|entry| {
                self.access
                    .as_ref()
                    .is_none_or(|access| access == &entry.value.access)
            })
            .filter(|entry| {
                self.status
                    .as_ref()
                    .is_none_or(|status| entry.value.status.as_ref() == Some(status))
            })
            .filter(|entry| {
                self.location.as_ref().is_none_or(|location| {
                    entry
                        .value
                        .location
                        .as_ref()
                        .is_some_and(|key| key.id.as_str() == location)
                })
            })
            .map(|entry| ReplicantHandle {
                client: self.client.clone(),
                key: entry.value.key,
            })
            .collect())
    }
}

#[derive(Clone, Debug)]
pub struct ReplicantHandle {
    client: Client,
    key: ReplicantKey,
}
impl ReplicantsGateway {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }
    /// Starts a local query over committed replicant snapshots.
    #[must_use]
    pub fn find(&self) -> ReplicantQuery {
        ReplicantQuery::new(self.client.clone())
    }
    pub async fn get_owned(&self, code: &str) -> Result<ReplicantHandle> {
        self.client.ensure_open()?;
        let response = self.client.managed_raw().replicants().get(code).await?;
        let observation =
            domain::owned_replicant_detail(&response.value, Realm::Live, observed_at())
                .map_err(normalization)?;
        let key = observation.value.key.clone();
        self.client
            .managed_state()
            .persist_replicant(observation)
            .map_err(|_| Error::Persistence {
                message: "SQLite store operation failed".into(),
            })?;
        Ok(ReplicantHandle {
            client: self.client.clone(),
            key,
        })
    }
}
impl ReplicantHandle {
    #[must_use]
    pub fn id(&self) -> &ReplicantId {
        &self.key.id
    }
    #[must_use]
    pub fn realm(&self) -> &Realm {
        &self.key.realm
    }
    pub async fn snapshot(&self) -> Result<Replicant> {
        self.client.ensure_open()?;
        self.client
            .managed_state()
            .replicant(&self.key)
            .map(|observation| observation.value)
            .ok_or_else(|| Error::Configuration {
                message: "replicant is not cached".into(),
            })
    }
    pub async fn refresh(&self) -> Result<ReplicantHandle> {
        self.client.replicants().get_owned(self.id().as_str()).await
    }
    pub async fn watch(&self) -> Result<DeviceWatch> {
        self.client.ensure_open()?;
        Ok(DeviceWatch {
            receiver: self.client.managed_state().subscribe(),
            key: DeviceKey::live(DeviceId::from(self.id().as_str())),
        })
    }

    /// Updates this replicant's profile fields as a durable operation.
    pub async fn update(
        &self,
        request: raw::replicants::ReplicantUpdateRequest,
    ) -> Result<Operation> {
        operation::replicant_update(&self.client, self.id().as_str(), request).await
    }

    /// Broadcasts a BobNet message from this replicant.
    pub async fn message(
        &self,
        request: raw::replicants::ReplicantMessageRequest,
    ) -> Result<Operation> {
        operation::replicant_message(&self.client, self.id().as_str(), request).await
    }

    /// Begins a mining operation for this replicant.
    pub async fn mine(&self, request: raw::replicants::MineRequest) -> Result<Operation> {
        operation::replicant_mine(&self.client, self.id().as_str(), request).await
    }

    /// Stops this replicant's current mining operation.
    pub async fn stop_mining(&self) -> Result<Operation> {
        operation::replicant_stop_mining(&self.client, self.id().as_str()).await
    }

    /// Queues a device to be printed by this replicant.
    pub async fn print(&self, request: raw::replicants::PrintRequest) -> Result<Operation> {
        operation::replicant_print(&self.client, self.id().as_str(), request).await
    }

    /// Runs a full system scan from this replicant's current location.
    pub async fn scan(&self) -> Result<Operation> {
        operation::replicant_scan(&self.client, self.id().as_str()).await
    }

    /// Teleports this replicant to a new matrix, incurring offline time.
    pub async fn teleport(&self, request: raw::replicants::TeleportRequest) -> Result<Operation> {
        operation::replicant_teleport(&self.client, self.id().as_str(), request).await
    }

    /// Transfers this replicant to a new hosting device or account.
    pub async fn transfer(&self, request: raw::replicants::TransferRequest) -> Result<Operation> {
        operation::replicant_transfer(&self.client, self.id().as_str(), request).await
    }

    /// Starts building a travel request: `.to(destination).preview()` to
    /// compute the route without departing, or `.depart()` to register a
    /// durable departure operation.
    #[must_use]
    pub fn travel(&self) -> TravelBuilder {
        TravelBuilder::new(self.client.clone(), self.id().as_str().to_string())
    }

    /// Cancels this replicant's current travel.
    pub async fn cancel_travel(&self) -> Result<Operation> {
        operation::replicant_cancel_travel(&self.client, self.id().as_str()).await
    }
}

/// Public directory gateway. It never treats public data as owned authority.
#[derive(Clone, Debug)]
pub struct DirectoryGateway {
    client: Client,
}
impl DirectoryGateway {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }
    pub async fn replicant(&self, code: &str) -> Result<Replicant> {
        self.client.ensure_open()?;
        let response = self.client.managed_raw().replicants().get(code).await?;
        Ok(
            domain::public_replicant_detail(&response.value, Realm::Live, observed_at())
                .map_err(normalization)?
                .value,
        )
    }
    pub async fn search(
        &self,
        query: &raw::replicants::ReplicantListQuery,
    ) -> Result<Vec<domain::DirectoryProfile>> {
        self.client.ensure_open()?;
        let response = self.client.managed_raw().replicants().list(query).await?;
        response
            .value
            .replicants
            .iter()
            .map(|profile| {
                domain::directory_profile(profile, observed_at())
                    .map(|observation| observation.value)
                    .map_err(normalization)
            })
            .collect()
    }
}

/// Gateway for resource inventory (`GET /v1/inventory`,
/// `GET /v1/replicants/{code}/inventory`). Reads commit before returning.
#[derive(Clone, Debug)]
pub struct InventoryGateway {
    client: Client,
}
impl InventoryGateway {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    /// Fetches a replicant's current-system inventory: its current location
    /// plus every other location in that star system.
    pub async fn for_replicant(&self, replicant_code: &str) -> Result<Vec<domain::Inventory>> {
        self.client.ensure_open()?;
        let response = self
            .client
            .managed_raw()
            .inventory()
            .for_replicant(replicant_code, None)
            .await?;
        let owner = domain::InventoryOwner::Replicant(ReplicantKey::live(ReplicantId::from(
            replicant_code,
        )));
        let mut raw_locations = vec![raw::inventory::LocationInventory {
            items: response.value.items.clone(),
            location: response.value.location.clone(),
            location_name: response.value.location_name.clone(),
        }];
        raw_locations.extend(response.value.locations.iter().cloned());

        let mut inventories = Vec::with_capacity(raw_locations.len());
        for raw_location in &raw_locations {
            if raw_location.location.is_none() {
                continue;
            }
            let observation =
                domain::location_inventory(raw_location, owner.clone(), Realm::Live, observed_at())
                    .map_err(normalization)?;
            let value = observation.value.clone();
            self.client
                .managed_state()
                .persist_inventory(observation)
                .map_err(|_| Error::Persistence {
                    message: "SQLite store operation failed".into(),
                })?;
            inventories.push(value);
        }
        Ok(inventories)
    }
}

#[cfg(test)]
mod tests {
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use super::*;
    use crate::managed::client::StartupPolicy;
    use crate::raw::{SecretString, Url};

    fn cached_device(
        id: &str,
        device_type: DeviceType,
        status: DeviceStatus,
    ) -> domain::Observation<Device> {
        domain::Observation {
            value: Device {
                key: DeviceKey::live(DeviceId::from(id)),
                device_type: Some(device_type),
                status: Some(status),
                location: None,
                features: Vec::new(),
                available_commands: Vec::new(),
                available_directives: Vec::new(),
                tags: Vec::new(),
                relationships: domain::DeviceRelationships::default(),
                access: AccessScope::Owned,
            },
            metadata: domain::ObservationMetadata {
                source: domain::ObservationSource::RestDetail,
                authority: domain::ObservationAuthority::EntitySnapshot,
                observed_at: "2026-07-25T00:00:00Z".into(),
                access: AccessScope::Owned,
                reachability: domain::Reachability::Reachable,
                stale: false,
                source_document: domain::SourceDocument {
                    operation: "test".into(),
                    request_id: None,
                    document_id: None,
                },
            },
        }
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
    async fn for_replicant_normalizes_current_and_system_locations_and_commits_before_returning() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/replicants/R1/inventory"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "location": "SOL-4",
                "location_name": "Earth",
                "items": [{"resource_type": "structural", "quantity": 50}],
                "locations": [
                    {"location": "SOL-BELT-1", "location_name": "Sol Belt", "items": [{"resource_type": "rares", "quantity": 5}]}
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;
        let client = client_at(&server.uri()).await;

        let inventories = client
            .inventory()
            .for_replicant("R1")
            .await
            .expect("inventory");
        assert_eq!(inventories.len(), 2);
        assert!(
            inventories
                .iter()
                .any(|inventory| inventory.location.as_ref().unwrap().id.as_str() == "SOL-4")
        );
        assert!(
            inventories
                .iter()
                .any(|inventory| inventory.location.as_ref().unwrap().id.as_str() == "SOL-BELT-1")
        );

        server.verify().await;
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn local_device_queries_filter_relationships_and_never_use_the_network() {
        let server = MockServer::start().await;
        let client = client_at(&server.uri()).await;
        let controller = cached_device("CTRL", DeviceType::MiningController, DeviceStatus::Idle);
        let mut drone = cached_device("DRONE", DeviceType::MiningDrone, DeviceStatus::Idle);
        drone.value.location = Some(domain::LocationKey::live("SOL-1".into()));
        drone.value.tags.push("ore".into());
        drone.value.features.push(DeviceFeature::Mining);
        drone.value.relationships.controller = Some(controller.value.key.clone());
        let mut public = cached_device("PUBLIC", DeviceType::MiningDrone, DeviceStatus::Idle);
        public.value.access = AccessScope::Public;
        public.metadata.access = AccessScope::Public;
        public.value.location = Some(domain::LocationKey::live("SOL-2".into()));
        client
            .managed_state()
            .persist_devices(&[controller, drone, public])
            .expect("persist");

        let miner_ids: Vec<_> = client
            .devices()
            .miners()
            .idle()
            .in_system("SOL")
            .with_tag("ore")
            .with_feature(DeviceFeature::Mining)
            .owned()
            .collect()
            .await
            .expect("query")
            .into_iter()
            .map(|device| device.id().as_str().to_owned())
            .collect();
        assert_eq!(miner_ids, ["DRONE"]);

        let snapshot = client
            .devices()
            .miners()
            .idle()
            .owned()
            .collect()
            .await
            .expect("snapshot");
        client
            .managed_state()
            .persist_devices(&[cached_device(
                "LATER",
                DeviceType::MiningDrone,
                DeviceStatus::Idle,
            )])
            .expect("later revision");
        assert_eq!(snapshot.len(), 1, "collected views are immutable snapshots");

        let controllers = client
            .devices()
            .controllers(DeviceType::MiningController)
            .idle()
            .without_adopted_devices()
            .collect()
            .await
            .expect("query");
        assert!(controllers.is_empty());

        // No mock is mounted: every successful assertion above proves that
        // local query evaluation made no HTTP request.
        server.verify().await;
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn query_subscription_is_initial_stable_and_coalesces_revisions() {
        let server = MockServer::start().await;
        let client = client_at(&server.uri()).await;
        let subscription = client
            .devices()
            .miners()
            .idle()
            .subscribe()
            .await
            .expect("subscription");
        assert!(matches!(
            subscription.try_next(),
            Some(DeviceQueryChange::Initial { results }) if results.is_empty()
        ));

        let first = cached_device("B", DeviceType::MiningDrone, DeviceStatus::Idle);
        let second = cached_device("A", DeviceType::MiningDrone, DeviceStatus::Idle);
        client
            .managed_state()
            .persist_devices(&[first])
            .expect("first revision");
        client
            .managed_state()
            .persist_devices(&[second])
            .expect("second revision");
        match subscription.try_next().expect("coalesced update") {
            DeviceQueryChange::Updated { added, results, .. } => {
                assert_eq!(added.len(), 2);
                let ids: Vec<_> = results.iter().map(|device| device.id().as_str()).collect();
                assert_eq!(ids, ["A", "B"]);
            }
            DeviceQueryChange::Initial { .. } => panic!("initial already consumed"),
        }
        assert!(subscription.try_next().is_none());

        let mut changed = cached_device("B", DeviceType::MiningDrone, DeviceStatus::Idle);
        changed.value.tags.push("changed".into());
        client
            .managed_state()
            .persist_devices(&[changed])
            .expect("changed revision");
        match subscription.try_next().expect("changed result") {
            DeviceQueryChange::Updated { changed, .. } => {
                assert_eq!(changed[0].id().as_str(), "B");
            }
            DeviceQueryChange::Initial { .. } => panic!("initial already consumed"),
        }

        let inactive = cached_device("A", DeviceType::MiningDrone, DeviceStatus::Active);
        client
            .managed_state()
            .persist_devices(&[inactive])
            .expect("third revision");
        match subscription.try_next().expect("removal") {
            DeviceQueryChange::Updated {
                removed, results, ..
            } => {
                assert_eq!(removed[0].id().as_str(), "A");
                assert_eq!(results[0].id().as_str(), "B");
            }
            DeviceQueryChange::Initial { .. } => panic!("initial already consumed"),
        }

        client.close().await.expect("close");
        assert!(subscription.try_next().is_none());
        server.verify().await;
    }
}
