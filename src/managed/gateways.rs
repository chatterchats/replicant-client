//! Managed read gateways. They normalize the one response they fetch, commit it,
//! publish the resulting revision, and only then return a domain value.

#![allow(missing_docs)] // Gateway module documentation explains the common contract.

use std::sync::mpsc;

use crate::domain::{
    self, AccessScope, Account, AccountId, Device, DeviceId, DeviceKey, DeviceStatus, DeviceType,
    Realm, Replicant, ReplicantId, ReplicantKey,
};
use crate::raw;
use crate::{Client, Error, Result};

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
}

/// Local-only device query. It cannot perform network I/O.
#[derive(Clone, Debug)]
pub struct DeviceQuery {
    client: Client,
    device_type: Option<DeviceType>,
    status: Option<DeviceStatus>,
    location: Option<String>,
}
impl DeviceQuery {
    fn new(client: Client) -> Self {
        Self {
            client,
            device_type: None,
            status: None,
            location: None,
        }
    }
    #[must_use]
    pub fn of_type(mut self, value: DeviceType) -> Self {
        self.device_type = Some(value);
        self
    }
    #[must_use]
    pub fn with_status(mut self, value: DeviceStatus) -> Self {
        self.status = Some(value);
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
    #[must_use]
    pub fn at(mut self, location: impl Into<String>) -> Self {
        self.location = Some(location.into());
        self
    }
    pub async fn collect(self) -> Result<Vec<DeviceHandle>> {
        self.client.ensure_open()?;
        Ok(self
            .client
            .managed_state()
            .devices()
            .into_iter()
            .filter(|entry| {
                self.device_type
                    .as_ref()
                    .is_none_or(|value| entry.value.device_type.as_ref() == Some(value))
                    && self
                        .status
                        .as_ref()
                        .is_none_or(|value| entry.value.status.as_ref() == Some(value))
                    && self.location.as_ref().is_none_or(|value| {
                        entry
                            .value
                            .location
                            .as_ref()
                            .is_some_and(|location| location.id.as_str() == value)
                    })
            })
            .map(|entry| DeviceHandle::new(self.client.clone(), entry.value.key))
            .collect())
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
#[derive(Clone, Debug)]
pub struct ReplicantHandle {
    client: Client,
    key: ReplicantKey,
}
impl ReplicantsGateway {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
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
