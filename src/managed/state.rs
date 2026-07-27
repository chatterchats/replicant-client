//! Immutable, crate-private state snapshots published after durable commits.

#![allow(dead_code)] // Internal state is shared by gateways and background workers.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use tokio::sync::watch;
use tracing::{debug, info};

use crate::domain::{
    Account, Device, DeviceKey, Event, Inventory, InventoryOwner, Location, LocationKey,
    Observation, Realm, Replicant, ReplicantKey, Simulation, SimulationId, Star, StarKey,
    StarKnowledge,
};

use super::store::{OperationJournalEntry, ReconciliationWork, StoreError, StoreHandle};

#[derive(Clone, Debug, Default)]
pub(crate) struct StateSnapshot {
    revision: u64,
    devices: BTreeMap<DeviceKey, Observation<Device>>,
    account: Option<Observation<Account>>,
    replicants: BTreeMap<ReplicantKey, Observation<Replicant>>,
    locations: BTreeMap<LocationKey, Observation<Location>>,
    inventories: BTreeMap<InventoryOwner, Observation<Inventory>>,
    simulations: BTreeMap<SimulationId, Observation<Simulation>>,
}

#[derive(Clone, Debug, Default)]
struct GalaxySnapshot {
    catalogue: BTreeMap<StarKey, Observation<Star>>,
    generated_at: Option<String>,
    knowledge: BTreeMap<(ReplicantKey, StarKey), Observation<StarKnowledge>>,
}

impl StateSnapshot {
    pub(crate) fn revision(&self) -> u64 {
        self.revision
    }

    pub(crate) fn devices(&self) -> &BTreeMap<DeviceKey, Observation<Device>> {
        &self.devices
    }
}

pub(crate) struct StateEngine {
    store: StoreHandle,
    snapshot: RwLock<Arc<StateSnapshot>>,
    snapshots: watch::Sender<Arc<StateSnapshot>>,
    galaxy: RwLock<Arc<GalaxySnapshot>>,
}

impl StateEngine {
    pub(crate) fn open_memory() -> Result<Self, StoreError> {
        Self::from_store(StoreHandle::open_memory_blocking()?)
    }

    pub(crate) fn open_file(path: &Path) -> Result<Self, StoreError> {
        Self::from_store(StoreHandle::open_file_blocking(path.into())?)
    }

    pub(crate) fn from_store(store: StoreHandle) -> Result<Self, StoreError> {
        let restore_started = Instant::now();
        let (
            devices,
            account,
            replicants,
            locations,
            inventories,
            simulations,
            catalogue,
            knowledge,
        ) = store.execute_blocking(|opened| {
            Ok((
                opened.restore_devices()?,
                opened.restore_account()?,
                opened.restore_replicants()?,
                opened.restore_locations()?,
                opened.restore_inventories()?,
                opened.restore_simulations()?,
                opened.restore_catalogue()?,
                opened.restore_star_knowledge()?,
            ))
        })?;
        let snapshot = Arc::new(StateSnapshot {
            revision: 0,
            devices,
            account,
            replicants,
            locations,
            inventories,
            simulations,
        });
        let (snapshots, _) = watch::channel(Arc::clone(&snapshot));
        info!(
            target: "replicant_client::state",
            event = "state.restored",
            elapsed_ms = restore_started.elapsed().as_millis() as u64,
            account_present = snapshot.account.is_some(),
            devices = snapshot.devices.len(),
            replicants = snapshot.replicants.len(),
            locations = snapshot.locations.len(),
            inventories = snapshot.inventories.len(),
            simulations = snapshot.simulations.len(),
            catalogue = catalogue.0.len(),
            star_knowledge = knowledge.len(),
            "restored durable managed state"
        );
        Ok(Self {
            store,
            snapshot: RwLock::new(snapshot),
            snapshots,
            galaxy: RwLock::new(Arc::new(GalaxySnapshot {
                catalogue: catalogue.0,
                generated_at: catalogue.1,
                knowledge,
            })),
        })
    }

    pub(crate) fn snapshot(&self) -> Arc<StateSnapshot> {
        Arc::clone(&self.snapshot.read().expect("state snapshot lock poisoned"))
    }

    pub(crate) fn device(&self, key: &DeviceKey) -> Option<Observation<Device>> {
        self.snapshot().devices.get(key).cloned()
    }

    pub(crate) fn devices(&self) -> Vec<Observation<Device>> {
        self.snapshot().devices.values().cloned().collect()
    }

    pub(crate) fn account(&self) -> Option<Observation<Account>> {
        self.snapshot().account.clone()
    }

    pub(crate) fn replicant(&self, key: &ReplicantKey) -> Option<Observation<Replicant>> {
        self.snapshot().replicants.get(key).cloned()
    }

    pub(crate) fn replicants(&self) -> Vec<Observation<Replicant>> {
        self.snapshot().replicants.values().cloned().collect()
    }

    pub(crate) fn location(&self, key: &LocationKey) -> Option<Observation<Location>> {
        self.snapshot().locations.get(key).cloned()
    }

    pub(crate) fn locations(&self) -> Vec<Observation<Location>> {
        self.snapshot().locations.values().cloned().collect()
    }

    pub(crate) fn catalogue(&self) -> Vec<Observation<Star>> {
        self.galaxy
            .read()
            .expect("galaxy snapshot lock poisoned")
            .catalogue
            .values()
            .cloned()
            .collect()
    }

    pub(crate) fn catalogue_generated_at(&self) -> Option<String> {
        self.galaxy
            .read()
            .expect("galaxy snapshot lock poisoned")
            .generated_at
            .clone()
    }

    pub(crate) fn star_knowledge(
        &self,
        replicant: &ReplicantKey,
    ) -> Vec<Observation<StarKnowledge>> {
        self.galaxy
            .read()
            .expect("galaxy snapshot lock poisoned")
            .knowledge
            .iter()
            .filter(|((known_by, _), _)| known_by == replicant)
            .map(|(_, value)| value.clone())
            .collect()
    }

    pub(crate) fn replace_catalogue(
        &self,
        stars: Vec<Observation<Star>>,
        generated_at: Option<String>,
    ) -> Result<(), StoreError> {
        self.store
            .lock()
            .as_mut()
            .ok_or(StoreError::Closed)?
            .replace_catalogue(&stars, generated_at.as_deref())?;
        let mut galaxy = (*self.galaxy.read().expect("galaxy snapshot lock poisoned"))
            .as_ref()
            .clone();
        galaxy.catalogue = stars
            .into_iter()
            .map(|star| (star.value.key.clone(), star))
            .collect();
        galaxy.generated_at = generated_at;
        *self.galaxy.write().expect("galaxy snapshot lock poisoned") = Arc::new(galaxy);
        let mut snapshot = (*self.snapshot()).clone();
        snapshot.revision += 1;
        self.publish(snapshot);
        Ok(())
    }

    pub(crate) fn persist_star_knowledge(
        &self,
        knowledge: Observation<StarKnowledge>,
    ) -> Result<(), StoreError> {
        self.store
            .lock()
            .as_mut()
            .ok_or(StoreError::Closed)?
            .persist_star_knowledge(&knowledge)?;
        let mut galaxy = (*self.galaxy.read().expect("galaxy snapshot lock poisoned"))
            .as_ref()
            .clone();
        galaxy.knowledge.insert(
            (
                knowledge.value.replicant.clone(),
                knowledge.value.star.clone(),
            ),
            knowledge,
        );
        *self.galaxy.write().expect("galaxy snapshot lock poisoned") = Arc::new(galaxy);
        let mut snapshot = (*self.snapshot()).clone();
        snapshot.revision += 1;
        self.publish(snapshot);
        Ok(())
    }

    pub(crate) fn inventory(&self, owner: &InventoryOwner) -> Option<Observation<Inventory>> {
        self.snapshot().inventories.get(owner).cloned()
    }

    pub(crate) fn persist_account(&self, account: Observation<Account>) -> Result<(), StoreError> {
        self.store
            .lock()
            .as_mut()
            .ok_or(StoreError::Closed)?
            .persist_account(&account)?;
        let previous = self.snapshot();
        self.publish(StateSnapshot {
            revision: previous.revision + 1,
            devices: previous.devices.clone(),
            account: Some(account),
            replicants: previous.replicants.clone(),
            locations: previous.locations.clone(),
            inventories: previous.inventories.clone(),
            simulations: previous.simulations.clone(),
        });
        Ok(())
    }

    pub(crate) fn persist_replicant(
        &self,
        replicant: Observation<Replicant>,
    ) -> Result<(), StoreError> {
        self.store
            .lock()
            .as_mut()
            .ok_or(StoreError::Closed)?
            .persist_replicant(&replicant)?;
        let previous = self.snapshot();
        let mut replicants = previous.replicants.clone();
        replicants.insert(replicant.value.key.clone(), replicant);
        let simulations = previous.simulations.clone();
        self.publish(StateSnapshot {
            revision: previous.revision + 1,
            devices: previous.devices.clone(),
            account: previous.account.clone(),
            replicants,
            locations: previous.locations.clone(),
            inventories: previous.inventories.clone(),
            simulations,
        });
        Ok(())
    }

    pub(crate) fn simulation(&self, id: SimulationId) -> Option<Observation<Simulation>> {
        self.snapshot().simulations.get(&id).cloned()
    }

    pub(crate) fn simulations(&self) -> Vec<Observation<Simulation>> {
        self.snapshot().simulations.values().cloned().collect()
    }

    /// Commits a simulation run's current observation (start, then later its
    /// archived result). Never removed: simulation rows are account history.
    pub(crate) fn persist_simulation(
        &self,
        simulation: Observation<Simulation>,
    ) -> Result<(), StoreError> {
        self.store
            .lock()
            .as_mut()
            .ok_or(StoreError::Closed)?
            .persist_simulation(&simulation)?;
        let previous = self.snapshot();
        let mut simulations = previous.simulations.clone();
        simulations.insert(simulation.value.id, simulation);
        self.publish(StateSnapshot {
            revision: previous.revision + 1,
            devices: previous.devices.clone(),
            account: previous.account.clone(),
            replicants: previous.replicants.clone(),
            locations: previous.locations.clone(),
            inventories: previous.inventories.clone(),
            simulations,
        });
        Ok(())
    }

    /// Publishes a simulation seed only after its run record and every
    /// successfully fetched initial device committed together.
    pub(crate) fn persist_simulation_and_devices(
        &self,
        simulation: Observation<Simulation>,
        devices: &[Observation<Device>],
    ) -> Result<(), StoreError> {
        self.store
            .lock()
            .as_mut()
            .ok_or(StoreError::Closed)?
            .persist_simulation_and_devices(&simulation, devices)?;
        let previous = self.snapshot();
        let mut next_devices = previous.devices.clone();
        for device in devices {
            next_devices.insert(device.value.key.clone(), device.clone());
        }
        let mut simulations = previous.simulations.clone();
        simulations.insert(simulation.value.id, simulation);
        self.publish(StateSnapshot {
            revision: previous.revision + 1,
            devices: next_devices,
            account: previous.account.clone(),
            replicants: previous.replicants.clone(),
            locations: previous.locations.clone(),
            inventories: previous.inventories.clone(),
            simulations,
        });
        Ok(())
    }

    /// Removes every device observation in `realm`: simulation cleanup on
    /// abandonment, completion, or expiry. Live devices are never affected.
    pub(crate) fn purge_realm_devices(&self, realm: &Realm) -> Result<(), StoreError> {
        let removed = self
            .store
            .lock()
            .as_mut()
            .ok_or(StoreError::Closed)?
            .purge_realm_devices(realm)?;
        if removed.is_empty() {
            return Ok(());
        }
        let previous = self.snapshot();
        let mut devices = previous.devices.clone();
        for key in &removed {
            devices.remove(key);
        }
        self.publish(StateSnapshot {
            revision: previous.revision + 1,
            devices,
            account: previous.account.clone(),
            replicants: previous.replicants.clone(),
            locations: previous.locations.clone(),
            inventories: previous.inventories.clone(),
            simulations: previous.simulations.clone(),
        });
        Ok(())
    }

    /// Commits a targeted inventory observation before publishing a new revision.
    pub(crate) fn persist_inventory(
        &self,
        inventory: Observation<Inventory>,
    ) -> Result<(), StoreError> {
        self.store
            .lock()
            .as_mut()
            .ok_or(StoreError::Closed)?
            .persist_inventory(&inventory)?;
        let previous = self.snapshot();
        let mut inventories = previous.inventories.clone();
        inventories.insert(inventory.value.owner.clone(), inventory);
        self.publish(StateSnapshot {
            revision: previous.revision + 1,
            devices: previous.devices.clone(),
            account: previous.account.clone(),
            replicants: previous.replicants.clone(),
            locations: previous.locations.clone(),
            inventories,
            simulations: previous.simulations.clone(),
        });
        Ok(())
    }

    /// Commits a targeted location observation before publishing a new revision.
    pub(crate) fn persist_location(
        &self,
        mut location: Observation<Location>,
    ) -> Result<(), StoreError> {
        let previous = self.snapshot();
        if let Some(existing) = previous.locations.get(&location.value.key) {
            let mut merged = existing.value.clone();
            merged.merge_from(&location.value);
            location.value = merged;
        }
        self.store
            .lock()
            .as_mut()
            .ok_or(StoreError::Closed)?
            .persist_location(&location)?;
        let mut locations = previous.locations.clone();
        locations.insert(location.value.key.clone(), location);
        self.publish(StateSnapshot {
            revision: previous.revision + 1,
            devices: previous.devices.clone(),
            account: previous.account.clone(),
            replicants: previous.replicants.clone(),
            locations,
            inventories: previous.inventories.clone(),
            simulations: previous.simulations.clone(),
        });
        Ok(())
    }

    /// Durable dedup check shared by log catch-up and SSE delivery.
    pub(crate) fn has_event(&self, event_id: &str) -> Result<bool, StoreError> {
        self.store
            .lock()
            .as_ref()
            .ok_or(StoreError::Closed)?
            .has_event(event_id)
    }

    /// The last durably applied event cursor, restored across restarts.
    pub(crate) fn event_cursor(&self) -> Result<Option<String>, StoreError> {
        self.store
            .lock()
            .as_ref()
            .ok_or(StoreError::Closed)?
            .event_cursor()
    }

    /// Persists a baseline watermark with no accompanying event.
    pub(crate) fn set_event_cursor(&self, cursor: &str) -> Result<(), StoreError> {
        self.store
            .lock()
            .as_mut()
            .ok_or(StoreError::Closed)?
            .set_event_cursor(cursor)
    }

    /// Whether the applied cursor is old enough that continuity cannot be
    /// assumed. Never relies on an explicit server cursor rejection.
    pub(crate) fn event_cursor_is_stale(
        &self,
        threshold: std::time::Duration,
    ) -> Result<bool, StoreError> {
        self.store
            .lock()
            .as_ref()
            .ok_or(StoreError::Closed)?
            .event_cursor_is_stale(threshold.as_secs() as i64)
    }

    #[cfg(test)]
    pub(crate) fn backdate_event_cursor(&self, seconds: i64) -> Result<(), StoreError> {
        self.store
            .lock()
            .as_mut()
            .ok_or(StoreError::Closed)?
            .backdate_event_cursor(seconds)
    }

    /// Commits an event and advances the applied cursor atomically, then
    /// publishes a new state revision.
    pub(crate) fn apply_event(&self, event: &Event, cursor: &str) -> Result<bool, StoreError> {
        let inserted = self
            .store
            .lock()
            .as_mut()
            .ok_or(StoreError::Closed)?
            .append_event_and_project(event, cursor, &[])?;
        if !inserted {
            return Ok(false);
        }
        let previous = self.snapshot();
        self.publish(StateSnapshot {
            revision: previous.revision + 1,
            devices: previous.devices.clone(),
            account: previous.account.clone(),
            replicants: previous.replicants.clone(),
            locations: previous.locations.clone(),
            inventories: previous.inventories.clone(),
            simulations: previous.simulations.clone(),
        });
        Ok(true)
    }

    /// Commits an event, tombstones explicitly decommissioned devices, and
    /// advances the applied cursor atomically, then publishes a new revision.
    pub(crate) fn apply_event_with_decommission(
        &self,
        event: &Event,
        cursor: &str,
        decommissioned: &[DeviceKey],
    ) -> Result<bool, StoreError> {
        let inserted = self
            .store
            .lock()
            .as_mut()
            .ok_or(StoreError::Closed)?
            .append_event_and_decommission(event, cursor, decommissioned)?;
        if !inserted {
            return Ok(false);
        }
        let previous = self.snapshot();
        let mut devices = previous.devices.clone();
        for key in decommissioned {
            devices.remove(key);
        }
        self.publish(StateSnapshot {
            revision: previous.revision + 1,
            devices,
            account: previous.account.clone(),
            replicants: previous.replicants.clone(),
            locations: previous.locations.clone(),
            inventories: previous.inventories.clone(),
            simulations: previous.simulations.clone(),
        });
        Ok(true)
    }

    /// Enqueues (or coalesces) durable reconciliation work keyed by `work_id`.
    pub(crate) fn enqueue_reconciliation(
        &self,
        work_id: &str,
        realm: &Realm,
        kind: &str,
        payload: &serde_json::Value,
    ) -> Result<(), StoreError> {
        self.store
            .lock()
            .as_mut()
            .ok_or(StoreError::Closed)?
            .enqueue_reconciliation(work_id, realm, kind, payload)
    }

    /// Claims the next due reconciliation work item, if any.
    pub(crate) fn claim_reconciliation_work(
        &self,
    ) -> Result<Option<ReconciliationWork>, StoreError> {
        self.store
            .lock()
            .as_mut()
            .ok_or(StoreError::Closed)?
            .claim_reconciliation_work()
    }

    /// Completes successfully claimed reconciliation work.
    pub(crate) fn complete_reconciliation_work(&self, work_id: &str) -> Result<(), StoreError> {
        self.store
            .lock()
            .as_mut()
            .ok_or(StoreError::Closed)?
            .complete_reconciliation_work(work_id)
    }

    /// Requeues failed reconciliation work with bounded exponential backoff.
    pub(crate) fn retry_reconciliation_work(&self, work_id: &str) -> Result<(), StoreError> {
        self.store
            .lock()
            .as_mut()
            .ok_or(StoreError::Closed)?
            .retry_reconciliation_work(work_id)
    }

    /// The account ID bound to this store, if any account has bound yet.
    pub(crate) fn bound_account_id(&self) -> Result<Option<String>, StoreError> {
        self.store
            .lock()
            .as_ref()
            .ok_or(StoreError::Closed)?
            .bound_account_id()
    }

    pub(crate) fn rebind_account_and_persist(
        &self,
        previous: &crate::domain::AccountId,
        account: Observation<Account>,
    ) -> Result<(), StoreError> {
        self.store
            .lock()
            .as_mut()
            .ok_or(StoreError::Closed)?
            .rebind_account_and_persist(previous, &account)?;
        let previous = self.snapshot();
        self.publish(StateSnapshot {
            revision: previous.revision + 1,
            account: Some(account),
            ..(*previous).clone()
        });
        Ok(())
    }

    /// Persists a durable operation's initial intent, before any unsafe
    /// network transmission is attempted.
    pub(crate) fn record_operation(
        &self,
        operation_id: &str,
        state: &str,
        target_realm: Option<&str>,
        target_kind: Option<&str>,
        target_id: Option<&str>,
        intent: &serde_json::Value,
    ) -> Result<(), StoreError> {
        self.store
            .lock()
            .as_mut()
            .ok_or(StoreError::Closed)?
            .record_operation(
                operation_id,
                state,
                target_realm,
                target_kind,
                target_id,
                intent,
            )
    }

    /// Advances an operation's state with no accompanying projection.
    pub(crate) fn set_operation_state(
        &self,
        operation_id: &str,
        state: &str,
    ) -> Result<(), StoreError> {
        self.store
            .lock()
            .as_mut()
            .ok_or(StoreError::Closed)?
            .set_operation_state(operation_id, state)
    }

    pub(crate) fn claim_operation_submission(
        &self,
        operation_id: &str,
        attempt_id: &str,
    ) -> Result<bool, StoreError> {
        self.store
            .lock()
            .as_mut()
            .ok_or(StoreError::Closed)?
            .claim_operation_submission(operation_id, attempt_id)
    }

    #[cfg(test)]
    pub(crate) fn fail_next_operation_commit(&self) {
        self.store
            .lock()
            .as_mut()
            .expect("state store open")
            .fail_next_commit();
    }

    /// Atomically commits a device projection produced by an operation's
    /// response alongside the operation's resolved state.
    pub(crate) fn record_operation_and_project(
        &self,
        operation_id: &str,
        state: &str,
        devices: &[Observation<Device>],
    ) -> Result<(), StoreError> {
        self.store
            .lock()
            .as_mut()
            .ok_or(StoreError::Closed)?
            .record_operation_and_project(operation_id, state, devices)
    }

    /// Resolves an operation with a sanitized outcome projection.
    pub(crate) fn append_operation_projection(
        &self,
        operation_id: &str,
        state: &str,
        projection: &serde_json::Value,
    ) -> Result<(), StoreError> {
        self.store
            .lock()
            .as_mut()
            .ok_or(StoreError::Closed)?
            .append_operation_projection(operation_id, state, projection)
    }

    pub(crate) fn read_operation(
        &self,
        operation_id: &str,
    ) -> Result<Option<OperationJournalEntry>, StoreError> {
        self.store
            .lock()
            .as_ref()
            .ok_or(StoreError::Closed)?
            .read_operation(operation_id)
    }

    /// Restart recovery: promotes any operation caught mid-transmission to
    /// `ambiguous`, never blindly resubmitting it.
    pub(crate) fn promote_crashed_submissions(&self) -> Result<usize, StoreError> {
        self.store
            .lock()
            .as_mut()
            .ok_or(StoreError::Closed)?
            .promote_crashed_submissions()
    }

    pub(crate) fn list_unresolved_operations(
        &self,
    ) -> Result<Vec<(String, OperationJournalEntry)>, StoreError> {
        self.store
            .lock()
            .as_ref()
            .ok_or(StoreError::Closed)?
            .list_unresolved_operations()
    }

    pub(crate) fn find_operations_awaiting_evidence(
        &self,
        target_realm: &str,
        target_kind: &str,
        target_id: &str,
    ) -> Result<Vec<String>, StoreError> {
        self.store
            .lock()
            .as_ref()
            .ok_or(StoreError::Closed)?
            .find_operations_awaiting_evidence(target_realm, target_kind, target_id)
    }

    pub(crate) fn subscribe(&self) -> watch::Receiver<Arc<StateSnapshot>> {
        self.snapshots.subscribe()
    }

    /// Commits every projection before replacing or broadcasting the snapshot.
    pub(crate) fn persist_devices(
        &self,
        devices: &[Observation<Device>],
    ) -> Result<Arc<StateSnapshot>, StoreError> {
        self.store
            .lock()
            .as_mut()
            .ok_or(StoreError::Closed)?
            .persist_devices(devices)?;
        let previous = self.snapshot();
        let mut next_devices = previous.devices.clone();
        for device in devices {
            next_devices.insert(device.value.key.clone(), device.clone());
        }
        Ok(self.publish(StateSnapshot {
            revision: previous.revision + 1,
            devices: next_devices,
            account: previous.account.clone(),
            replicants: previous.replicants.clone(),
            locations: previous.locations.clone(),
            inventories: previous.inventories.clone(),
            simulations: previous.simulations.clone(),
        }))
    }

    /// Applies absence reconciliation after a complete unfiltered owned-device
    /// traversal. Inaccessible historical observations remain cached.
    pub(crate) fn reconcile_owned_devices(
        &self,
        present: &BTreeSet<DeviceKey>,
    ) -> Result<Arc<StateSnapshot>, StoreError> {
        self.store
            .lock()
            .as_mut()
            .ok_or(StoreError::Closed)?
            .reconcile_owned_devices(present)?;
        let previous = self.snapshot();
        let devices = previous
            .devices
            .iter()
            .filter(|(key, observation)| {
                present.contains(*key)
                    || observation.metadata.reachability != crate::domain::Reachability::Reachable
                    || observation.metadata.access != crate::domain::AccessScope::Owned
            })
            .map(|(key, observation)| (key.clone(), observation.clone()))
            .collect();
        Ok(self.publish(StateSnapshot {
            revision: previous.revision + 1,
            devices,
            account: previous.account.clone(),
            replicants: previous.replicants.clone(),
            locations: previous.locations.clone(),
            inventories: previous.inventories.clone(),
            simulations: previous.simulations.clone(),
        }))
    }

    fn publish(&self, next: StateSnapshot) -> Arc<StateSnapshot> {
        let publish_started = Instant::now();
        let revision = next.revision;
        let devices = next.devices.len();
        let replicants = next.replicants.len();
        let locations = next.locations.len();
        let inventories = next.inventories.len();
        let simulations = next.simulations.len();
        let next = Arc::new(next);
        *self
            .snapshot
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Arc::clone(&next);
        self.snapshots.send_replace(Arc::clone(&next));
        debug!(
            target: "replicant_client::state",
            event = "state.snapshot_published",
            revision,
            devices,
            replicants,
            locations,
            inventories,
            simulations,
            elapsed_ms = publish_started.elapsed().as_millis() as u64,
            "published managed state snapshot"
        );
        next
    }

    #[cfg(test)]
    pub(crate) fn fail_next_commit(&self) {
        self.store
            .lock()
            .as_mut()
            .expect("state store is open during this test")
            .fail_next_commit();
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::domain::{
        AccessScope, DeviceKey, DeviceRelationships, DeviceStatus, DeviceType,
        ObservationAuthority, ObservationMetadata, ObservationSource, Reachability, SourceDocument,
    };

    fn test_path() -> std::path::PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("replicant-client-state-{nonce}.sqlite"))
    }

    fn device(id: &str) -> Observation<Device> {
        Observation {
            value: Device {
                key: DeviceKey::live(id.into()),
                device_type: Some(DeviceType::from("miner")),
                status: Some(DeviceStatus::from("idle")),
                location: None,
                features: Vec::new(),
                available_commands: Vec::new(),
                available_directives: Vec::new(),
                tags: Vec::new(),
                relationships: DeviceRelationships::default(),
                access: AccessScope::Owned,
            },
            metadata: ObservationMetadata {
                source: ObservationSource::RestDetail,
                authority: ObservationAuthority::EntitySnapshot,
                observed_at: "2026-07-25T00:00:00Z".into(),
                access: AccessScope::Owned,
                reachability: Reachability::Reachable,
                stale: false,
                source_document: SourceDocument {
                    operation: "GET /v1/devices/{device_code}".into(),
                    request_id: None,
                    document_id: None,
                },
            },
        }
    }

    #[test]
    fn failed_commit_never_publishes_a_revision() {
        let engine = StateEngine::open_memory().expect("open engine");
        let receiver = engine.subscribe();
        engine.fail_next_commit();
        assert!(engine.persist_devices(&[device("d1")]).is_err());
        assert_eq!(engine.snapshot().revision(), 0);
        assert!(!receiver.has_changed().expect("watch is open"));
    }

    #[test]
    fn interrupted_transaction_restores_last_durable_snapshot_after_restart() {
        let path = test_path();
        let engine = StateEngine::open_file(&path).expect("open engine");
        engine
            .persist_devices(&[device("durable")])
            .expect("commit device");
        engine.fail_next_commit();
        assert!(engine.persist_devices(&[device("rolled-back")]).is_err());
        drop(engine);

        let restored = StateEngine::open_file(&path).expect("restore engine");
        assert_eq!(restored.snapshot().devices().len(), 1);
        assert!(
            restored
                .snapshot()
                .devices()
                .contains_key(&DeviceKey::live("durable".into()))
        );
        drop(restored);
        fs::remove_file(path).expect("remove test database");
    }

    #[test]
    fn location_environment_restores_and_incomplete_detail_cannot_erase_it() {
        let path = test_path();
        let engine = StateEngine::open_file(&path).expect("open engine");
        let metadata = || ObservationMetadata {
            source: ObservationSource::RestDetail,
            authority: ObservationAuthority::EntitySnapshot,
            observed_at: "2026-07-26T00:00:00Z".into(),
            access: AccessScope::Owned,
            reachability: Reachability::Reachable,
            stale: false,
            source_document: SourceDocument {
                operation: "test".into(),
                request_id: None,
                document_id: None,
            },
        };
        let key = LocationKey::live("SOL-2".into());
        let complete = Observation {
            value: Location {
                key: key.clone(),
                location_type: Some(crate::domain::LocationType::Planet),
                scanned: Some(true),
                system_scanned: None,
                system_tags: Vec::new(),
                system: Some("SOL".into()),
                parent: None,
                environment: crate::domain::LocationEnvironment {
                    atmosphere: crate::domain::Knowledge::Present(crate::domain::Atmosphere::from(
                        "thin",
                    )),
                    magnetic_field: crate::domain::Knowledge::Present(false),
                    gravity_g: crate::domain::Knowledge::Present(1.0),
                    surface_temp_c: crate::domain::Knowledge::Present(18.0),
                    in_habitable_zone: crate::domain::Knowledge::Present(true),
                    life_stage: crate::domain::Knowledge::Absent,
                    ..crate::domain::LocationEnvironment::default()
                },
                unknown: BTreeMap::new(),
            },
            metadata: metadata(),
        };
        engine
            .persist_location(complete)
            .expect("complete observation");
        let incomplete = Observation {
            value: Location {
                key: key.clone(),
                location_type: Some(crate::domain::LocationType::Planet),
                scanned: None,
                system_scanned: None,
                system_tags: Vec::new(),
                system: None,
                parent: None,
                environment: crate::domain::LocationEnvironment::default(),
                unknown: BTreeMap::new(),
            },
            metadata: metadata(),
        };
        engine
            .persist_location(incomplete)
            .expect("partial observation");
        drop(engine);

        let restored = StateEngine::open_file(&path).expect("restore engine");
        let location = restored.location(&key).expect("restored location").value;
        assert!(matches!(
            location.environment.life_stage,
            crate::domain::Knowledge::Absent
        ));
        assert!(
            matches!(location.environment.gravity_g, crate::domain::Knowledge::Present(value) if value == 1.0)
        );
        drop(restored);
        fs::remove_file(path).expect("remove test database");
    }
}
