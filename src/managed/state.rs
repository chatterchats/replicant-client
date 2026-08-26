//! Immutable, crate-private state snapshots published after durable commits.

#![allow(dead_code)] // Internal state is shared by gateways and background workers.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use tokio::sync::watch;
use tracing::{debug, info};

use crate::domain::{
    AccessScope, Account, Device, DeviceKey, Event, IncomingObject, Inventory, InventoryOwner,
    Location, LocationEvent, LocationKey, MergeOutcome, Message, Observation, Realm, Replicant,
    ReplicantKey, ResourceSite, Simulation, SimulationId, Star, StarKey, StarKnowledge,
    merge_device, merge_star,
};

use super::store::{
    EventProjectionBatch, MessageMetadata, OperationJournalEntry, ProjectionReplayState,
    ReconciliationWork, StoreError, StoreHandle,
};

/// Local-only managed-state revision gateway.
#[derive(Clone, Debug)]
pub struct StateGateway {
    client: super::Client,
}

impl StateGateway {
    pub(crate) fn new(client: super::Client) -> Self {
        Self { client }
    }

    /// Returns the current revision of durably committed managed state.
    pub fn revision(&self) -> crate::Result<u64> {
        self.client.ensure_open()?;
        Ok(self.client.managed_state().snapshot().revision())
    }

    /// Returns the revision of projections used by the galaxy scene.
    pub fn galaxy_revision(&self) -> crate::Result<u64> {
        self.client.ensure_open()?;
        Ok(self.client.managed_state().snapshot().galaxy_revision())
    }

    /// Returns current durable owned-device projections without network I/O.
    pub fn owned_devices(&self) -> crate::Result<Vec<Device>> {
        self.client.ensure_open()?;
        Ok(self
            .client
            .managed_state()
            .snapshot()
            .devices
            .values()
            .filter(|observation| observation.value.access == AccessScope::Owned)
            .map(|observation| observation.value.clone())
            .collect())
    }

    /// Returns current durable owned-Replicant projections without network I/O.
    pub fn owned_replicants(&self) -> crate::Result<Vec<Replicant>> {
        self.client.ensure_open()?;
        Ok(self
            .client
            .managed_state()
            .snapshot()
            .replicants
            .values()
            .filter(|observation| observation.value.access == AccessScope::Owned)
            .map(|observation| observation.value.clone())
            .collect())
    }

    /// Returns current durable inventory projections without network I/O.
    pub fn inventories(&self) -> crate::Result<Vec<Inventory>> {
        self.client.ensure_open()?;
        Ok(self
            .client
            .managed_state()
            .snapshot()
            .inventories
            .values()
            .map(|observation| observation.value.clone())
            .collect())
    }

    /// Watches coalesced local revisions. This never performs network I/O.
    pub fn watch(&self) -> crate::Result<StateRevisionWatch> {
        self.client.ensure_open()?;
        Ok(StateRevisionWatch {
            receiver: self.client.managed_state().subscribe(),
        })
    }

    /// Watches only revisions that can change the rendered galaxy scene.
    pub fn watch_galaxy(&self) -> crate::Result<GalaxyRevisionWatch> {
        self.client.ensure_open()?;
        let receiver = self.client.managed_state().subscribe();
        let last_seen = receiver.borrow().galaxy_revision();
        Ok(GalaxyRevisionWatch {
            receiver,
            last_seen,
        })
    }
}

/// Coalescing subscription for managed projections used by the galaxy scene.
pub struct GalaxyRevisionWatch {
    receiver: watch::Receiver<Arc<StateSnapshot>>,
    last_seen: u64,
}

impl GalaxyRevisionWatch {
    /// Waits for the next committed galaxy-scene revision.
    pub async fn next(&mut self) -> crate::Result<u64> {
        loop {
            self.receiver
                .changed()
                .await
                .map_err(|_| crate::Error::Closed)?;
            let revision = self.receiver.borrow_and_update().galaxy_revision();
            if revision != self.last_seen {
                self.last_seen = revision;
                return Ok(revision);
            }
        }
    }
}

/// Coalescing local managed-state revision subscription.
pub struct StateRevisionWatch {
    receiver: watch::Receiver<Arc<StateSnapshot>>,
}

impl StateRevisionWatch {
    /// Waits for the next committed semantic state revision.
    pub async fn next(&mut self) -> crate::Result<u64> {
        self.receiver
            .changed()
            .await
            .map_err(|_| crate::Error::Closed)?;
        Ok(self.receiver.borrow_and_update().revision())
    }
}

fn same_projected_observation<T: PartialEq>(
    existing: &Observation<T>,
    incoming: &Observation<T>,
) -> bool {
    existing.value == incoming.value
        && existing.metadata.source == incoming.metadata.source
        && existing.metadata.authority == incoming.metadata.authority
        && existing.metadata.access == incoming.metadata.access
        && existing.metadata.reachability == incoming.metadata.reachability
        && existing.metadata.stale == incoming.metadata.stale
}

#[derive(Clone, Debug, Default)]
pub(crate) struct StateSnapshot {
    revision: u64,
    galaxy_revision: u64,
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
}

impl StateSnapshot {
    pub(crate) fn revision(&self) -> u64 {
        self.revision
    }

    pub(crate) fn galaxy_revision(&self) -> u64 {
        self.galaxy_revision
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
        let (devices, account, replicants, locations, inventories, simulations, catalogue) = store
            .execute_blocking(|opened| {
                Ok((
                    opened.restore_devices()?,
                    opened.restore_account()?,
                    opened.restore_replicants()?,
                    opened.restore_locations()?,
                    opened.restore_inventories()?,
                    opened.restore_simulations()?,
                    opened.restore_catalogue()?,
                ))
            })?;
        let snapshot = Arc::new(StateSnapshot {
            revision: 0,
            galaxy_revision: 0,
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
            "restored durable managed state"
        );
        Ok(Self {
            store,
            snapshot: RwLock::new(snapshot),
            snapshots,
            galaxy: RwLock::new(Arc::new(GalaxySnapshot {
                catalogue: catalogue.0,
                generated_at: catalogue.1,
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
            .catalogue
            .values()
            .filter(|star| star.value.knowledge_observed)
            .cloned()
            .map(|star| crate::domain::star_knowledge_view(star, replicant.clone()))
            .collect()
    }

    pub(crate) fn replace_catalogue(
        &self,
        stars: Vec<Observation<Star>>,
        generated_at: Option<String>,
    ) -> Result<(), StoreError> {
        let existing = self
            .galaxy
            .read()
            .expect("galaxy snapshot lock poisoned")
            .catalogue
            .clone();
        let stars = stars
            .into_iter()
            .map(|incoming| {
                if let Some(current) = existing.get(&incoming.value.key).cloned() {
                    match merge_star(current, incoming) {
                        MergeOutcome::Replaced(value) | MergeOutcome::Retained(value, _) => value,
                    }
                } else {
                    incoming
                }
            })
            .collect::<Vec<_>>();
        let galaxy_changed = stars.len() != existing.len()
            || stars.iter().any(|star| {
                existing
                    .get(&star.value.key)
                    .is_none_or(|current| current.value != star.value)
            });
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
        snapshot.galaxy_revision += u64::from(galaxy_changed);
        self.publish(snapshot);
        Ok(())
    }

    pub(crate) fn persist_star_knowledge(
        &self,
        knowledge: Observation<StarKnowledge>,
    ) -> Result<(), StoreError> {
        self.persist_star_knowledge_batch(vec![knowledge])
    }

    /// Commits and publishes one complete star-knowledge batch.  Keeping the
    /// SQLite transaction, galaxy-map clone, and snapshot publication at page
    /// granularity avoids repeating all three for every row in a census page.
    pub(crate) fn persist_star_knowledge_batch(
        &self,
        knowledge: Vec<Observation<StarKnowledge>>,
    ) -> Result<(), StoreError> {
        if knowledge.is_empty() {
            return Ok(());
        }

        let existing = self
            .galaxy
            .read()
            .expect("galaxy snapshot lock poisoned")
            .catalogue
            .clone();
        let mut catalogue = existing.clone();
        let mut changed = BTreeMap::new();
        for incoming in knowledge {
            let incoming = crate::domain::account_star_from_knowledge(incoming);
            let key = incoming.value.key.clone();
            let merged = if let Some(current) = catalogue.get(&key).cloned() {
                let current_knowledge_observed = current.value.knowledge_observed;
                let current_explored = current.value.explored;
                let current_has_life = current.value.has_life;
                let incoming_knowledge_observed = incoming.value.knowledge_observed;
                let incoming_explored = incoming.value.explored;
                let incoming_has_life = incoming.value.has_life;
                let mut merged = match merge_star(current, incoming) {
                    MergeOutcome::Replaced(value) | MergeOutcome::Retained(value, _) => value,
                };
                merged.value.knowledge_observed =
                    current_knowledge_observed || incoming_knowledge_observed;
                merged.value.explored =
                    merge_positive_account_fact(current_explored, incoming_explored);
                merged.value.has_life =
                    merge_positive_account_fact(current_has_life, incoming_has_life);
                merged
            } else {
                incoming
            };
            if catalogue
                .get(&key)
                .is_none_or(|current| current.value != merged.value)
            {
                catalogue.insert(key.clone(), merged.clone());
                changed.insert(key, merged);
            }
        }

        if changed.is_empty() {
            return Ok(());
        }
        let changed_stars = changed.values().cloned().collect::<Vec<_>>();
        self.store
            .lock()
            .as_mut()
            .ok_or(StoreError::Closed)?
            .persist_stars(&changed_stars)?;

        let mut galaxy = (*self.galaxy.read().expect("galaxy snapshot lock poisoned"))
            .as_ref()
            .clone();
        for star in changed.into_values() {
            galaxy.catalogue.insert(star.value.key.clone(), star);
        }
        *self.galaxy.write().expect("galaxy snapshot lock poisoned") = Arc::new(galaxy);

        let mut snapshot = (*self.snapshot()).clone();
        snapshot.revision += 1;
        snapshot.galaxy_revision += 1;
        self.publish(snapshot);
        Ok(())
    }

    pub(crate) fn inventory(&self, owner: &InventoryOwner) -> Option<Observation<Inventory>> {
        self.snapshot().inventories.get(owner).cloned()
    }

    pub(crate) fn messages(
        &self,
    ) -> Result<(Vec<Observation<Message>>, MessageMetadata), StoreError> {
        let store = self.store.lock();
        Ok((store.restore_messages()?, store.message_metadata()?))
    }

    pub(crate) fn resource_sites(&self) -> Result<Vec<Observation<ResourceSite>>, StoreError> {
        self.store.lock().restore_resource_sites()
    }

    pub(crate) fn location_events(&self) -> Result<Vec<Observation<LocationEvent>>, StoreError> {
        self.store.lock().restore_location_events()
    }

    pub(crate) fn incoming_objects(&self) -> Result<Vec<Observation<IncomingObject>>, StoreError> {
        self.store.lock().restore_incoming_objects()
    }

    pub(crate) fn persist_messages(
        &self,
        messages: &[Observation<Message>],
    ) -> Result<(), StoreError> {
        self.store.lock().persist_messages(messages)
    }

    pub(crate) fn persist_message_metadata(
        &self,
        metadata: MessageMetadata,
    ) -> Result<(), StoreError> {
        self.store.lock().persist_message_metadata(metadata)
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
            galaxy_revision: previous.galaxy_revision,
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
        let galaxy_changed = previous
            .replicants
            .get(&replicant.value.key)
            .is_none_or(|current| !same_projected_observation(current, &replicant));
        let mut replicants = previous.replicants.clone();
        replicants.insert(replicant.value.key.clone(), replicant);
        let simulations = previous.simulations.clone();
        self.publish(StateSnapshot {
            revision: previous.revision + 1,
            galaxy_revision: previous.galaxy_revision + u64::from(galaxy_changed),
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
            galaxy_revision: previous.galaxy_revision,
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
            galaxy_revision: previous.galaxy_revision + u64::from(!devices.is_empty()),
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
            galaxy_revision: previous.galaxy_revision + 1,
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
            galaxy_revision: previous.galaxy_revision,
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
            galaxy_revision: previous.galaxy_revision,
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

    /// Returns durable, deduplicated account event history.
    pub(crate) fn events(&self) -> Result<Vec<Event>, StoreError> {
        self.store
            .lock()
            .as_ref()
            .ok_or(StoreError::Closed)?
            .read_events()
    }

    pub(crate) fn events_desc(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<Event>, StoreError> {
        self.store
            .lock()
            .as_ref()
            .ok_or(StoreError::Closed)?
            .read_events_desc(limit, offset)
    }

    pub(crate) fn prepare_projection_replay(
        &self,
        projection: &str,
        version: i64,
    ) -> Result<ProjectionReplayState, StoreError> {
        self.store
            .lock()
            .as_mut()
            .ok_or(StoreError::Closed)?
            .prepare_projection_replay(projection, version)
    }

    pub(crate) fn read_projection_history(
        &self,
        after_rowid: i64,
        high_water_rowid: i64,
        limit: usize,
    ) -> Result<Vec<(i64, Event)>, StoreError> {
        self.store
            .lock()
            .as_ref()
            .ok_or(StoreError::Closed)?
            .read_projection_history(after_rowid, high_water_rowid, limit)
    }

    pub(crate) fn apply_replay_projection(
        &self,
        projection: &str,
        version: i64,
        last_history_rowid: i64,
        high_water_rowid: i64,
        batch: EventProjectionBatch,
    ) -> Result<(), StoreError> {
        self.store
            .lock()
            .as_mut()
            .ok_or(StoreError::Closed)?
            .apply_replay_projection(
                projection,
                version,
                last_history_rowid,
                high_water_rowid,
                &batch,
            )
    }

    pub(crate) fn complete_projection_replay(
        &self,
        projection: &str,
        version: i64,
        high_water_rowid: i64,
    ) -> Result<(), StoreError> {
        self.store
            .lock()
            .as_mut()
            .ok_or(StoreError::Closed)?
            .complete_projection_replay(projection, version, high_water_rowid)
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

    /// Commits every declared event effect and advances its cursor atomically,
    /// then publishes one matching in-memory revision.
    pub(crate) fn apply_event_projection(
        &self,
        event: &Event,
        cursor: &str,
        mut batch: EventProjectionBatch,
    ) -> Result<bool, StoreError> {
        let previous = self.snapshot();
        let mut devices = previous.devices.clone();
        let mut replicants = previous.replicants.clone();
        let mut locations = previous.locations.clone();
        let mut simulations = previous.simulations.clone();
        for observation in &batch.devices {
            devices.insert(observation.value.key.clone(), observation.clone());
        }
        for observation in &batch.replicants {
            replicants.insert(observation.value.key.clone(), observation.clone());
        }
        for location in &mut batch.locations {
            if let Some(existing) = locations.get(&location.value.key) {
                let mut merged = existing.value.clone();
                merged.merge_from(&location.value);
                location.value = merged;
            }
            locations.insert(location.value.key.clone(), location.clone());
        }
        for observation in &batch.simulations {
            simulations.insert(observation.value.id, observation.clone());
        }
        for deletion in &batch.deletions {
            if deletion.kind == "device" {
                let key = DeviceKey::in_realm(
                    deletion.realm.clone(),
                    crate::domain::DeviceId::new(&deletion.item_id),
                );
                devices.remove(&key);
            }
        }
        let galaxy_changed = !batch.devices.is_empty()
            || !batch.locations.is_empty()
            || !batch.stars.is_empty()
            || batch
                .deletions
                .iter()
                .any(|deletion| deletion.kind == "device");
        let inserted = self
            .store
            .lock()
            .as_mut()
            .ok_or(StoreError::Closed)?
            .apply_event_projection(event, cursor, &batch)?;
        if !inserted {
            return Ok(false);
        }
        if !batch.stars.is_empty() {
            let mut galaxy = (*self.galaxy.read().expect("galaxy snapshot lock poisoned"))
                .as_ref()
                .clone();
            for incoming in &batch.stars {
                let observation = if let Some(current) =
                    galaxy.catalogue.get(&incoming.value.key).cloned()
                {
                    match merge_star(current, incoming.clone()) {
                        MergeOutcome::Replaced(value) | MergeOutcome::Retained(value, _) => value,
                    }
                } else {
                    incoming.clone()
                };
                galaxy
                    .catalogue
                    .insert(observation.value.key.clone(), observation);
            }
            *self.galaxy.write().expect("galaxy snapshot lock poisoned") = Arc::new(galaxy);
        }
        self.publish(StateSnapshot {
            revision: previous.revision + 1,
            galaxy_revision: previous.galaxy_revision + u64::from(galaxy_changed),
            devices,
            account: previous.account.clone(),
            replicants,
            locations,
            inventories: previous.inventories.clone(),
            simulations,
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

    /// Acquires or renews the single cross-process reconciliation worker lease.
    pub(crate) fn acquire_reconciliation_leadership(
        &self,
        owner: &str,
        lease_seconds: i64,
    ) -> Result<bool, StoreError> {
        self.store
            .lock()
            .as_mut()
            .ok_or(StoreError::Closed)?
            .acquire_reconciliation_leadership(owner, lease_seconds)
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
        let previous = self.snapshot();
        let mut next_devices = previous.devices.clone();
        let mut changed = Vec::new();
        let mut touches = Vec::new();

        for incoming in devices.iter().cloned() {
            let mut merged = match next_devices.get(&incoming.value.key).cloned() {
                Some(existing) => match merge_device(existing, incoming) {
                    MergeOutcome::Replaced(device) | MergeOutcome::Retained(device, _) => device,
                },
                None => incoming,
            };
            let existing = previous.devices.get(&merged.value.key);
            let unchanged =
                existing.is_some_and(|existing| same_projected_observation(existing, &merged));
            if unchanged && let Some(existing) = existing {
                // A touch only durably advances the high-watermark timestamp;
                // keep the existing provenance document consistent with what
                // remains serialized in SQLite.
                merged.metadata.source_document = existing.metadata.source_document.clone();
            }
            next_devices.insert(merged.value.key.clone(), merged.clone());
            if unchanged {
                touches.push((merged.value.key.clone(), merged.metadata.observed_at));
            } else {
                changed.push(merged);
            }
        }

        if changed.is_empty() && touches.is_empty() {
            return Ok(previous);
        }

        self.store
            .lock()
            .as_mut()
            .ok_or(StoreError::Closed)?
            .persist_devices_and_touch(&changed, &touches)?;

        if changed.is_empty() {
            // Refresh high-watermark metadata without waking projection
            // subscribers. The normalized device values did not change, so a
            // new semantic revision would only cause unrelated waiters to
            // spin; the newer observation time still protects merge ordering.
            let next = Arc::new(StateSnapshot {
                revision: previous.revision,
                galaxy_revision: previous.galaxy_revision,
                devices: next_devices,
                account: previous.account.clone(),
                replicants: previous.replicants.clone(),
                locations: previous.locations.clone(),
                inventories: previous.inventories.clone(),
                simulations: previous.simulations.clone(),
            });
            *self
                .snapshot
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Arc::clone(&next);
            return Ok(next);
        }

        Ok(self.publish(StateSnapshot {
            revision: previous.revision + 1,
            galaxy_revision: previous.galaxy_revision + 1,
            devices: next_devices,
            account: previous.account.clone(),
            replicants: previous.replicants.clone(),
            locations: previous.locations.clone(),
            inventories: previous.inventories.clone(),
            simulations: previous.simulations.clone(),
        }))
    }

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
            .collect::<BTreeMap<_, _>>();
        if devices == previous.devices {
            return Ok(previous);
        }
        Ok(self.publish(StateSnapshot {
            revision: previous.revision + 1,
            galaxy_revision: previous.galaxy_revision + 1,
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

fn merge_positive_account_fact(current: Option<bool>, incoming: Option<bool>) -> Option<bool> {
    match (current, incoming) {
        (Some(left), Some(right)) => Some(left || right),
        (left @ Some(_), None) => left,
        (None, right) => right,
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
        Star, StarKey, StarKnowledge,
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

    fn catalogue_star(id: &str, has_hub: Option<bool>, region: Option<&str>) -> Observation<Star> {
        Observation {
            value: Star {
                key: StarKey::live(id.into()),
                name: None,
                spectral_type: None,
                entry_point: None,
                position: None,
                has_hub,
                has_ward: None,
                knowledge_observed: false,
                explored: None,
                has_life: None,
                region: region.map(str::to_owned),
            },
            metadata: device("metadata").metadata,
        }
    }

    fn star_knowledge(
        replicant: &str,
        star: &str,
        has_hub: Option<bool>,
        region: Option<&str>,
    ) -> Observation<StarKnowledge> {
        Observation {
            value: StarKnowledge {
                replicant: ReplicantKey::live(replicant.into()),
                star: StarKey::live(star.into()),
                position: None,
                spectral_type: None,
                entry_point: None,
                explored: Some(true),
                has_hub,
                has_ward: None,
                has_life: Some(true),
                region: region.map(str::to_owned),
                distance_from_replicant: Some(42.0),
                estimated_travel_time: Some(120),
            },
            metadata: device("metadata").metadata,
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

    #[tokio::test]
    async fn galaxy_watch_ignores_unrelated_revisions() {
        let engine = StateEngine::open_memory().expect("open engine");
        let receiver = engine.subscribe();
        let last_seen = receiver.borrow().galaxy_revision();
        let mut watch = GalaxyRevisionWatch {
            last_seen,
            receiver,
        };

        let mut unrelated = (*engine.snapshot()).clone();
        unrelated.revision += 1;
        engine.publish(unrelated);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), watch.next())
                .await
                .is_err()
        );

        engine
            .persist_devices(&[device("D1")])
            .expect("persist scene device");
        assert_eq!(watch.next().await.expect("galaxy revision"), 1);
    }

    #[test]
    fn identical_device_refresh_does_not_publish_another_revision() {
        let engine = StateEngine::open_memory().expect("open engine");
        let first = device("D1");
        engine
            .persist_devices(std::slice::from_ref(&first))
            .expect("persist first observation");
        let revision = engine.snapshot().revision();

        let mut refreshed = first;
        refreshed.metadata.observed_at = "2026-07-26T00:00:00Z".into();
        refreshed.metadata.source_document.request_id = Some("new-request".into());
        engine
            .persist_devices(&[refreshed])
            .expect("ignore volatile-only refresh");

        assert_eq!(engine.snapshot().revision(), revision);
    }

    #[test]
    fn identical_device_refresh_persists_observation_high_watermark() {
        let path = test_path();
        let engine = StateEngine::open_file(&path).expect("open engine");
        let first = device("D1");
        engine
            .persist_devices(std::slice::from_ref(&first))
            .expect("persist first observation");
        let mut refreshed = first;
        let refreshed_at = crate::domain::ObservationTime::from("2026-07-26T00:00:00Z");
        refreshed.metadata.observed_at = refreshed_at;
        refreshed.metadata.source_document.request_id = Some("new-request".into());
        engine
            .persist_devices(&[refreshed])
            .expect("touch identical observation");
        drop(engine);

        let restored = StateEngine::open_file(&path).expect("restore engine");
        let observation = restored
            .device(&DeviceKey::live("D1".into()))
            .expect("restored device");
        assert_eq!(observation.metadata.observed_at, refreshed_at);
        drop(restored);
        fs::remove_file(path).expect("remove test database");
    }

    #[test]
    fn device_reachability_change_still_publishes_a_revision() {
        let engine = StateEngine::open_memory().expect("open engine");
        let first = device("D1");
        engine
            .persist_devices(std::slice::from_ref(&first))
            .expect("persist first observation");
        let revision = engine.snapshot().revision();

        let mut changed = first;
        changed.metadata.observed_at = "2026-07-26T00:00:00Z".into();
        changed.metadata.reachability = Reachability::OutOfRange;
        engine
            .persist_devices(&[changed])
            .expect("persist reachability change");

        assert_eq!(engine.snapshot().revision(), revision + 1);
    }

    #[test]
    fn public_device_observation_cannot_erase_owned_assignment_or_hosting() {
        let engine = StateEngine::open_memory().expect("open engine");
        let mut owned = device("D1");
        owned.value.relationships.assigned_replicant =
            Some(crate::domain::ReplicantKey::live("OWNER".into()));
        owned.value.relationships.hosting_replicant =
            Some(crate::domain::ReplicantKey::live("MATRIX".into()));
        engine
            .persist_devices(&[owned])
            .expect("persist owned device");

        let mut public = device("D1");
        public.value.access = AccessScope::Public;
        public.metadata.access = AccessScope::Public;
        public.metadata.observed_at = "2026-07-26T00:00:00Z".into();
        engine
            .persist_devices(&[public])
            .expect("persist public device");

        let snapshot = engine.snapshot();
        let device = snapshot
            .devices()
            .get(&DeviceKey::live("D1".into()))
            .expect("published device");
        assert_eq!(
            device
                .value
                .relationships
                .assigned_replicant
                .as_ref()
                .map(|key| key.id.as_str()),
            Some("OWNER")
        );
        assert_eq!(
            device
                .value
                .relationships
                .hosting_replicant
                .as_ref()
                .map(|key| key.id.as_str()),
            Some("MATRIX")
        );
    }

    #[test]
    fn partial_star_observations_preserve_known_region_and_hub() {
        let engine = StateEngine::open_memory().expect("open engine");
        engine
            .replace_catalogue(
                vec![catalogue_star("SOL", Some(true), Some("solzone"))],
                None,
            )
            .expect("persist catalogue");
        engine
            .replace_catalogue(vec![catalogue_star("SOL", None, None)], None)
            .expect("persist partial catalogue");
        assert_eq!(engine.snapshot().galaxy_revision(), 1);
        let catalogue = engine.catalogue();
        assert_eq!(catalogue[0].value.has_hub, Some(true));
        assert_eq!(catalogue[0].value.region.as_deref(), Some("solzone"));
        assert!(
            engine
                .star_knowledge(&ReplicantKey::live("R2".into()))
                .is_empty(),
            "catalogue membership alone is not account star knowledge"
        );

        engine
            .persist_star_knowledge(star_knowledge("R1", "SOL", Some(false), Some("alpha")))
            .expect("persist star knowledge");
        engine
            .persist_star_knowledge(star_knowledge("R1", "SOL", None, None))
            .expect("persist partial star knowledge");
        assert_eq!(engine.snapshot().galaxy_revision(), 2);
        let knowledge = engine.star_knowledge(&ReplicantKey::live("R2".into()));
        assert_eq!(knowledge[0].value.replicant.id.as_str(), "R2");
        assert_eq!(knowledge[0].value.has_hub, Some(false));
        assert_eq!(knowledge[0].value.region.as_deref(), Some("alpha"));
        assert_eq!(knowledge[0].value.explored, Some(true));
        assert_eq!(knowledge[0].value.has_life, Some(true));
        assert_eq!(knowledge[0].value.distance_from_replicant, None);
        assert_eq!(knowledge[0].value.estimated_travel_time, None);
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
                custom_name: None,
                survey_progress: Default::default(),
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
                custom_name: None,
                survey_progress: Default::default(),
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
