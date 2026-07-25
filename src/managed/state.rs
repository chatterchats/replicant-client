//! Immutable, crate-private state snapshots published after durable commits.

#![allow(dead_code)] // Phase 5 owns the engine; public state queries arrive later.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock, mpsc};

use crate::domain::{Device, DeviceKey, Observation};

use super::store::{Store, StoreError, StoreHandle};

#[derive(Clone, Debug, Default)]
pub(crate) struct StateSnapshot {
    revision: u64,
    devices: BTreeMap<DeviceKey, Observation<Device>>,
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
    subscribers: Mutex<Vec<mpsc::Sender<Arc<StateSnapshot>>>>,
}

impl StateEngine {
    pub(crate) fn open_memory() -> Result<Self, StoreError> {
        Self::from_store(Arc::new(Mutex::new(Some(Store::open_memory()?))))
    }

    pub(crate) fn open_file(path: &Path) -> Result<Self, StoreError> {
        Self::from_store(Arc::new(Mutex::new(Some(Store::open_file(path)?))))
    }

    pub(crate) fn from_store(store: StoreHandle) -> Result<Self, StoreError> {
        let devices = store
            .lock()
            .expect("state store lock poisoned")
            .as_ref()
            .ok_or(StoreError::Closed)?
            .restore_devices()?;
        Ok(Self {
            store,
            snapshot: RwLock::new(Arc::new(StateSnapshot {
                revision: 0,
                devices,
            })),
            subscribers: Mutex::new(Vec::new()),
        })
    }

    pub(crate) fn snapshot(&self) -> Arc<StateSnapshot> {
        Arc::clone(&self.snapshot.read().expect("state snapshot lock poisoned"))
    }

    pub(crate) fn subscribe(&self) -> mpsc::Receiver<Arc<StateSnapshot>> {
        let (sender, receiver) = mpsc::channel();
        self.subscribers
            .lock()
            .expect("state subscribers lock poisoned")
            .push(sender);
        receiver
    }

    /// Commits every projection before replacing or broadcasting the snapshot.
    pub(crate) fn persist_devices(
        &self,
        devices: &[Observation<Device>],
    ) -> Result<Arc<StateSnapshot>, StoreError> {
        self.store
            .lock()
            .expect("state store lock poisoned")
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
        }))
    }

    fn publish(&self, next: StateSnapshot) -> Arc<StateSnapshot> {
        let next = Arc::new(next);
        *self.snapshot.write().expect("state snapshot lock poisoned") = Arc::clone(&next);
        self.subscribers
            .lock()
            .expect("state subscribers lock poisoned")
            .retain(|sender| sender.send(Arc::clone(&next)).is_ok());
        next
    }

    #[cfg(test)]
    pub(crate) fn fail_next_commit(&self) {
        self.store
            .lock()
            .expect("state store lock poisoned")
            .as_mut()
            .expect("state store is open during this test")
            .fail_next_commit();
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::mpsc::TryRecvError;
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
        assert!(matches!(receiver.try_recv(), Err(TryRecvError::Empty)));
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
}
