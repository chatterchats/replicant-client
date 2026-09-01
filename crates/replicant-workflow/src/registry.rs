use std::{
    collections::{BTreeMap, BTreeSet, btree_map::Entry},
    sync::Arc,
};

use crate::model::canonical_device_code;
use crate::{
    RepositoryError, ResourceKey, WorkItem, WorkflowExecutor, WorkflowId, WorkflowInstance,
    WorkflowKind, WorkflowPlacementIntent, WorkflowPlacementIntentCoverage,
    WorkflowPlacementIntentEvidence, WorkflowPlacementIntentProjection,
    WorkflowPlacementIntentRelation, WorkflowPlacementIntentSnapshot,
    WorkflowPlacementIntentSubject, WorkflowServiceIntent, WorkflowServiceIntentCoverage,
    WorkflowServiceIntentEvidence, WorkflowServiceIntentProjection, WorkflowServiceIntentSnapshot,
    WorkflowServiceScope, WorkflowStatus,
};
use serde_json::Value;

/// Explicit replacement payload produced when upgrading a persisted workflow.
pub struct WorkflowMigration {
    pub(crate) config: Value,
    pub(crate) checkpoint: Value,
}

impl WorkflowMigration {
    /// Creates one complete config/checkpoint replacement.
    #[must_use]
    pub const fn new(config: Value, checkpoint: Value) -> Self {
        Self { config, checkpoint }
    }

    /// Returns the complete migrated configuration payload.
    #[must_use]
    pub const fn config(&self) -> &Value {
        &self.config
    }

    /// Returns the complete migrated checkpoint payload.
    #[must_use]
    pub const fn checkpoint(&self) -> &Value {
        &self.checkpoint
    }
}

/// Factory metadata required to load a persisted workflow kind.
///
pub trait WorkflowFactory: Send + Sync {
    /// Stable kind created by this factory.
    fn kind(&self) -> &WorkflowKind;

    /// Current schema version written by this factory.
    fn current_schema_version(&self) -> u32;

    /// Returns whether this factory can load a persisted schema version.
    fn supports_schema_version(&self, version: u32) -> bool {
        version == self.current_schema_version()
    }

    /// Explicitly migrates one supported old payload to the current schema.
    ///
    /// Factories that support old versions must override this method. Returning
    /// no migration for an old version fails the workflow before execution.
    fn migrate(&self, _instance: &WorkflowInstance) -> Result<Option<WorkflowMigration>, String> {
        Ok(None)
    }

    /// Constructs an executor for one persisted invocation.
    ///
    /// Metadata-only factories may retain the default until they are runnable.
    fn create_executor(&self) -> Option<Box<dyn WorkflowExecutor>> {
        None
    }

    /// Projects typed placement evidence from durable workflow state.
    ///
    /// The default is intentionally unknown: an empty projection would imply
    /// that the factory had proven that no placement intent exists.
    fn placement_intents(
        &self,
        _instance: &WorkflowInstance,
        _work_items: &[WorkItem],
    ) -> Result<WorkflowPlacementIntentProjection, String> {
        Ok(WorkflowPlacementIntentProjection::unknown())
    }

    /// Projects generic durable service intents from one workflow.
    ///
    /// Unrelated factories use the safe `NotApplicable` default. A
    /// service-capable factory should return scoped `Unknown` when it can
    /// identify a service scope but cannot decode exact route dimensions.
    fn service_intents(
        &self,
        _instance: &WorkflowInstance,
    ) -> Result<WorkflowServiceIntentProjection, String> {
        Ok(WorkflowServiceIntentProjection::not_applicable())
    }
}

/// Errors resolving persisted workflows through the registry.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RegistryError {
    /// The same stable kind was registered twice.
    #[error("workflow kind {0} is already registered")]
    DuplicateKind(WorkflowKind),
    /// No factory is registered for the persisted kind.
    #[error("workflow kind {0} is not registered")]
    UnknownKind(WorkflowKind),
    /// The factory cannot load the persisted payload schema.
    #[error("workflow kind {kind} does not support schema version {version}")]
    UnsupportedSchemaVersion {
        /// Workflow kind.
        kind: WorkflowKind,
        /// Persisted version.
        version: u32,
    },
    /// A supported old payload could not be migrated safely.
    #[error("workflow kind {kind} schema migration from version {from} to {to} failed: {reason}")]
    MigrationFailed {
        /// Workflow kind.
        kind: WorkflowKind,
        /// Persisted version.
        from: u32,
        /// Current version.
        to: u32,
        /// Actionable migration failure.
        reason: String,
    },
}

/// Registry of workflow factories keyed by stable kind.
#[derive(Default)]
pub struct WorkflowRegistry {
    factories: BTreeMap<WorkflowKind, Arc<dyn WorkflowFactory>>,
}

impl WorkflowRegistry {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a factory, rejecting duplicate stable kinds.
    pub fn register(&mut self, factory: Arc<dyn WorkflowFactory>) -> Result<(), RegistryError> {
        let kind = factory.kind().clone();
        match self.factories.entry(kind.clone()) {
            Entry::Vacant(entry) => {
                entry.insert(factory);
                Ok(())
            }
            Entry::Occupied(_) => Err(RegistryError::DuplicateKind(kind)),
        }
    }

    /// Returns whether a factory is registered for `kind`.
    #[must_use]
    pub fn contains(&self, kind: &WorkflowKind) -> bool {
        self.factories.contains_key(kind)
    }

    /// Resolves a factory that can load the instance's persisted schema.
    pub fn resolve(
        &self,
        instance: &WorkflowInstance,
    ) -> Result<&dyn WorkflowFactory, RegistryError> {
        let factory = self
            .factories
            .get(&instance.kind)
            .ok_or_else(|| RegistryError::UnknownKind(instance.kind.clone()))?;
        if !factory.supports_schema_version(instance.schema_version) {
            return Err(RegistryError::UnsupportedSchemaVersion {
                kind: instance.kind.clone(),
                version: instance.schema_version,
            });
        }
        Ok(factory.as_ref())
    }

    /// Derives typed placement evidence from every retained workflow row.
    ///
    /// This performs one workflow query, one bulk work-item query, and one
    /// device-claim query. Unknown factory coverage is retained as a blocker
    /// rather than being treated as proof of no intent.
    pub fn placement_intent_snapshot(
        &self,
        repository: &crate::WorkflowRepository,
        excluded_workflow_id: Option<WorkflowId>,
    ) -> Result<WorkflowPlacementIntentSnapshot, RepositoryError> {
        let instances = repository.list()?;
        let ids = instances
            .iter()
            .map(|instance| instance.id)
            .collect::<Vec<_>>();
        let work_items = repository.list_work_items_for_workflows(&ids)?;
        let claims = repository.device_claims()?;
        let mut snapshot = WorkflowPlacementIntentSnapshot::default();

        for instance in &instances {
            let live = is_live(instance.status);
            if live && excluded_workflow_id != Some(instance.id) {
                match instance.wait_intent() {
                    Ok(Some(wait)) => {
                        let mut codes = wait
                            .device_code
                            .into_iter()
                            .map(|code| canonical_device_code(&code))
                            .collect::<Vec<_>>();
                        codes.extend(
                            wait.device_codes
                                .into_iter()
                                .map(|code| canonical_device_code(&code)),
                        );
                        codes.sort();
                        codes.dedup();
                        snapshot.live.extend(codes.into_iter().map(|code| {
                            WorkflowPlacementIntentEvidence {
                                workflow_id: instance.id,
                                workflow_kind: instance.kind.clone(),
                                workflow_status: instance.status,
                                intent: WorkflowPlacementIntent {
                                    subject: WorkflowPlacementIntentSubject::Device(code),
                                    relation: WorkflowPlacementIntentRelation::Awaited,
                                    work_item_id: None,
                                    expected_location: None,
                                },
                            }
                        }));
                    }
                    Ok(None) => {}
                    Err(_) => snapshot.unknown_live_workflows.push(instance.id),
                }
            }
            let projection = self
                .resolve(instance)
                .and_then(|factory| {
                    factory
                        .placement_intents(
                            instance,
                            work_items.get(&instance.id).map_or(&[], Vec::as_slice),
                        )
                        .map_err(|_| RegistryError::UnknownKind(instance.kind.clone()))
                })
                .ok()
                .map(canonicalize_projection)
                .filter(valid_projection);

            let Some(projection) = projection else {
                if live && excluded_workflow_id != Some(instance.id) {
                    snapshot.unknown_live_workflows.push(instance.id);
                } else if matches!(
                    instance.status,
                    WorkflowStatus::Succeeded | WorkflowStatus::Failed | WorkflowStatus::Cancelled
                ) {
                    snapshot.unknown_terminal_outcomes.push(instance.id);
                }
                continue;
            };

            let evidence = |intent: WorkflowPlacementIntent| WorkflowPlacementIntentEvidence {
                workflow_id: instance.id,
                workflow_kind: instance.kind.clone(),
                workflow_status: instance.status,
                intent,
            };
            if live {
                if excluded_workflow_id != Some(instance.id) {
                    snapshot
                        .live
                        .extend(projection.intents.into_iter().map(evidence));
                }
            } else if instance.status == WorkflowStatus::Succeeded {
                snapshot.resolutions.extend(projection.resolutions);
                for intent in projection.intents {
                    if intent.relation == WorkflowPlacementIntentRelation::Deployed {
                        snapshot.settled_placements.push(evidence(intent));
                    } else {
                        snapshot.terminal_residuals.push(evidence(intent));
                    }
                }
            } else if instance.status == WorkflowStatus::Cancelled {
                snapshot.terminal_residuals.extend(
                    projection
                        .intents
                        .into_iter()
                        .filter(|intent| {
                            intent.relation != WorkflowPlacementIntentRelation::Deployed
                        })
                        .map(evidence),
                );
            } else if instance.status == WorkflowStatus::Failed {
                snapshot.failed_transient.extend(
                    projection
                        .intents
                        .into_iter()
                        .filter(|intent| {
                            matches!(
                                intent.relation,
                                WorkflowPlacementIntentRelation::Claimed
                                    | WorkflowPlacementIntentRelation::Staged
                                    | WorkflowPlacementIntentRelation::Transported
                                    | WorkflowPlacementIntentRelation::Awaited
                            )
                        })
                        .map(evidence),
                );
            }
        }

        for claim in claims {
            let ResourceKey::Device(code) = claim.resource else {
                continue;
            };
            let code = canonical_device_code(&code);
            let Some(owner) = instances
                .iter()
                .find(|instance| instance.id == claim.workflow_id)
            else {
                continue;
            };
            if excluded_workflow_id == Some(claim.workflow_id) || !is_live(owner.status) {
                continue;
            }
            snapshot.live.push(WorkflowPlacementIntentEvidence {
                workflow_id: claim.workflow_id,
                workflow_kind: owner.kind.clone(),
                workflow_status: owner.status,
                intent: WorkflowPlacementIntent {
                    subject: WorkflowPlacementIntentSubject::Device(code),
                    relation: WorkflowPlacementIntentRelation::Claimed,
                    work_item_id: None,
                    expected_location: None,
                },
            });
        }

        subtract_resolved(&mut snapshot);
        snapshot.live.sort_by_key(evidence_sort_key);
        snapshot.live.dedup();
        snapshot.settled_placements.sort_by_key(evidence_sort_key);
        snapshot.settled_placements.dedup();
        snapshot.terminal_residuals.sort_by_key(evidence_sort_key);

        snapshot.terminal_residuals.dedup();
        snapshot.failed_transient.sort_by_key(evidence_sort_key);
        snapshot.failed_transient.dedup();
        snapshot.resolved_transient.sort_by_key(evidence_sort_key);
        snapshot.resolved_transient.dedup();
        snapshot.resolutions.sort();
        snapshot.resolutions.dedup();
        snapshot.unknown_live_workflows.sort();
        snapshot.unknown_live_workflows.dedup();
        snapshot.unknown_terminal_outcomes.sort();
        snapshot.unknown_terminal_outcomes.dedup();
        Ok(snapshot)
    }
    /// Derives live generic service-intent evidence from active workflows.
    ///
    /// Terminal rows are deliberately excluded because only live durable work
    /// can be pending. An excluded workflow contributes no exact or unknown
    /// evidence, which lets an atomic compatibility check ignore its caller.
    pub fn service_intent_snapshot(
        &self,
        repository: &crate::WorkflowRepository,
        excluded_workflow_id: Option<WorkflowId>,
    ) -> Result<WorkflowServiceIntentSnapshot, RepositoryError> {
        let instances = repository.list_active()?;
        let mut snapshot = WorkflowServiceIntentSnapshot::default();
        for instance in instances {
            if excluded_workflow_id == Some(instance.id) {
                continue;
            }
            if let Some(evidence) = self.service_intent_evidence(&instance) {
                snapshot.live.push(evidence);
            }
        }
        snapshot.live.sort_by_key(service_evidence_sort_key);
        snapshot.live.dedup();
        Ok(snapshot)
    }

    fn service_intent_evidence(
        &self,
        instance: &WorkflowInstance,
    ) -> Option<WorkflowServiceIntentEvidence> {
        let Some(factory) = self.factories.get(&instance.kind) else {
            return Some(global_unknown_evidence(instance));
        };
        if !factory.supports_schema_version(instance.schema_version) {
            return Some(global_unknown_evidence(instance));
        }
        let projection = match factory.service_intents(instance) {
            Ok(projection) => canonicalize_service_projection(projection),
            Err(_) => return Some(global_unknown_evidence(instance)),
        };
        if !valid_service_projection(&projection) {
            return Some(global_unknown_evidence(instance));
        }
        if projection.coverage == WorkflowServiceIntentCoverage::NotApplicable {
            return None;
        }
        Some(WorkflowServiceIntentEvidence {
            workflow_id: instance.id,
            workflow_kind: instance.kind.clone(),
            workflow_status: instance.status,
            intents: projection.intents,
            unknown_scopes: projection.unknown_scopes,
        })
    }

    /// Queries one supplied workflow's service evidence without repository access.
    ///
    /// This is safe inside an atomic repository compatibility predicate.
    #[must_use]
    pub fn service_intent_state_for_instance(
        &self,
        instance: &WorkflowInstance,
        target: &WorkflowServiceIntent,
        region: Option<&str>,
        system: Option<&str>,
    ) -> crate::WorkflowServiceIntentState {
        let live = self.service_intent_evidence(instance).into_iter().collect();
        WorkflowServiceIntentSnapshot { live }.state_for(target, region, system)
    }

    /// Returns an explicit migration for a supported old workflow payload.
    pub fn migration(
        &self,
        instance: &WorkflowInstance,
    ) -> Result<Option<(u32, WorkflowMigration)>, RegistryError> {
        let factory = self
            .factories
            .get(&instance.kind)
            .ok_or_else(|| RegistryError::UnknownKind(instance.kind.clone()))?;
        let current = factory.current_schema_version();
        if instance.schema_version == current {
            return Ok(None);
        }
        if !factory.supports_schema_version(instance.schema_version) {
            return Err(RegistryError::UnsupportedSchemaVersion {
                kind: instance.kind.clone(),
                version: instance.schema_version,
            });
        }
        factory
            .migrate(instance)
            .map_err(|reason| RegistryError::MigrationFailed {
                kind: instance.kind.clone(),
                from: instance.schema_version,
                to: current,
                reason,
            })?
            .map(|migration| (current, migration))
            .ok_or_else(|| RegistryError::MigrationFailed {
                kind: instance.kind.clone(),
                from: instance.schema_version,
                to: current,
                reason: "factory declared this version supported but returned no migration"
                    .to_owned(),
            })
            .map(Some)
    }
}

fn is_live(status: WorkflowStatus) -> bool {
    matches!(
        status,
        WorkflowStatus::Queued
            | WorkflowStatus::Running
            | WorkflowStatus::Waiting
            | WorkflowStatus::Reconciling
            | WorkflowStatus::Paused
    )
}

fn canonicalize_projection(
    mut projection: WorkflowPlacementIntentProjection,
) -> WorkflowPlacementIntentProjection {
    for intent in &mut projection.intents {
        if let WorkflowPlacementIntentSubject::Device(code) = &mut intent.subject {
            let canonical = canonical_device_code(code.as_str());
            *code = canonical;
        }
    }
    for resolution in &mut projection.resolutions {
        resolution.device_code = canonical_device_code(&resolution.device_code);
    }
    projection
}

fn valid_projection(projection: &WorkflowPlacementIntentProjection) -> bool {
    projection.coverage == WorkflowPlacementIntentCoverage::Complete
        && projection
            .intents
            .iter()
            .all(|intent| match intent.relation {
                WorkflowPlacementIntentRelation::Deployed => intent.expected_location.is_some(),
                WorkflowPlacementIntentRelation::Claimed
                | WorkflowPlacementIntentRelation::Staged
                | WorkflowPlacementIntentRelation::Transported
                | WorkflowPlacementIntentRelation::Awaited => intent.expected_location.is_none(),
            })
}

fn evidence_sort_key(
    evidence: &WorkflowPlacementIntentEvidence,
) -> (WorkflowId, String, u8, WorkflowPlacementIntent) {
    (
        evidence.workflow_id,
        evidence.workflow_kind.as_str().to_owned(),
        match evidence.workflow_status {
            WorkflowStatus::Queued => 0,
            WorkflowStatus::Running => 1,
            WorkflowStatus::Waiting => 2,
            WorkflowStatus::Reconciling => 3,
            WorkflowStatus::Paused => 4,
            WorkflowStatus::Succeeded => 5,
            WorkflowStatus::Failed => 6,
            WorkflowStatus::Cancelled => 7,
        },
        evidence.intent.clone(),
    )
}

fn subtract_resolved(snapshot: &mut WorkflowPlacementIntentSnapshot) {
    let resolutions = snapshot.resolutions.clone();
    let mut retained = Vec::with_capacity(snapshot.failed_transient.len());
    for evidence in snapshot.failed_transient.drain(..) {
        let resolved = match &evidence.intent.subject {
            WorkflowPlacementIntentSubject::Device(code) => {
                let canonical_code = canonical_device_code(code);
                resolutions.iter().any(|resolution| {
                    canonical_device_code(&resolution.device_code) == canonical_code
                        && resolution.provenance.workflow_id == evidence.workflow_id
                        && resolution.provenance.work_item_id == evidence.intent.work_item_id
                })
            }
            WorkflowPlacementIntentSubject::DeviceTag(_) => false,
        };
        if resolved {
            snapshot.resolved_transient.push(evidence);
        } else {
            retained.push(evidence);
        }
    }
    snapshot.failed_transient = retained;
}
fn global_unknown_evidence(instance: &WorkflowInstance) -> WorkflowServiceIntentEvidence {
    WorkflowServiceIntentEvidence {
        workflow_id: instance.id,
        workflow_kind: instance.kind.clone(),
        workflow_status: instance.status,
        intents: Vec::new(),
        unknown_scopes: [WorkflowServiceScope::Global].into_iter().collect(),
    }
}

fn canonicalize_service_projection(
    mut projection: WorkflowServiceIntentProjection,
) -> WorkflowServiceIntentProjection {
    projection.unknown_scopes = projection
        .unknown_scopes
        .into_iter()
        .map(WorkflowServiceScope::canonical)
        .collect();
    projection.intents.sort();
    projection.intents.dedup();
    projection
}

fn valid_service_projection(projection: &WorkflowServiceIntentProjection) -> bool {
    let valid_intents = projection.intents.iter().all(|intent| {
        !intent.service.trim().is_empty()
            && intent
                .dimensions
                .iter()
                .all(|(key, value)| !key.trim().is_empty() && !value.trim().is_empty())
    });
    let valid_scopes = projection.unknown_scopes.iter().all(|scope| match scope {
        WorkflowServiceScope::Global => true,
        WorkflowServiceScope::Region(value) | WorkflowServiceScope::System(value) => {
            !value.trim().is_empty()
        }
    });
    valid_intents
        && valid_scopes
        && match projection.coverage {
            WorkflowServiceIntentCoverage::NotApplicable => {
                projection.intents.is_empty() && projection.unknown_scopes.is_empty()
            }
            WorkflowServiceIntentCoverage::Complete => projection.unknown_scopes.is_empty(),
            WorkflowServiceIntentCoverage::Unknown => !projection.unknown_scopes.is_empty(),
        }
}

fn service_evidence_sort_key(
    evidence: &WorkflowServiceIntentEvidence,
) -> (
    WorkflowId,
    String,
    u8,
    Vec<WorkflowServiceIntent>,
    BTreeSet<WorkflowServiceScope>,
) {
    (
        evidence.workflow_id,
        evidence.workflow_kind.as_str().to_owned(),
        match evidence.workflow_status {
            WorkflowStatus::Queued => 0,
            WorkflowStatus::Running => 1,
            WorkflowStatus::Waiting => 2,
            WorkflowStatus::Reconciling => 3,
            WorkflowStatus::Paused => 4,
            WorkflowStatus::Succeeded => 5,
            WorkflowStatus::Failed => 6,
            WorkflowStatus::Cancelled => 7,
        },
        evidence.intents.clone(),
        evidence.unknown_scopes.clone(),
    )
}

#[cfg(test)]
mod tests {

    use std::{collections::BTreeMap, sync::Arc};

    use serde_json::Value;

    use super::*;
    use crate::{
        NewWorkflow, WaitIntent, WorkItemId, WorkItemSpec, WorkflowPlacementProvenance,
        WorkflowPlacementResolution, WorkflowServiceIntentState, WorkflowState,
    };
    struct PlacementFactory {
        kind: WorkflowKind,
    }

    impl PlacementFactory {
        fn new(kind: &str) -> Arc<Self> {
            Arc::new(Self {
                kind: WorkflowKind::new(kind).expect("kind"),
            })
        }
    }

    impl WorkflowFactory for PlacementFactory {
        fn kind(&self) -> &WorkflowKind {
            &self.kind
        }

        fn current_schema_version(&self) -> u32 {
            1
        }

        fn placement_intents(
            &self,
            instance: &WorkflowInstance,
            work_items: &[WorkItem],
        ) -> Result<WorkflowPlacementIntentProjection, String> {
            let marker = instance
                .config::<String>()
                .map_err(|error| error.to_string())?;
            if marker == "malformed" {
                return Ok(WorkflowPlacementIntentProjection::unknown());
            }
            if let Some(code) = marker.strip_prefix("failed-items:") {
                return Ok(WorkflowPlacementIntentProjection {
                    coverage: WorkflowPlacementIntentCoverage::Complete,
                    intents: work_items
                        .iter()
                        .map(|item| WorkflowPlacementIntent {
                            subject: WorkflowPlacementIntentSubject::Device(code.to_owned()),
                            relation: WorkflowPlacementIntentRelation::Staged,
                            work_item_id: Some(item.id),
                            expected_location: None,
                        })
                        .collect(),
                    resolutions: Vec::new(),
                });
            }
            if let Some(value) = marker.strip_prefix("resolve-item:") {
                let mut parts = value.split(':');
                let workflow_id = parts
                    .next()
                    .ok_or_else(|| "missing workflow id".to_owned())?
                    .parse::<WorkflowId>()
                    .map_err(|error| error.to_string())?;
                let work_item_id = parts
                    .next()
                    .ok_or_else(|| "missing work item id".to_owned())?
                    .parse::<WorkItemId>()
                    .map_err(|error| error.to_string())?;
                let code = parts
                    .next()
                    .ok_or_else(|| "missing device code".to_owned())?;
                if parts.next().is_some() {
                    return Err("unexpected resolve-item fields".to_owned());
                }
                return Ok(WorkflowPlacementIntentProjection {
                    coverage: WorkflowPlacementIntentCoverage::Complete,
                    intents: Vec::new(),
                    resolutions: vec![WorkflowPlacementResolution {
                        device_code: code.to_owned(),
                        provenance: WorkflowPlacementProvenance {
                            workflow_id,
                            work_item_id: Some(work_item_id),
                        },
                    }],
                });
            }
            if let Some(tag) = marker.strip_prefix("tag:") {
                return Ok(projection(
                    WorkflowPlacementIntentSubject::DeviceTag(tag.to_owned()),
                    WorkflowPlacementIntentRelation::Staged,
                    None,
                ));
            }
            if let Some(value) = marker.strip_prefix("deploy:") {
                let (code, location) = value
                    .split_once(':')
                    .ok_or_else(|| "invalid deploy marker".to_owned())?;
                return Ok(projection(
                    WorkflowPlacementIntentSubject::Device(code.to_owned()),
                    WorkflowPlacementIntentRelation::Deployed,
                    Some(location.to_owned()),
                ));
            }
            if let Some(value) = marker.strip_prefix("resolve:") {
                let (workflow_id, code) = value
                    .split_once(':')
                    .ok_or_else(|| "invalid resolve marker".to_owned())?;
                let workflow_id = workflow_id
                    .parse::<WorkflowId>()
                    .map_err(|error| error.to_string())?;
                return Ok(WorkflowPlacementIntentProjection {
                    coverage: WorkflowPlacementIntentCoverage::Complete,
                    intents: Vec::new(),
                    resolutions: vec![WorkflowPlacementResolution {
                        device_code: code.to_owned(),
                        provenance: WorkflowPlacementProvenance {
                            workflow_id,
                            work_item_id: None,
                        },
                    }],
                });
            }
            let code = marker
                .strip_prefix("failed:")
                .or_else(|| marker.strip_prefix("cancelled:"))
                .unwrap_or(&marker);
            Ok(projection(
                WorkflowPlacementIntentSubject::Device(code.to_owned()),
                WorkflowPlacementIntentRelation::Staged,
                None,
            ))
        }
    }

    struct ServiceFactory {
        kind: WorkflowKind,
    }

    impl ServiceFactory {
        fn new(kind: &str) -> Arc<Self> {
            Arc::new(Self {
                kind: WorkflowKind::new(kind).expect("kind"),
            })
        }
    }

    impl WorkflowFactory for ServiceFactory {
        fn kind(&self) -> &WorkflowKind {
            &self.kind
        }

        fn current_schema_version(&self) -> u32 {
            1
        }

        fn service_intents(
            &self,
            instance: &WorkflowInstance,
        ) -> Result<WorkflowServiceIntentProjection, String> {
            match instance
                .config::<String>()
                .map_err(|error| error.to_string())?
                .as_str()
            {
                "present" => Ok(WorkflowServiceIntentProjection::complete(vec![
                    WorkflowServiceIntent::new(
                        "ami_transport",
                        BTreeMap::from([
                            ("collect".to_owned(), "BELT".to_owned()),
                            ("deliver".to_owned(), "HUB".to_owned()),
                        ]),
                    ),
                ])),
                "unknown-region" => Ok(WorkflowServiceIntentProjection::unknown([
                    WorkflowServiceScope::Region(" Alpha ".to_owned()),
                ])),
                "error" => Err("decode failed".to_owned()),
                _ => Ok(WorkflowServiceIntentProjection::not_applicable()),
            }
        }
    }

    fn projection(
        subject: WorkflowPlacementIntentSubject,
        relation: WorkflowPlacementIntentRelation,
        expected_location: Option<String>,
    ) -> WorkflowPlacementIntentProjection {
        WorkflowPlacementIntentProjection {
            coverage: WorkflowPlacementIntentCoverage::Complete,
            intents: vec![WorkflowPlacementIntent {
                subject,
                relation,
                work_item_id: None,
                expected_location,
            }],
            resolutions: Vec::new(),
        }
    }

    fn workflow(kind: &str, marker: &str) -> NewWorkflow<Value, Value> {
        NewWorkflow {
            kind: WorkflowKind::new(kind).expect("kind"),
            schema_version: 1,
            config: Value::String(marker.to_owned()),
            checkpoint: Value::Null,
            current_step: None,
            parent_id: None,
        }
    }
    fn work_item_spec(workflow_id: WorkflowId, dedupe_key: &str) -> WorkItemSpec {
        WorkItemSpec {
            workflow_id,
            dedupe_key: dedupe_key.to_owned(),
            kind: WorkflowKind::new("test.placement.item").expect("kind"),
            sort_key: dedupe_key.to_owned(),
            payload_json: Value::Null,
            preconditions_json: Value::Null,
            requirements_json: Value::Null,
            deadline_at_ms: None,
        }
    }

    fn state(status: WorkflowStatus) -> WorkflowState<Value, Value> {
        WorkflowState {
            status,
            current_step: None,
            checkpoint: Value::Null,
            last_error: None,
            result: None,
        }
    }

    fn set_status(
        repository: &crate::WorkflowRepository,
        mut instance: WorkflowInstance,
        status: WorkflowStatus,
    ) -> WorkflowInstance {
        if instance.status == status {
            return instance;
        }
        if instance.status == WorkflowStatus::Queued && status != WorkflowStatus::Running {
            instance = repository
                .update(
                    instance.id,
                    instance.revision,
                    state(WorkflowStatus::Running),
                )
                .expect("running");
        }
        repository
            .update(instance.id, instance.revision, state(status))
            .expect("status")
    }

    #[test]
    fn workflow_intent_matches_exact_codes_and_whole_tags() {
        let workflow_id = WorkflowId::new();
        let evidence = WorkflowPlacementIntentEvidence {
            workflow_id,
            workflow_kind: WorkflowKind::new("test.placement").expect("kind"),
            workflow_status: WorkflowStatus::Running,
            intent: WorkflowPlacementIntent {
                subject: WorkflowPlacementIntentSubject::Device(" d-1 ".to_owned()),
                relation: WorkflowPlacementIntentRelation::Staged,
                work_item_id: None,
                expected_location: None,
            },
        };
        let tag_evidence = WorkflowPlacementIntentEvidence {
            intent: WorkflowPlacementIntent {
                subject: WorkflowPlacementIntentSubject::DeviceTag("reserved:one".to_owned()),
                ..evidence.intent.clone()
            },
            ..evidence.clone()
        };
        let snapshot = WorkflowPlacementIntentSnapshot {
            live: vec![evidence, tag_evidence],
            ..WorkflowPlacementIntentSnapshot::default()
        };
        assert_eq!(snapshot.explain_device(" D-1 ", &[]).live.len(), 1);
        assert_eq!(
            snapshot
                .explain_device("other", &["reserved:one".to_owned()])
                .live
                .len(),
            1
        );
        assert!(
            snapshot
                .explain_device("other", &["reserved:one-extra".to_owned()])
                .live
                .is_empty()
        );
        assert!(
            snapshot
                .explain_device("other", &["RESERVED:ONE".to_owned()])
                .live
                .is_empty()
        );
    }

    #[test]
    fn workflow_intent_live_statuses_claims_waits_and_exclusion() {
        let repository = crate::WorkflowRepository::open_in_memory().expect("repository");
        let kind = "test.placement";
        let factory = PlacementFactory::new(kind);
        let mut registry = WorkflowRegistry::new();
        registry.register(factory).expect("register");
        let statuses = [
            WorkflowStatus::Queued,
            WorkflowStatus::Running,
            WorkflowStatus::Waiting,
            WorkflowStatus::Reconciling,
            WorkflowStatus::Paused,
        ];
        let mut instances = Vec::new();
        for (index, status) in statuses.iter().copied().enumerate() {
            let marker = if index == 0 {
                " d-0 ".to_owned()
            } else {
                format!("D-{index}")
            };
            let instance = repository.create(workflow(kind, &marker)).expect("create");
            instances.push(set_status(&repository, instance, status));
        }
        let waiting = &instances[2];
        let waiting = repository
            .update_with_wait(
                waiting.id,
                waiting.revision,
                state(WorkflowStatus::Waiting),
                Some(
                    &WaitIntent::state("device")
                        .for_devices([" wait-1 ".to_owned(), "wAiT-2".to_owned()]),
                ),
            )
            .expect("wait");
        repository
            .acquire_claim(waiting.id, ResourceKey::Device(" cLaIm-1 ".to_owned()))
            .expect("claim");
        let snapshot = registry
            .placement_intent_snapshot(&repository, None)
            .expect("snapshot");
        let live_statuses = snapshot
            .live
            .iter()
            .map(|evidence| evidence.workflow_status)
            .collect::<Vec<_>>();
        assert_eq!(snapshot.explain_device("D-0", &[]).live.len(), 1);
        assert!(statuses.iter().all(|status| live_statuses.contains(status)));
        assert!(
            snapshot
                .explain_device(" claim-1 ", &[])
                .live
                .iter()
                .any(
                    |evidence| evidence.intent.relation == WorkflowPlacementIntentRelation::Claimed
                )
        );
        assert_eq!(snapshot.explain_device(" WAIT-1 ", &[]).live.len(), 1);
        assert_eq!(snapshot.explain_device(" WAIT-2 ", &[]).live.len(), 1);
        let excluded = registry
            .placement_intent_snapshot(&repository, Some(waiting.id))
            .expect("excluded snapshot");
        assert!(excluded.explain_device("D-2", &[]).live.is_empty());
        assert!(excluded.explain_device(" claim-1 ", &[]).live.is_empty());
        assert!(excluded.explain_device("D-1", &[]).live.len() == 1);
    }

    #[test]
    fn workflow_intent_waiting_claims_survive_repository_reopen() {
        let directory =
            std::env::temp_dir().join(format!("replicant-placement-live-{}", uuid::Uuid::new_v4()));
        let path = directory.join("workflow.sqlite");
        let repository = crate::WorkflowRepository::open(&path).expect("repository");
        let kind = "test.placement";
        let mut registry = WorkflowRegistry::new();
        registry
            .register(PlacementFactory::new(kind))
            .expect("register");

        let waiting = repository
            .create(workflow(kind, "WAITING-PROJECTION"))
            .expect("waiting");
        let waiting = set_status(&repository, waiting, WorkflowStatus::Waiting);
        let wait_intent =
            WaitIntent::state("device").for_devices([" WAIT-1 ".to_owned(), "wAiT-2".to_owned()]);
        let waiting = repository
            .update_with_wait(
                waiting.id,
                waiting.revision,
                state(WorkflowStatus::Waiting),
                Some(&wait_intent),
            )
            .expect("persist wait");
        repository
            .acquire_claim(waiting.id, ResourceKey::Device(" cLaIm-1 ".to_owned()))
            .expect("persist claim");

        drop(repository);
        let reopened = crate::WorkflowRepository::open(&path).expect("reopen");
        let reopened_waiting = reopened
            .read(waiting.id)
            .expect("read reopened workflow")
            .expect("workflow row");
        assert_eq!(reopened_waiting.status, WorkflowStatus::Waiting);
        assert_eq!(
            reopened_waiting.wait_intent().expect("decode wait"),
            Some(wait_intent)
        );

        let snapshot = registry
            .placement_intent_snapshot(&reopened, None)
            .expect("reopened snapshot");
        assert!(
            snapshot
                .explain_device("WAITING-PROJECTION", &[])
                .live
                .iter()
                .any(|evidence| {
                    evidence.workflow_id == waiting.id
                        && evidence.workflow_status == WorkflowStatus::Waiting
                })
        );
        assert_eq!(snapshot.explain_device("WAIT-1", &[]).live.len(), 1);
        assert_eq!(snapshot.explain_device("WAIT-2", &[]).live.len(), 1);
        assert!(
            snapshot
                .explain_device("CLAIM-1", &[])
                .live
                .iter()
                .any(|evidence| {
                    evidence.workflow_id == waiting.id
                        && evidence.workflow_status == WorkflowStatus::Waiting
                        && evidence.intent.relation == WorkflowPlacementIntentRelation::Claimed
                })
        );
        drop(reopened);
        std::fs::remove_dir_all(directory).expect("cleanup");
    }

    #[test]
    fn workflow_intent_terminal_unknown_failed_resolution_and_reopen() {
        let directory =
            std::env::temp_dir().join(format!("replicant-placement-{}", uuid::Uuid::new_v4()));
        let path = directory.join("workflow.sqlite");
        let repository = crate::WorkflowRepository::open(&path).expect("repository");
        let kind = "test.placement";
        let mut registry = WorkflowRegistry::new();
        registry
            .register(PlacementFactory::new(kind))
            .expect("register");
        let failed = repository
            .create(workflow(kind, "failed: target "))
            .expect("failed");
        let failed = set_status(&repository, failed, WorkflowStatus::Failed);
        let other_failed = repository
            .create(workflow(kind, "failed:OTHER"))
            .expect("other failed");
        set_status(&repository, other_failed, WorkflowStatus::Failed);
        let succeeded = repository
            .create(workflow(kind, &format!("resolve:{}: TaRgEt ", failed.id)))
            .expect("resolver");
        set_status(&repository, succeeded, WorkflowStatus::Succeeded);
        let cancelled = repository
            .create(workflow(kind, "cancelled:CANCELLED"))
            .expect("cancelled");
        set_status(&repository, cancelled, WorkflowStatus::Cancelled);
        let deployed = repository
            .create(workflow(kind, "deploy:SETTLED:HOME"))
            .expect("deployed");
        set_status(&repository, deployed, WorkflowStatus::Succeeded);
        let succeeded_residual = repository
            .create(workflow(kind, "SUCCEEDED-RESIDUAL"))
            .expect("succeeded residual");
        set_status(&repository, succeeded_residual, WorkflowStatus::Succeeded);
        let unknown_live = repository
            .create(NewWorkflow {
                kind: WorkflowKind::new("test.unknown").expect("kind"),
                schema_version: 1,
                config: Value::Null,
                checkpoint: Value::Null,
                current_step: None,
                parent_id: None,
            })
            .expect("unknown live");
        let unknown_terminal = repository
            .create(NewWorkflow {
                kind: WorkflowKind::new("test.unknown").expect("kind"),
                schema_version: 1,
                config: Value::Null,
                checkpoint: Value::Null,
                current_step: None,
                parent_id: None,
            })
            .expect("unknown succeeded");
        let unknown_terminal = set_status(&repository, unknown_terminal, WorkflowStatus::Succeeded);
        let unknown_cancelled = repository
            .create(NewWorkflow {
                kind: WorkflowKind::new("test.unknown").expect("kind"),
                schema_version: 1,
                config: Value::Null,
                checkpoint: Value::Null,
                current_step: None,
                parent_id: None,
            })
            .expect("unknown cancelled");
        let unknown_cancelled =
            set_status(&repository, unknown_cancelled, WorkflowStatus::Cancelled);
        let snapshot = registry
            .placement_intent_snapshot(&repository, None)
            .expect("snapshot");
        let evidence = snapshot.explain_device(" target ", &[]);
        assert!(evidence.failed_transient.is_empty());
        assert_eq!(evidence.resolved_transient.len(), 1);
        assert_eq!(
            snapshot
                .explain_device(" oThEr ", &[])
                .failed_transient
                .len(),
            1
        );
        assert_eq!(
            snapshot
                .explain_device(" settled ", &[])
                .settled_placements
                .len(),
            1
        );
        assert_eq!(
            snapshot
                .explain_device(" cancelled ", &[])
                .terminal_residuals
                .len(),
            1
        );
        assert_eq!(
            snapshot
                .explain_device(" SUCCEEDED-RESIDUAL ", &[])
                .terminal_residuals
                .len(),
            1
        );
        assert!(snapshot.unknown_live_workflows.contains(&unknown_live.id));
        assert!(
            snapshot
                .unknown_terminal_outcomes
                .contains(&unknown_terminal.id)
        );
        assert!(
            snapshot
                .unknown_terminal_outcomes
                .contains(&unknown_cancelled.id)
        );

        drop(repository);
        let reopened = crate::WorkflowRepository::open(&path).expect("reopen");
        let reopened_snapshot = registry
            .placement_intent_snapshot(&reopened, None)
            .expect("reopened snapshot");
        assert_eq!(
            reopened_snapshot
                .explain_device(" other ", &[])
                .failed_transient
                .len(),
            1
        );
        drop(reopened);
        std::fs::remove_dir_all(directory).expect("cleanup");
    }

    #[test]
    fn workflow_intent_unknown_projector_coverage_including_failed_is_retained() {
        let repository = crate::WorkflowRepository::open_in_memory().expect("repository");
        let mut registry = WorkflowRegistry::new();
        registry
            .register(PlacementFactory::new("test.placement"))
            .expect("register");

        let malformed_live = repository
            .create(workflow("test.placement", "malformed"))
            .expect("malformed live");
        let malformed_terminal = repository
            .create(workflow("test.placement", "malformed"))
            .expect("malformed terminal");
        let malformed_terminal =
            set_status(&repository, malformed_terminal, WorkflowStatus::Succeeded);
        let unregistered_live = repository
            .create(workflow("test.unregistered", "opaque"))
            .expect("unregistered live");
        let unregistered_terminal = repository
            .create(workflow("test.unregistered", "opaque"))
            .expect("unregistered terminal");
        let unregistered_terminal = set_status(
            &repository,
            unregistered_terminal,
            WorkflowStatus::Cancelled,
        );
        let typed_failed = repository
            .create(workflow("test.placement", "failed:TYPED-FAILED"))
            .expect("typed failed");
        let typed_failed = set_status(&repository, typed_failed, WorkflowStatus::Failed);
        let opaque_failed = repository
            .create(workflow("test.unregistered", "opaque-failed"))
            .expect("opaque failed");
        let opaque_failed = set_status(&repository, opaque_failed, WorkflowStatus::Failed);

        let snapshot = registry
            .placement_intent_snapshot(&repository, None)
            .expect("snapshot");
        assert!(snapshot.unknown_live_workflows.contains(&malformed_live.id));
        assert!(
            snapshot
                .unknown_terminal_outcomes
                .contains(&malformed_terminal.id)
        );
        assert!(
            snapshot
                .unknown_live_workflows
                .contains(&unregistered_live.id)
        );
        assert!(
            snapshot
                .unknown_terminal_outcomes
                .contains(&unregistered_terminal.id)
        );
        assert!(
            snapshot
                .unknown_terminal_outcomes
                .contains(&opaque_failed.id)
        );

        let typed_evidence = snapshot.explain_device("typed-failed", &[]);
        assert_eq!(typed_evidence.failed_transient.len(), 1);
        assert_eq!(
            typed_evidence.failed_transient[0].workflow_id,
            typed_failed.id
        );

        // An opaque failed row has no device-specific projection, but its
        // unknown terminal coverage must still block absence inference.
        let opaque_evidence = snapshot.explain_device("opaque-failed", &[]);
        assert!(opaque_evidence.failed_transient.is_empty());
        assert!(
            opaque_evidence
                .unknown_terminal_outcomes
                .contains(&opaque_failed.id)
        );
        assert!(snapshot.live.is_empty());
        assert!(snapshot.settled_placements.is_empty());
        assert!(snapshot.terminal_residuals.is_empty());
    }

    #[test]
    fn workflow_intent_resolution_subtracts_only_exact_work_item_episode() {
        let repository = crate::WorkflowRepository::open_in_memory().expect("repository");
        let kind = "test.placement";
        let mut registry = WorkflowRegistry::new();
        registry
            .register(PlacementFactory::new(kind))
            .expect("register");

        let failed = repository
            .create(workflow(kind, "failed-items:TARGET"))
            .expect("failed");
        let failed_items = repository
            .reconcile_work_items(
                failed.id,
                &[
                    work_item_spec(failed.id, "first"),
                    work_item_spec(failed.id, "second"),
                ],
                1,
            )
            .expect("failed items");
        let failed = set_status(&repository, failed, WorkflowStatus::Failed);

        let other_failed = repository
            .create(workflow(kind, "failed-items:TARGET"))
            .expect("other failed");
        let other_items = repository
            .reconcile_work_items(
                other_failed.id,
                &[work_item_spec(other_failed.id, "other")],
                2,
            )
            .expect("other failed items");
        let other_failed = set_status(&repository, other_failed, WorkflowStatus::Failed);

        let resolver = repository
            .create(workflow(
                kind,
                &format!("resolve-item:{}:{}: target ", failed.id, failed_items[0].id),
            ))
            .expect("resolver");
        set_status(&repository, resolver, WorkflowStatus::Succeeded);

        let snapshot = registry
            .placement_intent_snapshot(&repository, None)
            .expect("snapshot");
        let evidence = snapshot.explain_device("target", &[]);
        assert_eq!(evidence.resolved_transient.len(), 1);
        assert_eq!(evidence.failed_transient.len(), 2);
        assert_eq!(evidence.resolved_transient[0].workflow_id, failed.id);
        assert_eq!(
            evidence.resolved_transient[0].intent.work_item_id,
            Some(failed_items[0].id)
        );
        assert!(evidence.failed_transient.iter().any(|item| {
            item.workflow_id == failed.id && item.intent.work_item_id == Some(failed_items[1].id)
        }));
        assert!(evidence.failed_transient.iter().any(|item| {
            item.workflow_id == other_failed.id
                && item.intent.work_item_id == Some(other_items[0].id)
        }));
    }

    #[test]
    fn workflow_intent_bulk_items_are_grouped() {
        let repository = crate::WorkflowRepository::open_in_memory().expect("repository");
        let kind = WorkflowKind::new("test.placement").expect("kind");
        let first = repository
            .create(NewWorkflow {
                kind: kind.clone(),
                schema_version: 1,
                config: Value::String("A".to_owned()),
                checkpoint: Value::Null,
                current_step: None,
                parent_id: None,
            })
            .expect("first");
        let second = repository
            .create(NewWorkflow {
                kind,
                schema_version: 1,
                config: Value::String("B".to_owned()),
                checkpoint: Value::Null,
                current_step: None,
                parent_id: None,
            })
            .expect("second");
        let spec = WorkItemSpec {
            workflow_id: first.id,
            dedupe_key: "one".to_owned(),
            kind: WorkflowKind::new("test.item").expect("item kind"),
            sort_key: "one".to_owned(),
            payload_json: Value::Null,
            preconditions_json: Value::Null,
            requirements_json: Value::Null,
            deadline_at_ms: None,
        };
        repository
            .reconcile_work_items(first.id, &[spec], 0)
            .expect("item");
        let grouped = repository
            .list_work_items_for_workflows(&[second.id, first.id])
            .expect("bulk");
        assert_eq!(grouped[&first.id].len(), 1);
        assert!(grouped[&second.id].is_empty());
    }

    #[test]
    fn service_intent_snapshot_is_live_scoped_exact_and_conservative() {
        let repository = crate::WorkflowRepository::open_in_memory().expect("repository");
        let kind = "test.service";
        let mut registry = WorkflowRegistry::new();
        registry
            .register(ServiceFactory::new(kind))
            .expect("register");
        let present = repository
            .create(workflow(kind, "present"))
            .expect("present workflow");
        let unknown = repository
            .create(workflow(kind, "unknown-region"))
            .expect("unknown workflow");
        let unknown = set_status(&repository, unknown, WorkflowStatus::Waiting);
        let terminal = repository
            .create(workflow(kind, "present"))
            .expect("terminal workflow");
        let terminal = set_status(&repository, terminal, WorkflowStatus::Succeeded);
        let unregistered = repository
            .create(workflow("test.unregistered-service", "present"))
            .expect("unregistered workflow");
        let target = WorkflowServiceIntent::new(
            "ami_transport",
            BTreeMap::from([
                ("collect".to_owned(), "BELT".to_owned()),
                ("deliver".to_owned(), "HUB".to_owned()),
            ]),
        );

        let snapshot = registry
            .service_intent_snapshot(&repository, None)
            .expect("service snapshot");
        assert_eq!(
            snapshot.state_for(&target, Some("ALPHA"), Some("SOL")),
            WorkflowServiceIntentState::Present(vec![present.id])
        );
        assert!(
            !snapshot
                .live
                .iter()
                .any(|evidence| evidence.workflow_id == terminal.id)
        );

        let unrelated = WorkflowServiceIntent::new(
            "ami_transport",
            BTreeMap::from([
                ("collect".to_owned(), "OTHER".to_owned()),
                ("deliver".to_owned(), "HUB".to_owned()),
            ]),
        );
        let WorkflowServiceIntentState::Unknown(alpha_unknown) =
            snapshot.state_for(&unrelated, Some(" alpha "), Some("SOL"))
        else {
            panic!("regional and global ambiguity must block");
        };
        assert_eq!(alpha_unknown.len(), 2);
        assert!(alpha_unknown.contains(&unknown.id));
        assert!(alpha_unknown.contains(&unregistered.id));
        assert_eq!(
            snapshot.state_for(&unrelated, Some("beta"), Some("SOL")),
            WorkflowServiceIntentState::Unknown(vec![unregistered.id])
        );
        assert_eq!(
            registry.service_intent_state_for_instance(
                &present,
                &target,
                Some("alpha"),
                Some("SOL"),
            ),
            WorkflowServiceIntentState::Present(vec![present.id])
        );
        assert_eq!(
            registry.service_intent_state_for_instance(
                &unknown,
                &target,
                Some("alpha"),
                Some("SOL"),
            ),
            WorkflowServiceIntentState::Unknown(vec![unknown.id])
        );
        assert_eq!(
            registry.service_intent_state_for_instance(
                &unregistered,
                &target,
                Some("beta"),
                Some("OTHER"),
            ),
            WorkflowServiceIntentState::Unknown(vec![unregistered.id])
        );
    }
    #[test]
    fn instance_service_compatibility_does_not_reenter_repository() {
        let repository = crate::WorkflowRepository::open_in_memory().expect("repository");
        let existing = repository
            .create(workflow("test.unregistered-service", "present"))
            .expect("existing workflow");
        let registry = WorkflowRegistry::new();
        let target = WorkflowServiceIntent::new("ami_transport", BTreeMap::new());

        let result = repository.create_or_reuse_active(
            workflow("test.candidate-service", "present"),
            |instance| match registry.service_intent_state_for_instance(
                instance,
                &target,
                Some("alpha"),
                Some("SOL"),
            ) {
                WorkflowServiceIntentState::Present(_) => Ok(true),
                WorkflowServiceIntentState::Absent => Ok(false),
                WorkflowServiceIntentState::Unknown(_) => Err(
                    crate::RepositoryError::Compatibility("unknown service".to_owned()),
                ),
            },
        );

        assert!(matches!(
            result,
            Err(crate::RepositoryError::Compatibility(message)) if message == "unknown service"
        ));
        let workflows = repository.list().expect("workflows");
        assert_eq!(workflows.len(), 1);
        assert_eq!(workflows[0].id, existing.id);
    }
}
