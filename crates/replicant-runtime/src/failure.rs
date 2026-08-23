use std::{error::Error, io};

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) enum FailureClass {
    EventInputsUnavailable,
    EventControlUnavailable,
    EventAssetStale,
    RelayPlanStale,
    ResourceClaimContention,
    ConnectivityDependency,
    TransientUpstream,
    LogisticsStateStale,
}

#[derive(Debug, Error)]
#[error("{source}")]
pub(crate) struct ClassifiedError {
    pub(crate) class: FailureClass,
    #[source]
    source: io::Error,
}

impl ClassifiedError {
    pub(crate) fn new(
        class: FailureClass,
        kind: io::ErrorKind,
        message: impl Into<String>,
    ) -> Self {
        Self {
            class,
            source: io::Error::new(kind, message.into()),
        }
    }
}

pub(crate) fn classified_error(
    class: FailureClass,
    kind: io::ErrorKind,
    message: impl Into<String>,
) -> Box<dyn Error + Send + Sync + 'static> {
    Box::new(ClassifiedError::new(class, kind, message))
}

pub(crate) fn failure_class(error: &(dyn Error + 'static)) -> Option<FailureClass> {
    let message = error.to_string();
    let mut current = Some(error);
    while let Some(error) = current {
        if let Some(error) = error.downcast_ref::<ClassifiedError>() {
            return Some(error.class);
        }
        if let Some(error) = error.downcast_ref::<replicant_client::Error>()
            && (matches!(error, replicant_client::Error::Closed) || error.status() == Some(500))
        {
            return Some(FailureClass::TransientUpstream);
        }
        if matches!(
            error.downcast_ref::<replicant_workflow::RepositoryError>(),
            Some(replicant_workflow::RepositoryError::ClaimConflict { .. })
        ) {
            return Some(FailureClass::ResourceClaimContention);
        }
        if matches!(
            error.downcast_ref::<replicant_route_planner::PlannerError>(),
            Some(
                replicant_route_planner::PlannerError::Disconnected
                    | replicant_route_planner::PlannerError::DisconnectedGap { .. }
                    | replicant_route_planner::PlannerError::DisconnectedRouteAround(_)
            )
        ) || matches!(
            error.downcast_ref::<replicant_printing::ScheduleError>(),
            Some(
                replicant_printing::ScheduleError::MissingBlueprint(_)
                    | replicant_printing::ScheduleError::MissingComponentBlueprint { .. }
                    | replicant_printing::ScheduleError::NoAutofactory
            )
        ) || matches!(
            error.downcast_ref::<replicant_mining_planner::PlannerError>(),
            Some(replicant_mining_planner::PlannerError::MissingBlueprint(_))
        ) || matches!(
            error.downcast_ref::<replicant_event_planner::PlannerError>(),
            Some(
                replicant_event_planner::PlannerError::MissingBlueprint(_)
                    | replicant_event_planner::PlannerError::NoAutofactory
            )
        ) {
            return Some(FailureClass::ConnectivityDependency);
        }
        current = error.source();
    }
    failure_class_from_message(&message)
}

pub(crate) fn failure_class_from_message(message: &str) -> Option<FailureClass> {
    let lower = message.to_ascii_lowercase();
    if lower.contains("all currently feasible events completed, but blocked events remain") {
        Some(FailureClass::EventInputsUnavailable)
    } else if lower.contains("not your device")
        || lower.contains("not present in the account-owned device projection")
    {
        Some(FailureClass::EventAssetStale)
    } else if lower.contains("out of comms range") || lower.contains("out of control range") {
        Some(FailureClass::EventControlUnavailable)
    } else if lower.contains("unexpected http status 500")
        || lower.contains("internal server error")
        || lower.contains("client is closed")
    {
        Some(FailureClass::TransientUpstream)
    } else if lower.contains("planned account-owned relay coverage is no longer relaying") {
        Some(FailureClass::RelayPlanStale)
    } else if lower.contains("resource is already claimed by workflow") {
        Some(FailureClass::ResourceClaimContention)
    } else if lower.contains("no relay network connects")
        || lower.contains("blueprint is not unlocked")
        || lower.contains("missing blueprint")
        || lower.contains("insufficient manufacturing inventory")
        || lower.contains("requires an idle attachment carrier")
        || lower.contains("no usable stow capacity")
        || lower.contains("not currently projected as stationary in a star system")
        || lower.contains("has no known l4 or l5 deployment location")
        || lower.contains("no eligible autofactory")
    {
        Some(FailureClass::ConnectivityDependency)
    } else if (message.contains("planned resource pickup at ")
        && message.contains(" is stale: need "))
        || (message.contains("Insufficient ")
            && message.contains(" at location: need ")
            && message.contains(", have "))
        || message.contains("not a free inactive payload")
        || message.contains("reserved by another workflow")
    {
        Some(FailureClass::LogisticsStateStale)
    } else {
        None
    }
}
