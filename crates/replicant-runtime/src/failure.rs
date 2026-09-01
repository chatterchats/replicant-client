use std::{error::Error, io};

use replicant_client::managed::{OperationOutcome, OperationStatus};
use replicant_workflow::WorkflowFailureDisposition;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) enum FailureClass {
    EventInputsUnavailable,
    EventControlUnavailable,
    EventAssetStale,
    DeviceTargetMissing,
    RelayPlanStale,
    EventExecutorContention,
    ResourceClaimContention,
    ConnectivityDependency,
    ManufacturingCapacity,
    TransientUpstream,
    LogisticsStateStale,
}

#[derive(Debug, Error)]
#[error("{source}")]
pub(crate) struct ClassifiedError {
    pub(crate) class: FailureClass,
    pub(crate) disposition: WorkflowFailureDisposition,
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
            disposition: WorkflowFailureDisposition::Retryable,
            source: io::Error::new(kind, message.into()),
        }
    }

    pub(crate) fn permanent(
        class: FailureClass,
        kind: io::ErrorKind,
        message: impl Into<String>,
    ) -> Self {
        Self {
            class,
            disposition: WorkflowFailureDisposition::Permanent,
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

pub(crate) fn permanent_classified_error(
    class: FailureClass,
    kind: io::ErrorKind,
    message: impl Into<String>,
) -> Box<dyn Error + Send + Sync + 'static> {
    Box::new(ClassifiedError::permanent(class, kind, message))
}
pub(crate) fn device_operation_is_missing(outcome: &OperationOutcome) -> bool {
    device_rejection_is_missing(
        outcome.status,
        outcome.http_status(),
        outcome.server_error(),
    )
}

fn device_rejection_is_missing(
    status: OperationStatus,
    http_status: Option<u16>,
    server_error: Option<&str>,
) -> bool {
    status == OperationStatus::Rejected
        && (http_status == Some(404)
            || server_error.is_some_and(|error| error.eq_ignore_ascii_case("Device not found")))
}

pub(crate) fn device_fetch_is_missing(error: &replicant_client::Error) -> bool {
    error.status() == Some(404)
}

pub(crate) fn failure_class(error: &(dyn Error + 'static)) -> Option<FailureClass> {
    let message = error.to_string();
    let mut current = Some(error);
    while let Some(error) = current {
        if let Some(error) = error.downcast_ref::<ClassifiedError>() {
            return Some(error.class);
        }
        if error
            .downcast_ref::<io::Error>()
            .is_some_and(|error| error.kind() == io::ErrorKind::TimedOut)
        {
            return Some(FailureClass::TransientUpstream);
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

pub(crate) fn failure_disposition(error: &(dyn Error + 'static)) -> WorkflowFailureDisposition {
    let mut current = Some(error);
    while let Some(error) = current {
        if let Some(error) = error.downcast_ref::<ClassifiedError>() {
            return error.disposition;
        }
        current = error.source();
    }
    WorkflowFailureDisposition::Retryable
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
    } else if lower.contains("timed out waiting for autofactory queue capacity") {
        Some(FailureClass::ManufacturingCapacity)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn autofactory_capacity_timeout_is_retryable_manufacturing_pressure() {
        assert_eq!(
            failure_class_from_message("timed out waiting for autofactory queue capacity"),
            Some(FailureClass::ManufacturingCapacity)
        );
    }

    #[test]
    fn structured_timeout_is_transient_upstream_failure() {
        let error = io::Error::new(io::ErrorKind::TimedOut, "timed out traveling to TARGET");
        assert_eq!(failure_class(&error), Some(FailureClass::TransientUpstream));
    }

    #[test]
    fn device_missing_requires_a_structured_rejected_outcome() {
        assert!(device_rejection_is_missing(
            OperationStatus::Rejected,
            Some(404),
            None
        ));
        assert!(device_rejection_is_missing(
            OperationStatus::Rejected,
            Some(400),
            Some("dEvIcE NoT FoUnD")
        ));
        assert!(!device_rejection_is_missing(
            OperationStatus::Accepted,
            Some(404),
            Some("Device not found")
        ));
        assert!(!device_rejection_is_missing(
            OperationStatus::Rejected,
            Some(400),
            Some("Not your device")
        ));
        assert!(!device_rejection_is_missing(
            OperationStatus::Rejected,
            None,
            None
        ));
    }

    #[test]
    fn structured_errors_carry_failure_disposition() {
        let retryable = ClassifiedError::new(
            FailureClass::TransientUpstream,
            io::ErrorKind::ConnectionReset,
            "upstream unavailable",
        );
        let permanent = ClassifiedError::permanent(
            FailureClass::DeviceTargetMissing,
            io::ErrorKind::NotFound,
            "device no longer exists",
        );

        assert_eq!(
            failure_disposition(&retryable),
            WorkflowFailureDisposition::Retryable
        );
        assert_eq!(
            failure_disposition(&permanent),
            WorkflowFailureDisposition::Permanent
        );
    }
}
