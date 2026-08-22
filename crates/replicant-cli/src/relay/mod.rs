//! Compatibility frontend for relay expansion.

use std::collections::BTreeMap;

use replicant_protocol::{OperationKind, StartWorkflowRequest};

use replicant_runtime::relay::RelayExpansionRequest;

/// Runs the relay command through the reusable runtime service.
pub(crate) async fn run_cli(mut arguments: Vec<String>) -> crate::AnyResult<()> {
    let direct = take_flag(&mut arguments, "--direct");
    let run = arguments.first().is_some_and(|argument| argument == "run");
    if !run || direct {
        return replicant_runtime::relay::run_cli(arguments).await;
    }
    if let Some(option) = arguments.iter().find(|argument| {
        matches!(
            argument.as_str(),
            "--database" | "--verbose" | "--log-file" | "--supply-strategy"
        )
    }) {
        return Err(crate::app_error(format!(
            "{option} configures standalone execution; use --direct or configure replicantd"
        )));
    }
    let request = replicant_runtime::relay::relay_workflow_request(arguments)?;
    crate::workflow::submit(start_request(request)).await
}

fn take_flag(arguments: &mut Vec<String>, flag: &str) -> bool {
    let found = arguments.iter().any(|argument| argument == flag);
    arguments.retain(|argument| argument != flag);
    found
}

fn start_request(request: RelayExpansionRequest) -> StartWorkflowRequest {
    let mut parameters = BTreeMap::new();
    parameters.insert("replicant".into(), request.replicant.into());
    parameters.insert("hub".into(), request.hub.into());
    parameters.insert("targets_csv".into(), request.targets.join(",").into());
    parameters.insert(
        "mission_file".into(),
        request.mission_file.to_string_lossy().into_owned().into(),
    );
    parameters.insert("max_hop_ly".into(), request.max_hop_ly.into());
    parameters.insert(
        "wait_timeout_seconds".into(),
        request.wait_timeout.as_secs().into(),
    );
    StartWorkflowRequest {
        kind: OperationKind("relay.expansion".to_owned()),
        parameters,
    }
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, time::Duration};

    use super::*;

    #[test]
    fn daemon_request_contains_typed_relay_options() {
        let request = start_request(RelayExpansionRequest {
            replicant: "TEST-1".into(),
            hub: "SOL-HUB".into(),
            targets: vec!["ALPHA".into(), "BETA".into()],
            mission_file: PathBuf::from("relay.json"),
            max_hop_ly: 7.4,
            wait_timeout: Duration::from_secs(30),
            unavailable_autofactories: Default::default(),
        });
        assert_eq!(request.kind.0, "relay.expansion");
        assert_eq!(request.parameters["targets_csv"], "ALPHA,BETA");
        assert_eq!(request.parameters["wait_timeout_seconds"], 30);
    }
}
