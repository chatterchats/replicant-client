//! Transfers every owned `exotic_matter_injector` at `TWAFFY-OBJ-1` to
//! `Chats-4`, verifies the ownership changes, then contributes all of those
//! devices to the location in one request.
//!
//! Environment variables:
//!
//! - `RS_API_TOKEN` — required bearer token.
//! - `REPLICANT_DB` — SQLite path; defaults to `replicant-client.sqlite`.
//!
//! Run with:
//!
//! ```text
//! cargo run --example contribute_twaffy_injectors
//! ```
//!
//! This performs a destructive contribution: successfully contributed devices
//! are consumed by the location project.

use std::{
    collections::BTreeSet,
    env,
    error::Error,
    io,
    path::PathBuf,
    time::{Duration, Instant},
};

use replicant_client::{
    Client, DeviceType, Operation, OperationStatus, SecretString, StartupPolicy, SyncDomain,
};

const LOCATION: &str = "TWAFFY-OBJ-1";
const TARGET_REPLICANT: &str = "Chats-4";
const DEVICE_TYPE: &str = "exotic_matter_injector";
const OWNER_VERIFY_TIMEOUT: Duration = Duration::from_secs(30);

type AnyError = Box<dyn Error + Send + Sync + 'static>;
type AnyResult<T> = Result<T, AnyError>;

#[tokio::main]
async fn main() -> AnyResult<()> {
    let token = env::var("RS_API_TOKEN")
        .map(SecretString::from)
        .map_err(|_| io::Error::new(io::ErrorKind::NotFound, "RS_API_TOKEN is not set"))?;
    let database = env::var_os("REPLICANT_DB")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("replicant-client.sqlite"));

    let client = Client::builder()
        .authentication_token(token)
        .sqlite(database)
        .startup_policy(StartupPolicy::Essential)
        .start()
        .await?;
    client.ready().await?;

    let result = contribute_injectors(&client).await;
    let close_result = client.close().await;
    result?;
    close_result?;
    Ok(())
}

async fn contribute_injectors(client: &Client) -> AnyResult<()> {
    let target_replicant_code = resolve_target_replicant(client).await?;

    let handles = client
        .devices()
        .refresh_many()
        .of_type(DeviceType::from(DEVICE_TYPE))
        .at(LOCATION)
        .collect()
        .await?;

    let mut codes = handles
        .iter()
        .map(|handle| handle.id().as_str().to_owned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    codes.sort();

    if codes.is_empty() {
        println!("No {DEVICE_TYPE} devices are currently at {LOCATION}; nothing to contribute.");
        return Ok(());
    }

    println!(
        "Found {} {DEVICE_TYPE} device(s) at {LOCATION}.",
        codes.len()
    );

    let mut transferred = 0usize;
    let mut already_owned = 0usize;

    for code in &codes {
        let handle = client.devices().get(code).await?;
        let snapshot = handle.snapshot().await?;
        ensure_still_at_location(code, &snapshot)?;

        if assigned_replicant(&snapshot) == Some(target_replicant_code.as_str()) {
            already_owned += 1;
            println!("owner {code}: already {TARGET_REPLICANT} ({target_replicant_code})");
            continue;
        }

        println!(
            "owner {code}: {} -> {TARGET_REPLICANT} ({target_replicant_code})",
            assigned_replicant(&snapshot).unwrap_or("unassigned")
        );
        let operation = handle.change_owner(&target_replicant_code).await?;
        wait_for_owner(client, code, &operation, &target_replicant_code).await?;
        transferred += 1;
    }

    // Refresh every device once more immediately before the destructive
    // contribution. This prevents us from submitting a stale device list if a
    // device moved or an ownership change did not actually become visible.
    for code in &codes {
        let handle = client.devices().get(code).await?;
        let snapshot = handle.snapshot().await?;
        ensure_still_at_location(code, &snapshot)?;

        if assigned_replicant(&snapshot) != Some(target_replicant_code.as_str()) {
            return Err(io::Error::other(format!(
                "device {code} is not assigned to {TARGET_REPLICANT} ({target_replicant_code}) immediately before contribution; current owner={:?}",
                assigned_replicant(&snapshot)
            ))
            .into());
        }
    }

    println!(
        "Contributing {} device(s) to {LOCATION} ({transferred} transferred, {already_owned} already owned by {TARGET_REPLICANT} ({target_replicant_code}))...",
        codes.len()
    );

    let operation = client
        .locations()
        .contribute(LOCATION, codes.clone())
        .await?;
    let outcome = operation.outcome().await?;
    ensure_successful_status(&operation, outcome.status, outcome.response.as_ref())?;

    println!(
        "Contribution accepted for {} {DEVICE_TYPE} device(s) at {LOCATION}.",
        codes.len()
    );
    if let Some(response) = outcome.response {
        println!("Server response: {response}");
    }

    Ok(())
}

async fn resolve_target_replicant(client: &Client) -> AnyResult<String> {
    // Essential startup does not refresh the owned-replicant domain. Refresh it
    // explicitly so name -> code resolution is authoritative for this run.
    client.sync().domain(SyncDomain::Replicants).await?;

    let handles = client.replicants().find().owned().collect().await?;
    let mut matches = Vec::new();

    for handle in handles {
        let snapshot = handle.snapshot().await?;
        if snapshot
            .key
            .id
            .as_str()
            .eq_ignore_ascii_case(TARGET_REPLICANT)
            || snapshot
                .name
                .as_deref()
                .is_some_and(|name| name.eq_ignore_ascii_case(TARGET_REPLICANT))
        {
            matches.push(snapshot);
        }
    }

    let replicant = match matches.len() {
        1 => matches.remove(0),
        0 => {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("no owned replicant matches {TARGET_REPLICANT:?}"),
            )
            .into());
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "owned replicant name {TARGET_REPLICANT:?} is ambiguous; use its code in the example"
                ),
            )
            .into());
        }
    };

    let code = replicant.key.id.as_str().to_owned();
    let actual = replicant
        .location
        .as_ref()
        .map(|location| location.id.as_str());

    if actual != Some(LOCATION) {
        return Err(io::Error::other(format!(
            "refusing to change ownership: {TARGET_REPLICANT} ({code}) must be at {LOCATION}, but currently reports location={actual:?}"
        ))
        .into());
    }

    println!("Verified {TARGET_REPLICANT} ({code}) is at {LOCATION}.");
    Ok(code)
}

async fn wait_for_owner(
    client: &Client,
    code: &str,
    operation: &Operation,
    target_replicant_code: &str,
) -> AnyResult<()> {
    let started = Instant::now();
    let mut delay = Duration::from_millis(250);

    loop {
        let handle = client.devices().get(code).await?;
        let snapshot = handle.snapshot().await?;
        ensure_still_at_location(code, &snapshot)?;

        if assigned_replicant(&snapshot) == Some(target_replicant_code) {
            println!("owner {code}: verified {TARGET_REPLICANT} ({target_replicant_code})");
            return Ok(());
        }

        let status = operation.status().await?;
        if matches!(
            status,
            OperationStatus::Cancelled | OperationStatus::Rejected | OperationStatus::Failed
        ) {
            return Err(io::Error::other(format!(
                "change_owner for {code} ended as {status:?}; current owner={:?}",
                assigned_replicant(&snapshot)
            ))
            .into());
        }

        if started.elapsed() >= OWNER_VERIFY_TIMEOUT {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "device {code} did not report owner={TARGET_REPLICANT} ({target_replicant_code}) within {OWNER_VERIFY_TIMEOUT:?}; current owner={:?}, operation_status={status:?}, operation_id={}",
                    assigned_replicant(&snapshot),
                    operation.id()
                ),
            )
            .into());
        }

        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(Duration::from_secs(2));
    }
}

fn assigned_replicant(device: &replicant_client::Device) -> Option<&str> {
    device
        .relationships
        .assigned_replicant
        .as_ref()
        .map(|replicant| replicant.id.as_str())
}

fn ensure_still_at_location(code: &str, device: &replicant_client::Device) -> AnyResult<()> {
    let actual = device
        .location
        .as_ref()
        .map(|location| location.id.as_str());
    if actual == Some(LOCATION) {
        return Ok(());
    }

    Err(io::Error::other(format!(
        "device {code} moved before contribution; expected {LOCATION}, current location={actual:?}"
    ))
    .into())
}

fn ensure_successful_status(
    operation: &Operation,
    status: OperationStatus,
    response: Option<&serde_json::Value>,
) -> AnyResult<()> {
    if matches!(
        status,
        OperationStatus::Cancelled | OperationStatus::Rejected | OperationStatus::Failed
    ) {
        return Err(io::Error::other(format!(
            "operation {} ended as {status:?}: {response:?}",
            operation.id()
        ))
        .into());
    }

    if status == OperationStatus::Ambiguous {
        return Err(io::Error::other(format!(
            "contribution operation {} is ambiguous; do not blindly resubmit the same device list. Refresh the location first (rerunning this example is safe because it rediscovers only devices still present at {LOCATION})",
            operation.id()
        ))
        .into());
    }

    Ok(())
}
