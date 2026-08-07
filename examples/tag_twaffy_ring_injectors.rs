//! Adds `twaffy-ring-001` to every owned `exotic_matter_injector` that does not
//! already carry the tag. Existing tags are preserved.
//!
//! Environment variables:
//!
//! - `RS_API_TOKEN` — required bearer token.
//! - `REPLICANT_DB` — SQLite path; defaults to `replicant-client.sqlite`.
//!
//! Run with:
//!
//! ```text
//! cargo run --example tag_twaffy_ring_injectors
//! ```

use std::{env, error::Error, io, path::PathBuf};

use replicant_client::{
    Client, DeviceType, Operation, OperationStatus, SecretString, StartupPolicy, raw,
};

type AnyError = Box<dyn Error + Send + Sync + 'static>;
type AnyResult<T> = Result<T, AnyError>;

const DEVICE_TYPE: &str = "exotic_matter_injector";
const TAG: &str = "twaffy-ring-001";

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

    let result = tag_injectors(&client).await;
    let close_result = client.close().await;
    result?;
    close_result?;
    Ok(())
}

async fn tag_injectors(client: &Client) -> AnyResult<()> {
    let handles = client
        .devices()
        .refresh_many()
        .of_type(DeviceType::from(DEVICE_TYPE))
        .collect()
        .await?;

    let total = handles.len();
    let mut tagged = 0usize;
    let mut already_tagged = 0usize;

    for handle in handles {
        let snapshot = handle.snapshot().await?;
        if snapshot.tags.iter().any(|tag| tag == TAG) {
            already_tagged += 1;
            println!("skip {}: already tagged {TAG}", handle.id().as_str());
            continue;
        }

        let operation = handle
            .configure(raw::devices::DeviceConfiguration {
                add_tags: Some(vec![TAG.to_owned()]),
                ..Default::default()
            })
            .await?;
        ensure_operation_accepted(&operation).await?;
        tagged += 1;
        println!("tagged {} with {TAG}", handle.id().as_str());
    }

    println!(
        "Done: {total} {DEVICE_TYPE} device(s), {tagged} newly tagged, {already_tagged} already tagged."
    );
    Ok(())
}

async fn ensure_operation_accepted(operation: &Operation) -> AnyResult<()> {
    // The managed mutation has already made its one durable submission attempt.
    // Check that immediate classification without serially waiting for SSE
    // confirmation after every independent tag update.
    let outcome = operation.outcome().await?;
    if matches!(
        outcome.status,
        OperationStatus::Cancelled | OperationStatus::Rejected | OperationStatus::Failed
    ) {
        return Err(io::Error::other(format!(
            "operation {} ended as {:?}: {:?}",
            operation.id().as_str(),
            outcome.status,
            outcome.response
        ))
        .into());
    }
    Ok(())
}
