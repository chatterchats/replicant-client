//! Adds `twaffy-ring-001` to every owned `exotic_matter_injector` that does not
//! already carry the tag. Existing tags are preserved.
//!
//! Environment variables:
//!
//! - `RS_API_TOKEN` — required bearer token.
//! - `REPLICANT_DB` — SQLite path; defaults under `~/.local/share/replicant`.
//!
//! Run with:
//!
//! ```text
//! cargo run --example tag_twaffy_ring_injectors
//! ```

use std::{env, error::Error, io, path::PathBuf};

use replicant_client::{Client, SecretString, StartupPolicy};
use replicant_runtime::actions::{TagDevicesAction, tag_devices};

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
        .unwrap_or_else(replicant_client::default_database_path);

    let client = Client::builder()
        .authentication_token(token)
        .sqlite(database)
        .startup_policy(StartupPolicy::Essential)
        .start()
        .await?;
    client.ready().await?;

    let result = tag_devices(&client, &TagDevicesAction::new(DEVICE_TYPE, TAG)).await;
    let close_result = client.close().await;
    let result = result?;
    for event in result.report.events {
        println!("{:?} {}: {}", event.kind, event.subject, event.detail);
    }
    println!(
        "Done: {} {DEVICE_TYPE} device(s), {} newly tagged, {} already tagged.",
        result.scanned_devices, result.changed_devices, result.already_tagged_devices
    );
    close_result?;
    Ok(())
}
