//! Contributes matching devices at a location after assigning them to one
//! owned replicant.
//!
//! ```text
//! cargo run --example contribute_twaffy_injectors -- [--dry-run] [--destination LOCATION]
//!     [--device-type TYPE] [--owner NAME_OR_CODE] [--tag TAG] [--count N]
//! ```
//!
//! With no arguments this preserves the original TWAFFY injector behavior.
//! A successful contribution consumes the selected devices.

use std::{env, error::Error, io, path::PathBuf};

use replicant_runtime::{
    actions::{ContributeDevicesAction, contribute_devices},
    config::ManagedClientConfig,
    start_managed_client,
};

const DEFAULT_DESTINATION: &str = "TWAFFY-OBJ-1";
const DEFAULT_DEVICE_TYPE: &str = "exotic_matter_injector";
const DEFAULT_OWNER: &str = "Chats-4";

type AnyError = Box<dyn Error + Send + Sync + 'static>;
type AnyResult<T> = Result<T, AnyError>;

#[tokio::main]
async fn main() -> AnyResult<()> {
    let action = parse_options()?;
    let database = env::var_os("REPLICANT_DB")
        .map(PathBuf::from)
        .unwrap_or_else(replicant_client::default_database_path);
    let client = start_managed_client(ManagedClientConfig::from_env(database)?).await?;
    client.ready().await?;

    let result = contribute_devices(&client, &action).await;
    let close_result = client.close().await;
    let result = result?;
    for event in &result.report.events {
        println!(
            "{:?} {}: {}{}",
            event.kind,
            event.subject,
            event.detail,
            event
                .operation_id
                .as_deref()
                .map(|id| format!(" (operation {id})"))
                .unwrap_or_default()
        );
    }
    close_result?;
    if result.report.failed() {
        return Err(io::Error::other("contribution action failed; see events above").into());
    }
    Ok(())
}

fn parse_options() -> AnyResult<ContributeDevicesAction> {
    let mut action =
        ContributeDevicesAction::new(DEFAULT_DESTINATION, DEFAULT_DEVICE_TYPE, DEFAULT_OWNER);
    let mut arguments = env::args().skip(1);
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--dry-run" => action.dry_run = true,
            "--destination" => action.destination = next_value(&mut arguments, &argument)?,
            "--device-type" => action.device_type = next_value(&mut arguments, &argument)?,
            "--owner" => action.owner = next_value(&mut arguments, &argument)?,
            "--tag" => action.tag = Some(next_value(&mut arguments, &argument)?),
            "--count" => {
                let value = next_value(&mut arguments, &argument)?;
                action.count = Some(value.parse().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidInput, "--count requires an integer")
                })?);
            }
            "-h" | "--help" => {
                println!(
                    "Usage: cargo run --example contribute_twaffy_injectors -- [--dry-run] \
                     [--destination LOCATION] [--device-type TYPE] [--owner NAME_OR_CODE] \
                     [--tag TAG] [--count N]\n\nDefaults: destination={DEFAULT_DESTINATION}, \
                     device-type={DEFAULT_DEVICE_TYPE}, owner={DEFAULT_OWNER}, all matching devices"
                );
                std::process::exit(0);
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown argument {argument:?}; use --help for usage"),
                )
                .into());
            }
        }
    }
    Ok(action)
}

fn next_value(arguments: &mut impl Iterator<Item = String>, option: &str) -> AnyResult<String> {
    arguments.next().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{option} requires a value"),
        )
        .into()
    })
}
