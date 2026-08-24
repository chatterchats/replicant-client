//! Removes every `evt-` tag from every owned device while preserving all
//! non-event tags.
//!
//! ```text
//! cargo run --example clear_tags -- [--dry-run] [--tag-prefix PREFIX]
//! ```

use std::{env, error::Error, io, path::PathBuf};

use replicant_client::{Client, SecretString, StartupPolicy};
use replicant_runtime::actions::{ClearTagsAction, clear_tags};

type AnyError = Box<dyn Error + Send + Sync + 'static>;
type AnyResult<T> = Result<T, AnyError>;

const DEFAULT_TAG_PREFIX: &str = "evt-";

#[tokio::main]
async fn main() -> AnyResult<()> {
    let action = parse_options()?;
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

    let result = clear_tags(&client, &action).await;
    let close_result = client.close().await;
    let result = result?;
    for device in &result.devices {
        let verb = if action.dry_run {
            "would clear"
        } else {
            "cleared"
        };
        println!("{verb} {}: {}", device.device, device.tags.join(", "));
    }
    if action.dry_run {
        println!(
            "Dry run: scanned {} device(s); {} device(s) carry {} {} tag(s).",
            result.scanned_devices,
            result.devices.len(),
            result.removed_tags(),
            action.tag_prefix
        );
    } else {
        println!(
            "Done: scanned {} device(s); cleared {} {} tag(s) from {} device(s).",
            result.scanned_devices,
            result.removed_tags(),
            action.tag_prefix,
            result.changed_devices()
        );
    }
    close_result?;
    Ok(())
}

fn parse_options() -> AnyResult<ClearTagsAction> {
    let mut action = ClearTagsAction::new(DEFAULT_TAG_PREFIX);
    let mut arguments = env::args().skip(1);
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--dry-run" => action.dry_run = true,
            "--tag-prefix" => {
                action.tag_prefix = arguments.next().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "--tag-prefix requires a value")
                })?;
                if action.tag_prefix.is_empty() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "--tag-prefix must not be empty",
                    )
                    .into());
                }
            }
            "-h" | "--help" => {
                println!(
                    "Usage: cargo run --example clear_tags -- [--dry-run] [--tag-prefix PREFIX]\n\n\
                     Removes tags whose names start with PREFIX from every owned device.\n\
                     Default prefix: {DEFAULT_TAG_PREFIX}"
                );
                std::process::exit(0);
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "unknown argument {argument:?}; expected --dry-run or --tag-prefix PREFIX"
                    ),
                )
                .into());
            }
        }
    }
    Ok(action)
}
