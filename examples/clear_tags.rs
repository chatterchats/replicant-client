//! Removes every `evt-` tag from every owned device while preserving all
//! non-event tags.
//!
//! Environment variables:
//!
//! - `RS_API_TOKEN` — required bearer token.
//! - `REPLICANT_DB` — SQLite path; defaults to `replicant-client.sqlite`.
//!
//! Run with:
//!
//! ```text
//! cargo run --example clear_tags
//! ```
//!
//! Preview without changing anything:
//!
//! ```text
//! cargo run --example clear_tags -- --dry-run
//! ```

use std::{env, error::Error, io, path::PathBuf};

use replicant_client::{Client, Operation, OperationStatus, SecretString, StartupPolicy, raw};

type AnyError = Box<dyn Error + Send + Sync + 'static>;
type AnyResult<T> = Result<T, AnyError>;

const DEFAULT_TAG_PREFIX: &str = "evt-";

#[tokio::main]
async fn main() -> AnyResult<()> {
    let options = parse_options()?;
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

    let result = clear_tags(&client, options.dry_run, &options.tag_prefix).await;
    let close_result = client.close().await;
    result?;
    close_result?;
    Ok(())
}

#[derive(Debug)]
struct Options {
    dry_run: bool,
    tag_prefix: String,
}

fn parse_options() -> AnyResult<Options> {
    let mut dry_run = false;
    let mut tag_prefix = DEFAULT_TAG_PREFIX.to_owned();
    let mut arguments = env::args().skip(1);

    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--dry-run" => dry_run = true,
            "--tag-prefix" => {
                tag_prefix = arguments.next().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "--tag-prefix requires a value")
                })?;
                if tag_prefix.is_empty() {
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

    Ok(Options {
        dry_run,
        tag_prefix,
    })
}

async fn clear_tags(client: &Client, dry_run: bool, event_prefix: &str) -> AnyResult<()> {
    // Use an unfiltered authoritative traversal so the script sees every owned
    // device, regardless of type, location, owner assignment, or current tags.
    let handles = client.devices().refresh_many().collect().await?;

    let total = handles.len();
    let mut matching_devices = 0usize;
    let mut changed_devices = 0usize;
    let mut removed_tags = 0usize;

    for handle in handles {
        let snapshot = handle.snapshot().await?;
        let event_tags: Vec<String> = snapshot
            .tags
            .iter()
            .filter(|tag| tag.starts_with(event_prefix))
            .cloned()
            .collect();

        if event_tags.is_empty() {
            continue;
        }

        matching_devices += 1;
        removed_tags += event_tags.len();
        let device_code = handle.id().as_str();
        let joined_tags = event_tags.join(", ");

        if dry_run {
            println!("would clear {device_code}: {joined_tags}");
            continue;
        }

        let operation = handle
            .configure(raw::devices::DeviceConfiguration {
                remove_tags: Some(event_tags),
                ..Default::default()
            })
            .await?;
        ensure_operation_accepted(&operation).await?;

        changed_devices += 1;
        println!("cleared {device_code}: {joined_tags}");
    }

    if dry_run {
        println!(
            "Dry run: scanned {total} device(s); {matching_devices} device(s) carry {removed_tags} {event_prefix} tag(s)."
        );
    } else {
        println!(
            "Done: scanned {total} device(s); cleared {removed_tags} {event_prefix} tag(s) from {changed_devices} device(s)."
        );
    }

    Ok(())
}

async fn ensure_operation_accepted(operation: &Operation) -> AnyResult<()> {
    // The managed mutation has already made its one durable submission attempt.
    // Check immediate classification without serially waiting for SSE
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
