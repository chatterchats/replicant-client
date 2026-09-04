//! Bulk device-ownership normalization by catalogue region.
//!
//! This workflow is intentionally account-scoped and managed-only. It uses
//! the complete owned-device baseline from managed startup, classifies devices
//! by their physical star (following stow/attachment parents when necessary),
//! excludes devices that physically host a replicant matrix, and submits
//! durable `change_owner` mutations for the remaining selected devices.

use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    fs::{self, OpenOptions},
    io::{self, ErrorKind},
    path::{Path, PathBuf},
};

use crate::{canonical_region, config::ManagedClientConfig, start_managed_client};
use replicant_client::{Client, Device, OperationStatus, Replicant, Star, SyncDomain};
use serde::Serialize;
use tracing::info;
use tracing_subscriber::{EnvFilter, prelude::*};

const DEFAULT_OWNER: &str = "Chats-1";

#[derive(Clone, Debug)]
struct Config {
    database: PathBuf,
    owner: String,
    regions: BTreeSet<String>,
    all_regions: bool,
    ignore_regions: BTreeSet<String>,
    execute: bool,
    verbose: bool,
    log_file: Option<PathBuf>,
    json: bool,
}

#[derive(Clone, Debug, Serialize)]
struct Candidate {
    code: String,
    device_type: String,
    location: String,
    region: String,
    current_owner: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize)]
struct SelectionSummary {
    selected_regions: Vec<String>,
    target_owner_name: Option<String>,
    target_owner_code: String,
    matched: Vec<Candidate>,
    excluded_replicant_vessels: usize,
    already_target_owned: usize,
    in_transit: Vec<String>,
    unresolved_location: Vec<String>,
    unregioned: Vec<String>,
    unsupported_change_owner: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
struct ExecutionResult {
    selection: SelectionSummary,
    submitted: usize,
    confirmed: usize,
    pending_confirmation: Vec<String>,
    failed: Vec<MutationFailure>,
}

#[derive(Clone, Debug, Serialize)]
struct MutationFailure {
    code: String,
    status: String,
    detail: String,
}

/// Runs the compatibility CLI adapter for the ownership action.
pub async fn run_cli(arguments: Vec<String>) -> crate::AnyResult<()> {
    if arguments.is_empty()
        || arguments
            .iter()
            .any(|argument| matches!(argument.as_str(), "-h" | "--help" | "help"))
    {
        print_help();
        return Ok(());
    }
    let config = parse_config(arguments)?;
    init_logging(&config)?;

    let client = start_client(&config.database).await?;
    let result = run(&client, &config).await;
    let close_result = client.close().await;
    close_result?;
    result
}

fn parse_config(arguments: Vec<String>) -> crate::AnyResult<Config> {
    let mut arguments = arguments.into_iter();
    let Some(operation) = arguments.next() else {
        print_help();
        return Err(app_error("ownership requires the `reassign` operation"));
    };
    if operation != "reassign" {
        return Err(app_error(format!(
            "unknown ownership operation {operation:?}; expected `reassign`"
        )));
    }

    let mut config = Config {
        database: env::var_os("REPLICANT_DB")
            .map(PathBuf::from)
            .unwrap_or_else(replicant_client::default_database_path),
        owner: env::var("RS_OWNERSHIP_TARGET").unwrap_or_else(|_| DEFAULT_OWNER.to_owned()),
        regions: BTreeSet::new(),
        all_regions: false,
        ignore_regions: BTreeSet::new(),
        execute: false,
        verbose: false,
        log_file: None,
        json: false,
    };

    if let Ok(value) = env::var("RS_OWNERSHIP_REGIONS") {
        insert_region_list(&mut config.regions, &value)?;
    }
    if env_bool("RS_OWNERSHIP_ALL_REGIONS") {
        config.all_regions = true;
    }
    if let Ok(value) = env::var("RS_OWNERSHIP_IGNORE_REGIONS") {
        insert_region_list(&mut config.ignore_regions, &value)?;
    }

    let mut arguments = arguments.peekable();
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--owner" | "--target-owner" => {
                config.owner = required(&mut arguments, &argument)?;
            }
            "--region" => {
                let value = required(&mut arguments, &argument)?;
                insert_region_list(&mut config.regions, &value)?;
            }
            "--all-regions" => config.all_regions = true,
            "--ignore-region" => {
                let value = required(&mut arguments, &argument)?;
                insert_region_list(&mut config.ignore_regions, &value)?;
            }
            "--execute" | "--apply" => config.execute = true,
            "--database" => config.database = PathBuf::from(required(&mut arguments, &argument)?),
            "--verbose" => config.verbose = true,
            "--log-file" => {
                config.log_file = Some(PathBuf::from(required(&mut arguments, &argument)?))
            }
            "--json" => config.json = true,
            other => return Err(app_error(format!("unknown ownership option {other:?}"))),
        }
    }

    config.owner = config.owner.trim().to_owned();
    if config.owner.is_empty() {
        return Err(app_error("--owner cannot be empty"));
    }
    if config.all_regions && !config.regions.is_empty() {
        return Err(app_error(
            "--all-regions cannot be combined with --region; use --ignore-region to subtract \
             regions",
        ));
    }
    if !config.all_regions && config.regions.is_empty() {
        return Err(app_error(
            "select at least one --region, or use --all-regions",
        ));
    }

    Ok(config)
}

fn print_help() {
    println!(
        "Regional ownership reassignment\n\n\
Usage:\n  replicant-cli ownership reassign [OPTIONS]\n\n\
Selection:\n  --region REGION          Include a catalogue region (repeatable/comma-separated)\n  --all-regions            Include every named region in the current star catalogue\n  --ignore-region REGION   Exclude a region from the selection (repeatable/comma-separated)\n\n\
Ownership:\n  --owner NAME_OR_CODE     New replicant owner (default: Chats-1)\n  --execute                Submit durable change_owner mutations; without this flag, preview only\n\n\
Other options:\n  --database PATH          Managed SQLite database\n  --verbose                Show tracing logs in the terminal\n  --log-file PATH          Append tracing logs to a file\n  --json                   Emit machine-readable selection/result JSON\n  -h, --help               Show this help\n\n\
Region names are case-insensitive. `solregion`, `solzone`, and `sol` all select\n\
the catalogue's `solzone`. Replicant-hosting vessels are always excluded.\n\
When a device has no direct location, its stow/attachment parent is followed\n\
for physical region classification. Unresolved and unregioned devices are\n\
reported and skipped. `--ignore-region` is validated before any mutation, so a\n\
typo cannot silently reassign devices in a region you intended to protect.\n\n\
Examples:\n  replicant-cli ownership reassign \\\n    --region solregion --region alpha --region beta --region gamma\n\n  replicant-cli ownership reassign \\\n    --all-regions --ignore-region delta --owner Chats-1 --execute"
    );
}

async fn start_client(database: &Path) -> crate::AnyResult<Client> {
    Ok(start_managed_client(ManagedClientConfig::from_env(database)?).await?)
}

async fn run(client: &Client, config: &Config) -> crate::AnyResult<()> {
    client.ready().await?;
    client.galaxy().refresh_catalogue().await?;
    client.sync().domain(SyncDomain::Replicants).await?;

    let target = resolve_owned_replicant(client, &config.owner).await?;
    let target_code = target.key.id.as_str().to_owned();
    let target_name = target.name.clone();
    let catalogue = client.galaxy().catalogue();
    let selected_regions = resolve_selected_regions(config, &catalogue)?;

    let handles = client.devices().find().owned().collect().await?;
    let mut devices = BTreeMap::new();
    for handle in handles {
        let snapshot = handle.snapshot().await?;
        devices.insert(snapshot.key.id.as_str().to_owned(), snapshot);
    }

    let selection = select_candidates(
        &devices,
        &catalogue,
        &selected_regions,
        &target_code,
        target_name.clone(),
    );

    if !config.execute {
        print_selection(&selection, config.json)?;
        if !config.json {
            println!("\nPreview only; add --execute to submit ownership changes.");
        }
        return Ok(());
    }

    if selection.matched.is_empty() {
        print_selection(&selection, config.json)?;
        return Ok(());
    }

    info!(
        target_owner = %target_code,
        devices = selection.matched.len(),
        regions = ?selection.selected_regions,
        "submitting regional ownership reassignment"
    );

    let mut failed = Vec::new();
    let mut submitted_codes = Vec::new();
    for candidate in &selection.matched {
        let Some(handle) = client.devices().cached(&candidate.code) else {
            failed.push(MutationFailure {
                code: candidate.code.clone(),
                status: "missing-managed-handle".to_owned(),
                detail: "device disappeared from managed state before submission".to_owned(),
            });
            continue;
        };
        match handle.change_owner(target_code.clone()).await {
            Ok(operation) => match operation.outcome().await {
                Ok(outcome)
                    if matches!(
                        outcome.status,
                        OperationStatus::Rejected
                            | OperationStatus::Failed
                            | OperationStatus::Cancelled
                    ) =>
                {
                    failed.push(MutationFailure {
                        code: candidate.code.clone(),
                        status: format!("{:?}", outcome.status),
                        detail: format!("{:?}", outcome.response),
                    });
                }
                Ok(outcome) if outcome.status == OperationStatus::Ambiguous => {
                    failed.push(MutationFailure {
                        code: candidate.code.clone(),
                        status: "Ambiguous".to_owned(),
                        detail: "submission outcome is ambiguous; the mutation was not retried"
                            .to_owned(),
                    });
                }
                Ok(_) => submitted_codes.push(candidate.code.clone()),
                Err(error) => failed.push(MutationFailure {
                    code: candidate.code.clone(),
                    status: "outcome-error".to_owned(),
                    detail: error.to_string(),
                }),
            },
            Err(error) => failed.push(MutationFailure {
                code: candidate.code.clone(),
                status: "submit-error".to_owned(),
                detail: error.to_string(),
            }),
        }
    }

    // One authoritative collection refresh confirms all successful transfers
    // without issuing a GET for every changed device.
    client.sync().essential().await?;

    let refreshed = client.devices().find().owned().collect().await?;
    let mut owners = BTreeMap::new();
    for handle in refreshed {
        let snapshot = handle.snapshot().await?;
        owners.insert(
            snapshot.key.id.as_str().to_owned(),
            snapshot
                .relationships
                .assigned_replicant
                .as_ref()
                .map(|owner| owner.id.as_str().to_owned()),
        );
    }

    let mut confirmed = 0_usize;
    let mut pending_confirmation = Vec::new();
    for code in &submitted_codes {
        if owners
            .get(code)
            .and_then(|owner| owner.as_deref())
            .is_some_and(|owner| owner.eq_ignore_ascii_case(&target_code))
        {
            confirmed += 1;
        } else {
            pending_confirmation.push(code.clone());
        }
    }

    let result = ExecutionResult {
        selection,
        submitted: submitted_codes.len(),
        confirmed,
        pending_confirmation,
        failed,
    };
    print_execution(&result, config.json)?;

    if !result.failed.is_empty() || !result.pending_confirmation.is_empty() {
        return Err(app_error(
            "one or more ownership changes failed or remain unconfirmed; rerun the command to \
             reconcile safely",
        ));
    }
    Ok(())
}

fn resolve_selected_regions(
    config: &Config,
    catalogue: &[Star],
) -> crate::AnyResult<BTreeSet<String>> {
    let available = catalogue
        .iter()
        .filter_map(|star| star.region.as_deref())
        .map(canonical_region)
        .collect::<BTreeSet<_>>();

    let unknown_ignored = config
        .ignore_regions
        .difference(&available)
        .cloned()
        .collect::<Vec<_>>();
    if !unknown_ignored.is_empty() {
        return Err(app_error(format!(
            "ignored region(s) are not present in the current catalogue: {}; available named regions: {}",
            unknown_ignored.join(", "),
            available.iter().cloned().collect::<Vec<_>>().join(", ")
        )));
    }

    let mut selected = if config.all_regions {
        available.clone()
    } else {
        let unknown = config
            .regions
            .difference(&available)
            .cloned()
            .collect::<Vec<_>>();
        if !unknown.is_empty() {
            return Err(app_error(format!(
                "requested region(s) are not present in the current catalogue: {}; available named regions: {}",
                unknown.join(", "),
                available.iter().cloned().collect::<Vec<_>>().join(", ")
            )));
        }
        config.regions.clone()
    };
    for ignored in &config.ignore_regions {
        selected.remove(ignored);
    }
    if selected.is_empty() {
        return Err(app_error(
            "region selection is empty after applying exclusions",
        ));
    }
    Ok(selected)
}

fn select_candidates(
    devices: &BTreeMap<String, Device>,
    catalogue: &[Star],
    selected_regions: &BTreeSet<String>,
    target_code: &str,
    target_name: Option<String>,
) -> SelectionSummary {
    let mut summary = SelectionSummary {
        selected_regions: selected_regions.iter().cloned().collect(),
        target_owner_name: target_name,
        target_owner_code: target_code.to_owned(),
        ..SelectionSummary::default()
    };

    for (code, device) in devices {
        if device.relationships.hosting_replicant.is_some() {
            summary.excluded_replicant_vessels += 1;
            continue;
        }

        let current_owner = device
            .relationships
            .assigned_replicant
            .as_ref()
            .map(|owner| owner.id.as_str().to_owned());
        if current_owner
            .as_deref()
            .is_some_and(|owner| owner.eq_ignore_ascii_case(target_code))
        {
            summary.already_target_owned += 1;
            continue;
        }

        if device_or_parent_is_traveling(code, devices, &mut BTreeSet::new()) {
            summary.in_transit.push(code.clone());
            continue;
        }

        let mut visiting = BTreeSet::new();
        let Some(location) = effective_location(code, devices, &mut visiting) else {
            summary.unresolved_location.push(code.clone());
            continue;
        };
        let Some(region) = region_for_location(catalogue, &location) else {
            summary.unregioned.push(code.clone());
            continue;
        };
        if !selected_regions.contains(&region) {
            continue;
        }

        if !device.available_commands.is_empty()
            && !device
                .available_commands
                .iter()
                .any(|command| command.as_str() == "change_owner")
        {
            summary.unsupported_change_owner.push(code.clone());
            continue;
        }

        summary.matched.push(Candidate {
            code: code.clone(),
            device_type: device
                .device_type
                .as_ref()
                .map(|kind| kind.as_str().to_owned())
                .unwrap_or_else(|| "<unknown>".to_owned()),
            location,
            region,
            current_owner,
        });
    }
    summary.matched.sort_by(|left, right| {
        left.region
            .cmp(&right.region)
            .then_with(|| left.location.cmp(&right.location))
            .then_with(|| left.code.cmp(&right.code))
    });
    summary
}

fn device_or_parent_is_traveling(
    code: &str,
    devices: &BTreeMap<String, Device>,
    visiting: &mut BTreeSet<String>,
) -> bool {
    if !visiting.insert(code.to_owned()) {
        return false;
    }
    let Some(device) = devices.get(code) else {
        return false;
    };
    if device.travel.is_some() {
        return true;
    }
    [
        device.relationships.stowed_in.as_ref(),
        device.relationships.attached_to.as_ref(),
    ]
    .into_iter()
    .flatten()
    .any(|parent| device_or_parent_is_traveling(parent.id.as_str(), devices, visiting))
}

fn effective_location(
    code: &str,
    devices: &BTreeMap<String, Device>,
    visiting: &mut BTreeSet<String>,
) -> Option<String> {
    if !visiting.insert(code.to_owned()) {
        return None;
    }
    let device = devices.get(code)?;
    if let Some(location) = &device.location {
        return Some(location.id.as_str().to_owned());
    }
    for parent in [
        device.relationships.stowed_in.as_ref(),
        device.relationships.attached_to.as_ref(),
    ]
    .into_iter()
    .flatten()
    {
        if let Some(location) = effective_location(parent.id.as_str(), devices, visiting) {
            return Some(location);
        }
    }
    None
}

fn region_for_location(catalogue: &[Star], location: &str) -> Option<String> {
    catalogue
        .iter()
        .filter(|star| {
            let designation = star.key.id.as_str();
            location.eq_ignore_ascii_case(designation)
                || location
                    .get(..designation.len())
                    .is_some_and(|prefix| prefix.eq_ignore_ascii_case(designation))
                    && location
                        .get(designation.len()..)
                        .is_some_and(|suffix| suffix.starts_with('-'))
        })
        .max_by_key(|star| star.key.id.as_str().len())
        .and_then(|star| star.region.as_deref())
        .map(canonical_region)
}

async fn resolve_owned_replicant(client: &Client, query: &str) -> crate::AnyResult<Replicant> {
    let handles = client.replicants().find().owned().collect().await?;
    let mut matches = Vec::new();
    for handle in handles {
        let snapshot = handle.snapshot().await?;
        if snapshot.key.id.as_str().eq_ignore_ascii_case(query)
            || snapshot
                .name
                .as_deref()
                .is_some_and(|name| name.eq_ignore_ascii_case(query))
        {
            matches.push(snapshot);
        }
    }
    match matches.len() {
        1 => Ok(matches.remove(0)),
        0 => Err(app_error(format!("no owned replicant matches {query:?}"))),
        _ => Err(app_error(format!(
            "owned replicant name {query:?} is ambiguous; use its code"
        ))),
    }
}

fn print_selection(selection: &SelectionSummary, json: bool) -> crate::AnyResult<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(selection)?);
        return Ok(());
    }
    println!("Regional ownership reassignment preview");
    println!(
        "  Target owner: {}{}",
        selection.target_owner_code,
        selection
            .target_owner_name
            .as_deref()
            .map(|name| format!(" ({name})"))
            .unwrap_or_default()
    );
    println!("  Regions: {}", selection.selected_regions.join(", "));
    println!("  Devices to change: {}", selection.matched.len());
    println!(
        "  Replicant vessels excluded: {}",
        selection.excluded_replicant_vessels
    );
    println!(
        "  Already assigned to target: {}",
        selection.already_target_owned
    );
    println!("  In transit: {}", selection.in_transit.len());
    println!(
        "  Unresolved location: {}",
        selection.unresolved_location.len()
    );
    println!("  Unregioned: {}", selection.unregioned.len());
    println!(
        "  No change_owner command: {}",
        selection.unsupported_change_owner.len()
    );

    if !selection.matched.is_empty() {
        println!("\nSelected devices:");
        for candidate in &selection.matched {
            println!(
                "  {:8}  {:24}  {:10}  {:24}  owner={}",
                candidate.code,
                candidate.device_type,
                candidate.region,
                candidate.location,
                candidate.current_owner.as_deref().unwrap_or("<none>")
            );
        }
    }
    print_skipped("In transit", &selection.in_transit);
    print_skipped("Unresolved location", &selection.unresolved_location);
    print_skipped("Unregioned", &selection.unregioned);
    print_skipped(
        "No change_owner command",
        &selection.unsupported_change_owner,
    );
    Ok(())
}

fn print_execution(result: &ExecutionResult, json: bool) -> crate::AnyResult<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(result)?);
        return Ok(());
    }
    println!("Regional ownership reassignment");
    println!("  Target owner: {}", result.selection.target_owner_code);
    println!("  Selected: {}", result.selection.matched.len());
    println!("  Submitted: {}", result.submitted);
    println!("  Confirmed: {}", result.confirmed);
    println!(
        "  Pending confirmation: {}",
        result.pending_confirmation.len()
    );
    println!("  Failed/ambiguous: {}", result.failed.len());
    print_skipped("Pending confirmation", &result.pending_confirmation);
    if !result.failed.is_empty() {
        println!("\nFailed/ambiguous mutations:");
        for failure in &result.failed {
            println!("  {}  {}  {}", failure.code, failure.status, failure.detail);
        }
    }
    Ok(())
}

fn print_skipped(label: &str, codes: &[String]) {
    if codes.is_empty() {
        return;
    }
    println!("\n{label}:");
    for code in codes {
        println!("  {code}");
    }
}

fn insert_region_list(target: &mut BTreeSet<String>, value: &str) -> crate::AnyResult<()> {
    let values = value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(canonical_region)
        .collect::<Vec<_>>();
    if values.is_empty() {
        return Err(app_error("region values cannot be empty"));
    }
    target.extend(values);
    Ok(())
}

fn required<I>(arguments: &mut std::iter::Peekable<I>, flag: &str) -> crate::AnyResult<String>
where
    I: Iterator<Item = String>,
{
    arguments
        .next()
        .filter(|value| !value.starts_with("--"))
        .ok_or_else(|| app_error(format!("{flag} requires a value")))
}

fn env_bool(name: &str) -> bool {
    env::var(name).is_ok_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

fn init_logging(config: &Config) -> crate::AnyResult<()> {
    if !config.verbose && config.log_file.is_none() {
        return Ok(());
    }
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new("warn,replicant_cli::ownership=info,replicant_client::ops=info")
    });
    match (&config.log_file, config.verbose) {
        (None, true) => tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer().with_writer(io::stderr))
            .try_init()
            .map_err(|error| app_error(error.to_string()))?,
        (Some(path), verbose) => {
            if let Some(parent) = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
            {
                fs::create_dir_all(parent)?;
            }
            let file = OpenOptions::new().create(true).append(true).open(path)?;
            let registry = tracing_subscriber::registry().with(filter);
            if verbose {
                registry
                    .with(tracing_subscriber::fmt::layer().with_writer(io::stderr))
                    .with(
                        tracing_subscriber::fmt::layer()
                            .with_ansi(false)
                            .with_writer(std::sync::Mutex::new(file)),
                    )
                    .try_init()
                    .map_err(|error| app_error(error.to_string()))?;
            } else {
                registry
                    .with(
                        tracing_subscriber::fmt::layer()
                            .with_ansi(false)
                            .with_writer(std::sync::Mutex::new(file)),
                    )
                    .try_init()
                    .map_err(|error| app_error(error.to_string()))?;
            }
        }
        (None, false) => {}
    }
    Ok(())
}

fn app_error(message: impl Into<String>) -> crate::AnyError {
    io::Error::new(ErrorKind::InvalidInput, message.into()).into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use replicant_client::domain::{
        AccessScope, DeviceId, DeviceKey, DeviceRelationships, LocationId, LocationKey,
        ReplicantId, ReplicantKey, StarId, StarKey,
    };

    fn device(code: &str, location: Option<&str>, owner: Option<&str>) -> Device {
        Device {
            key: DeviceKey::live(DeviceId::from(code)),
            device_type: Some(replicant_client::DeviceType::from("mining_drone")),
            status: None,
            location: location.map(|value| LocationKey::live(LocationId::from(value))),
            deployed_at: None,
            in_control_range: None,
            features: Vec::new(),
            available_commands: vec![replicant_client::DeviceCommand::from("change_owner")],
            available_directives: Vec::new(),
            tags: Vec::new(),
            settings: Default::default(),
            relationships: DeviceRelationships {
                assigned_replicant: owner.map(|value| ReplicantKey::live(ReplicantId::from(value))),
                ..DeviceRelationships::default()
            },
            cargo: Default::default(),
            cargo_capacity: None,
            attach_capacity: None,
            stow_capacity: None,
            stow_used: None,
            operational_capacity: None,
            grace_period_remaining: None,
            upkeep_requirements: Vec::new(),
            system_status: None,
            active_directive: None,
            travel: None,
            runtime: Default::default(),
            access: AccessScope::Owned,
        }
    }

    fn star(name: &str, region: Option<&str>) -> Star {
        Star {
            key: StarKey::live(StarId::from(name)),
            name: None,
            spectral_type: None,
            entry_point: None,
            position: None,
            has_hub: None,
            has_ward: None,
            knowledge_observed: false,
            explored: None,
            has_life: None,
            region: region.map(str::to_owned),
        }
    }

    #[test]
    fn all_regions_is_dynamic_and_ignore_region_subtracts_from_catalogue() {
        let catalogue = vec![
            star("SOL", Some("solzone")),
            star("ALPHA-STAR", Some("alpha")),
            star("DELTA", Some("delta")),
        ];
        let config = Config {
            database: PathBuf::from("test.sqlite"),
            owner: DEFAULT_OWNER.to_owned(),
            regions: BTreeSet::new(),
            all_regions: true,
            ignore_regions: BTreeSet::from(["delta".to_owned()]),
            execute: false,
            verbose: false,
            log_file: None,
            json: false,
        };

        let selected = resolve_selected_regions(&config, &catalogue).expect("region selection");
        assert_eq!(
            selected,
            BTreeSet::from(["alpha".to_owned(), "solzone".to_owned()])
        );
    }

    #[test]
    fn unknown_ignored_region_is_rejected_before_mutation_selection() {
        let catalogue = vec![star("SOL", Some("solzone")), star("A", Some("alpha"))];
        let config = Config {
            database: PathBuf::from("test.sqlite"),
            owner: DEFAULT_OWNER.to_owned(),
            regions: BTreeSet::new(),
            all_regions: true,
            ignore_regions: BTreeSet::from(["delta".to_owned()]),
            execute: true,
            verbose: false,
            log_file: None,
            json: false,
        };

        let error = resolve_selected_regions(&config, &catalogue).expect_err("unknown ignore");
        assert!(error.to_string().contains("ignored region(s)"));
    }

    #[test]
    fn child_device_inherits_stowed_parent_location() {
        let mut devices = BTreeMap::new();
        let parent = device("VESSEL", Some("SCEPTURUM-OORT"), Some("R3"));
        let mut child = device("CHILD", None, Some("R3"));
        child.relationships.stowed_in = Some(parent.key.clone());
        devices.insert("VESSEL".to_owned(), parent);
        devices.insert("CHILD".to_owned(), child);

        assert_eq!(
            effective_location("CHILD", &devices, &mut BTreeSet::new()).as_deref(),
            Some("SCEPTURUM-OORT")
        );
    }

    #[test]
    fn cargo_inside_a_traveling_vessel_is_skipped_as_in_transit() {
        let catalogue = vec![star("SCEPTURUM", Some("alpha"))];
        let mut vessel = device("VESSEL", Some("SCEPTURUM-OORT"), Some("R3"));
        vessel.travel = Some(replicant_client::domain::TravelState::default());
        let mut cargo = device("CARGO", None, Some("R3"));
        cargo.relationships.stowed_in = Some(vessel.key.clone());
        let devices = BTreeMap::from([("VESSEL".to_owned(), vessel), ("CARGO".to_owned(), cargo)]);
        let selected = BTreeSet::from(["alpha".to_owned()]);

        let result = select_candidates(
            &devices,
            &catalogue,
            &selected,
            "R1",
            Some("Chats-1".into()),
        );
        assert!(result.matched.is_empty());
        assert_eq!(result.in_transit, vec!["CARGO", "VESSEL"]);
    }

    #[test]
    fn replicant_hosting_vessels_are_never_selected_but_their_cargo_can_be() {
        let catalogue = vec![star("SCEPTURUM", Some("alpha"))];
        let mut vessel = device("VESSEL", Some("SCEPTURUM-OORT"), Some("R3"));
        vessel.relationships.hosting_replicant = Some(ReplicantKey::live(ReplicantId::from("R3")));
        let mut cargo = device("CARGO", None, Some("R3"));
        cargo.relationships.stowed_in = Some(vessel.key.clone());
        let devices = BTreeMap::from([("VESSEL".to_owned(), vessel), ("CARGO".to_owned(), cargo)]);
        let selected = BTreeSet::from(["alpha".to_owned()]);

        let result = select_candidates(
            &devices,
            &catalogue,
            &selected,
            "R1",
            Some("Chats-1".into()),
        );
        assert_eq!(result.excluded_replicant_vessels, 1);
        assert_eq!(result.matched.len(), 1);
        assert_eq!(result.matched[0].code, "CARGO");
        assert_eq!(result.matched[0].region, "alpha");
    }

    #[test]
    fn region_matching_uses_longest_catalogue_prefix_and_skips_unregioned_space() {
        let catalogue = vec![
            star("DEL", Some("alpha")),
            star("DELTA", Some("delta")),
            star("VOID", None),
        ];
        assert_eq!(
            region_for_location(&catalogue, "DELTA-KUIPER").as_deref(),
            Some("delta")
        );
        assert_eq!(region_for_location(&catalogue, "VOID-OORT"), None);
    }
}
