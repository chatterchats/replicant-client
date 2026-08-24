//! CLI adapter for the reusable Riker colony report.

use std::{env, io, path::PathBuf};

use replicant_runtime::{config::ManagedClientConfig, rikers::riker_report, start_managed_client};

struct Config {
    database: PathBuf,
    limit: usize,
    diagnostics: bool,
}

impl Config {
    fn parse(arguments: Vec<String>) -> crate::AnyResult<Option<Self>> {
        let mut database = env::var_os("REPLICANT_DB")
            .map(PathBuf::from)
            .unwrap_or_else(replicant_client::default_database_path);
        let mut limit = env::var("RS_RIKERS_LIMIT")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(10);
        let mut diagnostics = true;
        let mut arguments = arguments.into_iter();
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--database" => database = required(&mut arguments, "--database")?.into(),
                "--limit" => {
                    limit = required(&mut arguments, "--limit")?.parse().map_err(|_| {
                        io::Error::new(io::ErrorKind::InvalidInput, "--limit must be an integer")
                    })?
                }
                "--no-diagnostics" => diagnostics = false,
                "-h" | "--help" => {
                    print_help();
                    return Ok(None);
                }
                other => return Err(input_error(format!("unexpected argument: {other}"))),
            }
        }
        if limit == 0 {
            return Err(input_error("--limit must be greater than zero"));
        }
        Ok(Some(Self {
            database,
            limit,
            diagnostics,
        }))
    }
}

pub(crate) async fn run_cli(arguments: Vec<String>) -> crate::AnyResult<()> {
    let Some(config) = Config::parse(arguments)? else {
        return Ok(());
    };
    eprintln!("database: {}", config.database.display());
    let client = start_managed_client(ManagedClientConfig::from_env(&config.database)?).await?;
    let report = riker_report(&client, config.diagnostics).await?;

    if config.diagnostics {
        eprintln!("\nlocal location-query diagnostics:");
        for (label, count) in report.diagnostics {
            eprintln!("  {label:<48} {count:>6}");
        }
    }
    eprintln!(
        "final hard-filter query matched {} candidate world(s)",
        report.candidates.len()
    );
    if report.candidates.is_empty() {
        eprintln!("\nNo locally persisted worlds satisfy every hard filter.");
        eprintln!("Use the staged counts above to identify the first restrictive predicate.");
    }
    for candidate in report.candidates.into_iter().take(config.limit) {
        println!("Riker, how about {}?", candidate.designation);
        println!(
            "  heuristic score: {:.1} (local planning heuristic)",
            candidate.heuristic_score
        );
        if !candidate.strengths.is_empty() {
            println!("  strengths: {}", candidate.strengths.join("; "));
        }
        if !candidate.cautions.is_empty() {
            println!("  cautions: {}", candidate.cautions.join("; "));
        }
    }
    client.close().await?;
    Ok(())
}

fn required(arguments: &mut impl Iterator<Item = String>, flag: &str) -> crate::AnyResult<String> {
    arguments
        .next()
        .ok_or_else(|| input_error(format!("{flag} requires a value")))
}

fn input_error(message: impl Into<String>) -> crate::AnyError {
    io::Error::new(io::ErrorKind::InvalidInput, message.into()).into()
}

fn print_help() {
    println!(
        "Riker colony candidates\n\n\
Usage:\n  replicant-cli rikers [OPTIONS]\n\n\
Options:\n  --database PATH     Managed SQLite database\n  --limit N           Maximum candidates to print (default: 10)\n  --no-diagnostics    Hide staged local-query counts\n  -h, --help          Show this help\n\n\
This command synchronizes known survey data, scores candidates locally, and\n\
prints suggestions. It never sends a BobNet message or performs gameplay mutations."
    );
}
