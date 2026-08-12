use std::{env, error::Error as StdError, io};

mod belt_search;
mod bootstrap;
mod event;
mod mining;
mod observatory;
mod printing;
mod relay;
mod rikers;
mod survey;
mod trade;
mod transport;

type AnyError = Box<dyn StdError + Send + Sync + 'static>;
type AnyResult<T> = Result<T, AnyError>;

fn app_error(message: impl Into<String>) -> AnyError {
    io::Error::new(io::ErrorKind::InvalidInput, message.into()).into()
}

#[tokio::main]
async fn main() -> AnyResult<()> {
    let mut arguments = env::args().skip(1).collect::<Vec<_>>();
    let Some(command) = arguments.first().cloned() else {
        print_help();
        return Ok(());
    };
    arguments.remove(0);

    match command.as_str() {
        "-h" | "--help" | "help" if arguments.is_empty() => {
            print_help();
            Ok(())
        }
        "-V" | "--version" | "version" => {
            println!("replicant-cli {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        "help" => dispatch_help(arguments).await,
        "print" | "printing" => {
            printing::run_cli(normalize_operation_flag(
                arguments,
                &["queue", "clear", "status"],
            ))
            .await
        }
        "transport" | "deliver" | "delivery" => transport::run_cli(arguments).await,
        "trade" | "trades" | "shop" | "shops" => trade::run_cli(arguments).await,
        "belt-search" | "belt-scan" => belt_search::run_cli(arguments).await,
        "survey" => {
            survey::run_cli(normalize_operation_flag(
                arguments,
                &["plan", "run", "status"],
            ))
            .await
        }
        "relay" | "relays" => {
            relay::run_cli(normalize_operation_flag(
                arguments,
                &["plan", "run", "status"],
            ))
            .await
        }
        "mining" | "mine" => {
            mining::run_cli(normalize_operation_flag(
                arguments,
                &["plan", "run", "status"],
            ))
            .await
        }
        "observatory" | "observatories" | "obs" => {
            observatory::run_cli(normalize_operation_flag(
                arguments,
                &["status", "prospect", "triangulate"],
            ))
            .await
        }
        "event" | "events" => {
            event::run_cli(normalize_operation_flag(
                arguments,
                &["list", "plan", "run", "status"],
            ))
            .await
        }
        "bootstrap" => {
            bootstrap::run_cli(normalize_operation_flag(
                arguments,
                &["plan", "stage", "deliver", "run", "status"],
            ))
            .await
        }
        "rikers" | "riker" => rikers::run_cli(arguments).await,
        other => Err(app_error(format!(
            "unknown command {other:?}; run `replicant-cli --help` for available commands"
        ))),
    }
}

async fn dispatch_help(mut arguments: Vec<String>) -> AnyResult<()> {
    if arguments.len() != 1 {
        return Err(app_error("usage: replicant-cli help COMMAND"));
    }
    arguments.push("--help".to_owned());
    let command = arguments.remove(0);
    match command.as_str() {
        "print" | "printing" => printing::run_cli(arguments).await,
        "transport" | "deliver" | "delivery" => transport::run_cli(arguments).await,
        "trade" | "trades" | "shop" | "shops" => trade::run_cli(arguments).await,
        "belt-search" | "belt-scan" => belt_search::run_cli(arguments).await,
        "survey" => survey::run_cli(arguments).await,
        "relay" | "relays" => relay::run_cli(arguments).await,
        "mining" | "mine" => mining::run_cli(arguments).await,
        "observatory" | "observatories" | "obs" => observatory::run_cli(arguments).await,
        "event" | "events" => event::run_cli(arguments).await,
        "bootstrap" => bootstrap::run_cli(arguments).await,
        "rikers" | "riker" => rikers::run_cli(arguments).await,
        other => Err(app_error(format!("unknown command {other:?}"))),
    }
}

fn normalize_operation_flag(mut arguments: Vec<String>, operations: &[&str]) -> Vec<String> {
    let Some(first) = arguments.first_mut() else {
        return arguments;
    };
    let Some(operation) = first.strip_prefix("--") else {
        return arguments;
    };
    if operations.contains(&operation) {
        *first = operation.to_owned();
    }
    arguments
}

fn print_help() {
    println!(
        "Replicant Space CLI\n\n\
Usage:\n  replicant-cli COMMAND [OPERATION] [OPTIONS]\n\n\
Commands:\n  print       Distributed Autofactory queueing, status, and clearing\n  transport   Point-to-point resource and device delivery\n  trade       Interactive player-run shop directory and trade viewer\n  belt-search Fast Replicant-only system scans for asteroid belts\n  survey      Survey-route planning and execution\n  relay       FTL relay-network expansion\n  mining      Mining-network expansion\n  observatory Galactic Observatory prospecting and triangulation\n  event       Civilisation-event planning and execution\n  bootstrap   Regional bootstrap and landing delivery automation\n  rikers      Local Riker colony-candidate report\n\n\
Operation syntax:\n  Stateful commands accept either an operation word or its flag form.\n  For example, `survey plan ...` and `survey --plan ...` are equivalent.\n\n\
Examples:\n  replicant-cli print --status --system SCEPTURUM \\\n    --print 17 exotic_matter_injector --tag twaffy-ring-001\n\n  replicant-cli trade --replicant Chats-1\n\n  replicant-cli belt-search SOL YINU MENKUNT\n\n  replicant-cli survey --plan --replicant B7AF4A8C \\\n    --vessel 6592B774 --center THYFFAWFF --radius 30\n\n  replicant-cli observatory triangulate --all\n\n  replicant-cli bootstrap --deliver --landing-star LUMBUNGA \\\n    --mission-file bootstrap-lumbunga.json \\\n    --log-file logs/bootstrap-lumbunga.log\n\n\
Help:\n  replicant-cli help COMMAND\n  replicant-cli COMMAND --help\n  -h, --help       Show this help\n  -V, --version    Show version"
    );
}
