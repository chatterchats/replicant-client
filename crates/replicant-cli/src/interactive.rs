use std::{
    collections::BTreeSet,
    env,
    io::{self, Write},
};

use replicant_client::raw::{Client as RawClient, SecretString};

use crate::{AnyResult, app_error};

const TOP_LEVEL_COMMANDS: &[(&str, &str)] = &[
    ("print", "Distributed Autofactory printing"),
    ("transport", "Point-to-point device/resource delivery"),
    ("trade", "Player-run shop directory and trade viewer"),
    ("belt-search", "Fast system scans for asteroid belts"),
    ("survey", "Survey-route planning and execution"),
    ("relay", "FTL relay-network expansion"),
    ("mining", "Mining-network expansion"),
    ("observatory", "Galactic Observatory operations"),
    ("event", "Civilisation-event planning and execution"),
    ("bootstrap", "Regional bootstrap / landing delivery"),
    ("rikers", "Riker colony-candidate report"),
];

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Invocation {
    pub(crate) command: String,
    pub(crate) arguments: Vec<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SmartKind {
    System,
    Location,
}

#[derive(Clone, Copy)]
enum ValueKind {
    Text,
    System,
    Location,
    Choice(&'static [&'static str]),
}

#[derive(Clone, Copy)]
enum OptionInput {
    Flag,
    One {
        label: &'static str,
        kind: ValueKind,
    },
    Two {
        first_label: &'static str,
        first_kind: ValueKind,
        second_label: &'static str,
        second_kind: ValueKind,
    },
    Carrier,
}

#[derive(Clone, Copy)]
struct OptionPrompt {
    flag: &'static str,
    description: &'static str,
    input: OptionInput,
    repeatable: bool,
}

impl OptionPrompt {
    const fn flag(flag: &'static str, description: &'static str) -> Self {
        Self {
            flag,
            description,
            input: OptionInput::Flag,
            repeatable: false,
        }
    }

    const fn value(
        flag: &'static str,
        description: &'static str,
        label: &'static str,
        kind: ValueKind,
    ) -> Self {
        Self {
            flag,
            description,
            input: OptionInput::One { label, kind },
            repeatable: false,
        }
    }

    const fn repeat_value(
        flag: &'static str,
        description: &'static str,
        label: &'static str,
        kind: ValueKind,
    ) -> Self {
        Self {
            flag,
            description,
            input: OptionInput::One { label, kind },
            repeatable: true,
        }
    }

    const fn pair(
        flag: &'static str,
        description: &'static str,
        first_label: &'static str,
        first_kind: ValueKind,
        second_label: &'static str,
        second_kind: ValueKind,
    ) -> Self {
        Self {
            flag,
            description,
            input: OptionInput::Two {
                first_label,
                first_kind,
                second_label,
                second_kind,
            },
            repeatable: true,
        }
    }
}

#[derive(Default)]
struct SmartResolver {
    index: Option<SmartIndex>,
    attempted: bool,
}

#[derive(Default)]
struct SmartIndex {
    systems: Vec<String>,
    locations: Vec<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CandidateKind {
    System,
    Location,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Candidate {
    value: String,
    kind: CandidateKind,
}

pub(crate) async fn build_invocation(arguments: Vec<String>) -> AnyResult<Option<Invocation>> {
    if arguments
        .first()
        .is_some_and(|value| matches!(value.as_str(), "-h" | "--help" | "help"))
    {
        print_help();
        return Ok(None);
    }

    println!("Replicant Space interactive CLI");
    println!(
        "Builds a normal `replicant-cli` command, previews it, then runs the existing handler.\n"
    );

    let (command, operation) = select_command_and_operation(arguments)?;
    let mut resolver = SmartResolver::default();
    let mut argv = Vec::new();
    if let Some(operation) = operation {
        argv.push(operation.to_owned());
    }

    collect_required_arguments(&command, operation, &mut argv, &mut resolver).await?;
    let options = options_for(&command, operation);
    let protected_len = argv.len();

    loop {
        println!("\nCurrent command:\n  {}", render_command(&command, &argv));
        let used = used_flags(&argv);
        let available = options
            .iter()
            .filter(|option| option.repeatable || !used.contains(option.flag))
            .copied()
            .collect::<Vec<_>>();

        println!("\nOptions:");
        println!("  0. Run this command");
        for (index, option) in available.iter().enumerate() {
            let suffix = if option.repeatable {
                " (repeatable)"
            } else {
                ""
            };
            println!(
                "  {}. {:<24} {}{}",
                index + 1,
                option.flag,
                option.description,
                suffix
            );
        }
        let remove_index = available.len() + 1;
        let other_index = available.len() + 2;
        let cancel_index = available.len() + 3;
        println!("  {remove_index}. Remove last added argument/option");
        println!("  {other_index}. Add another/raw CLI option");
        println!("  {cancel_index}. Cancel");

        let choice = prompt_usize("Select")?;
        if choice == 0 {
            if let Some(message) = missing_required_argument(&command, operation, &argv) {
                println!("Cannot run yet: {message}");
                continue;
            }
            println!("\nReady to run:\n  {}", render_command(&command, &argv));
            if prompt_yes_no("Run it now?", true)? {
                return Ok(Some(Invocation {
                    command,
                    arguments: argv,
                }));
            }
            continue;
        }
        if choice == remove_index {
            if argv.len() <= protected_len {
                println!("Nothing optional has been added yet.");
            } else {
                remove_last_option(&mut argv, protected_len);
            }
            continue;
        }
        if choice == other_index {
            add_raw_option(&mut argv, &mut resolver).await?;
            continue;
        }
        if choice == cancel_index {
            return Ok(None);
        }
        let Some(option) = choice.checked_sub(1).and_then(|index| available.get(index)) else {
            println!("Invalid selection.");
            continue;
        };
        append_option(&mut argv, *option, &mut resolver).await?;
    }
}

fn select_command_and_operation(
    mut arguments: Vec<String>,
) -> AnyResult<(String, Option<&'static str>)> {
    let command = if let Some(command) = arguments.first().cloned() {
        if !TOP_LEVEL_COMMANDS
            .iter()
            .any(|(candidate, _)| candidate == &command)
        {
            return Err(app_error(format!(
                "unknown interactive command {command:?}; omit it to choose from the menu"
            )));
        }
        arguments.remove(0);
        command
    } else {
        println!("Choose a command:");
        for (index, (command, description)) in TOP_LEVEL_COMMANDS.iter().enumerate() {
            println!("  {}. {:<12} {description}", index + 1, command);
        }
        let selection = choose_index(TOP_LEVEL_COMMANDS.len(), "Command")?;
        TOP_LEVEL_COMMANDS[selection].0.to_owned()
    };

    let operations = operations_for(&command);
    let operation = if operations.is_empty() {
        if !arguments.is_empty() {
            return Err(app_error(format!(
                "{command} has no operation selector; unexpected {:?}",
                arguments[0]
            )));
        }
        None
    } else if let Some(operation) = arguments.first() {
        let Some(operation) = operations
            .iter()
            .copied()
            .find(|candidate| candidate.eq_ignore_ascii_case(operation))
        else {
            return Err(app_error(format!(
                "unknown {command} operation {operation:?}; expected one of {}",
                operations.join(", ")
            )));
        };
        Some(operation)
    } else {
        println!("\nChoose a {command} operation:");
        for (index, operation) in operations.iter().enumerate() {
            println!("  {}. {operation}", index + 1);
        }
        Some(operations[choose_index(operations.len(), "Operation")?])
    };

    Ok((command, operation))
}

fn operations_for(command: &str) -> &'static [&'static str] {
    match command {
        "print" => &["queue", "clear", "status"],
        "trade" => &["interactive", "list", "show"],
        "survey" => &["plan", "run", "status"],
        "relay" => &["plan", "run", "status"],
        "mining" => &["plan", "run", "status"],
        "observatory" => &["status", "prospect", "triangulate"],
        "event" => &["interactive", "list", "plan", "run", "status"],
        "bootstrap" => &["plan", "stage", "deliver", "run", "status"],
        _ => &[],
    }
}

async fn collect_required_arguments(
    command: &str,
    operation: Option<&str>,
    argv: &mut Vec<String>,
    resolver: &mut SmartResolver,
) -> AnyResult<()> {
    match (command, operation) {
        ("transport", _) => {
            argv.push("--origin".to_owned());
            argv.push(
                resolver
                    .prompt(SmartKind::Location, "Origin location/system")
                    .await?,
            );
            argv.push("--destination".to_owned());
            argv.push(
                resolver
                    .prompt(SmartKind::Location, "Destination location/system")
                    .await?,
            );
        }
        ("belt-search", _) => {
            println!("\nAdd one or more systems to scan.");
            loop {
                argv.push(resolver.prompt(SmartKind::System, "System").await?);
                if !prompt_yes_no("Add another system?", false)? {
                    break;
                }
            }
        }
        ("relay", Some("plan")) => {
            println!("\nAdd one or more target systems for the relay network.");
            loop {
                argv.push(resolver.prompt(SmartKind::System, "Target system").await?);
                if !prompt_yes_no("Add another target system?", false)? {
                    break;
                }
            }
        }
        ("mining", Some("plan")) => {
            if prompt_yes_no("Specify mining systems now?", false)? {
                loop {
                    argv.push(resolver.prompt(SmartKind::System, "Mining system").await?);
                    if !prompt_yes_no("Add another mining system?", false)? {
                        break;
                    }
                }
            }
        }
        ("trade", Some("show")) => {
            argv.push(prompt_nonempty("Trade controller code")?);
        }
        _ => {}
    }
    Ok(())
}

fn options_for(command: &str, operation: Option<&str>) -> Vec<OptionPrompt> {
    let database = OptionPrompt::value(
        "--database",
        "Managed SQLite database",
        "Path",
        ValueKind::Text,
    );
    let verbose = OptionPrompt::flag("--verbose", "Show tracing logs in the terminal");
    let log_file = OptionPrompt::value(
        "--log-file",
        "Append tracing logs to this file",
        "Path",
        ValueKind::Text,
    );
    let json = OptionPrompt::flag("--json", "Emit machine-readable output");

    match (command, operation) {
        ("print", Some("queue")) => vec![
            OptionPrompt::pair(
                "--print",
                "Requested device quantity/type",
                "Quantity",
                ValueKind::Text,
                "Device type",
                ValueKind::Text,
            ),
            OptionPrompt::value("--hub", "Autofactory hub", "Location", ValueKind::Location),
            OptionPrompt::repeat_value(
                "--tag",
                "Tag printed devices/prerequisites",
                "Tag",
                ValueKind::Text,
            ),
            OptionPrompt::flag("--flatpack", "Print modular devices compacted"),
            database,
            timeout(),
            poll(),
            verbose,
            log_file,
            json,
        ],
        ("print", Some("clear")) => vec![
            OptionPrompt::value(
                "--system",
                "System whose Autofactories should be cleared",
                "System",
                ValueKind::System,
            ),
            OptionPrompt::value(
                "--hub",
                "Derive clear system from this hub",
                "Location",
                ValueKind::Location,
            ),
            OptionPrompt::repeat_value(
                "--exclude-active",
                "Preserve this factory's active print",
                "Autofactory code",
                ValueKind::Text,
            ),
            database,
            timeout(),
            poll(),
            verbose,
            log_file,
            json,
        ],
        ("print", Some("status")) => vec![
            OptionPrompt::value("--system", "System to inspect", "System", ValueKind::System),
            OptionPrompt::value(
                "--hub",
                "Derive status system from this hub",
                "Location",
                ValueKind::Location,
            ),
            OptionPrompt::pair(
                "--print",
                "Compare with desired quantity/type",
                "Quantity",
                ValueKind::Text,
                "Device type",
                ValueKind::Text,
            ),
            OptionPrompt::repeat_value("--tag", "Filter by tag", "Tag", ValueKind::Text),
            database,
            timeout(),
            poll(),
            verbose,
            log_file,
            json,
        ],
        ("transport", _) => vec![
            OptionPrompt::pair(
                "--device",
                "Move device quantity/type",
                "Quantity",
                ValueKind::Text,
                "Device type",
                ValueKind::Text,
            ),
            OptionPrompt::repeat_value(
                "--device-tag",
                "Move devices matching tag",
                "Tag",
                ValueKind::Text,
            ),
            OptionPrompt::pair(
                "--resource",
                "Move resource quantity/type",
                "Quantity",
                ValueKind::Text,
                "Resource",
                ValueKind::Text,
            ),
            OptionPrompt::value("--carbon", "Move carbon", "Quantity", ValueKind::Text),
            OptionPrompt::value(
                "--conductive",
                "Move conductive",
                "Quantity",
                ValueKind::Text,
            ),
            OptionPrompt::value("--rares", "Move rares", "Quantity", ValueKind::Text),
            OptionPrompt::value("--silicates", "Move silicates", "Quantity", ValueKind::Text),
            OptionPrompt::value(
                "--structural",
                "Move structural",
                "Quantity",
                ValueKind::Text,
            ),
            OptionPrompt::value("--volatiles", "Move volatiles", "Quantity", ValueKind::Text),
            OptionPrompt {
                flag: "--carrier",
                description: "Carrier type, optionally with count",
                input: OptionInput::Carrier,
                repeatable: false,
            },
            OptionPrompt::flag("--return-carriers", "Return carriers after delivery"),
            OptionPrompt::flag(
                "--no-unfurl",
                "Do not unfurl modular payload at destination",
            ),
            OptionPrompt::flag("--dry-run", "Plan without mutations"),
            database,
            timeout(),
            poll(),
            verbose,
            log_file,
            json,
        ],
        ("trade", Some("interactive")) | ("trade", Some("list")) | ("trade", Some("show")) => vec![
            OptionPrompt::value(
                "--replicant",
                "Replicant name/code",
                "Replicant",
                ValueKind::Text,
            ),
            database,
            OptionPrompt::flag("--no-color", "Disable ANSI colors"),
            OptionPrompt::flag("--no-clear", "Do not clear the terminal between views"),
        ],
        ("belt-search", _) => vec![
            OptionPrompt::value(
                "--replicant",
                "Replicant name/code",
                "Replicant",
                ValueKind::Text,
            ),
            OptionPrompt::value(
                "--systems-file",
                "Read additional systems from file",
                "Path",
                ValueKind::Text,
            ),
            database,
            timeout(),
            log_file,
            verbose,
        ],
        ("survey", Some(_)) => vec![
            database,
            OptionPrompt::value(
                "--replicant",
                "Survey replicant",
                "Replicant",
                ValueKind::Text,
            ),
            OptionPrompt::value("--vessel", "Survey vessel", "Device code", ValueKind::Text),
            OptionPrompt::value(
                "--center",
                "Survey center system",
                "System",
                ValueKind::System,
            ),
            OptionPrompt::value(
                "--radius",
                "Survey radius in ly",
                "Light years",
                ValueKind::Text,
            ),
            OptionPrompt::value(
                "--system-limit",
                "Maximum systems",
                "Count",
                ValueKind::Text,
            ),
            OptionPrompt::value(
                "--star-detail-concurrency",
                "Concurrent star detail reads",
                "Count",
                ValueKind::Text,
            ),
            OptionPrompt::value(
                "--mission-file",
                "Durable survey mission",
                "Path",
                ValueKind::Text,
            ),
            OptionPrompt::value(
                "--controller",
                "Survey controller override",
                "Device code",
                ValueKind::Text,
            ),
            OptionPrompt::value(
                "--drones",
                "Survey drone codes (comma-separated)",
                "Codes",
                ValueKind::Text,
            ),
            OptionPrompt::flag("--replace-plan", "Replace/rebuild an incomplete plan"),
            OptionPrompt::flag("--include-explored", "Include already explored systems"),
            OptionPrompt::value(
                "--travel-timeout-secs",
                "Travel timeout",
                "Seconds",
                ValueKind::Text,
            ),
            OptionPrompt::value(
                "--survey-timeout-secs",
                "Survey timeout",
                "Seconds",
                ValueKind::Text,
            ),
            OptionPrompt::value(
                "--maintenance-home",
                "Maintenance return location",
                "Location",
                ValueKind::Location,
            ),
            OptionPrompt::value(
                "--maintenance-interval-systems",
                "Maintenance check interval",
                "Systems",
                ValueKind::Text,
            ),
            OptionPrompt::value(
                "--maintenance-threshold-pct",
                "Recall below this capacity",
                "Percent",
                ValueKind::Text,
            ),
            OptionPrompt::value(
                "--maintenance-resume-pct",
                "Resume above this capacity",
                "Percent",
                ValueKind::Text,
            ),
            OptionPrompt::value(
                "--maintenance-check-secs",
                "Maintenance polling interval",
                "Seconds",
                ValueKind::Text,
            ),
            verbose,
            log_file,
        ],
        ("relay", Some(operation)) => {
            let mut options = vec![
                OptionPrompt::value(
                    "--replicant",
                    "Replicant name/code",
                    "Replicant",
                    ValueKind::Text,
                ),
                OptionPrompt::value(
                    "--hub",
                    "Manufacturing hub",
                    "Location",
                    ValueKind::Location,
                ),
                OptionPrompt::value("--plan", "Relay plan file", "Path", ValueKind::Text),
                database,
                OptionPrompt::value(
                    "--wait-timeout-secs",
                    "Stage wait timeout",
                    "Seconds",
                    ValueKind::Text,
                ),
                verbose,
                log_file,
            ];
            if operation == "plan" {
                let mut planning = vec![
                    OptionPrompt::flag("--replace-plan", "Replace/rebuild an existing plan"),
                    OptionPrompt::flag(
                        "--reuse-account-relays",
                        "Reuse account-wide active relays",
                    ),
                    OptionPrompt::repeat_value(
                        "--ignore-printer",
                        "Exclude Autofactory from relay printing",
                        "Autofactory code(s)",
                        ValueKind::Text,
                    ),
                    OptionPrompt::value(
                        "--supply-strategy",
                        "Relay supply strategy",
                        "Strategy",
                        ValueKind::Choice(&["auto", "staged", "minimal", "hub"]),
                    ),
                    OptionPrompt::value(
                        "--max-hop",
                        "Range of newly deployed relays",
                        "Light years",
                        ValueKind::Text,
                    ),
                ];
                planning.extend(options);
                options = planning;
            }
            options
        }
        ("mining", Some(operation)) => {
            let mut options = vec![
                OptionPrompt::value(
                    "--replicant",
                    "Replicant name/code",
                    "Replicant",
                    ValueKind::Text,
                ),
                OptionPrompt::value(
                    "--hub",
                    "Manufacturing hub",
                    "Location",
                    ValueKind::Location,
                ),
                database,
                OptionPrompt::value(
                    "--mission-file",
                    "Durable mining mission",
                    "Path",
                    ValueKind::Text,
                ),
                OptionPrompt::value(
                    "--wait-timeout-secs",
                    "Stage wait timeout",
                    "Seconds",
                    ValueKind::Text,
                ),
                OptionPrompt::value(
                    "--max-concurrency",
                    "Concurrent dispatch limit",
                    "Count",
                    ValueKind::Text,
                ),
                verbose,
                log_file,
                json,
            ];
            if operation == "plan" {
                let mut planning = vec![
                    OptionPrompt::repeat_value(
                        "--system",
                        "Add mining system",
                        "System",
                        ValueKind::System,
                    ),
                    OptionPrompt::value(
                        "--systems-file",
                        "Read systems from file",
                        "Path",
                        ValueKind::Text,
                    ),
                    OptionPrompt::flag("--replace-plan", "Replace/rebuild an existing plan"),
                ];
                planning.extend(options);
                options = planning;
            }
            options
        }
        ("observatory", Some("status")) => observatory_selector_options(database),
        ("observatory", Some("prospect")) => {
            let mut options = observatory_selector_options(database);
            options.extend([
                OptionPrompt::value(
                    "--direction",
                    "Prospecting direction",
                    "Direction",
                    ValueKind::Choice(&[
                        "auto",
                        "outward",
                        "away-sol",
                        "toward-sol",
                        "toward-star",
                        "away-star",
                        "+x",
                        "-x",
                        "+y",
                        "-y",
                        "+z",
                        "-z",
                    ]),
                ),
                OptionPrompt::value(
                    "--star",
                    "Reference star for toward/away-star",
                    "System",
                    ValueKind::System,
                ),
                OptionPrompt::value(
                    "--vector",
                    "Explicit direction vector",
                    "X,Y,Z",
                    ValueKind::Text,
                ),
                OptionPrompt::value(
                    "--analysis-radius",
                    "Automatic direction analysis radius",
                    "Light years",
                    ValueKind::Text,
                ),
                OptionPrompt::value(
                    "--samples",
                    "Automatic direction sample count",
                    "Count",
                    ValueKind::Text,
                ),
                OptionPrompt::value(
                    "--attempts",
                    "Blocked-direction retry attempts",
                    "Count",
                    ValueKind::Text,
                ),
                OptionPrompt::flag("--dry-run", "Show selected direction without submitting"),
            ]);
            options
        }
        ("observatory", Some("triangulate")) => {
            let mut options = observatory_selector_options(database);
            options.extend([
                OptionPrompt::value(
                    "--signature",
                    "Triangulation signature",
                    "Signature",
                    ValueKind::Text,
                ),
                OptionPrompt::value(
                    "--target",
                    "Explicit deep-space target",
                    "X,Y,Z",
                    ValueKind::Text,
                ),
                OptionPrompt::value(
                    "--radius",
                    "Automatic target radius",
                    "Light years",
                    ValueKind::Text,
                ),
                OptionPrompt::value(
                    "--seed",
                    "Deterministic target-spread seed",
                    "Seed",
                    ValueKind::Text,
                ),
                OptionPrompt::flag("--dry-run", "Show targets without submitting"),
            ]);
            options
        }
        ("event", Some(operation)) => {
            let mut options = vec![
                OptionPrompt::value(
                    "--replicant",
                    "Replicant name/code",
                    "Replicant",
                    ValueKind::Text,
                ),
                OptionPrompt::value(
                    "--home",
                    "Home manufacturing location",
                    "Location",
                    ValueKind::Location,
                ),
                database,
                OptionPrompt::value(
                    "--plan-file",
                    "Event campaign plan",
                    "Path",
                    ValueKind::Text,
                ),
                OptionPrompt::value(
                    "--region",
                    "Limit events to region",
                    "Region",
                    ValueKind::Text,
                ),
                OptionPrompt::value(
                    "--center",
                    "Radius center system",
                    "System",
                    ValueKind::System,
                ),
                OptionPrompt::value(
                    "--radius",
                    "Radius around --center",
                    "Light years",
                    ValueKind::Text,
                ),
                OptionPrompt::value(
                    "--wait-timeout-secs",
                    "Stage wait timeout",
                    "Seconds",
                    ValueKind::Text,
                ),
                verbose,
                log_file,
                json,
            ];
            if operation == "plan" {
                let mut planning = vec![
                    OptionPrompt::value(
                        "--event",
                        "Specific event designation",
                        "Event",
                        ValueKind::Text,
                    ),
                    OptionPrompt::value(
                        "--criterion",
                        "Event selection criterion",
                        "Criterion",
                        ValueKind::Text,
                    ),
                    OptionPrompt::flag("--all", "Plan every matching event"),
                    OptionPrompt::flag("--replace-plan", "Replace/replan incomplete campaign"),
                ];
                planning.extend(options);
                options = planning;
            }
            options
        }
        ("bootstrap", Some(_)) => vec![
            OptionPrompt::value(
                "--landing-star",
                "Ark landing star; region inferred",
                "System",
                ValueKind::System,
            ),
            OptionPrompt::value(
                "--region",
                "Optional beta/gamma constraint/default",
                "Region",
                ValueKind::Choice(&["beta", "gamma"]),
            ),
            OptionPrompt::value(
                "--source-hub",
                "Source manufacturing hub",
                "Location",
                ValueKind::Location,
            ),
            OptionPrompt::value(
                "--operator",
                "Capital/operator replicant",
                "Replicant",
                ValueKind::Text,
            ),
            OptionPrompt::value(
                "--explorer",
                "Explorer replicant",
                "Replicant",
                ValueKind::Text,
            ),
            OptionPrompt::value(
                "--mission-file",
                "Durable bootstrap mission",
                "Path",
                ValueKind::Text,
            ),
            database,
            OptionPrompt::value(
                "--mining-setups",
                "Initial complete mining setups",
                "Count",
                ValueKind::Text,
            ),
            OptionPrompt::value(
                "--autofactories",
                "Regional Autofactories",
                "Count",
                ValueKind::Text,
            ),
            OptionPrompt::value(
                "--freighters",
                "Seed/route freighters",
                "Count",
                ValueKind::Text,
            ),
            OptionPrompt::value(
                "--transport-controllers",
                "AMI transport controllers",
                "Count",
                ValueKind::Text,
            ),
            OptionPrompt::value(
                "--seed-quantity",
                "Resources per seed freighter",
                "Quantity",
                ValueKind::Text,
            ),
            OptionPrompt::value(
                "--quick-scout-radius",
                "Dense-belt quick-scout radius",
                "Light years",
                ValueKind::Text,
            ),
            OptionPrompt::value(
                "--survey-radius",
                "Regional survey radius",
                "Light years",
                ValueKind::Text,
            ),
            OptionPrompt::value(
                "--min-sites",
                "Minimum mining systems",
                "Count",
                ValueKind::Text,
            ),
            OptionPrompt::value(
                "--max-sites",
                "Maximum mining systems",
                "Count",
                ValueKind::Text,
            ),
            OptionPrompt::value(
                "--max-concurrency",
                "Concurrent dispatch limit",
                "Count",
                ValueKind::Text,
            ),
            OptionPrompt::value(
                "--wait-timeout-secs",
                "Stage wait timeout",
                "Seconds",
                ValueKind::Text,
            ),
            OptionPrompt::flag("--replace-plan", "Replace an incomplete plan"),
            verbose,
            log_file,
            json,
        ],
        ("rikers", _) => vec![
            database,
            OptionPrompt::value("--limit", "Maximum candidates", "Count", ValueKind::Text),
            OptionPrompt::flag("--no-diagnostics", "Hide staged local-query diagnostics"),
        ],
        _ => Vec::new(),
    }
}

fn observatory_selector_options(database: OptionPrompt) -> Vec<OptionPrompt> {
    vec![
        OptionPrompt::repeat_value(
            "--observatory",
            "Select Observatory by device code",
            "Device code",
            ValueKind::Text,
        ),
        OptionPrompt::flag("--all", "Use all matching Observatories"),
        OptionPrompt::value("--tag", "Select Observatory by tag", "Tag", ValueKind::Text),
        database,
    ]
}

const fn timeout() -> OptionPrompt {
    OptionPrompt::value(
        "--wait-timeout-secs",
        "Wait timeout",
        "Seconds",
        ValueKind::Text,
    )
}

const fn poll() -> OptionPrompt {
    OptionPrompt::value(
        "--poll-seconds",
        "State polling interval",
        "Seconds",
        ValueKind::Text,
    )
}

async fn append_option(
    argv: &mut Vec<String>,
    option: OptionPrompt,
    resolver: &mut SmartResolver,
) -> AnyResult<()> {
    argv.push(option.flag.to_owned());
    match option.input {
        OptionInput::Flag => {}
        OptionInput::One { label, kind } => argv.push(prompt_value(label, kind, resolver).await?),
        OptionInput::Two {
            first_label,
            first_kind,
            second_label,
            second_kind,
        } => {
            argv.push(prompt_value(first_label, first_kind, resolver).await?);
            argv.push(prompt_value(second_label, second_kind, resolver).await?);
        }
        OptionInput::Carrier => {
            let count = prompt_text("Carrier count (blank for 1)")?;
            if !count.trim().is_empty() {
                argv.push(count.trim().to_owned());
            }
            argv.push(prompt_nonempty("Carrier device type")?);
        }
    }
    Ok(())
}

async fn prompt_value(
    label: &str,
    kind: ValueKind,
    resolver: &mut SmartResolver,
) -> AnyResult<String> {
    match kind {
        ValueKind::Text => prompt_nonempty(label),
        ValueKind::System => resolver.prompt(SmartKind::System, label).await,
        ValueKind::Location => resolver.prompt(SmartKind::Location, label).await,
        ValueKind::Choice(values) => {
            println!("{label}:");
            for (index, value) in values.iter().enumerate() {
                println!("  {}. {value}", index + 1);
            }
            Ok(values[choose_index(values.len(), label)?].to_owned())
        }
    }
}

async fn add_raw_option(argv: &mut Vec<String>, resolver: &mut SmartResolver) -> AnyResult<()> {
    let flag = prompt_nonempty("Option/argument")?;
    if !flag.starts_with('-') {
        println!("Adding positional argument {flag:?}.");
        argv.push(flag);
        return Ok(());
    }

    argv.push(flag);
    let value_count = loop {
        let raw = prompt_text("How many values does this option take? [0/1/2]")?;
        match raw.trim() {
            "0" => break 0,
            "1" | "" => break 1,
            "2" => break 2,
            _ => println!("Enter 0, 1, or 2."),
        }
    };
    for index in 0..value_count {
        println!("Value {} type:", index + 1);
        println!("  1. Text");
        println!("  2. SYSTEM (smart suggestions)");
        println!("  3. LOCATION (smart suggestions)");
        let kind = match choose_index(3, "Type")? {
            0 => ValueKind::Text,
            1 => ValueKind::System,
            _ => ValueKind::Location,
        };
        argv.push(prompt_value("Value", kind, resolver).await?);
    }
    Ok(())
}

fn remove_last_option(argv: &mut Vec<String>, protected_len: usize) {
    if argv.len() <= protected_len {
        return;
    }
    let mut start = argv.len() - 1;
    while start > protected_len && !argv[start].starts_with('-') {
        start -= 1;
    }
    if start == protected_len && !argv[start].starts_with('-') {
        start = argv.len() - 1;
    }
    let removed = argv.drain(start..).collect::<Vec<_>>();
    println!("Removed: {}", removed.join(" "));
}

fn missing_required_argument(
    command: &str,
    operation: Option<&str>,
    argv: &[String],
) -> Option<&'static str> {
    match (command, operation) {
        ("print", Some("queue")) if !argv.iter().any(|value| value == "--print") => {
            Some("print queue requires at least one --print QUANTITY DEVICE_TYPE request")
        }
        ("transport", _)
            if !argv.iter().any(|value| {
                matches!(
                    value.as_str(),
                    "--device"
                        | "--devices"
                        | "--device-tag"
                        | "--resource"
                        | "--carbon"
                        | "--conductive"
                        | "--rares"
                        | "--rare"
                        | "--silicates"
                        | "--silicate"
                        | "--structural"
                        | "--volatiles"
                        | "--volatile"
                )
            }) =>
        {
            Some("transport requires at least one device, device-tag, or resource payload")
        }
        _ => None,
    }
}

fn used_flags(argv: &[String]) -> BTreeSet<&str> {
    argv.iter()
        .filter(|value| value.starts_with("--"))
        .map(String::as_str)
        .collect()
}

impl SmartResolver {
    async fn prompt(&mut self, kind: SmartKind, label: &str) -> AnyResult<String> {
        if !self.attempted {
            self.attempted = true;
            match SmartIndex::load().await {
                Ok(index) => self.index = Some(index),
                Err(error) => {
                    eprintln!("Smart SYSTEM/LOCATION suggestions unavailable: {error}");
                    eprintln!("Manual entry will still work.\n");
                }
            }
        }

        let Some(index) = &self.index else {
            return Ok(prompt_nonempty(label)?.trim().to_ascii_uppercase());
        };

        loop {
            let query = prompt_nonempty(&format!("{label} (type part of the name)"))?;
            let query = query.trim().to_ascii_uppercase();
            let candidates = index.candidates(kind);
            if let Some(exact) = candidates
                .iter()
                .find(|candidate| candidate.value.eq_ignore_ascii_case(&query))
            {
                return Ok(exact.value.clone());
            }

            let matches = rank_candidates(&query, &candidates, 8);
            if matches.is_empty() {
                println!("No catalogue match for {query:?}.");
                if prompt_yes_no(&format!("Use {query} exactly?"), true)? {
                    return Ok(query);
                }
                continue;
            }

            println!("Suggestions:");
            for (index, candidate) in matches.iter().enumerate() {
                let kind = match candidate.kind {
                    CandidateKind::System => "system",
                    CandidateKind::Location => "location",
                };
                println!("  {}. {:<32} [{kind}]", index + 1, candidate.value);
            }
            println!("  0. Use {query} exactly");
            println!("  r. Re-enter search");
            let selection = prompt_text("Selection [1]")?;
            let selection = selection.trim();
            if selection.is_empty() {
                return Ok(matches[0].value.clone());
            }
            if selection.eq_ignore_ascii_case("r") {
                continue;
            }
            if selection == "0" {
                return Ok(query);
            }
            if let Ok(number) = selection.parse::<usize>()
                && (1..=matches.len()).contains(&number)
            {
                return Ok(matches[number - 1].value.clone());
            }
            println!("Invalid selection.");
        }
    }
}

impl SmartIndex {
    async fn load() -> AnyResult<Self> {
        let token = env::var("RS_API_TOKEN")
            .map(SecretString::from)
            .map_err(|_| app_error("RS_API_TOKEN is not set"))?;
        let client = RawClient::builder().authentication_token(token).build()?;
        let galaxy = client.galaxy();
        let locations = client.locations();
        let (catalogue, location_map) = tokio::join!(galaxy.catalogue(), locations.system_map());
        let catalogue = catalogue?;
        let location_map = location_map?;

        let mut systems = BTreeSet::new();
        let mut locations = BTreeSet::new();
        for star in catalogue.value.stars {
            if let Some(designation) = star.designation {
                systems.insert(designation.trim().to_ascii_uppercase());
            }
            if let Some(entry_point) = star.entry_point {
                locations.insert(entry_point.trim().to_ascii_uppercase());
            }
        }
        for location in location_map.value.locations.into_keys() {
            locations.insert(location.trim().to_ascii_uppercase());
        }

        Ok(Self {
            systems: systems.into_iter().collect(),
            locations: locations.into_iter().collect(),
        })
    }

    fn candidates(&self, kind: SmartKind) -> Vec<Candidate> {
        let mut candidates = self
            .systems
            .iter()
            .map(|value| Candidate {
                value: value.clone(),
                kind: CandidateKind::System,
            })
            .collect::<Vec<_>>();
        if kind == SmartKind::Location {
            candidates.extend(self.locations.iter().map(|value| Candidate {
                value: value.clone(),
                kind: CandidateKind::Location,
            }));
        }
        candidates.sort_by(|left, right| left.value.cmp(&right.value));
        candidates.dedup_by(|left, right| left.value == right.value);
        candidates
    }
}

fn rank_candidates(query: &str, candidates: &[Candidate], limit: usize) -> Vec<Candidate> {
    let query = query.trim().to_ascii_uppercase();
    if query.is_empty() {
        return Vec::new();
    }
    let mut scored = candidates
        .iter()
        .filter_map(|candidate| {
            match_score(&query, &candidate.value).map(|score| (score, candidate.clone()))
        })
        .collect::<Vec<_>>();
    scored.sort_by(|(left_score, left), (right_score, right)| {
        left_score
            .cmp(right_score)
            .then_with(|| left.value.len().cmp(&right.value.len()))
            .then_with(|| left.value.cmp(&right.value))
    });
    scored
        .into_iter()
        .take(limit)
        .map(|(_, candidate)| candidate)
        .collect()
}

fn match_score(query: &str, candidate: &str) -> Option<usize> {
    let candidate = candidate.to_ascii_uppercase();
    if candidate == query {
        return Some(0);
    }
    if candidate.starts_with(query) {
        return Some(10 + candidate.len().saturating_sub(query.len()));
    }
    if let Some(position) = candidate.find(query) {
        return Some(30 + position + candidate.len().saturating_sub(query.len()));
    }

    let distance = levenshtein(query.as_bytes(), candidate.as_bytes());
    let allowed = 2usize.max(query.len() / 3);
    if distance <= allowed {
        return Some(60 + distance * 5 + candidate.len().abs_diff(query.len()));
    }

    subsequence_gap_score(query, &candidate).map(|gaps| 100 + gaps)
}

fn levenshtein(left: &[u8], right: &[u8]) -> usize {
    let mut previous = (0..=right.len()).collect::<Vec<_>>();
    let mut current = vec![0; right.len() + 1];
    for (left_index, left_byte) in left.iter().enumerate() {
        current[0] = left_index + 1;
        for (right_index, right_byte) in right.iter().enumerate() {
            let substitution = previous[right_index] + if left_byte == right_byte { 0 } else { 1 };
            current[right_index + 1] = (current[right_index] + 1)
                .min(previous[right_index + 1] + 1)
                .min(substitution);
        }
        std::mem::swap(&mut previous, &mut current);
    }
    previous[right.len()]
}

fn subsequence_gap_score(query: &str, candidate: &str) -> Option<usize> {
    let mut position = 0usize;
    let mut gaps = 0usize;
    for byte in query.bytes() {
        let relative = candidate
            .as_bytes()
            .get(position..)?
            .iter()
            .position(|value| *value == byte)?;
        gaps += relative;
        position += relative + 1;
    }
    Some(gaps + candidate.len().saturating_sub(query.len()))
}

fn choose_index(count: usize, label: &str) -> AnyResult<usize> {
    loop {
        let value = prompt_usize(label)?;
        if (1..=count).contains(&value) {
            return Ok(value - 1);
        }
        println!("Enter a number from 1 to {count}.");
    }
}

fn prompt_usize(label: &str) -> AnyResult<usize> {
    loop {
        let value = prompt_text(label)?;
        match value.trim().parse::<usize>() {
            Ok(value) => return Ok(value),
            Err(_) => println!("Enter a number."),
        }
    }
}

fn prompt_nonempty(label: &str) -> AnyResult<String> {
    loop {
        let value = prompt_text(label)?;
        if !value.trim().is_empty() {
            return Ok(value.trim().to_owned());
        }
        println!("A value is required.");
    }
}

fn prompt_text(label: &str) -> AnyResult<String> {
    print!("{label}: ");
    io::stdout().flush()?;
    let mut line = String::new();
    let read = io::stdin().read_line(&mut line)?;
    if read == 0 {
        return Err(app_error("interactive input closed"));
    }
    Ok(line.trim_end_matches(&['\r', '\n'][..]).to_owned())
}

fn prompt_yes_no(label: &str, default: bool) -> AnyResult<bool> {
    let hint = if default { "[Y/n]" } else { "[y/N]" };
    loop {
        let value = prompt_text(&format!("{label} {hint}"))?;
        match value.trim().to_ascii_lowercase().as_str() {
            "" => return Ok(default),
            "y" | "yes" => return Ok(true),
            "n" | "no" => return Ok(false),
            _ => println!("Enter y or n."),
        }
    }
}

fn render_command(command: &str, arguments: &[String]) -> String {
    let mut parts = vec!["replicant-cli".to_owned(), shell_quote(command)];
    if matches!(command, "trade" | "event")
        && arguments
            .first()
            .is_some_and(|value| value == "interactive")
    {
        parts.extend(arguments.iter().skip(1).map(|value| shell_quote(value)));
    } else {
        parts.extend(arguments.iter().map(|value| shell_quote(value)));
    }
    parts.join(" ")
}

fn shell_quote(value: &str) -> String {
    if value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || b"-._/:,@+=".contains(&byte))
    {
        return value.to_owned();
    }
    format!("'{}'", value.replace('\'', "'\\''"))
}

pub(crate) fn print_help() {
    println!(
        "Interactive command builder\n\n\
Usage:\n  replicant-cli interactive\n  replicant-cli interactive COMMAND [OPERATION]\n\n\
The wizard builds a normal replicant-cli invocation and dispatches through the\n\
same command handler used by non-interactive calls. SYSTEM fields use the live\n\
star catalogue; LOCATION fields use both the star catalogue and galaxy-wide\n\
location map. Partial and typo-tolerant matching is supported, with manual\n\
override when no suggestion is appropriate.\n\n\
Aliases: menu, wizard"
    );
}

pub(crate) fn normalize_invocation(mut invocation: Invocation) -> Invocation {
    if matches!(invocation.command.as_str(), "trade" | "event")
        && invocation
            .arguments
            .first()
            .is_some_and(|value| value == "interactive")
    {
        invocation.arguments.remove(0);
    }
    invocation
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_match_prefers_system_designation() {
        let candidates = vec![
            Candidate {
                value: "SCEPTURUM-BELT-1".to_owned(),
                kind: CandidateKind::Location,
            },
            Candidate {
                value: "SCEPTURUM".to_owned(),
                kind: CandidateKind::System,
            },
        ];
        let ranked = rank_candidates("SCEPT", &candidates, 8);
        assert_eq!(ranked[0].value, "SCEPTURUM");
        assert!(
            ranked
                .iter()
                .any(|candidate| candidate.value == "SCEPTURUM-BELT-1")
        );
    }

    #[test]
    fn typo_match_finds_nearby_system() {
        let candidates = vec![Candidate {
            value: "SCEPTURUM".to_owned(),
            kind: CandidateKind::System,
        }];
        let ranked = rank_candidates("SCEPTRUM", &candidates, 8);
        assert_eq!(ranked[0].value, "SCEPTURUM");
    }

    #[test]
    fn location_candidates_include_systems_and_locations() {
        let index = SmartIndex {
            systems: vec!["SCEPTURUM".to_owned()],
            locations: vec!["SCEPTURUM-BELT-1".to_owned()],
        };
        let candidates = index.candidates(SmartKind::Location);
        assert!(
            candidates
                .iter()
                .any(|candidate| candidate.value == "SCEPTURUM")
        );
        assert!(
            candidates
                .iter()
                .any(|candidate| candidate.value == "SCEPTURUM-BELT-1")
        );
    }

    #[test]
    fn normalize_synthetic_interactive_operation() {
        for command in ["trade", "event"] {
            let invocation = normalize_invocation(Invocation {
                command: command.to_owned(),
                arguments: vec![
                    "interactive".to_owned(),
                    "--replicant".to_owned(),
                    "Chats-1".to_owned(),
                ],
            });
            assert_eq!(invocation.arguments, vec!["--replicant", "Chats-1"]);
        }
    }

    #[test]
    fn shell_preview_quotes_spaces() {
        assert_eq!(shell_quote("hello world"), "'hello world'");
        assert_eq!(shell_quote("SCEPTURUM"), "SCEPTURUM");
    }
}
