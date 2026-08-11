use std::{
    cmp::Ordering,
    env,
    io::{self, IsTerminal, Write},
    path::PathBuf,
};

use replicant_client::{Client, Replicant, SecretString, StartupPolicy, SyncDomain};
use serde::Deserialize;
use serde_json::Value;

const DEFAULT_REPLICANT: &str = "Chats-1";
const DEFAULT_DATABASE: &str = "replicant-client.sqlite";
const DEFAULT_WIDTH: usize = 118;
const MIN_WIDTH: usize = 84;
const MAX_WIDTH: usize = 160;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Command {
    Interactive,
    List,
    Show,
}

#[derive(Clone, Debug)]
struct Config {
    command: Command,
    replicant: Option<String>,
    controller: Option<String>,
    database: PathBuf,
    color: bool,
    clear: bool,
}

impl Config {
    fn from_args_and_env(arguments: impl IntoIterator<Item = String>) -> crate::AnyResult<Self> {
        let mut arguments = arguments.into_iter().peekable();
        let mut command = Command::Interactive;
        let mut replicant = env::var("RS_TRADE_REPLICANT").ok();
        let mut controller = None;
        let mut database = PathBuf::from(
            env::var("REPLICANT_DB").unwrap_or_else(|_| DEFAULT_DATABASE.to_owned()),
        );
        let terminal = io::stdout().is_terminal();
        let mut color = terminal && env::var_os("NO_COLOR").is_none();
        let mut clear = terminal;

        if let Some(first) = arguments.peek().map(String::as_str) {
            match first {
                "list" | "ls" => {
                    command = Command::List;
                    arguments.next();
                }
                "show" | "view" => {
                    command = Command::Show;
                    arguments.next();
                    controller = arguments.next();
                    if controller.is_none() {
                        return Err(app_error("trade show requires a controller code"));
                    }
                }
                _ => {}
            }
        }

        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "-h" | "--help" | "help" => {
                    print_help();
                    std::process::exit(0);
                }
                "--replicant" => {
                    replicant = Some(next_value(&mut arguments, "--replicant")?);
                }
                "--database" | "--db" => {
                    database = PathBuf::from(next_value(&mut arguments, &argument)?);
                }
                "--no-color" => color = false,
                "--no-clear" => clear = false,
                other => {
                    return Err(app_error(format!(
                        "unknown trade option {other:?}; run `replicant-cli trade --help`"
                    )));
                }
            }
        }

        Ok(Self {
            command,
            replicant,
            controller,
            database,
            color,
            clear,
        })
    }
}

fn next_value(
    arguments: &mut impl Iterator<Item = String>,
    option: &str,
) -> crate::AnyResult<String> {
    arguments
        .next()
        .ok_or_else(|| app_error(format!("{option} requires a value")))
}

#[derive(Clone, Debug, Default, Deserialize)]
struct TraderSummary {
    #[serde(default)]
    controller_code: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    is_local: bool,
    #[serde(default)]
    location: Option<String>,
    #[serde(default)]
    owner_name: Option<String>,
    #[serde(default)]
    owner_replicant_code: Option<String>,
    #[serde(default)]
    shop_name: Option<String>,
    #[serde(default)]
    star: Option<String>,
    #[serde(default)]
    total_stock: Option<i64>,
    #[serde(default)]
    trade_count: Option<i64>,
}

impl TraderSummary {
    fn display_name(&self) -> &str {
        self.shop_name.as_deref().unwrap_or("<unnamed shop>")
    }

    fn owner(&self) -> &str {
        self.owner_name.as_deref().unwrap_or("<unknown>")
    }

    fn place(&self) -> &str {
        self.location
            .as_deref()
            .or(self.star.as_deref())
            .unwrap_or("hidden")
    }

    fn matches(&self, needle: &str) -> bool {
        if needle.is_empty() {
            return true;
        }
        let needle = needle.to_ascii_lowercase();
        [
            Some(self.controller_code.as_str()),
            self.shop_name.as_deref(),
            self.owner_name.as_deref(),
            self.owner_replicant_code.as_deref(),
            self.location.as_deref(),
            self.star.as_deref(),
            self.description.as_deref(),
        ]
        .into_iter()
        .flatten()
        .any(|value| value.to_ascii_lowercase().contains(&needle))
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
struct ShopTrade {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    trade_code: String,
    #[serde(default)]
    current_stock: Option<i64>,
    #[serde(default)]
    initial_stock: Option<i64>,
    #[serde(default)]
    criteria: Option<Value>,
    #[serde(default)]
    rewards: Option<Value>,
    #[serde(default)]
    created_at: Option<String>,
}

impl ShopTrade {
    fn display_name(&self) -> &str {
        self.name.as_deref().unwrap_or("<unnamed trade>")
    }

    fn stock(&self) -> String {
        match (self.current_stock, self.initial_stock) {
            (Some(current), Some(initial)) => format!("{current}/{initial}"),
            (Some(current), None) => current.to_string(),
            _ => "?".to_owned(),
        }
    }

    fn matches(&self, needle: &str) -> bool {
        if needle.is_empty() {
            return true;
        }
        let needle = needle.to_ascii_lowercase();
        let give = exchange_summary(self.criteria.as_ref()).to_ascii_lowercase();
        let get = exchange_summary(self.rewards.as_ref()).to_ascii_lowercase();
        self.display_name().to_ascii_lowercase().contains(&needle)
            || self.trade_code.to_ascii_lowercase().contains(&needle)
            || give.contains(&needle)
            || get.contains(&needle)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ShopLoopResult {
    Back,
    Quit,
}

struct Ui {
    color: bool,
    clear: bool,
    width: usize,
}

impl Ui {
    fn new(config: &Config) -> Self {
        let width = env::var("COLUMNS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(DEFAULT_WIDTH)
            .clamp(MIN_WIDTH, MAX_WIDTH);
        Self {
            color: config.color,
            clear: config.clear,
            width,
        }
    }

    fn clear_screen(&self) {
        if self.clear {
            print!("\x1b[2J\x1b[H");
        }
    }

    fn style<'a>(&self, code: &'static str, text: &'a str) -> Styled<'a> {
        Styled {
            enabled: self.color,
            code,
            text,
        }
    }

    fn rule(&self, title: &str) {
        let available = self.width.saturating_sub(4);
        let title = truncate(title, available);
        let rest = available.saturating_sub(char_len(&title) + 1);
        println!("┌─ {} {}┐", title, "─".repeat(rest));
    }

    fn bottom_rule(&self) {
        println!("└{}┘", "─".repeat(self.width.saturating_sub(2)));
    }
}

struct Styled<'a> {
    enabled: bool,
    code: &'static str,
    text: &'a str,
}

impl std::fmt::Display for Styled<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.enabled {
            write!(formatter, "\x1b[{}m{}\x1b[0m", self.code, self.text)
        } else {
            formatter.write_str(self.text)
        }
    }
}

pub(crate) async fn run_cli(arguments: Vec<String>) -> crate::AnyResult<()> {
    let config = Config::from_args_and_env(arguments)?;
    let token = env::var("RS_API_TOKEN")
        .map(SecretString::from)
        .map_err(|_| app_error("RS_API_TOKEN is not set"))?;
    let client = Client::builder()
        .authentication_token(token)
        .sqlite(&config.database)
        .startup_policy(StartupPolicy::RestoreOnly)
        .start()
        .await?;

    let result = run(&client, &config).await;
    let close_result = client.close().await;
    result?;
    close_result?;
    Ok(())
}

async fn run(client: &Client, config: &Config) -> crate::AnyResult<()> {
    // The trader directory is replicant-scoped, but it does not require a
    // galaxy/location/device baseline. Refresh only the small owned-replicant
    // roster so names such as "Chats-1" resolve without triggering a full sync.
    client.sync().domain(SyncDomain::Replicants).await?;
    let replicant = select_replicant(client, config.replicant.as_deref(), config.command).await?;

    match config.command {
        Command::Interactive => interactive_directory(client, config, &replicant).await,
        Command::List => {
            let traders = fetch_directory(client, replicant.key.id.as_str()).await?;
            render_directory_plain(&replicant, &traders);
            Ok(())
        }
        Command::Show => {
            let controller = config
                .controller
                .as_deref()
                .ok_or_else(|| app_error("trade show requires a controller code"))?;
            let traders = fetch_directory(client, replicant.key.id.as_str()).await?;
            let trader = traders
                .iter()
                .find(|trader| trader.controller_code.eq_ignore_ascii_case(controller));
            let trades = fetch_trades(client, controller).await?;
            render_shop_plain(trader, controller, &trades);
            Ok(())
        }
    }
}

async fn select_replicant(
    client: &Client,
    requested: Option<&str>,
    command: Command,
) -> crate::AnyResult<Replicant> {
    let handles = client.replicants().find().owned().collect().await?;
    let mut replicants = Vec::with_capacity(handles.len());
    for handle in handles {
        replicants.push(handle.snapshot().await?);
    }
    replicants.sort_by(|left, right| {
        left.name
            .cmp(&right.name)
            .then_with(|| left.key.id.as_str().cmp(right.key.id.as_str()))
    });
    if replicants.is_empty() {
        return Err(app_error("no owned replicants were found"));
    }

    if let Some(requested) = requested {
        return resolve_replicant(&replicants, requested);
    }

    let default_index = replicants
        .iter()
        .position(|replicant| {
            replicant
                .name
                .as_deref()
                .is_some_and(|name| name.eq_ignore_ascii_case(DEFAULT_REPLICANT))
        })
        .unwrap_or(0);

    if command != Command::Interactive {
        return Ok(replicants.remove(default_index));
    }
    if replicants.len() == 1 {
        return Ok(replicants.remove(0));
    }

    println!("Trade directory visibility depends on the viewing replicant.\n");
    for (index, replicant) in replicants.iter().enumerate() {
        let location = replicant
            .location
            .as_ref()
            .map_or("unknown", |location| location.id.as_str());
        println!(
            "  {:>2}. {:<20} {:<10} {:<24}{}",
            index + 1,
            replicant.name.as_deref().unwrap_or("<unnamed>"),
            replicant.key.id.as_str(),
            location,
            if index == default_index { "  [default]" } else { "" }
        );
    }
    let selected = prompt_index("Select replicant", replicants.len(), default_index + 1)?;
    Ok(replicants.remove(selected - 1))
}

fn resolve_replicant(replicants: &[Replicant], requested: &str) -> crate::AnyResult<Replicant> {
    let mut matches = replicants
        .iter()
        .filter(|replicant| {
            replicant.key.id.as_str().eq_ignore_ascii_case(requested)
                || replicant
                    .name
                    .as_deref()
                    .is_some_and(|name| name.eq_ignore_ascii_case(requested))
        })
        .cloned()
        .collect::<Vec<_>>();
    match matches.len() {
        1 => Ok(matches.remove(0)),
        0 => Err(app_error(format!("replicant {requested:?} was not found"))),
        _ => Err(app_error(format!(
            "replicant {requested:?} matched more than one owned replicant"
        ))),
    }
}

async fn fetch_directory(client: &Client, replicant_code: &str) -> crate::AnyResult<Vec<TraderSummary>> {
    let value = client.trading().visible_to(replicant_code).await?;
    let mut traders = value
        .get("traders")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|value| serde_json::from_value::<TraderSummary>(value.clone()).ok())
        .filter(|trader| !trader.controller_code.is_empty())
        .collect::<Vec<_>>();
    traders.sort_by(compare_traders);
    Ok(traders)
}

fn compare_traders(left: &TraderSummary, right: &TraderSummary) -> Ordering {
    right
        .is_local
        .cmp(&left.is_local)
        .then_with(|| left.display_name().to_ascii_lowercase().cmp(&right.display_name().to_ascii_lowercase()))
        .then_with(|| left.controller_code.cmp(&right.controller_code))
}

async fn fetch_trades(client: &Client, controller: &str) -> crate::AnyResult<Vec<ShopTrade>> {
    let values = client.trading().for_controller(controller).trades().await?;
    let mut trades = values
        .into_iter()
        .filter_map(|value| serde_json::from_value::<ShopTrade>(value).ok())
        .collect::<Vec<_>>();
    trades.sort_by(|left, right| {
        right
            .current_stock
            .unwrap_or_default()
            .cmp(&left.current_stock.unwrap_or_default())
            .then_with(|| {
                left.display_name()
                    .to_ascii_lowercase()
                    .cmp(&right.display_name().to_ascii_lowercase())
            })
    });
    Ok(trades)
}

async fn interactive_directory(
    client: &Client,
    config: &Config,
    replicant: &Replicant,
) -> crate::AnyResult<()> {
    let ui = Ui::new(config);
    let mut traders = fetch_directory(client, replicant.key.id.as_str()).await?;
    let mut filter = String::new();
    let mut notice = None::<String>;

    loop {
        let visible = traders
            .iter()
            .filter(|trader| trader.matches(&filter))
            .collect::<Vec<_>>();
        render_directory(&ui, replicant, &visible, &filter, notice.as_deref());
        notice = None;
        let input = prompt("trade> ")?;
        let command = input.trim();
        if command.is_empty() {
            continue;
        }
        match command {
            "q" | "quit" | "exit" => return Ok(()),
            "?" | "help" => {
                notice = Some(
                    "number=open shop • /text=search • /=clear search • r=refresh • q=quit"
                        .to_owned(),
                );
            }
            "r" | "refresh" => match fetch_directory(client, replicant.key.id.as_str()).await {
                Ok(updated) => {
                    traders = updated;
                    notice = Some("Directory refreshed.".to_owned());
                }
                Err(error) => notice = Some(format!("Refresh failed: {error}")),
            },
            _ if command.starts_with('/') => {
                filter = command[1..].trim().to_owned();
            }
            _ => match command.parse::<usize>() {
                Ok(index) if index > 0 && index <= visible.len() => {
                    let selected = (*visible[index - 1]).clone();
                    match interactive_shop(client, &ui, &selected).await? {
                        ShopLoopResult::Back => {}
                        ShopLoopResult::Quit => return Ok(()),
                    }
                }
                _ => notice = Some(format!("Unknown command {command:?}. Press ? for help.")),
            },
        }
    }
}

async fn interactive_shop(
    client: &Client,
    ui: &Ui,
    trader: &TraderSummary,
) -> crate::AnyResult<ShopLoopResult> {
    let mut trades = match fetch_trades(client, &trader.controller_code).await {
        Ok(trades) => trades,
        Err(error) => {
            render_shop_error(ui, trader, &error.to_string());
            prompt("Press Enter to return... ")?;
            return Ok(ShopLoopResult::Back);
        }
    };
    let mut filter = String::new();
    let mut notice = None::<String>;

    loop {
        let visible = trades
            .iter()
            .filter(|trade| trade.matches(&filter))
            .collect::<Vec<_>>();
        render_shop(ui, trader, &visible, &filter, notice.as_deref());
        notice = None;
        let input = prompt("shop> ")?;
        let command = input.trim();
        match command {
            "" => {}
            "b" | "back" => return Ok(ShopLoopResult::Back),
            "q" | "quit" | "exit" => return Ok(ShopLoopResult::Quit),
            "?" | "help" => {
                notice = Some("/text=search trades • /=clear search • r=refresh • b=back • q=quit".to_owned());
            }
            "r" | "refresh" => match fetch_trades(client, &trader.controller_code).await {
                Ok(updated) => {
                    trades = updated;
                    notice = Some("Trade stock refreshed.".to_owned());
                }
                Err(error) => notice = Some(format!("Refresh failed: {error}")),
            },
            _ if command.starts_with('/') => filter = command[1..].trim().to_owned(),
            _ => notice = Some(format!("Unknown command {command:?}. Press ? for help.")),
        }
    }
}

fn render_directory(
    ui: &Ui,
    replicant: &Replicant,
    traders: &[&TraderSummary],
    filter: &str,
    notice: Option<&str>,
) {
    ui.clear_screen();
    ui.rule("Replicant Space • Trade Directory");
    let name = replicant.name.as_deref().unwrap_or("<unnamed>");
    let location = replicant
        .location
        .as_ref()
        .map_or("unknown", |location| location.id.as_str());
    println!(
        "│ Replicant: {} ({})  •  Location: {}",
        ui.style("1;36", name),
        replicant.key.id.as_str(),
        location
    );
    let stock = traders
        .iter()
        .filter_map(|trader| trader.total_stock)
        .sum::<i64>();
    println!(
        "│ Shops: {}  •  Listed stock: {}  •  Filter: {}",
        traders.len(),
        stock,
        if filter.is_empty() { "all" } else { filter }
    );
    ui.bottom_rule();
    println!();

    if traders.is_empty() {
        println!("  No shops match this view.");
    } else {
        println!(
            " {:>2}  {:<28}  {:<15}  {:<22}  {:>6}  {:>6}  {:<8}",
            "#", "SHOP", "OWNER", "WHERE", "TRADES", "STOCK", "ACCESS"
        );
        println!(" {}", "─".repeat(ui.width.saturating_sub(2)));
        for (index, trader) in traders.iter().enumerate() {
            let access = if trader.is_local { "local" } else { "network" };
            let access = format!("{access:<8}");
            let access = if trader.is_local {
                ui.style("1;32", &access)
            } else {
                ui.style("36", &access)
            };
            println!(
                " {:>2}  {:<28}  {:<15}  {:<22}  {:>6}  {:>6}  {}",
                index + 1,
                truncate(trader.display_name(), 28),
                truncate(trader.owner(), 15),
                truncate(trader.place(), 22),
                trader.trade_count.map_or_else(|| "?".to_owned(), |value| value.to_string()),
                trader.total_stock.map_or_else(|| "?".to_owned(), |value| value.to_string()),
                access
            );
        }
    }
    println!();
    if let Some(notice) = notice {
        println!("  {}", ui.style("33", notice));
    }
    println!(
        "  {}",
        ui.style(
            "2",
            "[number] open shop   /text search   r refresh   ? help   q quit"
        )
    );
}

fn render_shop(
    ui: &Ui,
    trader: &TraderSummary,
    trades: &[&ShopTrade],
    filter: &str,
    notice: Option<&str>,
) {
    ui.clear_screen();
    ui.rule(trader.display_name());
    println!(
        "│ Owner: {}  •  Controller: {}  •  Location: {}",
        ui.style("1;36", trader.owner()),
        trader.controller_code,
        trader.place()
    );
    if let Some(description) = trader.description.as_deref() {
        for line in wrap_text(description, ui.width.saturating_sub(4)) {
            println!("│ {line}");
        }
    }
    println!(
        "│ Trades: {}  •  Filter: {}",
        trades.len(),
        if filter.is_empty() { "all" } else { filter }
    );
    ui.bottom_rule();
    println!();

    if trades.is_empty() {
        println!("  No available trades match this view.");
    } else {
        for (index, trade) in trades.iter().enumerate() {
            let title = format!("{}. {}", index + 1, trade.display_name());
            println!(
                "  {}  {}",
                ui.style("1", &title),
                ui.style("1;32", &format!("[stock {}]", trade.stock()))
            );
            println!("     You give  {}", exchange_summary(trade.criteria.as_ref()));
            println!("     You get   {}", exchange_summary(trade.rewards.as_ref()));
            println!(
                "     {}",
                ui.style(
                    "2",
                    &format!(
                        "{}{}",
                        if trade.trade_code.is_empty() {
                            "trade code unknown".to_owned()
                        } else {
                            trade.trade_code.clone()
                        },
                        trade
                            .created_at
                            .as_deref()
                            .map_or_else(String::new, |created| format!("  •  created {created}"))
                    )
                )
            );
            println!();
        }
    }
    if let Some(notice) = notice {
        println!("  {}", ui.style("33", notice));
    }
    println!(
        "  {}",
        ui.style("2", "/text search   r refresh stock   b back   ? help   q quit")
    );
}

fn render_shop_error(ui: &Ui, trader: &TraderSummary, error: &str) {
    ui.clear_screen();
    ui.rule(trader.display_name());
    println!("│ Controller: {}", trader.controller_code);
    println!("│ Location: {}", trader.place());
    ui.bottom_rule();
    println!();
    println!("  {}", ui.style("1;31", "Unable to inspect this shop right now."));
    println!("  {error}");
    println!();
    println!(
        "  {}",
        ui.style(
            "2",
            "Remote stock requires the shop to be reachable through your current local/FTL network."
        )
    );
}

fn render_directory_plain(replicant: &Replicant, traders: &[TraderSummary]) {
    println!(
        "Trade directory for {} ({}):",
        replicant.name.as_deref().unwrap_or("<unnamed>"),
        replicant.key.id.as_str()
    );
    println!(
        "{:<10}  {:<30}  {:<18}  {:<24}  {:>6}  {:>6}  {:<7}",
        "CODE", "SHOP", "OWNER", "WHERE", "TRADES", "STOCK", "ACCESS"
    );
    for trader in traders {
        println!(
            "{:<10}  {:<30}  {:<18}  {:<24}  {:>6}  {:>6}  {:<7}",
            trader.controller_code,
            truncate(trader.display_name(), 30),
            truncate(trader.owner(), 18),
            truncate(trader.place(), 24),
            trader.trade_count.map_or_else(|| "?".to_owned(), |value| value.to_string()),
            trader.total_stock.map_or_else(|| "?".to_owned(), |value| value.to_string()),
            if trader.is_local { "local" } else { "network" },
        );
    }
}

fn render_shop_plain(trader: Option<&TraderSummary>, controller: &str, trades: &[ShopTrade]) {
    if let Some(trader) = trader {
        println!(
            "{} — {} — {} ({controller})",
            trader.display_name(),
            trader.owner(),
            trader.place()
        );
    } else {
        println!("Trade controller {controller}");
    }
    println!();
    for trade in trades {
        println!("{} [{}]", trade.display_name(), trade.stock());
        println!("  give: {}", exchange_summary(trade.criteria.as_ref()));
        println!("  get:  {}", exchange_summary(trade.rewards.as_ref()));
        if !trade.trade_code.is_empty() {
            println!("  code: {}", trade.trade_code);
        }
        println!();
    }
}

fn exchange_summary(value: Option<&Value>) -> String {
    let Some(Value::Object(root)) = value else {
        return "nothing".to_owned();
    };
    let mut parts = Vec::new();

    if let Some(Value::Object(resources)) = root.get("resources") {
        let mut resources = resources.iter().collect::<Vec<_>>();
        resources.sort_by(|left, right| left.0.cmp(right.0));
        for (name, amount) in resources {
            if !is_zero(amount) {
                parts.push(format!("{} {}", format_amount(amount), pretty_name(name)));
            }
        }
    }

    if let Some(Value::Object(devices)) = root.get("devices") {
        let mut devices = devices.iter().collect::<Vec<_>>();
        devices.sort_by(|left, right| left.0.cmp(right.0));
        for (name, amount) in devices {
            if !is_zero(amount) {
                parts.push(format!("{}× {}", format_amount(amount), pretty_name(name)));
            }
        }
    }

    let known = ["resources", "devices"];
    for (key, nested) in root {
        if known.contains(&key.as_str()) || is_empty_value(nested) {
            continue;
        }
        parts.push(format!("{}={}", pretty_name(key), compact_value(nested)));
    }

    if parts.is_empty() {
        "nothing".to_owned()
    } else {
        parts.join(" + ")
    }
}

fn is_zero(value: &Value) -> bool {
    value.as_i64() == Some(0) || value.as_f64() == Some(0.0)
}

fn is_empty_value(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::Array(values) => values.is_empty(),
        Value::Object(values) => values.is_empty(),
        _ => false,
    }
}

fn format_amount(value: &Value) -> String {
    if let Some(value) = value.as_i64() {
        return value.to_string();
    }
    if let Some(value) = value.as_u64() {
        return value.to_string();
    }
    if let Some(value) = value.as_f64() {
        return if value.fract().abs() < f64::EPSILON {
            format!("{value:.0}")
        } else {
            format!("{value:.2}")
                .trim_end_matches('0')
                .trim_end_matches('.')
                .to_owned()
        };
    }
    compact_value(value)
}

fn compact_value(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        _ => serde_json::to_string(value).unwrap_or_else(|_| "?".to_owned()),
    }
}

fn pretty_name(value: &str) -> String {
    value.replace('_', " ")
}

fn prompt(label: &str) -> crate::AnyResult<String> {
    print!("{label}");
    io::stdout().flush()?;
    let mut line = String::new();
    io::stdin().read_line(&mut line)?;
    Ok(line)
}

fn prompt_index(label: &str, count: usize, default: usize) -> crate::AnyResult<usize> {
    loop {
        let input = prompt(&format!("{label} [{default}]: "))?;
        let input = input.trim();
        if input.is_empty() {
            return Ok(default);
        }
        if let Ok(index) = input.parse::<usize>()
            && (1..=count).contains(&index)
        {
            return Ok(index);
        }
        eprintln!("Enter a number from 1 to {count}.");
    }
}

fn wrap_text(text: &str, width: usize) -> Vec<String> {
    if width == 0 {
        return vec![String::new()];
    }
    let mut lines = Vec::new();
    let mut current = String::new();
    for word in text.split_whitespace() {
        let extra = if current.is_empty() { 0 } else { 1 };
        if !current.is_empty() && char_len(&current) + extra + char_len(word) > width {
            lines.push(current);
            current = String::new();
        }
        if !current.is_empty() {
            current.push(' ');
        }
        current.push_str(word);
    }
    if !current.is_empty() {
        lines.push(current);
    }
    if lines.is_empty() {
        lines.push(String::new());
    }
    lines
}

fn truncate(value: &str, width: usize) -> String {
    if char_len(value) <= width {
        return value.to_owned();
    }
    if width <= 1 {
        return "…".chars().take(width).collect();
    }
    let mut result = value.chars().take(width - 1).collect::<String>();
    result.push('…');
    result
}

fn char_len(value: &str) -> usize {
    value.chars().count()
}

fn app_error(message: impl Into<String>) -> crate::AnyError {
    io::Error::new(io::ErrorKind::InvalidInput, message.into()).into()
}

fn print_help() {
    println!(
        "Replicant Space trade directory\n\n\
Usage:\n  replicant-cli trade [OPTIONS]\n  replicant-cli trade list [OPTIONS]\n  replicant-cli trade show CONTROLLER [OPTIONS]\n\n\
Modes:\n  trade                Interactive shop directory (default)\n  trade list           Print the visible shop directory and exit\n  trade show CODE      Print one controller's current trades and exit\n\n\
Options:\n  --replicant NAME     Viewing replicant name or code\n  --database PATH      Managed SQLite database [env: REPLICANT_DB]\n  --no-color           Disable ANSI color\n  --no-clear           Do not clear the terminal between interactive views\n  -h, --help           Show this help\n\n\
Environment:\n  RS_API_TOKEN         Replicant Space API token (required)\n  RS_TRADE_REPLICANT   Default viewing replicant\n\n\
Interactive keys:\n  Directory: number=open, /text=search, r=refresh, q=quit\n  Shop:      /text=search, r=refresh stock, b=back, q=quit\n\n\
Examples:\n  replicant-cli trade\n  replicant-cli trade --replicant Chats-1\n  replicant-cli trade list --replicant Chats-1\n  replicant-cli trade show TC4488AA --replicant Chats-1"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exchange_summary_formats_resources_and_devices() {
        let value = serde_json::json!({
            "resources": {"volatiles": 100, "structural": 50},
            "devices": {"mining_drone": 2}
        });
        assert_eq!(
            exchange_summary(Some(&value)),
            "50 structural + 100 volatiles + 2× mining drone"
        );
    }

    #[test]
    fn trader_search_covers_owner_location_and_description() {
        let trader = TraderSummary {
            controller_code: "TC123".to_owned(),
            description: Some("Rare engineering hardware".to_owned()),
            location: Some("SOL-3-1".to_owned()),
            owner_name: Some("Riker".to_owned()),
            shop_name: Some("Lunar Supplies".to_owned()),
            ..TraderSummary::default()
        };
        assert!(trader.matches("riker"));
        assert!(trader.matches("sol"));
        assert!(trader.matches("engineering"));
        assert!(!trader.matches("twaffy"));
    }

    #[test]
    fn truncate_preserves_short_values_and_marks_long_ones() {
        assert_eq!(truncate("SOL", 8), "SOL");
        assert_eq!(truncate("SCEPTURUM", 6), "SCEPT…");
    }

    #[test]
    fn trades_sort_available_stock_first() {
        let mut trades = [
            ShopTrade {
                name: Some("Empty".to_owned()),
                current_stock: Some(0),
                ..ShopTrade::default()
            },
            ShopTrade {
                name: Some("Available".to_owned()),
                current_stock: Some(3),
                ..ShopTrade::default()
            },
        ];
        trades.sort_by(|left, right| {
            right
                .current_stock
                .unwrap_or_default()
                .cmp(&left.current_stock.unwrap_or_default())
        });
        assert_eq!(trades[0].display_name(), "Available");
    }

}
