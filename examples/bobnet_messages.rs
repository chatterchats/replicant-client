//! Displays recent BobNet relay history and optionally follows new messages.
//!
//! Examples:
//!
//! ```text
//! cargo run --example bobnet_messages -- 008A353D
//! cargo run --example bobnet_messages -- 008A353D --limit 100 --channel general --follow
//! cargo run --example bobnet_messages -- --follow
//! ```
//!
//! The relay code may also be supplied through `RS_BOBNET_RELAY`. Live
//! following uses account `bobnet.new` events and therefore does not require a
//! relay code, but relay history does.

use std::{
    collections::BTreeSet,
    env,
    error::Error as StdError,
    io::{self, Write as _},
    path::PathBuf,
};

use replicant_client::{Client, Event, SecretString, StartupPolicy};
use serde_json::Value;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

type AnyError = Box<dyn StdError + Send + Sync + 'static>;
type AnyResult<T> = Result<T, AnyError>;

#[derive(Debug)]
struct Config {
    token: String,
    database: PathBuf,
    relay: Option<String>,
    limit: i64,
    channel: Option<String>,
    include_npcs: bool,
    follow: bool,
}

impl Config {
    fn from_args() -> AnyResult<Option<Self>> {
        let token = env::var("RS_API_TOKEN")
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "RS_API_TOKEN is required"))?;

        let mut relay = env::var("RS_BOBNET_RELAY").ok();
        let mut limit = env_i64("RS_BOBNET_LIMIT", 50)?.clamp(1, 100);
        let mut channel = env::var("RS_BOBNET_CHANNEL").ok();
        let mut include_npcs = env_bool("RS_BOBNET_INCLUDE_NPCS", true)?;
        let mut follow = env_bool("RS_BOBNET_FOLLOW", false)?;

        let mut args = env::args().skip(1);
        while let Some(argument) = args.next() {
            match argument.as_str() {
                "-h" | "--help" => {
                    print_help();
                    return Ok(None);
                }
                "--relay" => relay = Some(required_value(&mut args, "--relay")?),
                "--limit" => {
                    let value = required_value(&mut args, "--limit")?;
                    limit = value.parse::<i64>()?.clamp(1, 100);
                }
                "--channel" => channel = Some(required_value(&mut args, "--channel")?),
                "--follow" => follow = true,
                "--history-only" => follow = false,
                "--no-npcs" => include_npcs = false,
                "--include-npcs" => include_npcs = true,
                unknown if !unknown.starts_with('-') && relay.is_none() => {
                    relay = Some(unknown.to_owned());
                }
                unknown => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("unknown argument `{unknown}`; use --help"),
                    )
                    .into());
                }
            }
        }

        if relay.is_none() && !follow {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "provide a relay code for history, or pass --follow for live-only mode",
            )
            .into());
        }

        Ok(Some(Self {
            token,
            database: env::var_os("REPLICANT_DB")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("replicant-client.sqlite")),
            relay,
            limit,
            channel: channel.map(|value| normalize_channel(&value)),
            include_npcs,
            follow,
        }))
    }
}

fn required_value(args: &mut impl Iterator<Item = String>, option: &str) -> AnyResult<String> {
    args.next().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{option} requires a value"),
        )
        .into()
    })
}

fn env_i64(name: &str, default: i64) -> AnyResult<i64> {
    match env::var(name) {
        Ok(value) => Ok(value.parse()?),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

fn env_bool(name: &str, default: bool) -> AnyResult<bool> {
    match env::var(name) {
        Ok(value) => match value.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{name} must be true/false, yes/no, on/off, or 1/0"),
            )
            .into()),
        },
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

fn print_help() {
    println!(
        "BobNet message viewer\n\n\
         Usage:\n  \
         cargo run --example bobnet_messages -- [RELAY_CODE] [OPTIONS]\n\n\
         Options:\n  \
         --relay CODE       Relay or System Hub used for recent history\n  \
         --limit N          Number of recent messages, 1-100 (default: 50)\n  \
         --channel NAME     Show only one channel, with or without #\n  \
         --follow           Continue displaying live bobnet.new events\n  \
         --history-only     Print history and exit\n  \
         --no-npcs          Exclude NPC chatter from relay history\n  \
         --include-npcs     Include NPC chatter (default)\n  \
         -h, --help         Show this help\n\n\
         Environment:\n  \
         RS_API_TOKEN, REPLICANT_DB, RS_BOBNET_RELAY, RS_BOBNET_LIMIT,\n  \
         RS_BOBNET_CHANNEL, RS_BOBNET_FOLLOW, RS_BOBNET_INCLUDE_NPCS"
    );
}

#[derive(Debug)]
struct DisplayMessage {
    id: Option<i64>,
    channel: String,
    sender_name: String,
    sender_code: Option<String>,
    current_star: Option<String>,
    timestamp: String,
    body: String,
    source: String,
}

#[tokio::main]
async fn main() -> AnyResult<()> {
    let Some(config) = Config::from_args()? else {
        return Ok(());
    };
    install_tracing()?;

    let startup_policy = if config.follow {
        StartupPolicy::Essential
    } else {
        StartupPolicy::RestoreOnly
    };

    let client = Client::builder()
        .authentication_token(SecretString::from(config.token))
        .sqlite(&config.database)
        .startup_policy(startup_policy)
        .start()
        .await?;

    if config.follow {
        client.ready().await?;
    }

    let mut watch = if config.follow {
        Some(client.events().watch().await?)
    } else {
        None
    };
    let mut displayed_ids = BTreeSet::new();

    if let Some(relay) = &config.relay {
        let history = client
            .bobnet()
            .history(relay)
            .include_npcs(config.include_npcs)
            .latest(config.limit)
            .await?;

        let mut messages = history.messages;
        messages.sort_by(|left, right| {
            left.id
                .cmp(&right.id)
                .then_with(|| left.time.cmp(&right.time))
        });

        let mut displayed = 0usize;
        for message in &messages {
            let display = DisplayMessage {
                id: message.id,
                channel: display_channel(message.channel.as_deref().unwrap_or("unknown")),
                sender_name: message
                    .replicant_name
                    .clone()
                    .or_else(|| message.replicant_code.clone())
                    .unwrap_or_else(|| "System".to_owned()),
                sender_code: message.replicant_code.clone(),
                current_star: message.current_star.clone(),
                timestamp: message
                    .time
                    .clone()
                    .unwrap_or_else(|| "unknown time".to_owned()),
                body: message
                    .message
                    .clone()
                    .unwrap_or_else(|| "<empty message>".to_owned()),
                source: format!("relay {relay}"),
            };

            if channel_matches(config.channel.as_deref(), &display.channel) {
                print_message(&display)?;
                displayed += 1;
            }
            if let Some(id) = display.id {
                displayed_ids.insert(id);
            }
        }

        info!(
            target: "replicant_client::bobnet_viewer",
            event = "bobnet.history_displayed",
            relay,
            fetched = messages.len(),
            displayed,
            next_cursor = history.next_cursor,
            "displayed recent BobNet history"
        );
    }

    if let Some(watch) = &mut watch {
        info!(
            target: "replicant_client::bobnet_viewer",
            event = "bobnet.follow_started",
            channel = config.channel.as_deref().unwrap_or("all"),
            "following live BobNet messages; press Ctrl-C to stop"
        );

        loop {
            tokio::select! {
                result = watch.next() => {
                    let event = result?;
                    if event.name.as_str() != "bobnet.new" {
                        continue;
                    }
                    let Some(message) = message_from_event(&event) else {
                        warn!(
                            target: "replicant_client::bobnet_viewer",
                            event = "bobnet.event_unreadable",
                            event_id = %event.id,
                            payload = ?event.payload,
                            "bobnet.new event did not contain a displayable message"
                        );
                        continue;
                    };
                    if message.id.is_some_and(|id| displayed_ids.contains(&id)) {
                        continue;
                    }
                    if !channel_matches(config.channel.as_deref(), &message.channel) {
                        continue;
                    }
                    print_message(&message)?;
                    if let Some(id) = message.id {
                        displayed_ids.insert(id);
                    }
                }
                signal = tokio::signal::ctrl_c() => {
                    signal?;
                    info!(
                        target: "replicant_client::bobnet_viewer",
                        event = "bobnet.follow_stopped",
                        "received Ctrl-C"
                    );
                    break;
                }
            }
        }
    }

    client.close().await?;
    Ok(())
}

fn install_tracing() -> AnyResult<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new("replicant_client=warn,replicant_client::bobnet_viewer=info")
    });
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_writer(io::stderr)
        .try_init()
        .map_err(|error| io::Error::other(error.to_string()))?;
    Ok(())
}

fn message_from_event(event: &Event) -> Option<DisplayMessage> {
    let payload = &event.payload;
    let body = string_field(payload, &["message", "text", "body"])?;
    let channel = string_field(payload, &["channel"]).unwrap_or_else(|| "unknown".to_owned());
    let sender_code = string_field(payload, &["replicant_code"]);
    let sender_name = string_field(payload, &["replicant_name", "sender_name"])
        .or_else(|| sender_code.clone())
        .unwrap_or_else(|| "System".to_owned());
    let current_star = string_field(payload, &["current_star"])
        .or_else(|| event.star.as_ref().map(|star| star.id.as_str().to_owned()));
    let timestamp =
        string_field(payload, &["time", "created_at"]).unwrap_or_else(|| event.occurred_at.clone());

    Some(DisplayMessage {
        id: integer_field(payload.get("id")),
        channel: display_channel(&channel),
        sender_name,
        sender_code,
        current_star,
        timestamp,
        body,
        source: "event stream".to_owned(),
    })
}

fn string_field(
    payload: &std::collections::BTreeMap<String, Value>,
    names: &[&str],
) -> Option<String> {
    names
        .iter()
        .find_map(|name| payload.get(*name).and_then(Value::as_str))
        .map(str::to_owned)
}

fn integer_field(value: Option<&Value>) -> Option<i64> {
    value.and_then(|value| {
        value
            .as_i64()
            .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
    })
}

fn print_message(message: &DisplayMessage) -> AnyResult<()> {
    println!("{}", message.channel);

    let mut header = message.sender_name.clone();
    if let Some(code) = &message.sender_code
        && code != &message.sender_name
    {
        header.push_str(" [");
        header.push_str(code);
        header.push(']');
    }
    if let Some(star) = &message.current_star {
        header.push_str(" · ");
        header.push_str(star);
    }
    header.push_str(" · ");
    header.push_str(&message.source);
    header.push_str(" · ");
    header.push_str(&message.timestamp);

    println!("{header}");
    println!();
    println!("{}", message.body);
    println!();
    io::stdout().flush()?;
    Ok(())
}

fn normalize_channel(channel: &str) -> String {
    channel.trim().trim_start_matches('#').to_ascii_lowercase()
}

fn display_channel(channel: &str) -> String {
    format!("#{}", channel.trim().trim_start_matches('#'))
}

fn channel_matches(filter: Option<&str>, actual: &str) -> bool {
    filter.is_none_or(|filter| normalize_channel(actual) == filter)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use replicant_client::{
        EventId, Realm,
        domain::{EventCategory, EventName},
    };

    #[test]
    fn channel_filter_ignores_hash_and_case() {
        assert!(channel_matches(Some("general"), "#General"));
        assert!(!channel_matches(Some("trade"), "#general"));
    }

    #[test]
    fn formats_bobnet_event_payload() {
        let event = Event {
            id: EventId::from("1-0"),
            realm: Some(Realm::Live),
            name: EventName::from("bobnet.new"),
            category: EventCategory::from("bobnet"),
            device: None,
            replicant: None,
            location: None,
            star: None,
            occurred_at: "2026-07-27T00:00:00Z".to_owned(),
            payload: BTreeMap::from([
                ("id".to_owned(), Value::from(4821)),
                ("replicant_name".to_owned(), Value::from("Riker")),
                ("replicant_code".to_owned(), Value::from("4BBA7CBE")),
                ("current_star".to_owned(), Value::from("SOL")),
                ("channel".to_owned(), Value::from("general")),
                ("message".to_owned(), Value::from("Find us a home.")),
            ]),
        };

        let message = message_from_event(&event).expect("message");
        assert_eq!(message.id, Some(4821));
        assert_eq!(message.channel, "#general");
        assert_eq!(message.sender_name, "Riker");
        assert_eq!(message.body, "Find us a home.");
    }
}
