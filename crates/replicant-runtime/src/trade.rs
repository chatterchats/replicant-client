//! Read-only player trade-directory reports.

use std::{cmp::Ordering, io};

use replicant_client::{Client, Replicant, SyncDomain};
use serde::Deserialize;
use serde_json::Value;

use crate::ReportResult;

#[derive(Clone, Debug, Default, Deserialize)]
/// One shop visible in a replicant's trade directory.
pub struct TraderSummary {
    /// Controller device code.
    #[serde(default)]
    pub controller_code: String,
    /// Shop description.
    #[serde(default)]
    pub description: Option<String>,
    /// Whether the shop is local to the viewing replicant.
    #[serde(default)]
    pub is_local: bool,
    /// Shop location when public.
    #[serde(default)]
    pub location: Option<String>,
    /// Public owner name.
    #[serde(default)]
    pub owner_name: Option<String>,
    /// Public owner replicant code.
    #[serde(default)]
    pub owner_replicant_code: Option<String>,
    /// Public shop name.
    #[serde(default)]
    pub shop_name: Option<String>,
    /// Shop system when public.
    #[serde(default)]
    pub star: Option<String>,
    /// Total items currently in stock.
    #[serde(default)]
    pub total_stock: Option<i64>,
    /// Number of offered trades.
    #[serde(default)]
    pub trade_count: Option<i64>,
}

impl TraderSummary {
    /// Returns the shop name or a stable fallback.
    #[must_use]
    pub fn display_name(&self) -> &str {
        self.shop_name.as_deref().unwrap_or("<unnamed shop>")
    }
    /// Returns the public owner name or a stable fallback.
    #[must_use]
    pub fn owner(&self) -> &str {
        self.owner_name.as_deref().unwrap_or("<unknown>")
    }
    /// Returns the most precise public place label.
    #[must_use]
    pub fn place(&self) -> &str {
        self.location
            .as_deref()
            .or(self.star.as_deref())
            .unwrap_or("hidden")
    }
    /// Tests a case-insensitive filter against all searchable fields.
    #[must_use]
    pub fn matches(&self, needle: &str) -> bool {
        let needle = needle.to_ascii_lowercase();
        needle.is_empty()
            || [
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
/// One trade offered by a shop controller.
pub struct ShopTrade {
    /// Public trade name.
    #[serde(default)]
    pub name: Option<String>,
    /// Stable trade code.
    #[serde(default)]
    pub trade_code: String,
    /// Remaining stock.
    #[serde(default)]
    pub current_stock: Option<i64>,
    /// Initial stock.
    #[serde(default)]
    pub initial_stock: Option<i64>,
    /// Items required from the buyer.
    #[serde(default)]
    pub criteria: Option<Value>,
    /// Items returned to the buyer.
    #[serde(default)]
    pub rewards: Option<Value>,
    /// Server creation timestamp.
    #[serde(default)]
    pub created_at: Option<String>,
}

impl ShopTrade {
    /// Returns the trade name or a stable fallback.
    #[must_use]
    pub fn display_name(&self) -> &str {
        self.name.as_deref().unwrap_or("<unnamed trade>")
    }
    /// Formats current and initial stock.
    #[must_use]
    pub fn stock(&self) -> String {
        match (self.current_stock, self.initial_stock) {
            (Some(current), Some(initial)) => format!("{current}/{initial}"),
            (Some(current), None) => current.to_string(),
            _ => "?".to_owned(),
        }
    }
    /// Tests a case-insensitive filter against trade fields and exchanges.
    #[must_use]
    pub fn matches(&self, needle: &str) -> bool {
        let needle = needle.to_ascii_lowercase();
        needle.is_empty()
            || self.display_name().to_ascii_lowercase().contains(&needle)
            || self.trade_code.to_ascii_lowercase().contains(&needle)
            || exchange_summary(self.criteria.as_ref())
                .to_ascii_lowercase()
                .contains(&needle)
            || exchange_summary(self.rewards.as_ref())
                .to_ascii_lowercase()
                .contains(&needle)
    }
}

/// Returns owned replicants that can view trade directories.
pub async fn trade_viewers(client: &Client) -> ReportResult<Vec<Replicant>> {
    client.sync().domain(SyncDomain::Replicants).await?;
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
    Ok(replicants)
}

/// Resolves a viewing replicant by exact code or name.
pub fn resolve_trade_viewer(replicants: &[Replicant], requested: &str) -> ReportResult<Replicant> {
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
        0 => Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("replicant {requested:?} was not found"),
        )
        .into()),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("replicant {requested:?} matched more than one owned replicant"),
        )
        .into()),
    }
}

/// Returns the sorted shop directory visible to one replicant.
pub async fn trader_directory(
    client: &Client,
    replicant_code: &str,
) -> ReportResult<Vec<TraderSummary>> {
    let value = client.trading().visible_to(replicant_code).await?;
    let mut traders = value
        .get("traders")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|value| serde_json::from_value(value.clone()).ok())
        .filter(|trader: &TraderSummary| !trader.controller_code.is_empty())
        .collect::<Vec<_>>();
    traders.sort_by(compare_traders);
    Ok(traders)
}

/// Returns a controller's trades sorted by availability and name.
pub async fn shop_trades(client: &Client, controller: &str) -> ReportResult<Vec<ShopTrade>> {
    let mut trades = client
        .trading()
        .for_controller(controller)
        .trades()
        .await?
        .into_iter()
        .filter_map(|value| serde_json::from_value(value).ok())
        .collect::<Vec<_>>();
    trades.sort_by(|left: &ShopTrade, right| {
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

fn compare_traders(left: &TraderSummary, right: &TraderSummary) -> Ordering {
    right
        .is_local
        .cmp(&left.is_local)
        .then_with(|| {
            left.display_name()
                .to_ascii_lowercase()
                .cmp(&right.display_name().to_ascii_lowercase())
        })
        .then_with(|| left.controller_code.cmp(&right.controller_code))
}

#[must_use]
/// Formats an exchange payload for frontend display and filtering.
pub fn exchange_summary(value: Option<&Value>) -> String {
    let Some(value) = value else {
        return "nothing".to_owned();
    };
    let Value::Object(entries) = value else {
        return compact_value(value);
    };
    let mut parts = entries
        .iter()
        .filter(|(_, amount)| !is_zero(amount) && !is_empty_value(amount))
        .map(|(name, amount)| {
            format!(
                "{} {}",
                format_amount(amount),
                name.replace(['_', '-'], " ")
            )
        })
        .collect::<Vec<_>>();
    parts.sort();
    if parts.is_empty() {
        "nothing".to_owned()
    } else {
        parts.join(" + ")
    }
}

fn is_zero(value: &Value) -> bool {
    value.as_i64() == Some(0) || value.as_u64() == Some(0) || value.as_f64() == Some(0.0)
}
fn is_empty_value(value: &Value) -> bool {
    matches!(value, Value::Null)
        || value.as_str().is_some_and(str::is_empty)
        || value.as_array().is_some_and(Vec::is_empty)
        || value.as_object().is_some_and(serde_json::Map::is_empty)
}
fn format_amount(value: &Value) -> String {
    if let Some(value) = value.as_i64() {
        return value.to_string();
    }
    if let Some(value) = value.as_u64() {
        return value.to_string();
    }
    if let Some(value) = value.as_f64() {
        return if value.fract() == 0.0 {
            format!("{value:.0}")
        } else {
            format!("{value:.2}")
        };
    }
    compact_value(value)
}
fn compact_value(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "?".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filters_directory_fields() {
        let trader = TraderSummary {
            shop_name: Some("Riker Engineering".into()),
            star: Some("SOL".into()),
            ..Default::default()
        };
        assert!(trader.matches("riker"));
        assert!(trader.matches("sol"));
        assert!(!trader.matches("twaffy"));
    }

    #[test]
    fn summarizes_exchange_objects() {
        assert_eq!(
            exchange_summary(Some(&serde_json::json!({"IRON": 4, "WATER": 0}))),
            "4 IRON"
        );
    }
}
