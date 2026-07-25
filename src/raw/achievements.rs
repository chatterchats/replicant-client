//! Public achievement catalogue (`GET /v1/achievements`,
//! `GET /v1/achievements/{achievement_key}`). Both are unauthenticated.
//!
//! The authenticated, account-scoped "achievements I've earned" list
//! (`GET /v1/accounts/achievements`) is a distinct schema and lives on
//! [`crate::raw::accounts::AccountsClient::achievements`], matching the
//! server's own route grouping.

use reqwest::Method;
use serde::Deserialize;

use crate::error::Error;
use crate::raw::common::encode_path_segment;
use crate::raw::{Client, RawResponse, RequestSafety};

/// Summary of one achievement in the public catalogue.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct AchievementSummary {
    /// Stable achievement identifier.
    pub achievement_key: Option<String>,
    /// Achievement category.
    pub category: Option<String>,
    /// Flavor description.
    pub description: Option<String>,
    /// When any player last earned this achievement, RFC3339.
    pub last_achieved_at: Option<String>,
    /// Number of accounts that have earned this achievement.
    pub player_count: Option<i64>,
    /// Display title.
    pub title: Option<String>,
    /// XP reward for earning this achievement.
    pub xp_reward: Option<i64>,
}

/// Response body for `GET /v1/achievements`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct AchievementIndexResponse {
    /// All achievements in the catalogue.
    #[serde(default)]
    pub achievements: Vec<AchievementSummary>,
}

/// One account that has earned a given achievement.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct AchievementPlayer {
    /// The earning account's display name.
    pub account_name: Option<String>,
    /// When this account earned it, RFC3339.
    pub achieved_at: Option<String>,
}

/// Response body for `GET /v1/achievements/{achievement_key}`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct AchievementDetailResponse {
    /// Stable achievement identifier.
    pub achievement_key: Option<String>,
    /// Achievement category.
    pub category: Option<String>,
    /// Flavor description.
    pub description: Option<String>,
    /// Number of accounts that have earned this achievement.
    pub player_count: Option<i64>,
    /// Accounts that have earned this achievement.
    #[serde(default)]
    pub players: Vec<AchievementPlayer>,
    /// Display title.
    pub title: Option<String>,
    /// XP reward for earning this achievement.
    pub xp_reward: Option<i64>,
}

/// Typed client for the public achievement catalogue.
#[derive(Clone, Debug)]
pub struct AchievementsClient {
    client: Client,
}

impl AchievementsClient {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    /// Lists all achievements with player counts. Unauthenticated.
    pub async fn list(&self) -> Result<RawResponse<AchievementIndexResponse>, Error> {
        self.client
            .execute(
                Method::GET,
                "v1/achievements",
                false,
                RequestSafety::SafeRead,
            )
            .await
    }

    /// Lists all players who earned a specific achievement. Unauthenticated.
    pub async fn get(
        &self,
        achievement_key: &str,
    ) -> Result<RawResponse<AchievementDetailResponse>, Error> {
        let path = format!("v1/achievements/{}", encode_path_segment(achievement_key));
        self.client
            .execute(Method::GET, &path, false, RequestSafety::SafeRead)
            .await
    }
}
