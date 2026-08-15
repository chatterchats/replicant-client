//! Read-only application access to volatile account-intelligence APIs.

use replicant_client::{
    Client,
    raw::{
        accounts::{AccountAchievementListResponse, AccountMeResponse},
        bobnet::{DeviceChannelsResponse, DeviceMessagesResponse},
        leaderboards::{LeaderboardIndexResponse, LeaderboardResponse},
        messages::{MessageListQuery, MessageListResponse},
        reputation::AccountReputationResponse,
    },
};

use crate::ReportResult;

/// Reads the account inbox without adding it to durable managed state.
pub async fn inbox(client: &Client, limit: i64) -> ReportResult<MessageListResponse> {
    let query = MessageListQuery {
        limit: Some(limit),
        ..MessageListQuery::default()
    };
    Ok(client.raw().messages().list(&query).await?.value)
}

/// Reads one relay's observed channels and recent history.
pub async fn relay_history(
    client: &Client,
    relay: &str,
    limit: i64,
) -> ReportResult<(DeviceChannelsResponse, DeviceMessagesResponse)> {
    let bobnet = client.bobnet();
    let (channels, messages) =
        tokio::try_join!(bobnet.channels(relay), bobnet.history(relay).latest(limit),)?;
    Ok((channels, messages))
}

/// Reads the authenticated account profile for network presentation.
pub async fn account_profile(client: &Client) -> ReportResult<AccountMeResponse> {
    Ok(client.raw().accounts().me().await?.value)
}

/// Reads the account profile, achievements, and reputation together.
pub async fn standing(
    client: &Client,
) -> ReportResult<(
    AccountMeResponse,
    AccountAchievementListResponse,
    AccountReputationResponse,
)> {
    let raw = client.raw();
    let account_client = raw.accounts();
    let achievement_client = raw.accounts();
    let reputation_client = raw.accounts();
    let (account, achievements, reputation) = tokio::try_join!(
        account_client.me(),
        achievement_client.achievements(),
        reputation_client.reputation(),
    )?;
    Ok((account.value, achievements.value, reputation.value))
}

/// Lists published leaderboard descriptors.
pub async fn leaderboard_index(client: &Client) -> ReportResult<LeaderboardIndexResponse> {
    Ok(client.raw().leaderboards().index().await?.value)
}

/// Reads one supported standard leaderboard.
pub async fn leaderboard(client: &Client, board: &str) -> ReportResult<LeaderboardResponse> {
    let leaderboards = client.raw().leaderboards();
    let response = match board {
        "colony_moon" => leaderboards.colony_moon().await?,
        "colony_planet" => leaderboards.colony_planet().await?,
        "distance" => leaderboards.distance().await?,
        "fleet" => leaderboards.fleet().await?,
        "megastructure" => leaderboards.megastructure().await?,
        "reputation" => leaderboards.reputation().await?,
        "trades" => leaderboards.trades().await?,
        "xp" => leaderboards.xp().await?,
        _ => return Err(format!("unsupported leaderboard: {board}").into()),
    };
    Ok(response.value)
}
