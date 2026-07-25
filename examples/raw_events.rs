//! Reads filtered event history, then resumes the raw SSE stream.

use futures::StreamExt as _;
use replicant_client::{
    events::EventLogQuery,
    raw::{Client, SecretString},
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), replicant_client::Error> {
    let token =
        std::env::var("REPLICANT_API_KEY").map_err(|_| replicant_client::Error::Configuration {
            message: "set REPLICANT_API_KEY".into(),
        })?;
    let client = Client::builder()
        .authentication_token(SecretString::from(token))
        .build()?;

    let page = client
        .events()
        .list(&EventLogQuery {
            filtered: Some(true),
            ..EventLogQuery::default()
        })
        .await?;
    let mut stream = client
        .events()
        .stream(page.value.next_cursor.as_deref())
        .await?;
    while let Some(event) = stream.next().await {
        let event = event?;
        println!("{} {}", event.id, event.event);
    }
    Ok(())
}
