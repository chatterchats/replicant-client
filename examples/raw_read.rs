//! Performs one authenticated raw read and prints transport metadata.

use replicant_client::raw::{Client, SecretString};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), replicant_client::Error> {
    let token =
        std::env::var("REPLICANT_API_KEY").map_err(|_| replicant_client::Error::Configuration {
            message: "set REPLICANT_API_KEY".into(),
        })?;
    let client = Client::builder()
        .authentication_token(SecretString::from(token))
        .build()?;
    let response = client.accounts().me().await?;
    println!(
        "status={} request_id={:?} rate_limit={:?}",
        response.metadata.status, response.metadata.request_id, response.metadata.rate_limit
    );
    Ok(())
}
