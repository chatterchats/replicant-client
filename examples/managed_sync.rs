//! Start an explicit essential REST synchronization sweep.

use replicant_client::Client;

#[tokio::main]
async fn main() -> replicant_client::Result<()> {
    let client = Client::builder().in_memory().start().await?;
    let report = client.sync().essential().await?;
    println!("synchronized {} domains", report.completed.len());
    client.close().await
}
