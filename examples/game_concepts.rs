//! Travel, AMI, BobNet, trading, and simulations: game concepts, not
//! endpoint groupings.

use std::collections::BTreeMap;

use replicant_client::{Client, MiningDirective};

#[tokio::main]
async fn main() -> replicant_client::Result<()> {
    let client = Client::builder().in_memory().start().await?;

    // Travel: preview a route before committing to it, then depart.
    let replicant = client.replicants().get_owned("R1").await?;
    let plan = replicant.travel().to("SOL").via_direct();
    let preview = plan.preview().await?;
    println!("route takes about {:?} seconds", preview.total_time_seconds);
    let departure = plan.depart().await?;
    departure.wait().await?;

    // AMI: hand a mining controller a directive and a fleet.
    let controller = client.devices().get("MC1").await?;
    let mining = controller.as_mining_controller()?;
    mining.adopt(["D1", "D2"]).await?;
    let mut targets = BTreeMap::new();
    targets.insert("structural".to_string(), 200);
    mining
        .set_directive(MiningDirective::GatherResources { resources: targets })
        .await?;
    mining.launch().await?;

    // BobNet: send a message and read a relay's recent chatter.
    client
        .bobnet()
        .send("R1", "#trade", "selling rares")
        .await?;
    let history = client.bobnet().history("RELAY1").latest(20).await?;
    println!("{} recent BobNet messages", history.messages.len());

    // Trading: list a controller's trades and fulfill one.
    let trades = client.trading().for_controller("TC1").trades().await?;
    if let Some(trade_code) = trades
        .first()
        .and_then(|trade| trade["trade_code"].as_str())
    {
        client.trading().execute("TC1", trade_code).await?;
    }

    // Simulations: start a timed run, then abandon it.
    let operation = client
        .simulations()
        .start("SIMDEV1", "R1", "mining_rush")
        .await?;
    operation.wait().await?;
    client.simulations().abandon("SIMDEV1", 1).await?;

    client.close().await
}
