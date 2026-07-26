//! Compile-checked local query API examples. These builders only inspect the
//! client's committed local state; the surrounding application owns startup.

use replicant_client::{Client, DeviceStatus, DeviceType, Result};

#[allow(dead_code)]
async fn preferred_queries(client: Client) -> Result<()> {
    client
        .devices()
        .find()
        .of_type(DeviceType::MiningDrone)
        .with_status(DeviceStatus::Idle)
        .collect()
        .await?;

    client.devices().miners().idle().at("SOL").collect().await?;

    client
        .devices()
        .controllers(DeviceType::MiningController)
        .idle()
        .without_adopted_devices()
        .collect()
        .await?;

    Ok(())
}

fn main() {}
