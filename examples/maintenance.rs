//! Maintenance example

use etcd_client::*;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let mut client = Client::connect(["localhost:2379"], None).await?;

    // Get alarm.
    let resp = client
        .alarm(AlarmAction::Get, AlarmType::None, Some(AlarmOptions::new()))
        .await?;
    let mems = resp.alarms();
    print!("{} members have alarm.\n", mems.len());

    // Get status.
    let resp = client.status().await?;
    print!(
        "version: {}, db_size: {} \n",
        resp.version(),
        resp.db_size()
    );

    // Defragment.
    let _resp = client.defragment().await?;

    // Get hash value.
    let resp = client.hash().await?;
    print!("hash: {} \n", resp.hash());

    // Get hash key value.
    let resp = client.hash_kv(0).await?;
    print!(
        "hash: {} revision: {} \n",
        resp.hash(),
        resp.compact_version()
    );

    // Get snapshot.
    let mut msg = client.snapshot().await?;
    loop {
        let resp = msg.message().await?;
        if let Some(r) = resp {
            print!("Receive blob len {}\n", r.blob().len());
            if r.remaining_bytes() == 0 {
                break;
            }
        }
    }

    Ok(())
}
