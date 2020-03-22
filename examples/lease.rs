//! Lease example.

use etcd_client::*;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let mut client = Client::connect(["localhost:2379"]).await?;

    // grant a key
    let resp = client.grant(60, None).await?;
    println!(
        "grant a lease with id {:?}, ttl {:?}",
        resp.id(),
        resp.ttl()
    );
    let id = resp.id();

    let resp = client.keep_alive(id, None).await?;
    println!(
        "lease keep alive with id {:?}, new ttl {:?}",
        resp.id(),
        resp.ttl()
    );

    let resp = client.time_to_live(id, None).await?;
    println!(
        "lease({:?}) remain ttl {:?} granted ttl {:?}",
        resp.id(),
        resp.ttl(),
        resp.granted_ttl()
    );

    // lease
    let resp = client.leases().await?;
    let lease_status = resp.take_leases();
    println!("lease status {:?}", lease_status[0].id);

    // revoke a key
    let _resp = client.revoke(id, None).await?;
    println!("revoke a lease with id {:?}", id);
    Ok(())
}
