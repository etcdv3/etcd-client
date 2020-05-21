//! Cluster example.

use etcd_client::*;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let mut client = Client::connect(["localhost:2379"], None).await?;

    // grant a key
    let resp = client.member_add(["localhost:2379"], false).await?;
    println!(
        "add a member with cluster is as {:?}",
        resp.member_list()
    );
    let member = resp.member();
    let id = member.unwrap().id;

    // query time to live
    let resp = client.member_promote(id).await?;
    println!(
        "member list with current cluster is {:?}",
        resp.member_list()
    );


    // get lease list
    let resp = client.member_list().await?;
    let member_list = resp.member_list();
    println!("member id is {:?}", member_list[0].id());

    // revoke a lease
    let _resp = client.member_remove(id).await?;
    println!("remove a member with id {:?}", id);
    Ok(())
}
