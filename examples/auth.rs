//! Auth example

use etcd_client::*;

#[tokio::main]
async fn main() -> Result<(), Error> {
    println!("connect without user authenticate");
    let mut client = Client::connect(["localhost:2379"], None).await?;

    println!("enable authenticate by normal client");
    client.auth_enable().await?;

    // connect with authenticate
    println!("connect with user authenticate");
    let options = Some(ConnectOptions::new().with_user(
        String::from("root"),    // user name
        String::from("rootpwd"), // password
    ));
    let mut client_auth = Client::connect(["localhost:2379"], options).await?;

    println!("disable authenticate by authenticated client");
    client_auth.auth_disable().await?;

    Ok(())
}
