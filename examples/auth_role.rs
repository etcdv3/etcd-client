//! Auth role example.

use etcd_client::*;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let mut client = Client::connect(["localhost:2379"], None).await?;
    let role1 = "test_role2";
    let role2 = "test_role3";

    client.role_add(role1).await?;
    println!("add role successfully");

    client.role_add(role2).await?;
    println!("add role successfully");


    let mut perm = Permission::new(PermissionType::Read, "456");
    perm = perm.with_range_end("457");
    client.role_grant_permission(role1, perm).await?;

    let perm1 = Permission::new(PermissionType::Read, "abc");
     client.role_grant_permission(role1, perm1).await?;
    println!("grant permission successfully");

    let mut perm2 = Permission::new(PermissionType::Readwrite, "123");
    perm2 = perm2.with_prefix();
    client.role_grant_permission(role1, perm2).await?;
    println!("grant permission successfully");

    let perm3 = Permission::read("a");
    client.role_grant_permission(role1, perm3).await?;
    println!("grant permission successfully");

    let mut perm4 = Permission::write("b");
    //the last set is valid
    perm4 = perm4.with_range_end("e");
    perm4 = perm4.with_prefix();
    let resp = client.role_grant_permission(role1, perm4).await?;
    println!("{:?}", resp.header());
    let mut perm5 = Permission::readwrite("d");
    perm5 = perm5.with_prefix();
    perm5 = perm5.with_range_end("h");

    let resp = client.role_grant_permission(role1, perm5).await?;
    println!("{:?}", resp.header());

    let resp = client.role_get("test role3".to_string()).await?;
    //show the result
    println!("Role {}\n{}", role1, resp);

    let resp = client.role_revoke_permission(role1, "abc",
                                             None).await?;
    println!("{:?}", resp.header());

    let resp = client.role_revoke_permission(role1, "456",
                                             Some(RoleRevokePermissionOption::new().with_range_end("457"))).await?;
    println!("{:?}", resp.header());

    let resp = client.role_revoke_permission(role1, "b",
                                             Some(RoleRevokePermissionOption::new().with_prefix())).await?;
    println!("{:?}", resp.header());

    let resp = client.role_get(role1).await?;
    //show the result
    println!("Role {}\n{}", role1, resp);

    let resp = client.role_list().await?;
    //show the result
    println!("Roles: \n {:?} ", resp.roles());

    client.role_delete(role1).await?;
    println!("delete role successfully", );
    client.role_delete(role2).await?;
    println!("delete role successfully", );

    let resp = client.role_list().await?;
    //show the result
    println!("Roles: \n {:?} ", resp.roles());

    Ok(())
}