mod testing;

use crate::testing::{get_client, Result};
use etcd_client::KvClientPrefix;

#[tokio::test]
async fn test_namespace_put_get() -> Result<()> {
    let mut client = get_client().await?;
    let mut c = KvClientPrefix::new(client.kv_client(), "foo/".into());

    c.put("abc", "bar", None).await?;
    let kvs = c.get("abc", None).await?.take_kvs();
    assert_eq!(kvs.len(), 1);
    assert_eq!(kvs[0].key(), "abc".as_bytes());
    assert_eq!(kvs[0].value(), "bar".as_bytes());

    let kvs = client.get("foo/abc", None).await?.take_kvs();
    assert_eq!(kvs.len(), 1);
    assert_eq!(kvs[0].key(), "foo/abc".as_bytes());
    assert_eq!(kvs[0].value(), "bar".as_bytes());
    Ok(())
}
