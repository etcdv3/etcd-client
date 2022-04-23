//! Watch example

use etcd_client::*;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let mut client = Client::connect(["localhost:2379"], None).await?;

    client.put("foo", "bar", None).await?;
    println!("put kv: {{foo: bar}}");

    let (mut watcher, mut stream) = client.watch("foo", None).await?;
    println!("create watcher {}", watcher.watch_id());
    println!();

    client.put("foo", "bar2", None).await?;
    watcher.request_progress().await?;
    client.delete("foo", None).await?;

    while let Some(resp) = stream.message().await? {
        println!("[{}] receive watch response", resp.watch_id());
        println!("compact revision: {}", resp.compact_revision());

        if resp.canceled() {
            println!("watch canceled: {}", resp.cancel_reason());
            break;
        }

        for event in resp.events() {
            println!("event type: {:?}", event.event_type());
            if let Some(kv) = event.kv() {
                println!("kv: {{{}: {}}}", kv.key_str()?, kv.value_str()?);
            }

            if EventType::Delete == event.event_type() {
                watcher.cancel().await?;
            }
        }

        println!();
    }

    Ok(())
}
