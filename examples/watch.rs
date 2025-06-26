//! Watch example

use etcd_client::*;
use std::{collections::HashMap, time::Duration};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let mut client = Client::connect(["localhost:2379"], None).await?;

    client.put("foo", "bar", None).await?;
    println!("put kv: {{foo: bar}}");

    client.put("foo1", "bar1", None).await?;
    println!("put kv: {{foo1: bar1}}");

    // Record watch IDs and their starting revisions
    let mut watches: HashMap<i64 /* watch ID */, i64 /* revision */> = HashMap::new();

    let opts = WatchOptions::new().with_watch_id(1);
    let mut watch_stream = client.watch("foo", Some(opts)).await?;
    let resp = watch_stream
        .message()
        .await?
        .ok_or(Error::WatchError("No initial watch response".into()))?;
    let resp = match (resp.created(), resp.canceled()) {
        (true, false) => Ok(resp),
        (true, true) => Err(Error::WatchError(resp.cancel_reason().into())),
        _ => Err(Error::WatchError("Unexpected watch response".into())),
    }?;

    println!("create watcher, watch ID: {}", resp.watch_id());
    println!();
    let rev = resp
        .header()
        .map(|h| h.revision())
        .ok_or_else(|| Error::WatchError("No header revision in initial watch response".into()))?;
    watches.insert(resp.watch_id(), rev);

    client.put("foo", "bar2", None).await?;
    watch_stream.request_progress().await?;
    client.delete("foo", None).await?;

    // Reuse the same watch stream to watch another key
    let opts = WatchOptions::new().with_watch_id(2);
    watch_stream.watch("foo1", Some(opts)).await?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    client.put("foo1", "bar2", None).await?;
    client.delete("foo1", None).await?;

    // NOTE: The responses to the following watches may receive out of order.
    while let Some(resp) = watch_stream.message().await? {
        let watch_id = resp.watch_id();
        println!("[{}] receive watch response", watch_id);
        println!("compact revision: {}", resp.compact_revision());

        match (resp.created(), resp.canceled()) {
            (true, false) if watch_id == 2 => {
                let rev = resp.header().map(|h| h.revision()).ok_or_else(|| {
                    Error::WatchError("No header revision in watch creation response".into())
                })?;
                watches.insert(resp.watch_id(), rev);
            }
            (true, true) if watch_id == 2 => {
                println!(
                    "watch ID {} creation canceled: {}",
                    resp.watch_id(),
                    resp.cancel_reason()
                );
            }
            (false, true) => {
                watches.remove(&resp.watch_id());
                println!(
                    "watch ID {} canceled: {}, reason: {}",
                    resp.watch_id(),
                    resp.canceled(),
                    resp.cancel_reason()
                );
            }
            (created, canceled) => {
                println!(
                    "watch ID {} created: {}, canceled: {}, reason: {}",
                    resp.watch_id(),
                    created,
                    canceled,
                    resp.cancel_reason()
                );
            }
        }

        for event in resp.events() {
            println!("event type: {:?}", event.event_type());
            if let Some(kv) = event.kv() {
                println!("kv: {{{}: {}}}", kv.key_str()?, kv.value_str()?);
            }

            if EventType::Delete == event.event_type() {
                println!("canceling watch ID {}", resp.watch_id());
                watch_stream.cancel(resp.watch_id()).await?;
            }
        }

        if watches.is_empty() {
            println!("all watches canceled, exiting...");
            break;
        }

        println!();
    }

    Ok(())
}
