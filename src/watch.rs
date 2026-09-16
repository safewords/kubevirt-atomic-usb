//! Reflector plumbing shared by the agent and the controller.

use std::fmt::Debug;
use std::hash::Hash;
use std::path::Path;
use std::time::Duration;

use futures::StreamExt;
use kube::runtime::reflector::{self, Store};
use kube::runtime::{WatchStreamExt, watcher};
use kube::{Api, Resource};
use serde::de::DeserializeOwned;
use tokio::sync::watch as tokio_watch;
use tracing::warn;

/// Bumped whenever any watched store changes.
pub type Changes = tokio_watch::Sender<u64>;

pub fn changes() -> Changes {
    tokio_watch::Sender::new(0)
}

/// Starts a reflector for `api` and returns its store; every event bumps `changes`.
pub fn reflect<K>(api: Api<K>, dyntype: K::DynamicType, changes: &Changes) -> Store<K>
where
    K: Resource + Clone + DeserializeOwned + Debug + Send + Sync + 'static,
    K::DynamicType: Eq + Hash + Clone + Default + Send + Sync,
{
    reflect_with(api, dyntype, watcher::Config::default(), changes)
}

pub fn reflect_with<K>(api: Api<K>, dyntype: K::DynamicType, config: watcher::Config, changes: &Changes) -> Store<K>
where
    K: Resource + Clone + DeserializeOwned + Debug + Send + Sync + 'static,
    K::DynamicType: Eq + Hash + Clone + Send + Sync,
{
    let writer = reflector::store::Writer::new(dyntype);
    let store = writer.as_reader();
    let changes = changes.clone();
    let kind = std::any::type_name::<K>()
        .rsplit("::")
        .next()
        .unwrap_or("object")
        .to_string();
    tokio::spawn(async move {
        let mut stream = watcher(api, config).default_backoff().reflect(writer).boxed();
        while let Some(event) = stream.next().await {
            match event {
                Ok(_) => changes.send_modify(|v| *v = v.wrapping_add(1)),
                Err(err) => warn!(kind, %err, "watch error"),
            }
        }
    });
    store
}

/// Waits until `changes` is bumped or `timeout` elapses, then lets further events settle for
/// `debounce` so bursts are handled in one pass.
pub async fn wait_for_change(rx: &mut tokio_watch::Receiver<u64>, timeout: Duration, debounce: Duration) {
    if tokio::time::timeout(timeout, rx.changed()).await.is_ok() {
        tokio::time::sleep(debounce).await;
    }
    rx.mark_unchanged();
}

/// Reads the pre-shared key, trimming surrounding whitespace.
pub fn read_psk(path: &Path) -> anyhow::Result<Vec<u8>> {
    let raw = std::fs::read(path).map_err(|e| anyhow::anyhow!("reading pre-shared key {}: {e}", path.display()))?;
    let key = raw.trim_ascii().to_vec();
    anyhow::ensure!(
        key.len() >= 16,
        "pre-shared key {} is shorter than 16 bytes",
        path.display()
    );
    Ok(key)
}
