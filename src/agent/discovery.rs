//! Discovers USB devices on this node and keeps their `UsbDevice` objects up to date.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use kube::api::{Patch, PatchParams};
use kube::runtime::reflector::Store;
use kube::{Api, Resource, ResourceExt};
use serde_json::{Value, json};
use tokio::sync::watch;
use tracing::{debug, info, warn};

use super::{AgentConfig, LocalDevices};
use crate::crd::{self, DevicePhase, UsbDevice, UsbDeviceSpec};
use crate::identity::{
    self, AssignContext, Assignment, Decision, Fingerprint, IdentitySource, InstanceKey, KnownDevice,
};
use crate::sysfs::{self, UsbDeviceInfo};
use crate::uevent::UeventSocket;
use crate::util;

const FIELD_MANAGER: &str = "atomic-usb-agent";
/// Time to let a burst of uevents (and late descriptor attributes) settle before rescanning.
const UEVENT_SETTLE: Duration = Duration::from_millis(500);
/// Rescan interval while an identity is waiting to be released by another node.
const DEFERRED_RESCAN: Duration = Duration::from_secs(3);

pub struct Discovery {
    config: Arc<AgentConfig>,
    api: Api<UsbDevice>,
    store: Store<UsbDevice>,
    local_tx: watch::Sender<LocalDevices>,
    sticky: HashMap<InstanceKey, Assignment>,
    first_seen: HashMap<InstanceKey, Instant>,
    deferred_logged: HashSet<InstanceKey>,
    /// Last status written per device (without `lastSeen`) and when, to avoid duplicate writes
    /// while the watch cache catches up.
    status_writes: HashMap<String, (Value, Instant)>,
}

impl Discovery {
    pub fn new(
        config: Arc<AgentConfig>,
        api: Api<UsbDevice>,
        store: Store<UsbDevice>,
        local_tx: watch::Sender<LocalDevices>,
    ) -> Self {
        Self {
            config,
            api,
            store,
            local_tx,
            sticky: HashMap::new(),
            first_seen: HashMap::new(),
            deferred_logged: HashSet::new(),
            status_writes: HashMap::new(),
        }
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        self.recover_assignments();
        let uevents = match UeventSocket::open() {
            Ok(socket) => Some(socket),
            Err(err) => {
                warn!(%err, "kernel uevents unavailable; relying on periodic rescans");
                None
            }
        };

        loop {
            let deferred = match self.scan_once().await {
                Ok(deferred) => deferred,
                Err(err) => {
                    warn!("scan failed: {err:#}");
                    false
                }
            };
            let interval = if deferred {
                DEFERRED_RESCAN.min(self.config.scan_interval)
            } else {
                self.config.scan_interval
            };
            wait_for_usb_event(uevents.as_ref(), interval).await;
        }
    }

    /// After a restart, re-adopt the names of devices this node published that are still plugged
    /// into the same port with the same device number (i.e. the same enumeration).
    fn recover_assignments(&mut self) {
        for obj in self.store.state() {
            let status = obj.status_ref();
            if status.node.as_deref() != Some(self.config.node.as_str()) || status.phase != Some(DevicePhase::Available)
            {
                continue;
            }
            if let (Some(port_path), Some(bus_num), Some(dev_num)) =
                (status.port_path.clone(), status.bus_num, status.dev_num)
            {
                let key = InstanceKey {
                    port_path,
                    bus_num,
                    dev_num,
                };
                self.sticky.insert(
                    key,
                    Assignment {
                        name: obj.name_any(),
                        source: obj.spec.identity,
                    },
                );
            }
        }
        if !self.sticky.is_empty() {
            info!(
                devices = self.sticky.len(),
                "recovered device identities from previous run"
            );
        }
    }

    /// Returns whether any device is waiting for another node to release its identity.
    async fn scan_once(&mut self) -> anyhow::Result<bool> {
        let root = self.config.sysfs_root.clone();
        let mut devices = tokio::task::spawn_blocking(move || sysfs::scan(&root)).await??;
        devices
            .retain(|d| (self.config.include_hubs || !d.is_hub()) && !self.config.ignore.iter().any(|p| p.matches(d)));

        let now = util::now();
        let lost_after = self.config.lost_after.as_secs() as i64;
        let objects = self.store.state();
        let objects_by_name: HashMap<String, &UsbDevice> = objects.iter().map(|o| (o.name_any(), o.as_ref())).collect();

        // Forget instances that disappeared, and recovered assignments whose device changed.
        let fingerprints: HashMap<InstanceKey, Fingerprint> = devices
            .iter()
            .map(|d| (InstanceKey::from(d), Fingerprint::from(d)))
            .collect();
        self.sticky.retain(|key, assignment| {
            fingerprints.get(key).is_some_and(|fp| {
                objects_by_name
                    .get(&assignment.name)
                    .is_none_or(|obj| obj.fingerprint() == *fp)
            })
        });
        let started = Instant::now();
        self.first_seen.retain(|key, _| fingerprints.contains_key(key));
        self.deferred_logged.retain(|key| fingerprints.contains_key(key));
        for key in fingerprints.keys() {
            self.first_seen.entry(key.clone()).or_insert(started);
        }
        let may_defer: HashSet<InstanceKey> = self
            .first_seen
            .iter()
            .filter(|(_, seen)| started.duration_since(**seen) < self.config.lost_after)
            .map(|(key, _)| key.clone())
            .collect();

        let known: Vec<KnownDevice> = objects
            .iter()
            .map(|o| KnownDevice {
                name: o.name_any(),
                fingerprint: o.fingerprint(),
                present_on: if o.is_available(now, lost_after) {
                    o.status_ref().node.clone()
                } else {
                    None
                },
            })
            .collect();
        let ctx = AssignContext {
            node: &self.config.node,
            sticky: &self.sticky,
            known: &known,
            may_defer: &may_defer,
        };
        let decisions = identity::assign(&ctx, &devices);

        let mut local: BTreeMap<String, (UsbDeviceInfo, IdentitySource)> = BTreeMap::new();
        let mut deferred = false;
        for device in &devices {
            let key = InstanceKey::from(device);
            match &decisions[&key] {
                Decision::Assigned(assignment) => {
                    if self.sticky.get(&key) != Some(assignment) {
                        info!(
                            device = %assignment.name,
                            identity = ?assignment.source,
                            port = %device.port_path,
                            id = %format!("{}:{}", device.vendor_id, device.product_id),
                            product = device.product.as_deref().unwrap_or(""),
                            "device plugged in"
                        );
                        self.sticky.insert(key, assignment.clone());
                    }
                    local.insert(assignment.name.clone(), (device.clone(), assignment.source));
                }
                Decision::Deferred { name, held_by } => {
                    deferred = true;
                    if self.deferred_logged.insert(key) {
                        info!(device = %name, %held_by, port = %device.port_path, "waiting for the previous node to release the device identity");
                    }
                }
            }
        }

        // Publish the local view first so the exporter drops sessions of removed devices at once.
        self.local_tx.send_if_modified(|current| {
            let next: BTreeMap<String, UsbDeviceInfo> =
                local.iter().map(|(n, (d, _))| (n.clone(), d.clone())).collect();
            if **current == next {
                return false;
            }
            *current = Arc::new(next);
            true
        });

        for (name, (device, source)) in &local {
            if let Err(err) = self
                .publish(name, device, *source, objects_by_name.get(name).copied(), now)
                .await
            {
                warn!(device = %name, %err, "failed to publish device");
            }
        }
        for obj in &objects {
            let status = obj.status_ref();
            if status.node.as_deref() == Some(self.config.node.as_str())
                && status.phase == Some(DevicePhase::Available)
                && !local.contains_key(&obj.name_any())
            {
                self.mark_unplugged(obj, now).await;
            }
        }
        self.status_writes.retain(|name, _| local.contains_key(name));
        Ok(deferred)
    }

    async fn publish(
        &mut self,
        name: &str,
        device: &UsbDeviceInfo,
        source: IdentitySource,
        existing: Option<&UsbDevice>,
        now: jiff::Timestamp,
    ) -> kube::Result<()> {
        let spec = UsbDeviceSpec {
            vendor_id: device.vendor_id.clone(),
            product_id: device.product_id.clone(),
            serial: device.serial.clone(),
            manufacturer: device.manufacturer.clone(),
            product: device.product.clone(),
            bcd_device: device.bcd_device.clone(),
            identity: source,
        };
        let labels: BTreeMap<String, String> = [
            (crd::LABEL_VENDOR_ID, Some(device.vendor_id.clone())),
            (crd::LABEL_PRODUCT_ID, Some(device.product_id.clone())),
            (crd::LABEL_NODE, identity::label_value(&self.config.node)),
            (crd::LABEL_IDENTITY, Some(format!("{source:?}"))),
        ]
        .into_iter()
        .filter_map(|(k, v)| Some((k.to_string(), v?)))
        .collect();

        let spec_current =
            existing.is_some_and(|o| o.spec == spec && labels.iter().all(|(k, v)| o.labels().get(k) == Some(v)));
        if !spec_current {
            let object = json!({
                "apiVersion": UsbDevice::api_version(&()),
                "kind": UsbDevice::kind(&()),
                "metadata": { "name": name, "labels": labels },
                "spec": spec,
            });
            self.api
                .patch(name, &PatchParams::apply(FIELD_MANAGER).force(), &Patch::Apply(&object))
                .await?;
        }

        let desired = json!({
            "phase": DevicePhase::Available,
            "node": self.config.node,
            "portPath": device.port_path,
            "busNum": device.bus_num,
            "devNum": device.dev_num,
            "speed": device.speed,
            "deviceClass": format!("{:02x}", device.device_class),
            "interfaceClasses": device.interface_classes.iter().map(|c| format!("{c:02x}")).collect::<Vec<_>>(),
            "exporter": {
                "address": self.config.pod_ip,
                "port": self.config.listen.port(),
                "pod": self.config.pod_name,
            },
            "message": null,
        });
        let observed = existing.and_then(|o| o.status.as_ref());
        let status_current = observed.is_some_and(|s| {
            let mut s = serde_json::to_value(s).unwrap_or_default();
            if let Some(map) = s.as_object_mut() {
                map.remove("lastSeen");
                map.remove("attachedTo");
            }
            without_nulls(&desired) == s
        });
        let heartbeat_due = observed
            .and_then(|s| util::age_secs(s.last_seen.as_deref(), now))
            .is_none_or(|age| age >= self.config.heartbeat_interval.as_secs() as i64);
        let written_recently = self.status_writes.get(name).is_some_and(|(value, at)| {
            *value == desired && at.elapsed() < self.config.heartbeat_interval.min(Duration::from_secs(10))
        });
        if (status_current && !heartbeat_due) || written_recently {
            return Ok(());
        }

        let mut body = desired.clone();
        body["lastSeen"] = json!(util::rfc3339(now));
        self.api
            .patch_status(name, &PatchParams::default(), &Patch::Merge(&json!({ "status": body })))
            .await?;
        if !status_current {
            debug!(device = %name, "status updated");
        }
        self.status_writes.insert(name.to_string(), (desired, Instant::now()));
        Ok(())
    }

    /// Marks a device this node published as unplugged, unless another node took it over in the
    /// meantime (guarded by resourceVersion).
    async fn mark_unplugged(&self, obj: &UsbDevice, now: jiff::Timestamp) {
        let name = obj.name_any();
        let patch = json!({
            "metadata": { "resourceVersion": obj.resource_version() },
            "status": {
                "phase": DevicePhase::Unplugged,
                "exporter": null,
                "busNum": null,
                "devNum": null,
                "message": format!("unplugged from {} at {}", self.config.node, util::rfc3339(now)),
            }
        });
        match self
            .api
            .patch_status(&name, &PatchParams::default(), &Patch::Merge(&patch))
            .await
        {
            Ok(_) => info!(device = %name, "device unplugged"),
            Err(err) if util::is_conflict(&err) || util::is_not_found(&err) => {
                debug!(device = %name, "device changed concurrently; re-evaluating on next scan")
            }
            Err(err) => warn!(device = %name, %err, "failed to mark device unplugged"),
        }
    }
}

fn without_nulls(value: &Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.iter()
                .filter(|(_, v)| !v.is_null())
                .map(|(k, v)| (k.clone(), without_nulls(v)))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// Waits for a USB-related uevent (then lets the burst settle) or until `interval` elapses.
async fn wait_for_usb_event(uevents: Option<&UeventSocket>, interval: Duration) {
    let Some(socket) = uevents else {
        tokio::time::sleep(interval).await;
        return;
    };
    let deadline = tokio::time::sleep(interval);
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = &mut deadline => return,
            event = socket.recv() => match event {
                Ok(Some(ev)) if ev.affects_usb_devices() => {
                    debug!(action = %ev.action, devpath = %ev.devpath, "usb uevent");
                    break;
                }
                Ok(_) => continue,
                Err(err) => {
                    // ENOBUFS: events were dropped, so rescan everything.
                    debug!(%err, "uevent receive error");
                    break;
                }
            }
        }
    }
    // Swallow the rest of the burst, but never postpone the rescan indefinitely.
    let settle_until = tokio::time::Instant::now() + UEVENT_SETTLE * 4;
    while tokio::time::Instant::now() < settle_until {
        if !matches!(tokio::time::timeout(UEVENT_SETTLE, socket.recv()).await, Ok(Ok(_))) {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_nulls_recursively() {
        let v = json!({"a": null, "b": {"c": null, "d": 1}, "e": [null]});
        assert_eq!(without_nulls(&v), json!({"b": {"d": 1}, "e": [null]}));
    }
}
