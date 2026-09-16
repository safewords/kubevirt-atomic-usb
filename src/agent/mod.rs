//! Node agent (DaemonSet). On every node it
//!
//! * discovers USB devices and publishes them as `UsbDevice` objects ([`discovery`]),
//! * exports locally plugged devices to other nodes over the pod network ([`exporter`]),
//! * attaches leased devices to VMs running on this node ([`attacher`]).

pub mod attacher;
pub mod discovery;
pub mod exporter;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use kube::api::DynamicObject;
use kube::{Api, Client};
use tokio::sync::watch;
use tracing::info;

use crate::crd::{UsbDevice, UsbDeviceClaim};
use crate::kubevirt::vmi_resource;
use crate::launcher::SocketLocator;
use crate::sysfs::UsbDeviceInfo;

/// Devices currently plugged into this node, keyed by `UsbDevice` name.
pub type LocalDevices = Arc<BTreeMap<String, UsbDeviceInfo>>;

#[derive(Clone, Debug)]
pub struct AgentConfig {
    pub node: String,
    pub pod_name: String,
    pub pod_ip: String,
    pub listen: SocketAddr,
    pub psk_file: PathBuf,
    pub sysfs_root: PathBuf,
    pub locator: SocketLocator,
    pub usbredirect: PathBuf,
    pub include_hubs: bool,
    pub ignore: Vec<DevicePattern>,
    pub scan_interval: Duration,
    pub heartbeat_interval: Duration,
    pub lost_after: Duration,
}

/// `vendor:product` pattern where either side may be `*`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DevicePattern {
    vendor_id: Option<String>,
    product_id: Option<String>,
}

impl std::str::FromStr for DevicePattern {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (vendor, product) = s
            .split_once(':')
            .ok_or_else(|| format!("{s:?}: expected VENDOR:PRODUCT"))?;
        let part = |p: &str| -> Result<Option<String>, String> {
            if p == "*" {
                return Ok(None);
            }
            crate::identity::normalize_usb_id(p)
                .map(Some)
                .ok_or_else(|| format!("{s:?}: {p:?} is not a hex id or *"))
        };
        Ok(Self {
            vendor_id: part(vendor)?,
            product_id: part(product)?,
        })
    }
}

impl DevicePattern {
    pub fn matches(&self, device: &UsbDeviceInfo) -> bool {
        self.vendor_id.as_ref().is_none_or(|v| *v == device.vendor_id)
            && self.product_id.as_ref().is_none_or(|p| *p == device.product_id)
    }
}

pub async fn run(client: Client, config: AgentConfig) -> anyhow::Result<()> {
    let config = Arc::new(config);
    crate::watch::read_psk(&config.psk_file).context("agent needs the pre-shared key")?;
    info!(node = %config.node, listen = %config.listen, "starting agent");

    let changes = crate::watch::changes();
    let devices_api: Api<UsbDevice> = Api::all(client.clone());
    let claims_api: Api<UsbDeviceClaim> = Api::all(client.clone());
    let vmi_api: Api<DynamicObject> = Api::all_with(client.clone(), &vmi_resource());

    let devices = crate::watch::reflect(devices_api.clone(), (), &changes);
    let claims = crate::watch::reflect(claims_api.clone(), (), &changes);
    let vmis = crate::watch::reflect_with(vmi_api, vmi_resource(), Default::default(), &changes);
    tokio::try_join!(
        devices.wait_until_ready(),
        claims.wait_until_ready(),
        vmis.wait_until_ready()
    )
    .context("waiting for initial lists")?;
    info!(
        devices = devices.len(),
        claims = claims.len(),
        vmis = vmis.len(),
        "caches synced"
    );

    let (local_tx, local_rx) = watch::channel(LocalDevices::default());

    let exporter = exporter::Exporter::new(config.clone(), devices_api.clone(), local_rx.clone());
    let discovery = discovery::Discovery::new(config.clone(), devices_api, devices.clone(), local_tx);
    let attacher = attacher::Attacher::new(config.clone(), claims_api, claims, devices, vmis, changes.subscribe());

    tokio::select! {
        r = exporter.run() => r.context("exporter"),
        r = discovery.run() => r.context("discovery"),
        r = attacher.run() => r.context("attacher"),
        _ = shutdown_signal() => {
            info!("shutting down");
            Ok(())
        }
    }
}

pub async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut term =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("SIGTERM handler");
        tokio::select! {
            _ = term.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}

/// Applies nodelay and aggressive keepalives so dead peers (e.g. a crashed node) are noticed
/// within about a minute instead of the kernel default of two hours.
pub fn tune_tcp(stream: &tokio::net::TcpStream) {
    let _ = stream.set_nodelay(true);
    let sock = socket2::SockRef::from(stream);
    let keepalive = socket2::TcpKeepalive::new()
        .with_time(Duration::from_secs(20))
        .with_interval(Duration::from_secs(10));
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    let keepalive = keepalive.with_retries(4);
    let _ = sock.set_tcp_keepalive(&keepalive);
    #[cfg(target_os = "linux")]
    let _ = sock.set_tcp_user_timeout(Some(Duration::from_secs(60)));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info(vid: &str, pid: &str) -> UsbDeviceInfo {
        UsbDeviceInfo {
            port_path: "1-1".into(),
            bus_num: 1,
            dev_num: 2,
            vendor_id: vid.into(),
            product_id: pid.into(),
            serial: None,
            manufacturer: None,
            product: None,
            bcd_device: None,
            device_class: 0,
            interface_classes: vec![],
            speed: None,
        }
    }

    #[test]
    fn device_patterns() {
        let p: DevicePattern = "abcd:*".parse().unwrap();
        assert!(p.matches(&info("abcd", "0601")));
        assert!(!p.matches(&info("1a86", "7523")));
        let p: DevicePattern = "*:7523".parse().unwrap();
        assert!(p.matches(&info("1a86", "7523")));
        let p: DevicePattern = "1234:5678".parse().unwrap();
        assert!(p.matches(&info("1234", "5678")));
        assert!("1234".parse::<DevicePattern>().is_err());
        assert!("xx:5678".parse::<DevicePattern>().is_err());
    }
}
