//! Custom resources: `UsbDevice` (cluster-scoped, maintained by agents) and `UsbDeviceClaim`
//! (namespaced, created by users to attach a device to a KubeVirt VM).

// Repeated `printcolumn(type_ = ...)` arguments look like duplicated attributes to clippy.
#![allow(clippy::duplicated_attributes)]

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::identity::{Fingerprint, IdentitySource, normalize_usb_id};

pub const LABEL_VENDOR_ID: &str = "atomicusb.safewords.io/vendor-id";
pub const LABEL_PRODUCT_ID: &str = "atomicusb.safewords.io/product-id";
pub const LABEL_NODE: &str = "atomicusb.safewords.io/node";
pub const LABEL_IDENTITY: &str = "atomicusb.safewords.io/identity";

/// A physical USB device plugged into a node. Maintained by the atomic-usb agent; do not edit.
#[derive(CustomResource, Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "atomicusb.safewords.io",
    version = "v1alpha1",
    kind = "UsbDevice",
    plural = "usbdevices",
    shortname = "usbdev",
    category = "atomic-usb",
    status = "UsbDeviceStatus",
    printcolumn(name = "Vendor", type_ = "string", json_path = ".spec.vendorId"),
    printcolumn(name = "Product", type_ = "string", json_path = ".spec.productId"),
    printcolumn(name = "Description", type_ = "string", json_path = ".spec.product"),
    printcolumn(name = "Identity", type_ = "string", json_path = ".spec.identity"),
    printcolumn(name = "Node", type_ = "string", json_path = ".status.node"),
    printcolumn(name = "Port", type_ = "string", json_path = ".status.portPath", priority = 1),
    printcolumn(name = "Phase", type_ = "string", json_path = ".status.phase"),
    printcolumn(name = "Claim", type_ = "string", json_path = ".status.attachedTo.claim"),
    printcolumn(name = "Age", type_ = "date", json_path = ".metadata.creationTimestamp")
)]
#[serde(rename_all = "camelCase")]
pub struct UsbDeviceSpec {
    /// USB vendor id, 4 lowercase hex digits.
    pub vendor_id: String,
    /// USB product id, 4 lowercase hex digits.
    pub product_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub serial: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manufacturer: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub product: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bcd_device: Option<String>,
    /// Which attributes this object's name was derived from.
    pub identity: IdentitySource,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct UsbDeviceStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<DevicePhase>,
    /// Node the device is (or was last) plugged into.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    /// Physical port path on that node, e.g. `3-1.4`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<i64>")]
    pub bus_num: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<i64>")]
    pub dev_num: Option<u32>,
    /// Negotiated speed in Mbit/s.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub speed: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device_class: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interface_classes: Option<Vec<String>>,
    /// Where the agent owning the device accepts usbredir sessions (pod network).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exporter: Option<ExporterEndpoint>,
    /// RFC 3339 time of the last agent heartbeat.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_seen: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// Exclusive lease: the claim this device is reserved for. Written by the controller with
    /// optimistic concurrency, so at most one claim ever holds a device.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attached_to: Option<Attachment>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum DevicePhase {
    /// Plugged in and exported by a healthy agent.
    Available,
    /// Removed from its node.
    Unplugged,
    /// The agent on its node stopped sending heartbeats.
    Lost,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ExporterEndpoint {
    /// Pod IP of the exporting agent.
    pub address: String,
    #[schemars(with = "i32")]
    pub port: u16,
    pub pod: String,
}

impl ExporterEndpoint {
    pub fn socket_addr(&self) -> String {
        if self.address.contains(':') {
            format!("[{}]:{}", self.address, self.port)
        } else {
            format!("{}:{}", self.address, self.port)
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Attachment {
    pub namespace: String,
    pub claim: String,
    pub claim_uid: String,
    pub vm_name: String,
    /// RFC 3339 time the lease was acquired.
    pub since: String,
}

impl UsbDevice {
    pub fn fingerprint(&self) -> Fingerprint {
        Fingerprint {
            vendor_id: self.spec.vendor_id.clone(),
            product_id: self.spec.product_id.clone(),
            serial: self.spec.serial.clone(),
            bcd_device: self.spec.bcd_device.clone(),
            manufacturer: self.spec.manufacturer.clone(),
            product: self.spec.product.clone(),
        }
    }

    pub fn status_ref(&self) -> &UsbDeviceStatus {
        static EMPTY: UsbDeviceStatus = UsbDeviceStatus {
            phase: None,
            node: None,
            port_path: None,
            bus_num: None,
            dev_num: None,
            speed: None,
            device_class: None,
            interface_classes: None,
            exporter: None,
            last_seen: None,
            message: None,
            attached_to: None,
        };
        self.status.as_ref().unwrap_or(&EMPTY)
    }

    /// Available with an exporter and a heartbeat newer than `lost_after_secs`.
    pub fn is_available(&self, now: jiff::Timestamp, lost_after_secs: i64) -> bool {
        let status = self.status_ref();
        status.phase == Some(DevicePhase::Available)
            && status.exporter.is_some()
            && crate::util::age_secs(status.last_seen.as_deref(), now).is_some_and(|age| age <= lost_after_secs)
    }

    pub fn lease_holder(&self) -> Option<&str> {
        self.status_ref().attached_to.as_ref().map(|a| a.claim_uid.as_str())
    }
}

/// Requests exclusive access to a USB device for a KubeVirt VM in the same namespace.
///
/// The VM must have `spec.template.spec.domain.devices.clientPassthrough: {}`. The device is
/// hot-plugged into the running VM as soon as both are available, and re-attached automatically
/// after the device is replugged (on any node) or the VM restarts or migrates.
#[derive(CustomResource, Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "atomicusb.safewords.io",
    version = "v1alpha1",
    kind = "UsbDeviceClaim",
    plural = "usbdeviceclaims",
    shortname = "usbclaim",
    category = "atomic-usb",
    namespaced,
    status = "UsbDeviceClaimStatus",
    printcolumn(name = "VM", type_ = "string", json_path = ".spec.vmName"),
    printcolumn(name = "Phase", type_ = "string", json_path = ".status.phase"),
    printcolumn(name = "Device", type_ = "string", json_path = ".status.deviceName"),
    printcolumn(name = "Device-Node", type_ = "string", json_path = ".status.sourceNode"),
    printcolumn(name = "VM-Node", type_ = "string", json_path = ".status.connection.node"),
    printcolumn(name = "Slot", type_ = "integer", json_path = ".status.slot", priority = 1),
    printcolumn(name = "Message", type_ = "string", json_path = ".status.message", priority = 1),
    printcolumn(name = "Age", type_ = "date", json_path = ".metadata.creationTimestamp")
)]
#[serde(rename_all = "camelCase")]
pub struct UsbDeviceClaimSpec {
    /// Name of the VirtualMachine / VirtualMachineInstance in the claim's namespace.
    pub vm_name: String,
    /// Which device to attach. All specified fields must match; at least one is required.
    pub selector: DeviceSelector,
    /// Pin the usbredir slot (0-3). By default slots are allocated from 3 downwards, because
    /// `virtctl usbredir` allocates from 0 upwards.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 0, max = 3))]
    pub slot: Option<i32>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DeviceSelector {
    /// Exact `UsbDevice` object name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device_name: Option<String>,
    /// USB vendor id (hex, e.g. `10c4`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vendor_id: Option<String>,
    /// USB product id (hex, e.g. `ea60`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub product_id: Option<String>,
    /// Exact serial number.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub serial: Option<String>,
    /// Exact product string.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub product: Option<String>,
    /// Only devices plugged into this node.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    /// Only devices plugged into this port path (combine with `node`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port_path: Option<String>,
}

impl DeviceSelector {
    pub fn validate(&self) -> Result<(), String> {
        let any = [
            &self.device_name,
            &self.vendor_id,
            &self.product_id,
            &self.serial,
            &self.product,
            &self.node,
            &self.port_path,
        ]
        .iter()
        .any(|f| f.is_some());
        if !any {
            return Err("selector must set at least one field".into());
        }
        for (field, value) in [("vendorId", &self.vendor_id), ("productId", &self.product_id)] {
            if let Some(v) = value
                && normalize_usb_id(v).is_none()
            {
                return Err(format!("selector.{field} {v:?} is not a 16-bit hex id"));
            }
        }
        Ok(())
    }

    pub fn matches(&self, device: &UsbDevice) -> bool {
        let status = device.status_ref();
        let id_matches = |want: &Option<String>, have: &str| {
            want.as_deref()
                .is_none_or(|w| normalize_usb_id(w).as_deref() == Some(have))
        };
        let exact = |want: &Option<String>, have: Option<&str>| want.as_deref().is_none_or(|w| Some(w) == have);
        exact(&self.device_name, device.metadata.name.as_deref())
            && id_matches(&self.vendor_id, &device.spec.vendor_id)
            && id_matches(&self.product_id, &device.spec.product_id)
            && exact(&self.serial, device.spec.serial.as_deref())
            && exact(&self.product, device.spec.product.as_deref())
            && exact(&self.node, status.node.as_deref())
            && exact(&self.port_path, status.port_path.as_deref())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct UsbDeviceClaimStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<ClaimPhase>,
    /// `UsbDevice` leased to this claim.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device_name: Option<String>,
    /// Node the leased device is plugged into.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_node: Option<String>,
    /// usbredir slot (`virt-usbredir-N` socket) used in the VM.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slot: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// Data path state, reported by the agent on the VM's node.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection: Option<ConnectionStatus>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum ClaimPhase {
    /// No matching device is free.
    Pending,
    /// The leased device is unplugged or its node is unreachable.
    DeviceUnavailable,
    /// The VM is not running.
    WaitingForVM,
    /// The VM lacks `devices.clientPassthrough`.
    VMNotConfigured,
    /// Everything is ready; the data path is being established.
    Connecting,
    /// The device is attached to the VM.
    Attached,
    /// The claim is invalid (see message).
    Invalid,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionStatus {
    pub state: ConnectionState,
    /// Node of the VM, where the connection is terminated.
    pub node: String,
    pub device_name: String,
    pub slot: i32,
    /// RFC 3339 time of the last state change.
    pub since: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum ConnectionState {
    Connecting,
    Connected,
    Disconnected,
}

#[cfg(test)]
mod tests {
    use kube::CustomResourceExt;

    use super::*;

    fn device(name: &str, vid: &str, pid: &str, serial: Option<&str>, node: &str, port: &str) -> UsbDevice {
        let mut d = UsbDevice::new(
            name,
            UsbDeviceSpec {
                vendor_id: vid.into(),
                product_id: pid.into(),
                serial: serial.map(Into::into),
                product: Some("USB Serial".into()),
                ..Default::default()
            },
        );
        d.status = Some(UsbDeviceStatus {
            node: Some(node.into()),
            port_path: Some(port.into()),
            ..Default::default()
        });
        d
    }

    #[test]
    fn selector_matching() {
        let d = device("usb-1a86-7523-usb-serial-abcd", "1a86", "7523", None, "node-c", "3-3");
        let sel = |f: fn(&mut DeviceSelector)| {
            let mut s = DeviceSelector::default();
            f(&mut s);
            s
        };
        assert!(sel(|s| s.vendor_id = Some("1A86".into())).matches(&d));
        assert!(sel(|s| s.vendor_id = Some("0x1a86".into())).matches(&d));
        assert!(
            sel(|s| {
                s.vendor_id = Some("1a86".into());
                s.product_id = Some("7523".into());
                s.node = Some("node-c".into())
            })
            .matches(&d)
        );
        assert!(!sel(|s| s.node = Some("node-b".into())).matches(&d));
        assert!(!sel(|s| s.serial = Some("x".into())).matches(&d));
        assert!(sel(|s| s.product = Some("USB Serial".into())).matches(&d));
        assert!(sel(|s| s.device_name = Some("usb-1a86-7523-usb-serial-abcd".into())).matches(&d));
    }

    #[test]
    fn selector_validation() {
        assert!(DeviceSelector::default().validate().is_err());
        assert!(
            DeviceSelector {
                vendor_id: Some("zz".into()),
                ..Default::default()
            }
            .validate()
            .is_err()
        );
        assert!(
            DeviceSelector {
                vendor_id: Some("1a86".into()),
                ..Default::default()
            }
            .validate()
            .is_ok()
        );
    }

    #[test]
    fn crds_render() {
        let yaml = serde_yaml::to_string(&UsbDevice::crd()).unwrap();
        assert!(yaml.contains("usbdevices.atomicusb.safewords.io"));
        assert!(yaml.contains("status: {}"), "status subresource enabled");
        let yaml = serde_yaml::to_string(&UsbDeviceClaim::crd()).unwrap();
        assert!(yaml.contains("scope: Namespaced"));
    }

    #[test]
    fn ipv6_exporter_address() {
        let e = ExporterEndpoint {
            address: "fd00::1".into(),
            port: 7575,
            pod: "p".into(),
        };
        assert_eq!(e.socket_addr(), "[fd00::1]:7575");
    }
}
