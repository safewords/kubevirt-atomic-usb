//! Enumeration of USB devices from sysfs (`/sys/bus/usb/devices`).

use std::fs;
use std::io;
use std::path::Path;

use serde::Serialize;

/// USB class code for hubs; hubs are skipped unless explicitly requested.
pub const CLASS_HUB: u8 = 0x09;

/// A physical USB device as seen by the kernel on this node.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UsbDeviceInfo {
    /// Kernel device name, which is the physical port path, e.g. `3-1.4` (also the usbip "busid").
    pub port_path: String,
    pub bus_num: u32,
    pub dev_num: u32,
    /// Lowercase 4-digit hex, e.g. `1a86`.
    pub vendor_id: String,
    pub product_id: String,
    pub serial: Option<String>,
    pub manufacturer: Option<String>,
    pub product: Option<String>,
    /// Device release number, e.g. `0264`.
    pub bcd_device: Option<String>,
    pub device_class: u8,
    pub interface_classes: Vec<u8>,
    /// Negotiated speed in Mbit/s as reported by the kernel, e.g. `12`, `480`, `5000`.
    pub speed: Option<String>,
}

impl UsbDeviceInfo {
    /// Argument for `usbredirect --device`: `BUS-DEVNUM`.
    pub fn bus_dev(&self) -> String {
        format!("{}-{}", self.bus_num, self.dev_num)
    }

    pub fn is_hub(&self) -> bool {
        self.device_class == CLASS_HUB
    }
}

/// Scans `<sysfs_root>/bus/usb/devices` and returns all fully enumerated non-root-hub devices,
/// sorted by port path.
///
/// Devices that are still enumerating (missing descriptors) are skipped; the caller rescans later.
pub fn scan(sysfs_root: &Path) -> io::Result<Vec<UsbDeviceInfo>> {
    let dir = sysfs_root.join("bus/usb/devices");
    let mut names: Vec<String> = fs::read_dir(&dir)?
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| entry.file_name().into_string().ok())
        .collect();
    names.sort();

    let mut devices = Vec::new();
    for name in &names {
        // Interfaces look like `3-1.4:1.0`, root hubs like `usb3`.
        if name.contains(':') || name.starts_with("usb") {
            continue;
        }
        let path = dir.join(name);
        let Some(mut device) = read_device(&path, name) else {
            continue;
        };
        let prefix = format!("{name}:");
        device.interface_classes = names
            .iter()
            .filter(|iface| iface.starts_with(&prefix))
            .filter_map(|iface| read_hex_u8(&dir.join(iface), "bInterfaceClass"))
            .collect();
        devices.push(device);
    }
    Ok(devices)
}

fn read_device(path: &Path, name: &str) -> Option<UsbDeviceInfo> {
    Some(UsbDeviceInfo {
        port_path: name.to_string(),
        bus_num: read_attr(path, "busnum")?.parse().ok()?,
        dev_num: read_attr(path, "devnum")?.parse().ok()?,
        vendor_id: crate::identity::normalize_usb_id(&read_attr(path, "idVendor")?)?,
        product_id: crate::identity::normalize_usb_id(&read_attr(path, "idProduct")?)?,
        serial: read_attr(path, "serial"),
        manufacturer: read_attr(path, "manufacturer"),
        product: read_attr(path, "product"),
        bcd_device: read_attr(path, "bcdDevice"),
        device_class: read_hex_u8(path, "bDeviceClass")?,
        interface_classes: Vec::new(),
        speed: read_attr(path, "speed"),
    })
}

/// Reads a sysfs attribute, trimming the trailing newline; empty or unreadable attributes are `None`.
fn read_attr(dir: &Path, attr: &str) -> Option<String> {
    let raw = fs::read(dir.join(attr)).ok()?;
    let text = String::from_utf8_lossy(&raw);
    let value = text.trim_end_matches(['\n', '\0']).trim();
    (!value.is_empty()).then(|| value.to_string())
}

fn read_hex_u8(dir: &Path, attr: &str) -> Option<u8> {
    u8::from_str_radix(&read_attr(dir, attr)?, 16).ok()
}

#[cfg(test)]
pub mod fake {
    //! Builds fake sysfs trees for tests.
    use std::fs;
    use std::path::Path;

    pub struct FakeDevice<'a> {
        pub port_path: &'a str,
        pub bus: u32,
        pub dev: u32,
        pub vendor: &'a str,
        pub product_id: &'a str,
        pub serial: Option<&'a str>,
        pub class: &'a str,
        pub interfaces: &'a [&'a str],
    }

    pub fn add_device(root: &Path, d: &FakeDevice) {
        let base = root.join("bus/usb/devices");
        let dir = base.join(d.port_path);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("busnum"), format!("{}\n", d.bus)).unwrap();
        fs::write(dir.join("devnum"), format!("{}\n", d.dev)).unwrap();
        fs::write(dir.join("idVendor"), format!("{}\n", d.vendor)).unwrap();
        fs::write(dir.join("idProduct"), format!("{}\n", d.product_id)).unwrap();
        fs::write(dir.join("bDeviceClass"), format!("{}\n", d.class)).unwrap();
        fs::write(dir.join("speed"), "12\n").unwrap();
        fs::write(dir.join("bcdDevice"), "0264\n").unwrap();
        if let Some(serial) = d.serial {
            fs::write(dir.join("serial"), format!("{serial}\n")).unwrap();
        }
        for (i, class) in d.interfaces.iter().enumerate() {
            let iface = base.join(format!("{}:1.{i}", d.port_path));
            fs::create_dir_all(&iface).unwrap();
            fs::write(iface.join("bInterfaceClass"), format!("{class}\n")).unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fake::*;
    use super::*;

    #[test]
    fn scans_devices() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        fs::create_dir_all(root.join("bus/usb/devices/usb3")).unwrap();
        add_device(
            root,
            &FakeDevice {
                port_path: "3-1",
                bus: 3,
                dev: 2,
                vendor: "05e3",
                product_id: "0610",
                serial: None,
                class: "09",
                interfaces: &["09"],
            },
        );
        add_device(
            root,
            &FakeDevice {
                port_path: "3-3",
                bus: 3,
                dev: 3,
                vendor: "1a86",
                product_id: "7523",
                serial: None,
                class: "ff",
                interfaces: &["ff"],
            },
        );
        add_device(
            root,
            &FakeDevice {
                port_path: "3-4",
                bus: 3,
                dev: 4,
                vendor: "abcd",
                product_id: "1234",
                serial: Some("SN0001234"),
                class: "00",
                interfaces: &["03"],
            },
        );

        let devices = scan(root).unwrap();
        assert_eq!(devices.len(), 3);
        let hub = &devices[0];
        assert!(hub.is_hub());
        let ch340 = &devices[1];
        assert_eq!(ch340.port_path, "3-3");
        assert_eq!(ch340.bus_dev(), "3-3");
        assert_eq!(ch340.vendor_id, "1a86");
        assert_eq!(ch340.serial, None);
        assert_eq!(ch340.interface_classes, vec![0xff]);
        assert_eq!(ch340.bcd_device.as_deref(), Some("0264"));
        let hid = &devices[2];
        assert_eq!(hid.serial.as_deref(), Some("SN0001234"));
        assert_eq!(hid.bus_dev(), "3-4");
        assert_eq!(hid.interface_classes, vec![0x03]);
    }

    #[test]
    fn skips_half_enumerated_devices() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("bus/usb/devices/1-2");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("busnum"), "1\n").unwrap();
        assert!(scan(tmp.path()).unwrap().is_empty());
    }

    #[test]
    fn blank_serial_is_none() {
        let tmp = tempfile::tempdir().unwrap();
        add_device(
            tmp.path(),
            &FakeDevice {
                port_path: "1-1",
                bus: 1,
                dev: 5,
                vendor: "10c4",
                product_id: "ea60",
                serial: Some("  "),
                class: "00",
                interfaces: &[],
            },
        );
        assert_eq!(scan(tmp.path()).unwrap()[0].serial, None);
    }
}
