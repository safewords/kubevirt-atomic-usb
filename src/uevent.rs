//! Kernel uevent listener (`NETLINK_KOBJECT_UEVENT`) used to react to USB hotplug immediately.
//!
//! Kernel uevents are broadcast to every network namespace owned by the initial user namespace,
//! so this works from a regular (non host-network) pod as long as it does not use `hostUsers: false`.

use std::collections::HashMap;

/// A parsed kernel uevent, e.g. `add@/devices/pci0000:00/0000:00:10.0/usb3/3-3`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Uevent {
    pub action: String,
    pub devpath: String,
    pub vars: HashMap<String, String>,
}

impl Uevent {
    pub fn subsystem(&self) -> Option<&str> {
        self.vars.get("SUBSYSTEM").map(String::as_str)
    }

    /// Whether this event can change the set of USB devices or their descriptors.
    pub fn affects_usb_devices(&self) -> bool {
        self.subsystem() == Some("usb") && self.vars.get("DEVTYPE").is_none_or(|t| t == "usb_device")
    }
}

/// Parses a raw kernel uevent datagram (`ACTION@DEVPATH\0KEY=VALUE\0...`).
///
/// Messages from udevd (prefixed with `libudev\0`) are rejected; only kernel events are used.
pub fn parse(buf: &[u8]) -> Option<Uevent> {
    let mut fields = buf.split(|&b| b == 0).filter(|f| !f.is_empty());
    let header = std::str::from_utf8(fields.next()?).ok()?;
    let (action, devpath) = header.split_once('@')?;
    let vars = fields
        .filter_map(|f| std::str::from_utf8(f).ok())
        .filter_map(|f| f.split_once('='))
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    Some(Uevent {
        action: action.to_string(),
        devpath: devpath.to_string(),
        vars,
    })
}

#[cfg(target_os = "linux")]
mod socket {
    use std::io;
    use std::mem;
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

    use tokio::io::unix::AsyncFd;

    use super::{Uevent, parse};

    const KERNEL_GROUP: u32 = 1;
    const RECV_BUFFER_BYTES: libc::c_int = 4 * 1024 * 1024;

    pub struct UeventSocket {
        fd: AsyncFd<OwnedFd>,
    }

    impl UeventSocket {
        pub fn open() -> io::Result<Self> {
            // SAFETY: plain syscalls with checked return values; the fd is owned immediately.
            unsafe {
                let raw = libc::socket(
                    libc::AF_NETLINK,
                    libc::SOCK_DGRAM | libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK,
                    libc::NETLINK_KOBJECT_UEVENT,
                );
                if raw < 0 {
                    return Err(io::Error::last_os_error());
                }
                let fd = OwnedFd::from_raw_fd(raw);

                // Hotplug storms (hubs with many devices) can overflow the default buffer.
                // SO_RCVBUFFORCE needs CAP_NET_ADMIN; fall back to SO_RCVBUF silently.
                let size = RECV_BUFFER_BYTES;
                let size_ptr = &size as *const libc::c_int as *const libc::c_void;
                let size_len = mem::size_of::<libc::c_int>() as libc::socklen_t;
                if libc::setsockopt(raw, libc::SOL_SOCKET, libc::SO_RCVBUFFORCE, size_ptr, size_len) < 0 {
                    libc::setsockopt(raw, libc::SOL_SOCKET, libc::SO_RCVBUF, size_ptr, size_len);
                }

                let mut addr: libc::sockaddr_nl = mem::zeroed();
                addr.nl_family = libc::AF_NETLINK as libc::sa_family_t;
                addr.nl_groups = KERNEL_GROUP;
                let addr_ptr = &addr as *const libc::sockaddr_nl as *const libc::sockaddr;
                if libc::bind(raw, addr_ptr, mem::size_of::<libc::sockaddr_nl>() as libc::socklen_t) < 0 {
                    return Err(io::Error::last_os_error());
                }
                Ok(Self { fd: AsyncFd::new(fd)? })
            }
        }

        /// Receives the next uevent. `Ok(None)` means a datagram was dropped or unparseable; an
        /// `ENOBUFS` error means events were lost and the caller should rescan everything.
        pub async fn recv(&self) -> io::Result<Option<Uevent>> {
            let mut buf = [0u8; 8192];
            loop {
                let mut guard = self.fd.readable().await?;
                let result = guard.try_io(|inner| {
                    // SAFETY: buf is valid for writes of buf.len() bytes.
                    let n =
                        unsafe { libc::recv(inner.as_raw_fd(), buf.as_mut_ptr() as *mut libc::c_void, buf.len(), 0) };
                    if n < 0 {
                        Err(io::Error::last_os_error())
                    } else {
                        Ok(n as usize)
                    }
                });
                match result {
                    Ok(Ok(n)) => return Ok(parse(&buf[..n])),
                    Ok(Err(e)) => return Err(e),
                    Err(_would_block) => continue,
                }
            }
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod socket {
    use std::io;

    use super::Uevent;

    /// Uevents only exist on Linux; other platforms fall back to periodic rescans.
    pub struct UeventSocket;

    impl UeventSocket {
        pub fn open() -> io::Result<Self> {
            Err(io::Error::new(io::ErrorKind::Unsupported, "uevents require Linux"))
        }

        pub async fn recv(&self) -> io::Result<Option<Uevent>> {
            std::future::pending().await
        }
    }
}

pub use socket::UeventSocket;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_usb_add() {
        let raw = b"add@/devices/pci0000:00/0000:00:10.0/usb3/3-3\0ACTION=add\0DEVPATH=/devices/pci0000:00/0000:00:10.0/usb3/3-3\0SUBSYSTEM=usb\0MAJOR=189\0MINOR=258\0DEVNAME=bus/usb/003/003\0DEVTYPE=usb_device\0PRODUCT=1a86/7523/264\0TYPE=255/0/0\0BUSNUM=003\0DEVNUM=003\0SEQNUM=5123\0";
        let ev = parse(raw).unwrap();
        assert_eq!(ev.action, "add");
        assert_eq!(ev.devpath, "/devices/pci0000:00/0000:00:10.0/usb3/3-3");
        assert_eq!(ev.vars["PRODUCT"], "1a86/7523/264");
        assert!(ev.affects_usb_devices());
    }

    #[test]
    fn ignores_interfaces_and_other_subsystems() {
        let iface =
            parse(b"bind@/devices/x/usb3/3-3/3-3:1.0\0ACTION=bind\0SUBSYSTEM=usb\0DEVTYPE=usb_interface\0").unwrap();
        assert!(!iface.affects_usb_devices());
        let tty = parse(b"add@/devices/virtual/tty/ttyUSB0\0ACTION=add\0SUBSYSTEM=tty\0").unwrap();
        assert!(!tty.affects_usb_devices());
    }

    #[test]
    fn rejects_udev_messages() {
        assert_eq!(parse(b"libudev\0\xfe\xed\xca\xfe"), None);
    }
}
