//! Locating and connecting to the `virt-usbredir-N` unix sockets that QEMU listens on inside a
//! virt-launcher pod.
//!
//! KubeVirt creates them at `/var/run/kubevirt-private/<vmi-uid>/virt-usbredir-<N>` in the
//! launcher, backed by the pod's `private` emptyDir. From the host that is
//! `<kubelet-root>/pods/<pod-uid>/volumes/kubernetes.io~empty-dir/private/<vmi-uid>/...`, or, with
//! `hostPID`, `/proc/<launcher-pid>/root/var/run/kubevirt-private/<vmi-uid>/...` (the path
//! virt-handler itself uses).

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use tokio::net::UnixStream;

/// `sun_path` is 108 bytes including the terminator.
const MAX_SUN_PATH: usize = 107;

#[derive(Clone, Debug)]
pub struct SocketLocator {
    pub kubelet_root: PathBuf,
    /// Host `/proc`, when the agent runs with `hostPID`.
    pub proc_root: Option<PathBuf>,
}

pub fn socket_name(slot: i32) -> String {
    format!("virt-usbredir-{slot}")
}

impl SocketLocator {
    /// Candidate socket paths in order of preference.
    pub fn candidates(&self, vmi_uid: &str, launcher_pods: &[&str], slot: i32) -> Vec<PathBuf> {
        launcher_pods
            .iter()
            .map(|pod| {
                self.kubelet_root
                    .join("pods")
                    .join(pod)
                    .join("volumes/kubernetes.io~empty-dir/private")
                    .join(vmi_uid)
                    .join(socket_name(slot))
            })
            .collect()
    }

    /// Returns the first existing socket for the VMI, falling back to scanning host processes.
    pub fn locate(&self, vmi_uid: &str, launcher_pods: &[&str], slot: i32) -> Option<PathBuf> {
        if let Some(found) = self
            .candidates(vmi_uid, launcher_pods, slot)
            .into_iter()
            .find(|p| is_socket(p))
        {
            return Some(found);
        }
        let proc_root = self.proc_root.as_ref()?;
        let relative = Path::new("root/var/run/kubevirt-private")
            .join(vmi_uid)
            .join(socket_name(slot));
        fs::read_dir(proc_root)
            .ok()?
            .filter_map(Result::ok)
            .filter(|e| {
                e.file_name()
                    .to_str()
                    .is_some_and(|n| n.bytes().all(|b| b.is_ascii_digit()))
            })
            .map(|e| e.path().join(&relative))
            .find(|p| is_socket(p))
    }
}

fn is_socket(path: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileTypeExt;
        fs::metadata(path).is_ok_and(|m| m.file_type().is_socket())
    }
    #[cfg(not(unix))]
    {
        path.exists()
    }
}

/// Connects to a unix socket whose path may exceed `sun_path`, by going through an `O_PATH`
/// descriptor of its parent directory (`/proc/self/fd/<fd>/<name>`).
pub async fn connect(path: &Path) -> io::Result<UnixStream> {
    if path.as_os_str().len() <= MAX_SUN_PATH {
        return UnixStream::connect(path).await;
    }
    connect_via_dirfd(path).await
}

#[cfg(target_os = "linux")]
async fn connect_via_dirfd(path: &Path) -> io::Result<UnixStream> {
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::OpenOptionsExt;

    let (Some(parent), Some(name)) = (path.parent(), path.file_name()) else {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "socket path has no parent"));
    };
    let dir = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_PATH | libc::O_DIRECTORY | libc::O_CLOEXEC)
        .open(parent)?;
    let short = Path::new("/proc/self/fd").join(dir.as_raw_fd().to_string()).join(name);
    let stream = UnixStream::connect(&short).await;
    drop(dir);
    stream
}

#[cfg(not(target_os = "linux"))]
async fn connect_via_dirfd(path: &Path) -> io::Result<UnixStream> {
    UnixStream::connect(path).await
}

#[cfg(all(test, unix))]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::UnixListener;

    use super::*;

    fn bind_long(path: &Path) -> UnixListener {
        use std::os::fd::AsRawFd;
        use std::os::unix::fs::OpenOptionsExt;
        let dir = fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_PATH | libc::O_DIRECTORY)
            .open(path.parent().unwrap())
            .unwrap();
        let short = Path::new("/proc/self/fd")
            .join(dir.as_raw_fd().to_string())
            .join(path.file_name().unwrap());
        UnixListener::bind(short).unwrap()
    }

    const VMI_UID: &str = "8d3f6a7e-2b1c-4c55-9d0e-3f1a2b3c4d5e";
    const POD_UID: &str = "0659178d-bb0e-41ef-a3be-d486a723f441";

    #[tokio::test]
    async fn locates_and_connects_to_long_kubelet_path() {
        let tmp = tempfile::tempdir().unwrap();
        let locator = SocketLocator {
            kubelet_root: tmp.path().join("var/lib/kubelet"),
            proc_root: None,
        };
        let path = locator.candidates(VMI_UID, &[POD_UID], 2).remove(0);
        assert!(
            path.as_os_str().len() > MAX_SUN_PATH,
            "test must exercise the long-path fallback"
        );
        fs::create_dir_all(path.parent().unwrap()).unwrap();

        // QEMU binds from inside the pod with a short path; emulate that through a dir fd.
        let listener = bind_long(&path);
        assert_eq!(locator.locate(VMI_UID, &[POD_UID], 2), Some(path.clone()));
        assert_eq!(locator.locate(VMI_UID, &[POD_UID], 1), None);

        let server = tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            s.write_all(b"hello").await.unwrap();
        });
        let mut client = connect(&path).await.unwrap();
        let mut buf = [0u8; 5];
        client.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn falls_back_to_proc_root() {
        let tmp = tempfile::tempdir().unwrap();
        let proc_root = tmp.path().join("proc");
        let dir = proc_root.join("4242/root/var/run/kubevirt-private").join(VMI_UID);
        fs::create_dir_all(&dir).unwrap();
        fs::create_dir_all(proc_root.join("self")).unwrap();
        let _listener = bind_long(&dir.join("virt-usbredir-0"));
        let locator = SocketLocator {
            kubelet_root: tmp.path().join("kubelet"),
            proc_root: Some(proc_root),
        };
        assert_eq!(
            locator.locate(VMI_UID, &[POD_UID], 0),
            Some(dir.join("virt-usbredir-0"))
        );
    }
}
