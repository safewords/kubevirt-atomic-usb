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
    ///
    /// The fallback only considers processes that run in one of the VMI's launcher pods. Any
    /// container can create a `/var/run/kubevirt-private/<vmi-uid>/` directory in its own
    /// filesystem, and VMI UIDs are not secret, so accepting any process would let a pod on the
    /// VM's node receive another tenant's USB device.
    pub fn locate(&self, vmi_uid: &str, launcher_pods: &[&str], slot: i32) -> Option<PathBuf> {
        if let Some(found) = self
            .candidates(vmi_uid, launcher_pods, slot)
            .into_iter()
            .find(|p| is_socket(p))
        {
            return Some(found);
        }
        let proc_root = self.proc_root.as_ref()?;
        if launcher_pods.is_empty() {
            return None;
        }
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
            .filter(|e| process_in_pods(&e.path(), launcher_pods))
            .map(|e| e.path().join(&relative))
            .find(|p| is_socket(p))
    }
}

/// Whether `/proc/<pid>` belongs to one of the pods, judged by its cgroup path, which contains the
/// pod UID with dashes (cgroupfs driver) or underscores (systemd driver).
fn process_in_pods(proc_pid: &Path, pod_uids: &[&str]) -> bool {
    let Ok(cgroup) = fs::read_to_string(proc_pid.join("cgroup")) else {
        return false;
    };
    pod_uids
        .iter()
        .filter(|uid| !uid.is_empty())
        .any(|uid| cgroup.contains(&format!("pod{uid}")) || cgroup.contains(&format!("pod{}", uid.replace('-', "_"))))
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

    /// Creates `/proc/<pid>` with a cgroup file and a listening usbredir socket in its root.
    fn fake_process(proc_root: &Path, pid: u32, cgroup: &str) -> (PathBuf, UnixListener) {
        let base = proc_root.join(pid.to_string());
        let dir = base.join("root/var/run/kubevirt-private").join(VMI_UID);
        fs::create_dir_all(&dir).unwrap();
        fs::write(base.join("cgroup"), cgroup).unwrap();
        let socket = dir.join("virt-usbredir-0");
        let listener = bind_long(&socket);
        (socket, listener)
    }

    #[tokio::test]
    async fn falls_back_to_launcher_processes_only() {
        let tmp = tempfile::tempdir().unwrap();
        let proc_root = tmp.path().join("proc");
        fs::create_dir_all(proc_root.join("self")).unwrap();
        let locator = SocketLocator {
            kubelet_root: tmp.path().join("kubelet"),
            proc_root: Some(proc_root.clone()),
        };

        // Another pod on the node planted a socket for the VMI: it must never be used.
        let (_, _decoy) = fake_process(
            &proc_root,
            1000,
            "0::/kubepods.slice/kubepods-besteffort.slice/kubepods-besteffort-pod11111111_2222_3333_4444_555555555555.slice/cri-containerd-abc.scope\n",
        );
        assert_eq!(locator.locate(VMI_UID, &[POD_UID], 0), None);
        assert_eq!(locator.locate(VMI_UID, &[], 0), None);

        // systemd cgroup driver: pod UID with underscores.
        let systemd = format!(
            "0::/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod{}.slice/cri-containerd-def.scope\n",
            POD_UID.replace('-', "_")
        );
        let (socket, _launcher) = fake_process(&proc_root, 4242, &systemd);
        assert_eq!(locator.locate(VMI_UID, &[POD_UID], 0), Some(socket));
    }

    #[test]
    fn cgroupfs_driver_pod_uids_match() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(
            tmp.path().join("cgroup"),
            format!("0::/kubepods/burstable/pod{POD_UID}/0123abcd\n"),
        )
        .unwrap();
        assert!(process_in_pods(tmp.path(), &[POD_UID]));
        assert!(!process_in_pods(tmp.path(), &["11111111-2222-3333-4444-555555555555"]));
        assert!(!process_in_pods(tmp.path(), &[""]));
    }
}
