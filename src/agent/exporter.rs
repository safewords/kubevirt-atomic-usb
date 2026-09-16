//! Serves locally plugged USB devices to attaching agents on other nodes.
//!
//! For every authorized connection a `usbredirect` process opens the device and speaks the
//! usbredir protocol, which is spliced onto the pod-network TCP connection. At most one session per
//! device exists at any time; a new session for the current lease holder replaces the old one.

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Context;
use kube::Api;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::process::{Child, Command};
use tokio::sync::watch;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use super::{AgentConfig, LocalDevices};
use crate::crd::UsbDevice;
use crate::proto::{self, Hello, NonceCache, Reply};
use crate::sysfs::UsbDeviceInfo;
use crate::util;

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const USBREDIRECT_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const PREVIOUS_SESSION_STOP_TIMEOUT: Duration = Duration::from_secs(15);
const USBREDIRECT_STOP_TIMEOUT: Duration = Duration::from_secs(5);
const STDERR_TAIL_LINES: usize = 5;

pub struct Exporter {
    config: Arc<AgentConfig>,
    api: Api<UsbDevice>,
    local: watch::Receiver<LocalDevices>,
    sessions: Mutex<HashMap<String, DeviceSessions>>,
    nonces: Mutex<NonceCache>,
    next_id: AtomicU64,
}

#[derive(Default)]
struct DeviceSessions {
    /// Held for the lifetime of a session, so a replacement waits until the device is released.
    busy: Arc<tokio::sync::Mutex<()>>,
    active: Option<ActiveSession>,
}

struct ActiveSession {
    id: u64,
    claim: String,
    bus_dev: String,
    cancel: CancellationToken,
}

impl Exporter {
    pub fn new(config: Arc<AgentConfig>, api: Api<UsbDevice>, local: watch::Receiver<LocalDevices>) -> Arc<Self> {
        Arc::new(Self {
            config,
            api,
            local,
            sessions: Mutex::new(HashMap::new()),
            nonces: Mutex::new(NonceCache::default()),
            next_id: AtomicU64::new(1),
        })
    }

    pub async fn run(self: Arc<Self>) -> anyhow::Result<()> {
        let listener = TcpListener::bind(self.config.listen)
            .await
            .with_context(|| format!("binding {}", self.config.listen))?;
        info!(listen = %self.config.listen, "exporter listening");
        tokio::spawn(self.clone().end_sessions_of_removed_devices());
        loop {
            let (stream, peer) = match listener.accept().await {
                Ok(conn) => conn,
                Err(err) => {
                    warn!(%err, "accept failed");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            let this = self.clone();
            tokio::spawn(async move {
                if let Err(err) = this.handle(stream, peer).await {
                    warn!(%peer, "session failed: {err:#}");
                }
            });
        }
    }

    async fn end_sessions_of_removed_devices(self: Arc<Self>) {
        let mut local = self.local.clone();
        while local.changed().await.is_ok() {
            let devices = local.borrow_and_update().clone();
            let sessions = self.sessions.lock().unwrap();
            for (name, device_sessions) in sessions.iter() {
                let Some(active) = &device_sessions.active else {
                    continue;
                };
                if devices.get(name).is_none_or(|d| d.bus_dev() != active.bus_dev) {
                    info!(device = %name, claim = %active.claim, "device unplugged or re-enumerated; ending session");
                    active.cancel.cancel();
                }
            }
        }
    }

    async fn handle(self: Arc<Self>, mut stream: TcpStream, peer: SocketAddr) -> anyhow::Result<()> {
        super::tune_tcp(&stream);
        let hello: Hello = match timeout(HANDSHAKE_TIMEOUT, proto::read_line(&mut stream)).await {
            Ok(Ok(hello)) => hello,
            Ok(Err(err)) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                debug!(%peer, "connection closed before handshake (probe?)");
                return Ok(());
            }
            Ok(Err(err)) => return reject(&mut stream, &format!("bad handshake: {err}")).await,
            Err(_) => return reject(&mut stream, "handshake timeout").await,
        };
        let claim = format!("{}/{}", hello.claim.namespace, hello.claim.name);

        let device = match self.authorize(&hello).await {
            Ok(device) => device,
            Err(reason) => {
                warn!(%peer, device = %hello.device, %claim, %reason, "rejected session");
                return reject(&mut stream, &reason).await;
            }
        };

        let (id, cancel, busy) = self.begin_session(&hello, &device, &claim);
        let result = async {
            let _busy = match timeout(PREVIOUS_SESSION_STOP_TIMEOUT, busy.lock_owned()).await {
                Ok(guard) => guard,
                Err(_) => return reject(&mut stream, "previous session did not stop in time").await,
            };
            if cancel.is_cancelled() {
                return reject(&mut stream, "session superseded").await;
            }
            self.serve(&mut stream, peer, &hello.device, &claim, &device, &cancel)
                .await
        }
        .await;
        self.end_session(&hello.device, id);
        result
    }

    async fn authorize(&self, hello: &Hello) -> Result<UsbDeviceInfo, String> {
        let psk =
            crate::watch::read_psk(&self.config.psk_file).map_err(|e| format!("exporter misconfigured: {e:#}"))?;
        let now = util::now().as_second();
        hello
            .verify(&psk, now)
            .map_err(|e| format!("authentication failed: {e}"))?;
        self.nonces
            .lock()
            .unwrap()
            .check_and_insert(&hello.nonce, hello.timestamp, now)
            .map_err(|e| e.to_string())?;

        let device = self
            .local
            .borrow()
            .get(&hello.device)
            .cloned()
            .ok_or_else(|| format!("device {} is not plugged into node {}", hello.device, self.config.node))?;

        // The lease is authoritative; read it fresh rather than from a possibly stale cache.
        let object = self
            .api
            .get_opt(&hello.device)
            .await
            .map_err(|e| format!("checking lease: {e}"))?
            .ok_or_else(|| format!("UsbDevice {} does not exist", hello.device))?;
        match &object.status_ref().attached_to {
            Some(lease)
                if lease.claim_uid == hello.claim.uid
                    && lease.namespace == hello.claim.namespace
                    && lease.claim == hello.claim.name =>
            {
                Ok(device)
            }
            Some(lease) => Err(format!("device is leased to claim {}/{}", lease.namespace, lease.claim)),
            None => Err("device is not leased to any claim".into()),
        }
    }

    /// Registers a session as the device's active one, cancelling whichever session held the device
    /// before (the lease was just verified, so the newcomer is the legitimate user).
    fn begin_session(
        &self,
        hello: &Hello,
        device: &UsbDeviceInfo,
        claim: &str,
    ) -> (u64, CancellationToken, Arc<tokio::sync::Mutex<()>>) {
        let mut sessions = self.sessions.lock().unwrap();
        let entry = sessions.entry(hello.device.clone()).or_default();
        if let Some(previous) = entry.active.take() {
            info!(device = %hello.device, previous_claim = %previous.claim, "replacing existing session");
            previous.cancel.cancel();
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let cancel = CancellationToken::new();
        entry.active = Some(ActiveSession {
            id,
            claim: claim.to_string(),
            bus_dev: device.bus_dev(),
            cancel: cancel.clone(),
        });
        (id, cancel, entry.busy.clone())
    }

    fn end_session(&self, device: &str, id: u64) {
        let mut sessions = self.sessions.lock().unwrap();
        if let Some(entry) = sessions.get_mut(device)
            && entry.active.as_ref().is_some_and(|a| a.id == id)
        {
            entry.active = None;
        }
    }

    async fn serve(
        &self,
        stream: &mut TcpStream,
        peer: SocketAddr,
        name: &str,
        claim: &str,
        device: &UsbDeviceInfo,
        cancel: &CancellationToken,
    ) -> anyhow::Result<()> {
        let local = TcpListener::bind("127.0.0.1:0").await?;
        let local_addr = local.local_addr()?;
        let mut child = match Command::new(&self.config.usbredirect)
            .arg("--device")
            .arg(device.bus_dev())
            .arg("--to")
            .arg(local_addr.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
        {
            Ok(child) => child,
            Err(err) => {
                let reason = format!("cannot start {}: {err}", self.config.usbredirect.display());
                warn!(device = %name, %reason);
                return reject(stream, &reason).await;
            }
        };
        let stderr_tail = forward_stderr(&mut child, name);

        let usbredir = tokio::select! {
            accepted = local.accept() => accepted.map(|(conn, _)| conn)?,
            status = child.wait() => {
                // Give the stderr forwarder a moment to capture the reason.
                tokio::time::sleep(Duration::from_millis(100)).await;
                let reason = format!("usbredirect exited ({}): {}", status?, tail(&stderr_tail));
                warn!(device = %name, %reason);
                return reject(stream, &reason).await;
            }
            _ = tokio::time::sleep(USBREDIRECT_CONNECT_TIMEOUT) => {
                terminate(&mut child).await;
                return reject(stream, "usbredirect did not open the device in time").await;
            }
            _ = cancel.cancelled() => {
                terminate(&mut child).await;
                return reject(stream, "session cancelled").await;
            }
        };
        drop(local);
        let mut usbredir = usbredir;
        let _ = usbredir.set_nodelay(true);

        proto::write_line(stream, &Reply { ok: true, error: None }).await?;
        info!(device = %name, %claim, %peer, bus_dev = %device.bus_dev(), "session started");

        let outcome = tokio::select! {
            copied = tokio::io::copy_bidirectional(stream, &mut usbredir) => match copied {
                Ok((to_device, to_vm)) => format!("connection closed ({to_device} bytes to device, {to_vm} bytes to VM)"),
                Err(err) => format!("connection error: {err}"),
            },
            status = child.wait() => format!("usbredirect exited ({status:?}): {}", tail(&stderr_tail)),
            _ = cancel.cancelled() => "cancelled".to_string(),
        };
        drop(usbredir);
        terminate(&mut child).await;
        info!(device = %name, %claim, %outcome, "session ended");
        Ok(())
    }
}

async fn reject(stream: &mut TcpStream, reason: &str) -> anyhow::Result<()> {
    let _ = proto::write_line(
        stream,
        &Reply {
            ok: false,
            error: Some(reason.to_string()),
        },
    )
    .await;
    Ok(())
}

fn forward_stderr(child: &mut Child, device: &str) -> Arc<Mutex<VecDeque<String>>> {
    let lines = Arc::new(Mutex::new(VecDeque::with_capacity(STDERR_TAIL_LINES)));
    if let Some(stderr) = child.stderr.take() {
        let lines = lines.clone();
        let device = device.to_string();
        tokio::spawn(async move {
            let mut reader = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                info!(%device, "usbredirect: {line}");
                let mut tail = lines.lock().unwrap();
                if tail.len() == STDERR_TAIL_LINES {
                    tail.pop_front();
                }
                tail.push_back(line);
            }
        });
    }
    lines
}

fn tail(lines: &Mutex<VecDeque<String>>) -> String {
    let lines = lines.lock().unwrap();
    if lines.is_empty() {
        "no output".to_string()
    } else {
        lines.iter().cloned().collect::<Vec<_>>().join(" | ")
    }
}

/// Stops usbredirect gracefully (so it re-attaches kernel drivers), killing it if it lingers.
async fn terminate(child: &mut Child) {
    if matches!(child.try_wait(), Ok(Some(_))) {
        return;
    }
    #[cfg(unix)]
    if let Some(pid) = child.id() {
        // SAFETY: signalling our own child process.
        unsafe {
            libc::kill(pid as libc::pid_t, libc::SIGINT);
        }
    }
    if timeout(USBREDIRECT_STOP_TIMEOUT, child.wait()).await.is_err() {
        let _ = child.kill().await;
    }
}
