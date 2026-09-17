//! Attaches leased USB devices to VMs running on this node.
//!
//! For every claim whose VM runs here, a session task connects QEMU's `virt-usbredir-N` socket to
//! the exporting agent over the pod network. When the stream ends (device unplugged, exporter
//! restarted, network failure) QEMU hot-unplugs the device from the guest, and the task keeps
//! reconnecting, so the device is hot-plugged again as soon as it reappears on any node.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use kube::api::{DynamicObject, Patch, PatchParams};
use kube::runtime::reflector::{ObjectRef, Store};
use kube::{Api, ResourceExt};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use super::AgentConfig;
use crate::crd::{ConnectionState, ConnectionStatus, UsbDevice, UsbDeviceClaim};
use crate::kubevirt::{VmiInfo, vmi_resource};
use crate::proto::{self, ClaimRef, Hello, Reply};
use crate::{launcher, util};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// The exporter may first have to stop a previous session and start usbredirect.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(40);
/// QEMU sends its usbredir hello as soon as it accepts the connection.
const QEMU_GREETING_TIMEOUT: Duration = Duration::from_secs(10);
const MIN_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// A session that stayed up this long resets the reconnect backoff.
const HEALTHY_SESSION: Duration = Duration::from_secs(30);

pub struct Attacher {
    ctx: Arc<Context>,
    changes: watch::Receiver<u64>,
}

struct Context {
    config: Arc<AgentConfig>,
    claims_api: Api<UsbDeviceClaim>,
    claims: Store<UsbDeviceClaim>,
    devices: Store<UsbDevice>,
    vmis: Store<DynamicObject>,
    changes: watch::Receiver<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SessionSpec {
    claim: ClaimRef,
    device: String,
    slot: i32,
    vmi_uid: String,
    vm_name: String,
}

struct Running {
    spec: SessionSpec,
    cancel: CancellationToken,
    handle: JoinHandle<()>,
}

impl Attacher {
    pub fn new(
        config: Arc<AgentConfig>,
        claims_api: Api<UsbDeviceClaim>,
        claims: Store<UsbDeviceClaim>,
        devices: Store<UsbDevice>,
        vmis: Store<DynamicObject>,
        changes: watch::Receiver<u64>,
    ) -> Self {
        let ctx = Arc::new(Context {
            config,
            claims_api,
            claims,
            devices,
            vmis,
            changes: changes.clone(),
        });
        Self { ctx, changes }
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        let mut running: HashMap<String, Running> = HashMap::new();
        let mut stopping: HashMap<String, JoinHandle<()>> = HashMap::new();
        loop {
            let desired = self.ctx.desired_sessions();
            stopping.retain(|_, handle| !handle.is_finished());

            let stale: Vec<String> = running
                .iter()
                .filter(|(uid, r)| desired.get(*uid) != Some(&r.spec) || r.handle.is_finished())
                .map(|(uid, _)| uid.clone())
                .collect();
            for uid in stale {
                let r = running.remove(&uid).expect("present");
                debug!(claim = %format!("{}/{}", r.spec.claim.namespace, r.spec.claim.name), "stopping session");
                r.cancel.cancel();
                stopping.insert(uid, r.handle);
            }

            for (uid, spec) in desired {
                if running.contains_key(&uid) {
                    continue;
                }
                let cancel = CancellationToken::new();
                let previous = stopping.remove(&uid);
                let handle = tokio::spawn(run_session(self.ctx.clone(), spec.clone(), cancel.clone(), previous));
                running.insert(uid, Running { spec, cancel, handle });
            }

            crate::watch::wait_for_change(&mut self.changes, Duration::from_secs(10), Duration::from_millis(200)).await;
        }
    }
}

impl Context {
    /// Claims holding a device lease whose VM is running on this node with usbredir enabled.
    fn desired_sessions(&self) -> HashMap<String, SessionSpec> {
        let mut desired = HashMap::new();
        for claim in self.claims.state() {
            let (Some(uid), Some(namespace)) = (claim.uid(), claim.namespace()) else {
                continue;
            };
            if claim.metadata.deletion_timestamp.is_some() {
                continue;
            }
            let Some(status) = &claim.status else { continue };
            let (Some(device_name), Some(slot)) = (&status.device_name, status.slot) else {
                continue;
            };
            let lease_held = self
                .devices
                .get(&ObjectRef::new(device_name))
                .is_some_and(|d| d.lease_holder() == Some(uid.as_str()));
            let Some(vmi) = self.vmi(&namespace, &claim.spec.vm_name) else {
                continue;
            };
            if !lease_held
                || !vmi.is_running()
                || !vmi.client_passthrough
                || vmi.node.as_deref() != Some(self.config.node.as_str())
            {
                continue;
            }
            desired.insert(
                uid.clone(),
                SessionSpec {
                    claim: ClaimRef {
                        namespace,
                        name: claim.name_any(),
                        uid,
                    },
                    device: device_name.clone(),
                    slot,
                    vmi_uid: vmi.uid,
                    vm_name: claim.spec.vm_name.clone(),
                },
            );
        }
        desired
    }

    fn vmi(&self, namespace: &str, name: &str) -> Option<VmiInfo> {
        let key = ObjectRef::<DynamicObject>::new_with(name, vmi_resource()).within(namespace);
        self.vmis.get(&key).and_then(|o| VmiInfo::from_object(&o))
    }
}

enum AttemptError {
    /// A precondition is not met yet; retry when cluster state changes.
    Waiting(String),
    /// The data path failed; retry with backoff.
    Failed(String),
}

async fn run_session(
    ctx: Arc<Context>,
    spec: SessionSpec,
    cancel: CancellationToken,
    previous: Option<JoinHandle<()>>,
) {
    if let Some(previous) = previous {
        let _ = timeout(Duration::from_secs(15), previous).await;
    }
    let claim = format!("{}/{}", spec.claim.namespace, spec.claim.name);
    info!(%claim, device = %spec.device, vm = %spec.vm_name, slot = spec.slot, "managing attachment");
    let mut reporter = Reporter::new(ctx.clone(), spec.clone());
    let mut changes = ctx.changes.clone();
    let mut backoff = MIN_BACKOFF;

    loop {
        let attempt = tokio::select! {
            result = attempt(&ctx, &spec, &mut reporter) => result,
            _ = cancel.cancelled() => break,
        };
        match attempt {
            Ok(connected_for) => {
                if connected_for >= HEALTHY_SESSION {
                    backoff = MIN_BACKOFF;
                }
                reporter
                    .report(ConnectionState::Disconnected, "session ended; reconnecting".into())
                    .await;
            }
            Err(AttemptError::Waiting(message)) => {
                reporter.report(ConnectionState::Disconnected, message).await;
                changes.mark_unchanged();
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = changes.changed() => {}
                    _ = tokio::time::sleep(MAX_BACKOFF) => {}
                }
                continue;
            }
            Err(AttemptError::Failed(message)) => {
                warn!(%claim, device = %spec.device, %message, "attach attempt failed");
                reporter.report(ConnectionState::Disconnected, message).await;
            }
        }
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(backoff) => {}
        }
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }

    info!(%claim, device = %spec.device, "detached");
    reporter.report(ConnectionState::Disconnected, "detached".into()).await;
}

/// One connection attempt. Returns how long the device stayed attached.
async fn attempt(ctx: &Context, spec: &SessionSpec, reporter: &mut Reporter) -> Result<Duration, AttemptError> {
    use AttemptError::{Failed, Waiting};
    let config = &ctx.config;
    let now = util::now();

    let device = ctx
        .devices
        .get(&ObjectRef::new(&spec.device))
        .ok_or_else(|| Waiting(format!("UsbDevice {} not found", spec.device)))?;
    let status = device.status_ref();
    if device.lease_holder() != Some(spec.claim.uid.as_str()) {
        return Err(Waiting("device lease is not held by this claim".into()));
    }
    if !device.is_available(now, config.lost_after.as_secs() as i64) {
        let phase = status
            .phase
            .map(|p| format!("{p:?}"))
            .unwrap_or_else(|| "unknown".into());
        let node = status.node.as_deref().unwrap_or("unknown node");
        return Err(Waiting(format!("device is {phase} (last seen on {node})")));
    }
    let exporter = status.exporter.clone().expect("available devices have an exporter");
    let source_node = status.node.clone().unwrap_or_default();

    let vmi = ctx
        .vmi(&spec.claim.namespace, &spec.vm_name)
        .ok_or_else(|| Waiting("VirtualMachineInstance not found".into()))?;
    if vmi.uid != spec.vmi_uid {
        return Err(Waiting("VirtualMachineInstance was replaced".into()));
    }
    let pods = vmi.launcher_pods_on(&config.node);
    let socket = config.locator.locate(&spec.vmi_uid, &pods, spec.slot).ok_or_else(|| {
        Waiting(format!(
            "usbredir socket {} not found for the VM on this node",
            launcher::socket_name(spec.slot)
        ))
    })?;

    reporter
        .report(
            ConnectionState::Connecting,
            format!("connecting to exporter on {source_node}"),
        )
        .await;
    let psk = crate::watch::read_psk(&config.psk_file).map_err(|e| Failed(format!("{e:#}")))?;
    let mut tcp = timeout(CONNECT_TIMEOUT, TcpStream::connect(exporter.socket_addr()))
        .await
        .map_err(|_| {
            Failed(format!(
                "timed out connecting to exporter {} on {source_node}",
                exporter.socket_addr()
            ))
        })?
        .map_err(|e| {
            Failed(format!(
                "connecting to exporter {} on {source_node}: {e}",
                exporter.socket_addr()
            ))
        })?;
    super::tune_tcp(&tcp);
    let hello = Hello::new(&psk, &spec.device, spec.claim.clone(), now.as_second());
    proto::write_line(&mut tcp, &hello)
        .await
        .map_err(|e| Failed(format!("sending handshake: {e}")))?;
    let reply: Reply = timeout(HANDSHAKE_TIMEOUT, proto::read_line(&mut tcp))
        .await
        .map_err(|_| Failed(format!("exporter on {source_node} did not answer the handshake")))?
        .map_err(|e| Failed(format!("reading handshake reply: {e}")))?;
    if !reply.ok {
        return Err(Failed(format!(
            "exporter on {source_node} refused: {}",
            reply.error.unwrap_or_default()
        )));
    }

    let mut vm = launcher::connect(&socket)
        .await
        .map_err(|e| Failed(format!("connecting to {}: {e}", socket.display())))?;
    let mut greeting = vec![0u8; 64 * 1024];
    let n = read_greeting(ctx, spec, &mut vm, &mut greeting, reporter, &source_node).await?;
    tcp.write_all(&greeting[..n])
        .await
        .map_err(|e| Failed(format!("forwarding to exporter: {e}")))?;
    drop(greeting);

    let started = Instant::now();
    reporter
        .report(
            ConnectionState::Connected,
            format!("attached from {source_node} via {}", launcher::socket_name(spec.slot)),
        )
        .await;
    let result = tokio::io::copy_bidirectional(&mut tcp, &mut vm).await;
    let claim = format!("{}/{}", spec.claim.namespace, spec.claim.name);
    match result {
        Ok((to_vm, to_device)) => info!(%claim, device = %spec.device, to_vm, to_device, "usbredir stream closed"),
        Err(err) => info!(%claim, device = %spec.device, %err, "usbredir stream failed"),
    }
    Ok(started.elapsed())
}

/// Reads QEMU's usbredir hello, which only arrives once the VM's vCPUs run.
///
/// QEMU neither reads nor writes a usbredir chardev while the VM is not running, so a VM started
/// with `startStrategy: Paused` stays silent. Everything on our side is in place by then, so
/// report the device as awaiting the resume — that is what releases a held boot (see
/// `UsbDeviceClaimSpec::hold_boot`) — and keep the session open. QEMU greets us within
/// milliseconds of the resume, long before the guest's firmware looks at the USB bus, so the guest
/// finds the device plugged in from its very first boot.
///
/// A silent QEMU on a *running* VM is a different matter: the slot is taken by another usbredir
/// client (e.g. `virtctl usbredir`), which stays an error.
async fn read_greeting(
    ctx: &Context,
    spec: &SessionSpec,
    vm: &mut tokio::net::UnixStream,
    buffer: &mut [u8],
    reporter: &mut Reporter,
    source_node: &str,
) -> Result<usize, AttemptError> {
    loop {
        let paused = ctx
            .vmi(&spec.claim.namespace, &spec.vm_name)
            .is_some_and(|vmi| vmi.paused);
        if paused {
            reporter
                .report_awaiting_resume(format!(
                    "ready on {} with the device from {source_node}; waiting for the VM to resume",
                    launcher::socket_name(spec.slot)
                ))
                .await;
        }
        match timeout(QEMU_GREETING_TIMEOUT, vm.read(buffer)).await {
            Ok(Ok(0)) => return Err(AttemptError::Failed("QEMU closed the usbredir socket".into())),
            Ok(Ok(n)) => return Ok(n),
            Ok(Err(err)) => return Err(AttemptError::Failed(format!("reading from QEMU: {err}"))),
            Err(_) if paused => continue,
            Err(_) => {
                return Err(AttemptError::Failed(format!(
                    "QEMU did not answer on {}; is the slot used by another usbredir client?",
                    launcher::socket_name(spec.slot)
                )));
            }
        }
    }
}

/// Writes `status.connection` of the claim when it changes.
struct Reporter {
    ctx: Arc<Context>,
    spec: SessionSpec,
    last: Option<(ConnectionState, String, bool)>,
    since: String,
}

impl Reporter {
    fn new(ctx: Arc<Context>, spec: SessionSpec) -> Self {
        Self {
            ctx,
            spec,
            last: None,
            since: util::rfc3339(util::now()),
        }
    }

    async fn report(&mut self, state: ConnectionState, message: String) {
        self.report_with(state, message, false).await
    }

    /// The device is spliced onto the slot and only the paused VM is missing (see
    /// [`read_greeting`]); the controller releases held boots on this.
    async fn report_awaiting_resume(&mut self, message: String) {
        self.report_with(ConnectionState::Connecting, message, true).await
    }

    async fn report_with(&mut self, state: ConnectionState, message: String, awaiting_resume: bool) {
        let entry = (state, message, awaiting_resume);
        if self.last.as_ref() == Some(&entry) {
            return;
        }
        let changed_state = self
            .last
            .as_ref()
            .is_none_or(|(s, _, a)| *s != state || *a != awaiting_resume);
        // Do not overwrite the report of another node that took over the VM (migration).
        let current = self
            .ctx
            .claims
            .get(&ObjectRef::new(&self.spec.claim.name).within(&self.spec.claim.namespace))
            .and_then(|c| c.status.as_ref().and_then(|s| s.connection.clone()));
        if state == ConnectionState::Disconnected
            && current
                .as_ref()
                .is_some_and(|c| c.node != self.ctx.config.node && c.state == ConnectionState::Connected)
        {
            self.last = Some(entry);
            return;
        }
        let (state, message, awaiting_resume) = entry;
        if changed_state {
            self.since = util::rfc3339(util::now());
            let claim = format!("{}/{}", self.spec.claim.namespace, self.spec.claim.name);
            info!(%claim, device = %self.spec.device, ?state, %message, "connection state");
        }
        let connection = ConnectionStatus {
            state,
            node: self.ctx.config.node.clone(),
            device_name: self.spec.device.clone(),
            slot: self.spec.slot,
            since: self.since.clone(),
            message: Some(message.clone()),
            awaiting_resume: awaiting_resume.then_some(true),
        };
        let mut connection = serde_json::to_value(&connection).expect("connection status is serializable");
        // A merge patch only drops a field that is explicitly null, so clear a stale flag.
        connection["awaitingResume"] = json!(awaiting_resume.then_some(true));
        let api: Api<UsbDeviceClaim> =
            Api::namespaced(self.ctx.claims_api.clone().into_client(), &self.spec.claim.namespace);
        let patch = json!({ "status": { "connection": connection } });
        match api
            .patch_status(&self.spec.claim.name, &PatchParams::default(), &Patch::Merge(&patch))
            .await
        {
            Ok(_) => self.last = Some((state, message, awaiting_resume)),
            Err(err) if util::is_not_found(&err) => self.last = Some((state, message, awaiting_resume)),
            Err(err) => debug!(%err, "failed to report connection state"),
        }
    }
}
