//! Cluster controller (single replica). Binds `UsbDeviceClaim`s to `UsbDevice`s through exclusive
//! leases, allocates usbredir slots, detects agents that stopped heartbeating, and reports claim
//! phases. The data path itself is handled entirely by the agents, so attached devices keep
//! working while the controller is down.
//!
//! Leases are written with a `resourceVersion` precondition: two claims (or two controller
//! replicas) racing for the same device can never both win.
//!
//! A claim's lifetime is bound to its VM: the controller adds an owner reference to the
//! `VirtualMachine` (or bare VMI), so Kubernetes garbage-collects the claim when the VM is deleted,
//! and a claim only holds a device while its VM exists.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use k8s_openapi::api::core::v1::Secret;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::api::{DynamicObject, Patch, PatchParams, PostParams};
use kube::runtime::reflector::Store;
use kube::{Api, Client, ResourceExt};
use serde_json::{Value, json};
use tracing::{debug, info, warn};

use crate::crd::{Attachment, ClaimPhase, ConnectionState, DevicePhase, UsbDevice, UsbDeviceClaim};
use crate::kubevirt::{
    USBREDIR_SLOTS, VmInfo, VmiInfo, claim_owner_references, vm_owner, vm_present, vm_resource, vmi_resource,
};
use crate::util;

#[derive(Clone, Debug)]
pub struct ControllerConfig {
    pub namespace: String,
    /// Secret holding the agents' pre-shared key; created with a random key if missing.
    pub psk_secret: Option<String>,
    pub lost_after: Duration,
    pub resync: Duration,
}

struct Ctx {
    config: ControllerConfig,
    client: Client,
    devices_api: Api<UsbDevice>,
    claims: Store<UsbDeviceClaim>,
    devices: Store<UsbDevice>,
    vms: Store<DynamicObject>,
    vmis: Store<DynamicObject>,
}

pub async fn run(client: Client, config: ControllerConfig) -> anyhow::Result<()> {
    if let Some(secret) = &config.psk_secret {
        ensure_psk_secret(&client, &config.namespace, secret)
            .await
            .context("ensuring pre-shared key secret")?;
    }
    let changes = crate::watch::changes();
    let devices_api: Api<UsbDevice> = Api::all(client.clone());
    let devices = crate::watch::reflect(devices_api.clone(), (), &changes);
    let claims = crate::watch::reflect(Api::<UsbDeviceClaim>::all(client.clone()), (), &changes);
    let vms = crate::watch::reflect_with(
        Api::<DynamicObject>::all_with(client.clone(), &vm_resource()),
        vm_resource(),
        Default::default(),
        &changes,
    );
    let vmis = crate::watch::reflect_with(
        Api::<DynamicObject>::all_with(client.clone(), &vmi_resource()),
        vmi_resource(),
        Default::default(),
        &changes,
    );
    tokio::try_join!(
        devices.wait_until_ready(),
        claims.wait_until_ready(),
        vms.wait_until_ready(),
        vmis.wait_until_ready()
    )
    .context("waiting for initial lists")?;
    info!(
        devices = devices.len(),
        claims = claims.len(),
        "controller caches synced"
    );

    let ctx = Arc::new(Ctx {
        config: config.clone(),
        client,
        devices_api,
        claims,
        devices,
        vms,
        vmis,
    });
    let mut rx = changes.subscribe();
    let control_loop = async {
        loop {
            let requeue = match reconcile_all(&ctx).await {
                Ok(requeue) => requeue,
                Err(err) => {
                    warn!("reconcile failed: {err:#}");
                    true
                }
            };
            let wait = if requeue { Duration::from_secs(1) } else { config.resync };
            crate::watch::wait_for_change(&mut rx, wait, Duration::from_millis(250)).await;
        }
    };
    tokio::select! {
        _ = control_loop => Ok(()),
        _ = crate::agent::shutdown_signal() => {
            info!("shutting down");
            Ok(())
        }
    }
}

async fn ensure_psk_secret(client: &Client, namespace: &str, name: &str) -> anyhow::Result<()> {
    let api: Api<Secret> = Api::namespaced(client.clone(), namespace);
    if api.get_opt(name).await?.is_some() {
        return Ok(());
    }
    let secret = Secret {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            labels: Some(BTreeMap::from([(
                "app.kubernetes.io/name".to_string(),
                "atomic-usb".to_string(),
            )])),
            ..Default::default()
        },
        string_data: Some(BTreeMap::from([(
            "psk".to_string(),
            hex::encode(rand::random::<[u8; 32]>()),
        )])),
        ..Default::default()
    };
    match api.create(&PostParams::default(), &secret).await {
        Ok(_) => info!(%namespace, %name, "generated pre-shared key secret"),
        Err(err) if util::is_conflict(&err) => {}
        Err(err) => return Err(err.into()),
    }
    Ok(())
}

/// Returns whether a quick retry is needed (lost optimistic-concurrency races).
async fn reconcile_all(ctx: &Ctx) -> anyhow::Result<bool> {
    let now = util::now();
    let lost_after = ctx.config.lost_after.as_secs() as i64;
    let mut requeue = false;

    let mut claims: Vec<Arc<UsbDeviceClaim>> = ctx
        .claims
        .state()
        .into_iter()
        .filter(|c| c.metadata.deletion_timestamp.is_none() && c.uid().is_some())
        .collect();
    claims.sort_by_key(|c| (c.creation_timestamp().map(|t| t.0), c.namespace(), c.name_any()));
    let live_claims: HashSet<String> = claims.iter().filter_map(|c| c.uid()).collect();
    let vms: HashMap<(String, String), VmInfo> = ctx
        .vms
        .state()
        .iter()
        .filter_map(|o| VmInfo::from_object(o))
        .map(|v| ((v.namespace.clone(), v.name.clone()), v))
        .collect();
    let vmis: HashMap<(String, String), VmiInfo> = ctx
        .vmis
        .state()
        .iter()
        .filter_map(|o| VmiInfo::from_object(o))
        .map(|v| ((v.namespace.clone(), v.name.clone()), v))
        .collect();
    let mut devices: BTreeMap<String, UsbDevice> = ctx
        .devices
        .state()
        .iter()
        .map(|d| (d.name_any(), (**d).clone()))
        .collect();

    for device in devices.values_mut() {
        let status = device.status_ref();
        if status.phase == Some(DevicePhase::Available)
            && util::age_secs(status.last_seen.as_deref(), now).is_none_or(|age| age > lost_after)
        {
            let message = format!(
                "no heartbeat from the agent on {} for over {lost_after}s",
                status.node.as_deref().unwrap_or("?")
            );
            let patch = json!({ "status": { "phase": DevicePhase::Lost, "exporter": null, "message": message } });
            match cas_patch_status(ctx, device, patch).await? {
                Some(updated) => {
                    warn!(device = %device.name_any(), %message, "device lost");
                    *device = updated;
                }
                None => requeue = true,
            }
        }
        if let Some(lease) = &device.status_ref().attached_to
            && !live_claims.contains(&lease.claim_uid)
        {
            let released = format!("{}/{}", lease.namespace, lease.claim);
            match cas_patch_status(ctx, device, json!({ "status": { "attachedTo": null } })).await? {
                Some(updated) => {
                    info!(device = %device.name_any(), claim = %released, "released lease of deleted claim");
                    *device = updated;
                }
                None => requeue = true,
            }
        }
    }

    let slots = allocate_slots(
        &claims
            .iter()
            .map(|c| SlotRequest {
                uid: c.uid().unwrap_or_default(),
                name: c.name_any(),
                vm: (c.namespace().unwrap_or_default(), c.spec.vm_name.clone()),
                pinned: c.spec.slot,
                previous: c.status.as_ref().and_then(|s| s.slot),
            })
            .collect::<Vec<_>>(),
    );

    for claim in &claims {
        match reconcile_claim(ctx, claim, &mut devices, &vms, &vmis, &slots, now).await {
            Ok(r) => requeue |= r,
            Err(err) => {
                warn!(claim = %format!("{}/{}", claim.namespace().unwrap_or_default(), claim.name_any()), "reconcile failed: {err:#}");
                requeue = true;
            }
        }
    }
    Ok(requeue)
}

async fn reconcile_claim(
    ctx: &Ctx,
    claim: &UsbDeviceClaim,
    devices: &mut BTreeMap<String, UsbDevice>,
    vms: &HashMap<(String, String), VmInfo>,
    vmis: &HashMap<(String, String), VmiInfo>,
    slots: &HashMap<String, Result<i32, String>>,
    now: jiff::Timestamp,
) -> anyhow::Result<bool> {
    let uid = claim.uid().unwrap_or_default();
    let namespace = claim.namespace().unwrap_or_default();
    let lost_after = ctx.config.lost_after.as_secs() as i64;
    let selector = &claim.spec.selector;
    let previous = claim.status.clone().unwrap_or_default();
    let mut requeue = false;
    let vm_key = (namespace.clone(), claim.spec.vm_name.clone());
    let (vm, vmi) = (vms.get(&vm_key), vmis.get(&vm_key));

    // Bind the claim's lifetime to its VM so deleting the VM garbage-collects the claim.
    if !claim.spec.vm_name.is_empty()
        && let Some(owner_references) = claim_owner_references(
            claim.owner_references(),
            &claim.spec.vm_name,
            vm_owner(vm, vmi).as_ref(),
        )
    {
        let api: Api<UsbDeviceClaim> = Api::namespaced(ctx.client.clone(), &namespace);
        let patch = json!({
            "metadata": { "resourceVersion": claim.resource_version(), "ownerReferences": owner_references }
        });
        match api
            .patch(&claim.name_any(), &PatchParams::default(), &Patch::Merge(&patch))
            .await
        {
            Ok(_) => info!(
                claim = %format!("{namespace}/{}", claim.name_any()),
                owners = ?owner_references.iter().map(|r| format!("{}/{}", r.kind, r.name)).collect::<Vec<_>>(),
                "updated claim owner references"
            ),
            Err(err) if util::is_conflict(&err) || util::is_not_found(&err) => requeue = true,
            Err(err) => return Err(err.into()),
        }
    }
    // Only a claim whose VM exists may hold a device; a deleted VM releases it right away.
    let vm_present = vm_present(vm, vmi);

    let invalid = selector
        .validate()
        .err()
        .or_else(|| {
            claim
                .spec
                .vm_name
                .is_empty()
                .then(|| "spec.vmName is required".to_string())
        })
        .or_else(|| slots.get(&uid).and_then(|s| s.clone().err()));

    let held: Vec<String> = devices
        .values()
        .filter(|d| d.lease_holder() == Some(uid.as_str()))
        .map(|d| d.name_any())
        .collect();
    let mut target: Option<String> = None;
    if invalid.is_none() && vm_present {
        let current = held
            .iter()
            .filter(|n| selector.matches(&devices[*n]))
            .min_by_key(|n| {
                (
                    previous.device_name.as_deref() != Some(n.as_str()),
                    !devices[*n].is_available(now, lost_after),
                    n.to_string(),
                )
            })
            .cloned();
        if current
            .as_ref()
            .is_some_and(|n| devices[n].is_available(now, lost_after))
        {
            target = current;
        } else {
            // Prefer a free device that is available right now; otherwise reserve a free one that
            // will be attached when it is plugged in, unless we already hold one.
            let mut free: Vec<&UsbDevice> = devices
                .values()
                .filter(|d| d.lease_holder().is_none() && selector.matches(d))
                .collect();
            free.sort_by_key(|d| (!d.is_available(now, lost_after), d.name_any()));
            let candidate = free
                .first()
                .filter(|d| d.is_available(now, lost_after) || current.is_none())
                .map(|d| d.name_any());
            target = current;
            if let Some(candidate) = candidate {
                let lease = Attachment {
                    namespace: namespace.clone(),
                    claim: claim.name_any(),
                    claim_uid: uid.clone(),
                    vm_name: claim.spec.vm_name.clone(),
                    since: util::rfc3339(now),
                };
                let device = devices.get_mut(&candidate).expect("candidate exists");
                match cas_patch_status(ctx, device, json!({ "status": { "attachedTo": lease } })).await? {
                    Some(updated) => {
                        info!(claim = %format!("{namespace}/{}", claim.name_any()), device = %candidate, "leased device");
                        *device = updated;
                        target = Some(candidate);
                    }
                    None => requeue = true,
                }
            }
        }
    }

    for name in held.iter().filter(|n| Some(*n) != target.as_ref()) {
        let device = devices.get_mut(name).expect("held device exists");
        match cas_patch_status(ctx, device, json!({ "status": { "attachedTo": null } })).await? {
            Some(updated) => {
                info!(claim = %format!("{namespace}/{}", claim.name_any()), device = %name, "released device");
                *device = updated;
            }
            None => requeue = true,
        }
    }

    let device = target.as_ref().and_then(|n| devices.get(n));
    let slot = slots.get(&uid).and_then(|s| s.clone().ok());
    let (phase, message) = compute_phase(&PhaseInputs {
        invalid: invalid.as_deref(),
        device,
        device_available: device.is_some_and(|d| d.is_available(now, lost_after)),
        vm,
        vmi,
        vm_present,
        claim,
        slot,
    });

    let desired = json!({
        "phase": phase,
        "deviceName": target,
        "sourceNode": device.and_then(|d| d.status_ref().node.clone()),
        "slot": slot,
        "message": message,
        "observedGeneration": claim.metadata.generation,
    });
    let current = json!({
        "phase": previous.phase,
        "deviceName": previous.device_name,
        "sourceNode": previous.source_node,
        "slot": previous.slot,
        "message": previous.message,
        "observedGeneration": previous.observed_generation,
    });
    if desired != current {
        if previous.phase != Some(phase) {
            info!(claim = %format!("{namespace}/{}", claim.name_any()), ?phase, %message, "claim phase changed");
        }
        let api: Api<UsbDeviceClaim> = Api::namespaced(ctx.client.clone(), &namespace);
        match api
            .patch_status(
                &claim.name_any(),
                &PatchParams::default(),
                &Patch::Merge(&json!({ "status": desired })),
            )
            .await
        {
            Ok(_) => {}
            Err(err) if util::is_not_found(&err) => {}
            Err(err) => return Err(err.into()),
        }
    }
    Ok(requeue)
}

/// Merge-patches a device's status guarded by its resourceVersion. `None` means someone else
/// changed the device first (or it was deleted); the caller retries on fresh data.
async fn cas_patch_status(ctx: &Ctx, device: &UsbDevice, mut patch: Value) -> anyhow::Result<Option<UsbDevice>> {
    patch["metadata"] = json!({ "resourceVersion": device.resource_version() });
    match ctx
        .devices_api
        .patch_status(&device.name_any(), &PatchParams::default(), &Patch::Merge(&patch))
        .await
    {
        Ok(updated) => Ok(Some(updated)),
        Err(err) if util::is_conflict(&err) || util::is_not_found(&err) => {
            debug!(device = %device.name_any(), "lost optimistic concurrency race");
            Ok(None)
        }
        Err(err) => Err(err.into()),
    }
}

pub struct PhaseInputs<'a> {
    pub invalid: Option<&'a str>,
    pub device: Option<&'a UsbDevice>,
    pub device_available: bool,
    pub vm: Option<&'a VmInfo>,
    pub vmi: Option<&'a VmiInfo>,
    /// The VM exists and is not being deleted (see [`vm_present`]).
    pub vm_present: bool,
    pub claim: &'a UsbDeviceClaim,
    pub slot: Option<i32>,
}

pub fn compute_phase(inputs: &PhaseInputs) -> (ClaimPhase, String) {
    if let Some(reason) = inputs.invalid {
        return (ClaimPhase::Invalid, reason.to_string());
    }
    let vm = &inputs.claim.spec.vm_name;
    if !inputs.vm_present {
        let state = if inputs.vm.is_some_and(|v| v.deleting) || inputs.vmi.is_some_and(|v| v.deleting) {
            "is being deleted"
        } else {
            "does not exist"
        };
        return (
            ClaimPhase::WaitingForVM,
            format!("VirtualMachine {vm} {state}; no device is reserved"),
        );
    }
    let Some(device) = inputs.device else {
        return (ClaimPhase::Pending, "no free UsbDevice matches the selector".into());
    };
    let device_name = device.name_any();
    if !inputs.device_available {
        let status = device.status_ref();
        let phase = status
            .phase
            .map(|p| format!("{p:?}"))
            .unwrap_or_else(|| "not reported".into());
        let node = status
            .node
            .as_deref()
            .map(|n| format!(" (last seen on {n})"))
            .unwrap_or_default();
        return (
            ClaimPhase::DeviceUnavailable,
            format!("device {device_name} is {phase}{node}"),
        );
    }
    // A running VMI reflects the configuration the VM was started with; the template applies next.
    let client_passthrough = match (inputs.vmi, inputs.vm) {
        (Some(vmi), _) => vmi.client_passthrough,
        (None, Some(vm)) => vm.client_passthrough,
        (None, None) => true,
    };
    if !client_passthrough {
        return (
            ClaimPhase::VMNotConfigured,
            "VM needs spec.domain.devices.clientPassthrough: {} (set it in the VM template and restart)".into(),
        );
    }
    let Some(vmi) = inputs.vmi else {
        let status = inputs
            .vm
            .and_then(|v| v.printable_status.as_deref())
            .unwrap_or("Stopped");
        return (ClaimPhase::WaitingForVM, format!("VirtualMachine {vm} is {status}"));
    };
    if !vmi.is_running() {
        return (
            ClaimPhase::WaitingForVM,
            format!(
                "VirtualMachineInstance {vm} is {}",
                vmi.phase.as_deref().unwrap_or("starting")
            ),
        );
    }
    let connection = inputs.claim.status.as_ref().and_then(|s| s.connection.as_ref());
    let attached = connection.is_some_and(|c| {
        c.state == ConnectionState::Connected
            && Some(c.node.as_str()) == vmi.node.as_deref()
            && c.device_name == device_name
            && Some(c.slot) == inputs.slot
    });
    if attached {
        let source = device.status_ref().node.as_deref().unwrap_or("?");
        return (
            ClaimPhase::Attached,
            format!("{device_name} on {source} is attached to {vm}"),
        );
    }
    let detail = connection
        .filter(|c| Some(c.node.as_str()) == vmi.node.as_deref())
        .and_then(|c| c.message.clone())
        .unwrap_or_else(|| "waiting for the agent on the VM's node".into());
    (ClaimPhase::Connecting, detail)
}

pub struct SlotRequest {
    pub uid: String,
    pub name: String,
    /// (namespace, vm name)
    pub vm: (String, String),
    pub pinned: Option<i32>,
    pub previous: Option<i32>,
}

/// Assigns each claim a usbredir slot of its VM. Requests must be ordered oldest first.
///
/// Pinned slots win, then claims keep the slot they already have, then the remaining claims get
/// the highest free slot (virt-handler hands out slots to `virtctl usbredir` from 0 upwards).
pub fn allocate_slots(requests: &[SlotRequest]) -> HashMap<String, Result<i32, String>> {
    let mut result = HashMap::new();
    let mut by_vm: BTreeMap<&(String, String), Vec<&SlotRequest>> = BTreeMap::new();
    for request in requests {
        by_vm.entry(&request.vm).or_default().push(request);
    }
    let in_range = |slot: i32| (0..USBREDIR_SLOTS).contains(&slot);

    for requests in by_vm.values() {
        let mut used: HashMap<i32, &str> = HashMap::new();
        for r in requests {
            if let Some(slot) = r.pinned {
                if !in_range(slot) {
                    result.insert(
                        r.uid.clone(),
                        Err(format!("spec.slot must be between 0 and {}", USBREDIR_SLOTS - 1)),
                    );
                } else if let Some(owner) = used.get(&slot) {
                    result.insert(
                        r.uid.clone(),
                        Err(format!("slot {slot} is already pinned by claim {owner}")),
                    );
                } else {
                    used.insert(slot, &r.name);
                    result.insert(r.uid.clone(), Ok(slot));
                }
            }
        }
        for r in requests.iter().filter(|r| r.pinned.is_none()) {
            if let Some(slot) = r.previous.filter(|s| in_range(*s) && !used.contains_key(s)) {
                used.insert(slot, &r.name);
                result.insert(r.uid.clone(), Ok(slot));
            }
        }
        let unassigned: Vec<&&SlotRequest> = requests.iter().filter(|r| !result.contains_key(&r.uid)).collect();
        for r in unassigned {
            match (0..USBREDIR_SLOTS).rev().find(|s| !used.contains_key(s)) {
                Some(slot) => {
                    used.insert(slot, &r.name);
                    result.insert(r.uid.clone(), Ok(slot));
                }
                None => {
                    result.insert(
                        r.uid.clone(),
                        Err(format!("all {USBREDIR_SLOTS} usbredir slots of the VM are in use")),
                    );
                }
            }
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::crd::{
        ConnectionStatus, DeviceSelector, ExporterEndpoint, UsbDeviceClaimSpec, UsbDeviceClaimStatus, UsbDeviceSpec,
        UsbDeviceStatus,
    };

    fn req(uid: &str, vm: &str, pinned: Option<i32>, previous: Option<i32>) -> SlotRequest {
        SlotRequest {
            uid: uid.into(),
            name: uid.into(),
            vm: ("default".into(), vm.into()),
            pinned,
            previous,
        }
    }

    #[test]
    fn slots_are_allocated_from_the_top() {
        let slots = allocate_slots(&[
            req("a", "vm-a", None, None),
            req("b", "vm-a", None, None),
            req("c", "vm-b", None, None),
        ]);
        assert_eq!(slots["a"], Ok(3));
        assert_eq!(slots["b"], Ok(2));
        assert_eq!(slots["c"], Ok(3));
    }

    #[test]
    fn existing_slots_are_kept_and_pins_win() {
        let slots = allocate_slots(&[
            req("a", "vm-a", None, Some(3)),
            req("b", "vm-a", None, Some(1)),
            req("c", "vm-a", Some(3), None),
        ]);
        assert_eq!(slots["c"], Ok(3));
        assert_eq!(slots["b"], Ok(1));
        assert_eq!(slots["a"], Ok(2), "displaced by the pin, gets the next free slot");
    }

    #[test]
    fn slot_exhaustion_and_conflicting_pins() {
        let reqs: Vec<_> = (0..5).map(|i| req(&format!("c{i}"), "vm-a", None, None)).collect();
        let slots = allocate_slots(&reqs);
        assert_eq!(slots.values().filter(|s| s.is_ok()).count(), 4);
        assert!(slots["c4"].is_err());

        let slots = allocate_slots(&[
            req("a", "vm-a", Some(0), None),
            req("b", "vm-a", Some(0), None),
            req("c", "vm-a", Some(9), None),
        ]);
        assert_eq!(slots["a"], Ok(0));
        assert!(slots["b"].as_ref().unwrap_err().contains("already pinned"));
        assert!(slots["c"].is_err());
    }

    fn device(available: bool) -> UsbDevice {
        let mut d = UsbDevice::new("usb-1a86-7523-usb-serial-1234abcd", UsbDeviceSpec::default());
        d.status = Some(UsbDeviceStatus {
            phase: Some(if available {
                DevicePhase::Available
            } else {
                DevicePhase::Unplugged
            }),
            node: Some("node-c".into()),
            exporter: available.then(|| ExporterEndpoint {
                address: "192.0.2.10".into(),
                port: 7575,
                pod: "a".into(),
            }),
            ..Default::default()
        });
        d
    }

    fn claim(connection: Option<ConnectionStatus>) -> UsbDeviceClaim {
        let mut c = UsbDeviceClaim::new(
            "usb-claim",
            UsbDeviceClaimSpec {
                vm_name: "my-vm".into(),
                selector: DeviceSelector::default(),
                slot: None,
            },
        );
        c.status = Some(UsbDeviceClaimStatus {
            connection,
            ..Default::default()
        });
        c
    }

    fn vmi(running: bool, passthrough: bool) -> VmiInfo {
        VmiInfo {
            namespace: "default".into(),
            name: "my-vm".into(),
            uid: "u".into(),
            phase: Some(if running { "Running" } else { "Scheduling" }.into()),
            node: running.then(|| "node-a".to_string()),
            client_passthrough: passthrough,
            active_pods: BTreeMap::new(),
            controlled_by_vm: true,
            deleting: false,
        }
    }

    fn vm(passthrough: bool, deleting: bool) -> VmInfo {
        VmInfo {
            namespace: "default".into(),
            name: "my-vm".into(),
            uid: "vm-uid".into(),
            client_passthrough: passthrough,
            printable_status: Some("Stopped".into()),
            deleting,
        }
    }

    fn phase_of(
        device: Option<&UsbDevice>,
        vm: Option<&VmInfo>,
        vmi: Option<&VmiInfo>,
        claim: &UsbDeviceClaim,
    ) -> (ClaimPhase, String) {
        compute_phase(&PhaseInputs {
            invalid: None,
            device,
            device_available: device.is_some_and(|d| d.status_ref().phase == Some(DevicePhase::Available)),
            vm,
            vmi,
            vm_present: crate::kubevirt::vm_present(vm, vmi),
            claim,
            slot: Some(3),
        })
    }

    #[test]
    fn phases() {
        let c = claim(None);
        let dev = device(true);
        let unplugged = device(false);
        let the_vm = vm(true, false);
        let running = vmi(true, true);
        let phase = |device: Option<&UsbDevice>, vmi: Option<&VmiInfo>, claim: &UsbDeviceClaim| {
            phase_of(device, Some(&the_vm), vmi, claim).0
        };
        assert_eq!(phase(None, Some(&running), &c), ClaimPhase::Pending);
        assert_eq!(
            phase(Some(&unplugged), Some(&running), &c),
            ClaimPhase::DeviceUnavailable
        );
        assert_eq!(phase(Some(&dev), None, &c), ClaimPhase::WaitingForVM, "stopped VM");
        assert_eq!(phase(Some(&dev), Some(&vmi(false, true)), &c), ClaimPhase::WaitingForVM);
        assert_eq!(
            phase(Some(&dev), Some(&vmi(true, false)), &c),
            ClaimPhase::VMNotConfigured
        );
        assert_eq!(phase(Some(&dev), Some(&running), &c), ClaimPhase::Connecting);

        let connected = |node: &str, slot: i32| {
            claim(Some(ConnectionStatus {
                state: ConnectionState::Connected,
                node: node.into(),
                device_name: dev.name_any(),
                slot,
                since: "2026-09-16T12:00:00Z".into(),
                message: None,
            }))
        };
        assert_eq!(
            phase(Some(&dev), Some(&running), &connected("node-a", 3)),
            ClaimPhase::Attached
        );
        assert_eq!(
            phase(Some(&dev), Some(&running), &connected("node-b", 3)),
            ClaimPhase::Connecting,
            "stale report from before a migration"
        );
        assert_eq!(
            phase(Some(&dev), Some(&running), &connected("node-a", 1)),
            ClaimPhase::Connecting,
            "stale report from an old slot"
        );

        let invalid = compute_phase(&PhaseInputs {
            invalid: Some("bad"),
            device: None,
            device_available: false,
            vm: None,
            vmi: None,
            vm_present: false,
            claim: &c,
            slot: None,
        });
        assert_eq!(invalid.0, ClaimPhase::Invalid);
    }

    #[test]
    fn vm_lifecycle_phases() {
        let c = claim(None);
        let dev = device(true);

        let (phase, message) = phase_of(None, None, None, &c);
        assert_eq!(phase, ClaimPhase::WaitingForVM);
        assert_eq!(message, "VirtualMachine my-vm does not exist; no device is reserved");

        let deleting = vm(true, true);
        let (phase, message) = phase_of(Some(&dev), Some(&deleting), Some(&vmi(true, true)), &c);
        assert_eq!(phase, ClaimPhase::WaitingForVM, "a deleted VM does not keep its device");
        assert!(message.contains("is being deleted"), "{message}");

        let (phase, message) = phase_of(Some(&dev), Some(&vm(true, false)), None, &c);
        assert_eq!(
            (phase, message.as_str()),
            (ClaimPhase::WaitingForVM, "VirtualMachine my-vm is Stopped")
        );

        // A stopped VM without clientPassthrough in its template is reported before it is started.
        assert_eq!(
            phase_of(Some(&dev), Some(&vm(false, false)), None, &c).0,
            ClaimPhase::VMNotConfigured
        );

        // A bare VMI (no VirtualMachine object) is a VM too.
        let bare = VmiInfo {
            controlled_by_vm: false,
            ..vmi(true, true)
        };
        assert_eq!(phase_of(Some(&dev), None, Some(&bare), &c).0, ClaimPhase::Connecting);
    }
}
