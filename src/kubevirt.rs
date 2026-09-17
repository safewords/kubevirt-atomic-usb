//! Minimal, version-tolerant views of KubeVirt `VirtualMachine` and `VirtualMachineInstance`
//! objects, and how a `UsbDeviceClaim`'s lifetime is tied to them.

use std::collections::BTreeMap;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::ResourceExt;
use kube::api::{ApiResource, DynamicObject, GroupVersionKind};

/// Number of `virt-usbredir-N` sockets KubeVirt creates for `clientPassthrough`
/// (`v1.UsbClientPassthroughMaxNumberOf`).
pub const USBREDIR_SLOTS: i32 = 4;

const KIND_VM: &str = "VirtualMachine";
const KIND_VMI: &str = "VirtualMachineInstance";
const API_VERSION: &str = "kubevirt.io/v1";
const CONDITION_PAUSED: &str = "Paused";
const START_STRATEGY_PAUSED: &str = "Paused";

pub fn vmi_resource() -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk("kubevirt.io", "v1", KIND_VMI),
        "virtualmachineinstances",
    )
}

pub fn vm_resource() -> ApiResource {
    ApiResource::from_gvk_with_plural(&GroupVersionKind::gvk("kubevirt.io", "v1", KIND_VM), "virtualmachines")
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VmiInfo {
    pub namespace: String,
    pub name: String,
    pub uid: String,
    pub phase: Option<String>,
    pub node: Option<String>,
    /// `spec.domain.devices.clientPassthrough` is set, so usbredir sockets exist.
    pub client_passthrough: bool,
    /// `spec.startStrategy: Paused`: KubeVirt started this instance with its vCPUs stopped.
    pub start_paused: bool,
    /// The `Paused` condition is true: the vCPUs are stopped, so QEMU ignores its usbredir
    /// chardevs and the guest sees nothing of an attached device until it resumes.
    pub paused: bool,
    /// RFC 3339 time the `Paused` condition last changed, i.e. when the pause began.
    pub paused_since: Option<String>,
    /// virt-launcher pod UID -> node name. Contains two pods during live migration.
    pub active_pods: BTreeMap<String, String>,
    /// Created and controlled by a `VirtualMachine` (as opposed to a bare VMI).
    pub controlled_by_vm: bool,
    pub deleting: bool,
}

impl VmiInfo {
    pub fn from_object(obj: &DynamicObject) -> Option<Self> {
        let spec = obj.data.get("spec");
        let status = obj.data.get("status");
        let str_at = |v: Option<&serde_json::Value>, key: &str| {
            v.and_then(|v| v.get(key)).and_then(|v| v.as_str()).map(str::to_string)
        };
        let client_passthrough = spec
            .and_then(|s| s.pointer("/domain/devices/clientPassthrough"))
            .is_some_and(|v| !v.is_null());
        let active_pods = status
            .and_then(|s| s.get("activePods"))
            .and_then(|v| v.as_object())
            .map(|pods| {
                pods.iter()
                    .filter_map(|(uid, node)| Some((uid.clone(), node.as_str()?.to_string())))
                    .collect()
            })
            .unwrap_or_default();
        let controlled_by_vm = obj
            .owner_references()
            .iter()
            .any(|r| r.kind == KIND_VM && r.controller == Some(true));
        let paused_condition = status
            .and_then(|s| s.get("conditions"))
            .and_then(|v| v.as_array())
            .and_then(|conditions| {
                conditions
                    .iter()
                    .find(|c| c.get("type").and_then(|v| v.as_str()) == Some(CONDITION_PAUSED))
            })
            .filter(|c| c.get("status").and_then(|v| v.as_str()) == Some("True"));
        Some(Self {
            namespace: obj.namespace()?,
            name: obj.name_any(),
            uid: obj.uid()?,
            phase: str_at(status, "phase"),
            node: str_at(status, "nodeName").filter(|n| !n.is_empty()),
            client_passthrough,
            start_paused: str_at(spec, "startStrategy").as_deref() == Some(START_STRATEGY_PAUSED),
            paused: paused_condition.is_some(),
            paused_since: paused_condition.and_then(|c| str_at(Some(c), "lastTransitionTime")),
            active_pods,
            controlled_by_vm,
            deleting: obj.metadata.deletion_timestamp.is_some(),
        })
    }

    pub fn is_running(&self) -> bool {
        self.phase.as_deref() == Some("Running") && self.node.is_some()
    }

    /// virt-launcher pods of this VMI scheduled on `node`, preferring the VMI's current node.
    pub fn launcher_pods_on(&self, node: &str) -> Vec<&str> {
        self.active_pods
            .iter()
            .filter(|(_, n)| n.as_str() == node)
            .map(|(uid, _)| uid.as_str())
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VmInfo {
    pub namespace: String,
    pub name: String,
    pub uid: String,
    /// `spec.template.spec.domain.devices.clientPassthrough` is set.
    pub client_passthrough: bool,
    /// `spec.template.spec.startStrategy: Paused`, i.e. instances start with stopped vCPUs and
    /// their boot can be held (see `UsbDeviceClaimSpec::hold_boot`).
    pub start_paused: bool,
    /// `status.printableStatus`, e.g. `Stopped` or `Running`.
    pub printable_status: Option<String>,
    pub deleting: bool,
}

impl VmInfo {
    pub fn from_object(obj: &DynamicObject) -> Option<Self> {
        Some(Self {
            namespace: obj.namespace()?,
            name: obj.name_any(),
            uid: obj.uid()?,
            client_passthrough: obj
                .data
                .pointer("/spec/template/spec/domain/devices/clientPassthrough")
                .is_some_and(|v| !v.is_null()),
            start_paused: obj
                .data
                .pointer("/spec/template/spec/startStrategy")
                .and_then(|v| v.as_str())
                == Some(START_STRATEGY_PAUSED),
            printable_status: obj
                .data
                .pointer("/status/printableStatus")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            deleting: obj.metadata.deletion_timestamp.is_some(),
        })
    }
}

/// The KubeVirt object a claim's lifetime is bound to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VmOwner {
    pub kind: &'static str,
    pub name: String,
    pub uid: String,
}

impl VmOwner {
    /// A non-controller reference that does not block the owner's deletion, so setting it needs
    /// no permissions on the VM itself.
    pub fn owner_reference(&self) -> OwnerReference {
        OwnerReference {
            api_version: API_VERSION.to_string(),
            kind: self.kind.to_string(),
            name: self.name.clone(),
            uid: self.uid.clone(),
            controller: Some(false),
            block_owner_deletion: Some(false),
        }
    }
}

/// Resolves what a claim for a VM belongs to: the `VirtualMachine`, or a bare VMI. A VMI created
/// by a `VirtualMachine` is never the owner, because it is replaced on every restart.
pub fn vm_owner(vm: Option<&VmInfo>, vmi: Option<&VmiInfo>) -> Option<VmOwner> {
    if let Some(vm) = vm {
        return Some(VmOwner {
            kind: KIND_VM,
            name: vm.name.clone(),
            uid: vm.uid.clone(),
        });
    }
    vmi.filter(|vmi| !vmi.controlled_by_vm).map(|vmi| VmOwner {
        kind: KIND_VMI,
        name: vmi.name.clone(),
        uid: vmi.uid.clone(),
    })
}

/// Resumes a paused `VirtualMachineInstance` through KubeVirt's subresource API, the same call
/// `virtctl unpause` makes. Needs `update` on `virtualmachineinstances/unpause` in the
/// `subresources.kubevirt.io` group.
pub async fn unpause(client: &kube::Client, namespace: &str, name: &str) -> anyhow::Result<()> {
    let path =
        format!("/apis/subresources.kubevirt.io/v1/namespaces/{namespace}/virtualmachineinstances/{name}/unpause");
    let request = http::Request::put(path)
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(b"{}".to_vec())?;
    client.request_text(request).await?;
    Ok(())
}

/// Whether the claim's VM exists and is not being deleted, i.e. whether it may hold a device.
/// A stopped `VirtualMachine` counts as present, so it gets its device back when started.
pub fn vm_present(vm: Option<&VmInfo>, vmi: Option<&VmiInfo>) -> bool {
    match (vm, vmi) {
        (Some(vm), _) => !vm.deleting,
        (None, Some(vmi)) => !vmi.deleting,
        (None, None) => false,
    }
}

fn is_kubevirt_vm_reference(r: &OwnerReference) -> bool {
    r.api_version.starts_with("kubevirt.io/") && (r.kind == KIND_VM || r.kind == KIND_VMI)
}

/// The owner references a claim for `vm_name` should have, or `None` if `existing` is already
/// right. References to other kinds are left alone.
///
/// With an owner, all KubeVirt VM/VMI references are replaced by it (the VM may have been
/// recreated, or `vmName` changed). Without one, references to the same VM name are kept so
/// garbage collection can still delete the claim after its VM is gone; only references to other
/// VM names are dropped.
pub fn claim_owner_references(
    existing: &[OwnerReference],
    vm_name: &str,
    owner: Option<&VmOwner>,
) -> Option<Vec<OwnerReference>> {
    let mut desired: Vec<OwnerReference> = existing
        .iter()
        .filter(|r| !is_kubevirt_vm_reference(r) || (owner.is_none() && r.name == vm_name))
        .cloned()
        .collect();
    if let Some(owner) = owner {
        desired.push(owner.owner_reference());
    }
    (desired != existing).then_some(desired)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vmi(json: serde_json::Value) -> VmiInfo {
        let obj: DynamicObject = serde_json::from_value(json).unwrap();
        VmiInfo::from_object(&obj).unwrap()
    }

    fn vm(json: serde_json::Value) -> VmInfo {
        let obj: DynamicObject = serde_json::from_value(json).unwrap();
        VmInfo::from_object(&obj).unwrap()
    }

    #[test]
    fn parses_running_vmi() {
        let info = vmi(serde_json::json!({
            "apiVersion": "kubevirt.io/v1", "kind": "VirtualMachineInstance",
            "metadata": {"name": "my-vm", "namespace": "default", "uid": "b7e1"},
            "spec": {"domain": {"devices": {"clientPassthrough": {}}}},
            "status": {"phase": "Running", "nodeName": "node-a", "activePods": {"pod-a": "node-a"}}
        }));
        assert!(info.is_running());
        assert!(info.client_passthrough);
        assert!(!info.controlled_by_vm);
        assert!(!info.deleting);
        assert_eq!(info.node.as_deref(), Some("node-a"));
        assert_eq!(info.launcher_pods_on("node-a"), vec!["pod-a"]);
        assert!(info.launcher_pods_on("node-c").is_empty());
    }

    #[test]
    fn parses_a_vmi_that_started_paused() {
        let info = vmi(serde_json::json!({
            "apiVersion": "kubevirt.io/v1", "kind": "VirtualMachineInstance",
            "metadata": {"name": "my-vm", "namespace": "default", "uid": "b7e1"},
            "spec": {"startStrategy": "Paused", "domain": {"devices": {"clientPassthrough": {}}}},
            "status": {"phase": "Running", "nodeName": "node-a", "conditions": [
                {"type": "Ready", "status": "False", "lastTransitionTime": "2026-09-17T12:00:01Z"},
                {"type": "Paused", "status": "True", "reason": "PausedByUser",
                 "lastTransitionTime": "2026-09-17T12:00:02Z"}
            ]}
        }));
        assert!(info.start_paused);
        assert!(info.paused);
        assert!(info.is_running(), "a paused instance still runs, its vCPUs do not");
        assert_eq!(info.paused_since.as_deref(), Some("2026-09-17T12:00:02Z"));

        // Resumed: KubeVirt flips the condition to False rather than removing it.
        let resumed = vmi(serde_json::json!({
            "apiVersion": "kubevirt.io/v1", "kind": "VirtualMachineInstance",
            "metadata": {"name": "my-vm", "namespace": "default", "uid": "b7e1"},
            "spec": {"startStrategy": "Paused"},
            "status": {"conditions": [{"type": "Paused", "status": "False"}]}
        }));
        assert!(resumed.start_paused);
        assert!(!resumed.paused);
        assert_eq!(resumed.paused_since, None);
    }

    #[test]
    fn parses_vmi_without_passthrough_or_status() {
        let info = vmi(serde_json::json!({
            "apiVersion": "kubevirt.io/v1", "kind": "VirtualMachineInstance",
            "metadata": {"name": "vm", "namespace": "default", "uid": "u"},
            "spec": {"domain": {"devices": {}}}
        }));
        assert!(!info.is_running());
        assert!(!info.client_passthrough);
        assert!(info.active_pods.is_empty());
    }

    #[test]
    fn migration_has_two_pods() {
        let info = vmi(serde_json::json!({
            "apiVersion": "kubevirt.io/v1", "kind": "VirtualMachineInstance",
            "metadata": {"name": "vm", "namespace": "default", "uid": "u"},
            "status": {"phase": "Running", "nodeName": "a", "activePods": {"p1": "a", "p2": "b"}}
        }));
        assert_eq!(info.launcher_pods_on("b"), vec!["p2"]);
    }

    #[test]
    fn parses_vm_and_vm_controlled_vmi() {
        let v = vm(serde_json::json!({
            "apiVersion": "kubevirt.io/v1", "kind": "VirtualMachine",
            "metadata": {"name": "my-vm", "namespace": "default", "uid": "vm-uid",
                         "deletionTimestamp": "2026-09-17T02:00:00Z"},
            "spec": {"template": {"spec": {"startStrategy": "Paused",
                                           "domain": {"devices": {"clientPassthrough": {}}}}}},
            "status": {"printableStatus": "Stopped"}
        }));
        assert!(v.client_passthrough);
        assert!(v.start_paused);
        assert!(v.deleting);
        assert_eq!(v.printable_status.as_deref(), Some("Stopped"));

        let i = vmi(serde_json::json!({
            "apiVersion": "kubevirt.io/v1", "kind": "VirtualMachineInstance",
            "metadata": {"name": "my-vm", "namespace": "default", "uid": "vmi-uid",
                         "ownerReferences": [{"apiVersion": "kubevirt.io/v1", "kind": "VirtualMachine",
                                              "name": "my-vm", "uid": "vm-uid", "controller": true}]}
        }));
        assert!(i.controlled_by_vm);
    }

    fn vm_info(uid: &str, deleting: bool) -> VmInfo {
        VmInfo {
            namespace: "default".into(),
            name: "my-vm".into(),
            uid: uid.into(),
            client_passthrough: true,
            start_paused: false,
            printable_status: None,
            deleting,
        }
    }

    fn vmi_info(controlled_by_vm: bool, deleting: bool) -> VmiInfo {
        VmiInfo {
            namespace: "default".into(),
            name: "my-vm".into(),
            uid: "vmi-uid".into(),
            phase: Some("Running".into()),
            node: Some("node-a".into()),
            client_passthrough: true,
            start_paused: false,
            paused: false,
            paused_since: None,
            active_pods: BTreeMap::new(),
            controlled_by_vm,
            deleting,
        }
    }

    #[test]
    fn owner_is_the_vm_or_a_bare_vmi() {
        let vm = vm_info("vm-uid", false);
        let owned = vmi_info(true, false);
        let bare = vmi_info(false, false);
        assert_eq!(vm_owner(Some(&vm), Some(&owned)).unwrap().kind, "VirtualMachine");
        assert_eq!(vm_owner(None, Some(&bare)).unwrap().kind, "VirtualMachineInstance");
        assert_eq!(
            vm_owner(None, Some(&owned)),
            None,
            "a VM-controlled VMI is never the owner"
        );
        assert_eq!(vm_owner(None, None), None);
    }

    #[test]
    fn stopped_vms_keep_devices_deleted_ones_do_not() {
        let stopped = vm_info("vm-uid", false);
        assert!(vm_present(Some(&stopped), None));
        assert!(!vm_present(
            Some(&vm_info("vm-uid", true)),
            Some(&vmi_info(true, false))
        ));
        assert!(vm_present(None, Some(&vmi_info(false, false))));
        assert!(!vm_present(None, Some(&vmi_info(true, true))));
        assert!(!vm_present(None, None));
    }

    fn reference(kind: &str, name: &str, uid: &str) -> OwnerReference {
        OwnerReference {
            api_version: if kind == "ConfigMap" { "v1" } else { API_VERSION }.to_string(),
            kind: kind.into(),
            name: name.into(),
            uid: uid.into(),
            controller: Some(false),
            block_owner_deletion: Some(false),
        }
    }

    #[test]
    fn owner_references_are_reconciled() {
        let owner = vm_owner(Some(&vm_info("vm-uid", false)), None).unwrap();
        let unrelated = reference("ConfigMap", "cfg", "cm-uid");

        // Added next to unrelated references.
        let refs = claim_owner_references(std::slice::from_ref(&unrelated), "my-vm", Some(&owner)).unwrap();
        assert_eq!(refs, vec![unrelated.clone(), owner.owner_reference()]);
        // Already correct: nothing to do.
        assert_eq!(claim_owner_references(&refs, "my-vm", Some(&owner)), None);

        // The VM was recreated under the same name, or vmName changed: the old reference goes.
        let stale = vec![unrelated.clone(), reference("VirtualMachine", "my-vm", "old-uid")];
        assert_eq!(
            claim_owner_references(&stale, "my-vm", Some(&owner)).unwrap(),
            vec![unrelated.clone(), owner.owner_reference()]
        );

        // The VM is gone: keep its reference so garbage collection deletes the claim...
        let current = vec![reference("VirtualMachine", "my-vm", "vm-uid")];
        assert_eq!(claim_owner_references(&current, "my-vm", None), None);
        // ...but drop references to a VM the claim no longer names.
        assert_eq!(
            claim_owner_references(&[reference("VirtualMachine", "old-vm", "x")], "my-vm", None).unwrap(),
            Vec::<OwnerReference>::new()
        );
    }
}
