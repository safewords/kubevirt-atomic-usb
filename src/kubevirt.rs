//! Minimal, version-tolerant view of KubeVirt `VirtualMachineInstance` objects.

use std::collections::BTreeMap;

use kube::ResourceExt;
use kube::api::{ApiResource, DynamicObject, GroupVersionKind};

/// Number of `virt-usbredir-N` sockets KubeVirt creates for `clientPassthrough`
/// (`v1.UsbClientPassthroughMaxNumberOf`).
pub const USBREDIR_SLOTS: i32 = 4;

pub fn vmi_resource() -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk("kubevirt.io", "v1", "VirtualMachineInstance"),
        "virtualmachineinstances",
    )
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
    /// virt-launcher pod UID -> node name. Contains two pods during live migration.
    pub active_pods: BTreeMap<String, String>,
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
        Some(Self {
            namespace: obj.namespace()?,
            name: obj.name_any(),
            uid: obj.uid()?,
            phase: str_at(status, "phase"),
            node: str_at(status, "nodeName").filter(|n| !n.is_empty()),
            client_passthrough,
            active_pods,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn vmi(json: serde_json::Value) -> VmiInfo {
        let obj: DynamicObject = serde_json::from_value(json).unwrap();
        VmiInfo::from_object(&obj).unwrap()
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
        assert_eq!(info.node.as_deref(), Some("node-a"));
        assert_eq!(info.launcher_pods_on("node-a"), vec!["pod-a"]);
        assert!(info.launcher_pods_on("node-c").is_empty());
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
}
