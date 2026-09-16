//! Stable, cluster-unique Kubernetes object names for physical USB devices.
//!
//! Each device gets the name of the most unique identity tier that is unambiguous in the cluster:
//!
//! 1. [`IdentitySource::Serial`]: vendor id + product id + serial number.
//! 2. [`IdentitySource::SerialDescriptor`]: the above plus `bcdDevice`, manufacturer and product
//!    strings; separates clones that ship identical serials but different descriptors.
//! 3. [`IdentitySource::Descriptor`]: vendor id + product id + `bcdDevice` + strings, for devices
//!    without a serial number. Follows the device across ports and nodes as long as no other
//!    device in the cluster shares the fingerprint.
//! 4. [`IdentitySource::PortPath`]: node + physical port path. Always unique, but pinned to a port.
//!
//! Tiers 1-3 follow the device when it is moved to another port or node. A tier is skipped when
//! another known device (present anywhere, or previously published under a different name) shares
//! its attributes, so two identical serial-less dongles never get confused with each other.
//! Assignments are sticky for as long as the device stays plugged in, so a lookalike appearing
//! later never renames a device that is already attached to a VM.

use std::collections::{HashMap, HashSet};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::sysfs::UsbDeviceInfo;

/// Maximum length of the human-readable slug derived from a serial number or port path.
const MAX_SLUG_LEN: usize = 40;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
pub enum IdentitySource {
    #[default]
    Serial,
    SerialDescriptor,
    Descriptor,
    PortPath,
}

impl IdentitySource {
    /// Tiers from most to least unique.
    pub const ORDER: [IdentitySource; 4] = [Self::Serial, Self::SerialDescriptor, Self::Descriptor, Self::PortPath];

    fn tag(self) -> &'static str {
        match self {
            Self::Serial => "serial",
            Self::SerialDescriptor => "serial+descriptor",
            Self::Descriptor => "descriptor",
            Self::PortPath => "port",
        }
    }
}

/// Descriptor attributes that belong to the device itself (as opposed to where it is plugged in).
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Fingerprint {
    pub vendor_id: String,
    pub product_id: String,
    pub serial: Option<String>,
    pub bcd_device: Option<String>,
    pub manufacturer: Option<String>,
    pub product: Option<String>,
}

impl From<&UsbDeviceInfo> for Fingerprint {
    fn from(d: &UsbDeviceInfo) -> Self {
        Self {
            vendor_id: d.vendor_id.clone(),
            product_id: d.product_id.clone(),
            serial: d.serial.clone(),
            bcd_device: d.bcd_device.clone(),
            manufacturer: d.manufacturer.clone(),
            product: d.product.clone(),
        }
    }
}

impl Fingerprint {
    /// The serial number, unless it is blank (which identifies nothing).
    fn serial(&self) -> Option<&str> {
        self.serial.as_deref().filter(|s| !s.trim().is_empty())
    }

    /// Attribute tuple compared by a portable tier; `None` when the tier does not apply.
    fn tier_key(&self, tier: IdentitySource) -> Option<Vec<Option<&str>>> {
        let ids = [Some(self.vendor_id.as_str()), Some(self.product_id.as_str())];
        let descriptor = [
            self.bcd_device.as_deref(),
            self.manufacturer.as_deref(),
            self.product.as_deref(),
        ];
        match tier {
            IdentitySource::Serial => Some(ids.into_iter().chain([Some(self.serial()?)]).collect()),
            IdentitySource::SerialDescriptor => Some(
                ids.into_iter()
                    .chain([Some(self.serial()?)])
                    .chain(descriptor)
                    .collect(),
            ),
            IdentitySource::Descriptor => Some(ids.into_iter().chain(descriptor).collect()),
            IdentitySource::PortPath => None,
        }
    }

    /// Object name for a portable tier; `None` when the tier does not apply.
    fn portable_name(&self, tier: IdentitySource) -> Option<String> {
        let key = self.tier_key(tier)?;
        let prefix = format!("usb-{}-{}", slugify(&self.vendor_id), slugify(&self.product_id));
        let hash_input: Vec<&str> = key.iter().map(|v| v.unwrap_or("")).collect();
        Some(match tier {
            IdentitySource::Serial => {
                let serial = self.serial()?;
                let slug = slugify(serial);
                if slug == serial && !slug.is_empty() {
                    format!("{prefix}-{slug}")
                } else {
                    join_name(&prefix, &[&slug], &short_hash(tier, &hash_input))
                }
            }
            IdentitySource::SerialDescriptor => {
                join_name(&prefix, &[&slugify(self.serial()?)], &short_hash(tier, &hash_input))
            }
            IdentitySource::Descriptor => {
                let label = self.product.as_deref().or(self.manufacturer.as_deref()).unwrap_or("");
                join_name(&prefix, &[&slugify(label)], &short_hash(tier, &hash_input))
            }
            IdentitySource::PortPath => return None,
        })
    }

    fn port_name(&self, node: &str, port_path: &str) -> String {
        let prefix = format!("usb-{}-{}", slugify(&self.vendor_id), slugify(&self.product_id));
        let hash = short_hash(
            IdentitySource::PortPath,
            &[&self.vendor_id, &self.product_id, node, port_path],
        );
        join_name(&prefix, &[&slugify(node), &slugify(port_path)], &hash)
    }
}

fn join_name(prefix: &str, parts: &[&str], hash: &str) -> String {
    let mut name = prefix.to_string();
    for part in parts.iter().filter(|p| !p.is_empty()) {
        name.push('-');
        name.push_str(part);
    }
    name.push('-');
    name.push_str(hash);
    name
}

/// Identifies one enumeration of a device on this node. The kernel assigns a fresh device number
/// on every (re-)enumeration, so a replugged device is a new instance.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct InstanceKey {
    pub port_path: String,
    pub bus_num: u32,
    pub dev_num: u32,
}

impl From<&UsbDeviceInfo> for InstanceKey {
    fn from(d: &UsbDeviceInfo) -> Self {
        Self {
            port_path: d.port_path.clone(),
            bus_num: d.bus_num,
            dev_num: d.dev_num,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Assignment {
    pub name: String,
    pub source: IdentitySource,
}

/// A device already known to the cluster (a `UsbDevice` object).
#[derive(Clone, Debug)]
pub struct KnownDevice {
    pub name: String,
    pub fingerprint: Fingerprint,
    /// Node the device is currently plugged into according to a fresh heartbeat.
    pub present_on: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Decision {
    Assigned(Assignment),
    /// The preferred identity is still held by another node, which may simply not have noticed the
    /// device being unplugged yet (the device is being moved). Retry later instead of falling back.
    Deferred {
        name: String,
        held_by: String,
    },
}

pub struct AssignContext<'a> {
    pub node: &'a str,
    /// Assignments made earlier for devices that are still plugged in.
    pub sticky: &'a HashMap<InstanceKey, Assignment>,
    pub known: &'a [KnownDevice],
    /// Instances that may still wait for another node to release their preferred identity.
    pub may_defer: &'a HashSet<InstanceKey>,
}

/// Assigns identities to all devices currently plugged into this node.
pub fn assign(ctx: &AssignContext, local: &[UsbDeviceInfo]) -> HashMap<InstanceKey, Decision> {
    let known_by_name: HashMap<&str, &KnownDevice> = ctx.known.iter().map(|k| (k.name.as_str(), k)).collect();
    let fingerprints: Vec<(InstanceKey, Fingerprint)> = local
        .iter()
        .map(|d| (InstanceKey::from(d), Fingerprint::from(d)))
        .collect();
    let mut decisions = HashMap::new();
    let mut taken: HashSet<String> = HashSet::new();

    // Keep existing assignments, unless another node holds the same name and wins the tie-break
    // (two identical devices were plugged into two nodes at the same moment).
    for (key, _) in &fingerprints {
        let Some(assignment) = ctx.sticky.get(key) else {
            continue;
        };
        let lost_tie = known_by_name
            .get(assignment.name.as_str())
            .and_then(|k| k.present_on.as_deref())
            .is_some_and(|other| other != ctx.node && other < ctx.node);
        if !lost_tie && taken.insert(assignment.name.clone()) {
            decisions.insert(key.clone(), Decision::Assigned(assignment.clone()));
        }
    }

    for (key, fp) in &fingerprints {
        if decisions.contains_key(key) {
            continue;
        }
        let decision = choose(ctx, &known_by_name, &fingerprints, &taken, key, fp);
        if let Decision::Assigned(a) = &decision {
            taken.insert(a.name.clone());
        }
        decisions.insert(key.clone(), decision);
    }
    decisions
}

fn choose(
    ctx: &AssignContext,
    known_by_name: &HashMap<&str, &KnownDevice>,
    local: &[(InstanceKey, Fingerprint)],
    taken: &HashSet<String>,
    key: &InstanceKey,
    fp: &Fingerprint,
) -> Decision {
    for tier in IdentitySource::ORDER {
        let Some(name) = fp.portable_name(tier) else {
            if tier == IdentitySource::PortPath {
                break;
            }
            continue;
        };
        let tier_key = fp.tier_key(tier);
        let ambiguous_locally = local
            .iter()
            .any(|(k, other)| k != key && other.tier_key(tier) == tier_key);
        let ambiguous_in_cluster = ctx
            .known
            .iter()
            .any(|k| k.name != name && k.fingerprint.tier_key(tier) == tier_key);
        if ambiguous_locally || ambiguous_in_cluster || taken.contains(&name) {
            continue;
        }
        if let Some(held_by) = known_by_name.get(name.as_str()).and_then(|k| k.present_on.as_deref())
            && held_by != ctx.node
        {
            if ctx.may_defer.contains(key) {
                return Decision::Deferred {
                    name,
                    held_by: held_by.to_string(),
                };
            }
            continue;
        }
        return Decision::Assigned(Assignment { name, source: tier });
    }
    Decision::Assigned(Assignment {
        name: fp.port_name(ctx.node, &key.port_path),
        source: IdentitySource::PortPath,
    })
}

/// Lowercase `[a-z0-9-]` slug, runs of other characters collapsed to one `-`, trimmed and truncated.
fn slugify(input: &str) -> String {
    let mut out = String::with_capacity(input.len().min(MAX_SLUG_LEN));
    for ch in input.chars() {
        let mapped = match ch {
            'a'..='z' | '0'..='9' => ch,
            'A'..='Z' => ch.to_ascii_lowercase(),
            _ => '-',
        };
        if mapped == '-' && (out.is_empty() || out.ends_with('-')) {
            continue;
        }
        out.push(mapped);
        if out.len() >= MAX_SLUG_LEN {
            break;
        }
    }
    while out.ends_with('-') {
        out.pop();
    }
    out
}

fn short_hash(tier: IdentitySource, parts: &[&str]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(tier.tag().as_bytes());
    for part in parts {
        hasher.update([0u8]);
        hasher.update(part.as_bytes());
    }
    hex::encode(&hasher.finalize()[..4])
}

/// Converts an arbitrary string into a valid label value (<= 63 chars), or `None` if nothing remains.
pub fn label_value(input: &str) -> Option<String> {
    let mut out: String = input
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c
            } else {
                '-'
            }
        })
        .take(63)
        .collect();
    while out.ends_with(|c: char| !c.is_ascii_alphanumeric()) {
        out.pop();
    }
    let start = out.find(|c: char| c.is_ascii_alphanumeric())?;
    Some(out[start..].to_string())
}

/// Normalizes a USB vendor/product id: lowercase hex, `0x` prefix stripped, zero-padded to 4 digits.
pub fn normalize_usb_id(id: &str) -> Option<String> {
    let trimmed = id.trim();
    let hex = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
        .unwrap_or(trimmed);
    if hex.is_empty() || hex.len() > 4 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    Some(format!("{:0>4}", hex.to_ascii_lowercase()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn is_dns_subdomain(name: &str) -> bool {
        name.len() <= 253
            && name.split('.').all(|seg| {
                !seg.is_empty()
                    && seg
                        .chars()
                        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
                    && !seg.starts_with('-')
                    && !seg.ends_with('-')
            })
    }

    fn dev(
        port: &str,
        devnum: u32,
        vid: &str,
        pid: &str,
        serial: Option<&str>,
        product: Option<&str>,
    ) -> UsbDeviceInfo {
        UsbDeviceInfo {
            port_path: port.into(),
            bus_num: port.split('-').next().unwrap().parse().unwrap(),
            dev_num: devnum,
            vendor_id: vid.into(),
            product_id: pid.into(),
            serial: serial.map(Into::into),
            manufacturer: None,
            product: product.map(Into::into),
            bcd_device: Some("0264".into()),
            device_class: 0xff,
            interface_classes: vec![],
            speed: Some("12".into()),
        }
    }

    /// A CH340 USB serial adapter, which has no serial number.
    fn ch340(port: &str, devnum: u32) -> UsbDeviceInfo {
        dev(port, devnum, "1a86", "7523", None, Some("USB Serial"))
    }

    fn known_dev(d: &UsbDeviceInfo, name: &str, present_on: Option<&str>) -> KnownDevice {
        KnownDevice {
            name: name.into(),
            fingerprint: d.into(),
            present_on: present_on.map(Into::into),
        }
    }

    fn run(
        node: &str,
        local: &[UsbDeviceInfo],
        sticky: &HashMap<InstanceKey, Assignment>,
        known: &[KnownDevice],
        may_defer: &HashSet<InstanceKey>,
    ) -> HashMap<InstanceKey, Decision> {
        assign(
            &AssignContext {
                node,
                sticky,
                known,
                may_defer,
            },
            local,
        )
    }

    fn assigned(decisions: &HashMap<InstanceKey, Decision>, d: &UsbDeviceInfo) -> Assignment {
        match &decisions[&InstanceKey::from(d)] {
            Decision::Assigned(a) => a.clone(),
            other => panic!("expected assignment, got {other:?}"),
        }
    }

    fn none() -> (HashMap<InstanceKey, Assignment>, Vec<KnownDevice>, HashSet<InstanceKey>) {
        (HashMap::new(), Vec::new(), HashSet::new())
    }

    #[test]
    fn serial_tier_is_preferred_and_readable() {
        let (sticky, known, defer) = none();
        let d = dev(
            "1-2",
            7,
            "10c4",
            "ea60",
            Some("a1b2c3d4"),
            Some("CP2102N USB to UART Bridge Controller"),
        );
        let a = assigned(&run("node-c", std::slice::from_ref(&d), &sticky, &known, &defer), &d);
        assert_eq!(
            a,
            Assignment {
                name: "usb-10c4-ea60-a1b2c3d4".into(),
                source: IdentitySource::Serial
            }
        );
    }

    #[test]
    fn serial_less_device_gets_portable_descriptor_identity() {
        let (sticky, known, defer) = none();
        let d = ch340("3-3", 3);
        let a4 = assigned(&run("node-c", std::slice::from_ref(&d), &sticky, &known, &defer), &d);
        assert_eq!(a4.source, IdentitySource::Descriptor);
        assert!(a4.name.starts_with("usb-1a86-7523-usb-serial-"), "{}", a4.name);

        // Moved to another node and port: same identity, so claims follow it.
        let moved = ch340("1-4", 9);
        let known = vec![known_dev(&d, &a4.name, None)];
        let a2 = assigned(
            &run("node-a", std::slice::from_ref(&moved), &sticky, &known, &defer),
            &moved,
        );
        assert_eq!(a2, a4);
    }

    #[test]
    fn identical_serial_less_devices_on_one_node_fall_back_to_ports() {
        let (sticky, known, defer) = none();
        let a = ch340("3-3", 3);
        let b = ch340("3-4", 4);
        let decisions = run("node-c", &[a.clone(), b.clone()], &sticky, &known, &defer);
        let (aa, ab) = (assigned(&decisions, &a), assigned(&decisions, &b));
        assert_eq!(aa.source, IdentitySource::PortPath);
        assert_eq!(ab.source, IdentitySource::PortPath);
        assert_ne!(aa.name, ab.name);
    }

    #[test]
    fn sticky_assignment_survives_lookalike_arrival() {
        let (_, known, defer) = none();
        let a = ch340("3-3", 3);
        let first = assigned(
            &run("node-c", std::slice::from_ref(&a), &HashMap::new(), &known, &defer),
            &a,
        );
        let sticky = HashMap::from([(InstanceKey::from(&a), first.clone())]);

        let b = ch340("3-4", 4);
        let decisions = run("node-c", &[a.clone(), b.clone()], &sticky, &known, &defer);
        assert_eq!(assigned(&decisions, &a), first, "attached device must not be renamed");
        assert_eq!(assigned(&decisions, &b).source, IdentitySource::PortPath);
    }

    #[test]
    fn known_lookalike_makes_descriptor_tier_ambiguous() {
        let (sticky, _, defer) = none();
        let a = ch340("3-3", 3);
        let lookalike = ch340("1-1", 2);
        // The cluster has seen a second identical dongle before (published under its port identity).
        let known = vec![known_dev(&lookalike, "usb-1a86-7523-node-a-1-1-deadbeef", None)];
        let a4 = assigned(&run("node-c", std::slice::from_ref(&a), &sticky, &known, &defer), &a);
        assert_eq!(a4.source, IdentitySource::PortPath);
    }

    #[test]
    fn duplicate_serials_with_different_descriptors_use_serial_descriptor_tier() {
        let (sticky, defer) = (HashMap::new(), HashSet::new());
        let adapter = dev("1-1", 2, "1a86", "55d4", Some("0001"), Some("USB Dual Serial"));
        let other = dev("1-2", 3, "1a86", "55d4", Some("0001"), Some("USB Single Serial"));
        let decisions = run("node-b", &[adapter.clone(), other.clone()], &sticky, &[], &defer);
        let (az, ao) = (assigned(&decisions, &adapter), assigned(&decisions, &other));
        assert_eq!(az.source, IdentitySource::SerialDescriptor);
        assert_eq!(ao.source, IdentitySource::SerialDescriptor);
        assert_ne!(az.name, ao.name);
    }

    #[test]
    fn defers_while_previous_node_still_holds_identity() {
        let (sticky, _, _) = none();
        let d = ch340("3-3", 3);
        let name = assigned(
            &run("node-c", std::slice::from_ref(&d), &sticky, &[], &HashSet::new()),
            &d,
        )
        .name;
        let moved = ch340("1-4", 9);
        let known = vec![known_dev(&d, &name, Some("node-c"))];

        let defer = HashSet::from([InstanceKey::from(&moved)]);
        assert_eq!(
            run("node-a", std::slice::from_ref(&moved), &sticky, &known, &defer)[&InstanceKey::from(&moved)],
            Decision::Deferred {
                name: name.clone(),
                held_by: "node-c".into()
            }
        );
        // Grace period over and the other node still claims to have it: a genuine duplicate.
        let a = assigned(
            &run("node-a", std::slice::from_ref(&moved), &sticky, &known, &HashSet::new()),
            &moved,
        );
        assert_eq!(a.source, IdentitySource::PortPath);
    }

    #[test]
    fn simultaneous_claims_resolve_by_node_name() {
        let d = ch340("3-3", 3);
        let a = assigned(
            &run(
                "node-c",
                std::slice::from_ref(&d),
                &HashMap::new(),
                &[],
                &HashSet::new(),
            ),
            &d,
        );
        let sticky = HashMap::from([(InstanceKey::from(&d), a.clone())]);
        let held_by_lower = vec![known_dev(&d, &a.name, Some("node-a"))];
        let decision = &run(
            "node-c",
            std::slice::from_ref(&d),
            &sticky,
            &held_by_lower,
            &HashSet::new(),
        )[&InstanceKey::from(&d)];
        assert_ne!(decision, &Decision::Assigned(a.clone()), "higher node name yields");
        let held_by_higher = vec![known_dev(&d, &a.name, Some("node-d"))];
        assert_eq!(
            assigned(
                &run(
                    "node-c",
                    std::slice::from_ref(&d),
                    &sticky,
                    &held_by_higher,
                    &HashSet::new()
                ),
                &d
            ),
            a
        );
    }

    #[test]
    fn names_are_valid_for_hostile_input() {
        let (sticky, known, defer) = none();
        for serial in [
            Some(""),
            Some("   "),
            Some("DE:AD:BE:EF"),
            Some(&"x".repeat(300)),
            Some("üñí©ødé"),
            None,
        ] {
            let d = dev("1-1.4.3", 5, "1a86", "55d4", serial, Some("\u{1F600} weird / product"));
            let a = assigned(
                &run("node-c.example", std::slice::from_ref(&d), &sticky, &known, &defer),
                &d,
            );
            assert!(is_dns_subdomain(&a.name), "{a:?}");
            let port = Fingerprint::from(&d).port_name("Node_With.Weird", "1-1.4.3");
            assert!(is_dns_subdomain(&port), "{port}");
        }
    }

    #[test]
    fn lossy_serials_do_not_collide() {
        let upper = Fingerprint {
            vendor_id: "10c4".into(),
            product_id: "ea60".into(),
            serial: Some("A1B2".into()),
            ..Default::default()
        };
        let lower = Fingerprint {
            serial: Some("a1b2".into()),
            ..upper.clone()
        };
        assert_ne!(
            upper.portable_name(IdentitySource::Serial),
            lower.portable_name(IdentitySource::Serial)
        );
    }

    #[test]
    fn label_values() {
        assert_eq!(label_value("node-c").as_deref(), Some("node-c"));
        assert_eq!(label_value("-a b-").as_deref(), Some("a-b"));
        assert_eq!(label_value("___"), None);
        assert_eq!(label_value(&"n".repeat(100)).unwrap().len(), 63);
    }

    #[test]
    fn usb_ids() {
        assert_eq!(normalize_usb_id("10C4").as_deref(), Some("10c4"));
        assert_eq!(normalize_usb_id("0x1a86").as_deref(), Some("1a86"));
        assert_eq!(normalize_usb_id("451").as_deref(), Some("0451"));
        assert_eq!(normalize_usb_id("xyz"), None);
        assert_eq!(normalize_usb_id("12345"), None);
    }
}
