//! atomic-usb: attach USB devices plugged into any Kubernetes node to KubeVirt VMs anywhere in the
//! cluster, over the pod network, with hotplug.

mod agent;
mod controller;
mod crd;
mod identity;
mod kubevirt;
mod launcher;
mod proto;
mod sysfs;
mod uevent;
mod util;
mod watch;

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use clap::{Args, Parser, Subcommand, ValueEnum};
use kube::CustomResourceExt;

use crate::agent::{AgentConfig, DevicePattern};
use crate::identity::{AssignContext, Decision, InstanceKey};
use crate::launcher::SocketLocator;

#[derive(Parser)]
#[command(name = "atomic-usb", version, about)]
struct Cli {
    #[arg(long, env = "ATOMIC_USB_LOG_FORMAT", value_enum, default_value_t = LogFormat::Text, global = true)]
    log_format: LogFormat,
    #[command(subcommand)]
    command: Command,
}

#[derive(Clone, Copy, ValueEnum)]
enum LogFormat {
    Text,
    Json,
}

#[derive(Subcommand)]
enum Command {
    /// Run the node agent (DaemonSet): discovery, export and attachment.
    Agent(Box<AgentArgs>),
    /// Run the cluster controller: leases, slots and claim status.
    Controller(ControllerArgs),
    /// Print the CustomResourceDefinitions as YAML.
    Crds,
    /// Scan this machine's USB devices and print the identities the agent would assign.
    Scan(ScanArgs),
}

#[derive(Args)]
struct AgentArgs {
    #[arg(long, env = "NODE_NAME")]
    node_name: String,
    #[arg(long, env = "POD_NAME", default_value = "")]
    pod_name: String,
    /// Pod IP advertised to other agents.
    #[arg(long, env = "POD_IP")]
    pod_ip: String,
    #[arg(long, env = "ATOMIC_USB_LISTEN", default_value = "0.0.0.0:7575")]
    listen: SocketAddr,
    #[arg(long, env = "ATOMIC_USB_PSK_FILE", default_value = "/etc/atomic-usb/psk")]
    psk_file: PathBuf,
    #[arg(long, env = "ATOMIC_USB_SYSFS_ROOT", default_value = "/sys")]
    sysfs_root: PathBuf,
    /// Kubelet root directory on the host (k0s: /var/lib/k0s/kubelet, microk8s: /var/snap/microk8s/common/var/lib/kubelet).
    #[arg(long, env = "ATOMIC_USB_KUBELET_ROOT", default_value = "/var/lib/kubelet")]
    kubelet_root: PathBuf,
    /// Host /proc, used to find launcher sockets when the kubelet path does not work (needs hostPID).
    #[arg(long, env = "ATOMIC_USB_HOST_PROC")]
    host_proc: Option<PathBuf>,
    #[arg(long, env = "ATOMIC_USB_USBREDIRECT", default_value = "usbredirect")]
    usbredirect: PathBuf,
    /// Also publish USB hubs.
    #[arg(long, env = "ATOMIC_USB_INCLUDE_HUBS")]
    include_hubs: bool,
    /// Never publish devices matching VENDOR:PRODUCT (either may be `*`), comma separated.
    #[arg(long, env = "ATOMIC_USB_IGNORE", value_delimiter = ',')]
    ignore: Vec<String>,
    #[arg(long, env = "ATOMIC_USB_SCAN_INTERVAL_SECONDS", default_value_t = 15)]
    scan_interval_seconds: u64,
    #[arg(long, env = "ATOMIC_USB_HEARTBEAT_SECONDS", default_value_t = 30)]
    heartbeat_seconds: u64,
    #[arg(long, env = "ATOMIC_USB_LOST_AFTER_SECONDS", default_value_t = 90)]
    lost_after_seconds: u64,
}

#[derive(Args)]
struct ControllerArgs {
    #[arg(long, env = "POD_NAMESPACE", default_value = "atomic-usb")]
    namespace: String,
    /// Secret (in --namespace) with the agents' pre-shared key; generated if missing. Empty disables.
    #[arg(long, env = "ATOMIC_USB_PSK_SECRET", default_value = "atomic-usb-psk")]
    psk_secret: String,
    #[arg(long, env = "ATOMIC_USB_LOST_AFTER_SECONDS", default_value_t = 90)]
    lost_after_seconds: u64,
    #[arg(long, env = "ATOMIC_USB_RESYNC_SECONDS", default_value_t = 30)]
    resync_seconds: u64,
}

#[derive(Args)]
struct ScanArgs {
    #[arg(long, default_value = "/sys")]
    sysfs_root: PathBuf,
    #[arg(long, env = "NODE_NAME", default_value = "local")]
    node_name: String,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    init_logging(cli.log_format);
    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
    runtime.block_on(async move {
        match cli.command {
            Command::Agent(args) => {
                let client = kube::Client::try_default()
                    .await
                    .context("creating Kubernetes client")?;
                agent::run(client, agent_config(*args)?).await
            }
            Command::Controller(args) => {
                let client = kube::Client::try_default()
                    .await
                    .context("creating Kubernetes client")?;
                let config = controller::ControllerConfig {
                    namespace: args.namespace,
                    psk_secret: Some(args.psk_secret).filter(|s| !s.is_empty()),
                    lost_after: Duration::from_secs(args.lost_after_seconds),
                    resync: Duration::from_secs(args.resync_seconds),
                };
                controller::run(client, config).await
            }
            Command::Crds => {
                print!(
                    "{}---\n{}",
                    serde_yaml::to_string(&crd::UsbDevice::crd())?,
                    serde_yaml::to_string(&crd::UsbDeviceClaim::crd())?
                );
                Ok(())
            }
            Command::Scan(args) => scan(args),
        }
    })
}

fn agent_config(args: AgentArgs) -> anyhow::Result<AgentConfig> {
    let ignore = args
        .ignore
        .iter()
        .map(|p| p.trim())
        .filter(|p| !p.is_empty())
        .map(|p| p.parse::<DevicePattern>().map_err(anyhow::Error::msg))
        .collect::<anyhow::Result<Vec<_>>>()
        .context("--ignore")?;
    Ok(AgentConfig {
        node: args.node_name,
        pod_name: args.pod_name,
        pod_ip: args.pod_ip,
        listen: args.listen,
        psk_file: args.psk_file,
        sysfs_root: args.sysfs_root,
        locator: SocketLocator {
            kubelet_root: args.kubelet_root,
            proc_root: args.host_proc,
        },
        usbredirect: args.usbredirect,
        include_hubs: args.include_hubs,
        ignore,
        scan_interval: Duration::from_secs(args.scan_interval_seconds),
        heartbeat_interval: Duration::from_secs(args.heartbeat_seconds),
        lost_after: Duration::from_secs(args.lost_after_seconds),
    })
}

fn scan(args: ScanArgs) -> anyhow::Result<()> {
    let devices = sysfs::scan(&args.sysfs_root).context("scanning sysfs")?;
    let ctx = AssignContext {
        node: &args.node_name,
        sticky: &HashMap::new(),
        known: &[],
        may_defer: &HashSet::new(),
    };
    let decisions = identity::assign(&ctx, &devices);
    let rows: Vec<serde_json::Value> = devices
        .iter()
        .map(|d| {
            let (name, identity) = match &decisions[&InstanceKey::from(d)] {
                Decision::Assigned(a) => (a.name.clone(), format!("{:?}", a.source)),
                Decision::Deferred { name, .. } => (name.clone(), "Deferred".into()),
            };
            serde_json::json!({ "name": name, "identity": identity, "hub": d.is_hub(), "device": d })
        })
        .collect();
    println!("{}", serde_json::to_string_pretty(&rows)?);
    Ok(())
}

fn init_logging(format: LogFormat) {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_env("ATOMIC_USB_LOG_LEVEL").unwrap_or_else(|_| EnvFilter::new("info"));
    let ansi = std::io::IsTerminal::is_terminal(&std::io::stderr());
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_ansi(ansi)
        .with_writer(std::io::stderr);
    match format {
        LogFormat::Text => builder.init(),
        LogFormat::Json => builder.json().flatten_event(true).init(),
    }
}
