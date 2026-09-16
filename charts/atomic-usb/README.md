# atomic-usb Helm chart

Installs [atomic-usb](https://github.com/safewords/kubevirt-atomic-usb): the node agent (DaemonSet),
the controller (Deployment), the `UsbDevice` / `UsbDeviceClaim` CRDs and RBAC.

## Install

```sh
kubectl create namespace atomic-usb
kubectl label namespace atomic-usb pod-security.kubernetes.io/enforce=privileged

# OCI registry
helm install atomic-usb oci://ghcr.io/safewords/charts/atomic-usb --namespace atomic-usb

# or the classic Helm repository
helm repo add atomic-usb https://raw.githubusercontent.com/safewords/kubevirt-atomic-usb/gh-pages
helm install atomic-usb atomic-usb/atomic-usb --namespace atomic-usb
```

Helm does not upgrade CRDs. When upgrading, apply them first:

```sh
helm show crds oci://ghcr.io/safewords/charts/atomic-usb --version <version> | kubectl apply --server-side -f -
```

## Values

| Key | Default | Description |
| --- | --- | --- |
| `image.repository` | `ghcr.io/safewords/kubevirt-atomic-usb` | Container image |
| `image.tag` | chart `appVersion` | Image tag |
| `image.pullPolicy` | `IfNotPresent` | |
| `imagePullSecrets` | `[]` | |
| `logging.format` | `text` | `text` or `json` |
| `logging.level` | `info` | `trace`, `debug`, `info`, `warn`, `error` |
| `psk.secretName` | `<fullname>-psk` | Secret with the agents' pre-shared key under `psk` |
| `psk.generate` | `true` | Let the controller create the secret with a random key |
| `lostAfterSeconds` | `90` | Mark devices `Lost` after this long without an agent heartbeat |
| `controller.resyncSeconds` | `30` | Periodic full reconcile |
| `controller.resources` / `nodeSelector` / `tolerations` / `affinity` / `priorityClassName` / `podAnnotations` / `podLabels` | | Controller pod settings |
| `agent.port` | `7575` | Pod-network port for device sessions |
| `agent.listenAddress` | `0.0.0.0` | Use `[::]` on IPv6-only clusters |
| `agent.ignore` | `[]` | `VENDOR:PRODUCT` patterns (`*` allowed) that are never published |
| `agent.includeHubs` | `false` | Publish USB hubs too |
| `agent.scanIntervalSeconds` | `15` | Periodic rescan in addition to uevents |
| `agent.heartbeatSeconds` | `30` | Device heartbeat interval |
| `agent.kubeletRoot` | `/var/lib/kubelet` | Kubelet root dir on the nodes |
| `agent.hostPID` | `true` | Fall back to `/proc/<pid>/root` to find usbredir sockets |
| `agent.priorityClassName` | `system-node-critical` | |
| `agent.updateStrategy` | rolling, `maxUnavailable: 1` | |
| `agent.resources` / `nodeSelector` / `tolerations` / `affinity` / `podAnnotations` / `podLabels` | | Agent pod settings; tolerates all taints by default |
| `rbac.aggregateToDefaultRoles` | `true` | Let `admin`/`edit`/`view` manage or read `UsbDeviceClaim`s |
