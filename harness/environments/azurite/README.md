# Local Azurite Environment

This environment provides two independent Blob emulators and deterministic
network fault injection through Toxiproxy.

| Service | Docker endpoint | Host endpoint |
|---|---:|---:|
| Storage A | `https://storage-a:10000` | `https://127.0.0.1:12100` through Toxiproxy |
| Storage B | `https://storage-b:10000` | `https://127.0.0.1:12101` through Toxiproxy |
| Toxiproxy API | `toxiproxy:8474` | `127.0.0.1:18474` |

All published ports can be overridden with:

```text
HARNESS_TOXIPROXY_PORT
HARNESS_PROXY_A_PORT
HARNESS_PROXY_B_PORT
```

Azurite is not a substitute for live Microsoft Entra, Azure RBAC, Key Vault,
regional, performance, or complete Azure Blob conformance testing.

The local services use Azurite OAuth basic mode with an ephemeral self-signed
TLS certificate. OAuth basic mode does not emulate production signature or
Azure RBAC enforcement.
