# Client-observed evidence

This directory retains client-observed campaign bundles and nothing else.

> These measurements come from a three-account Overmesh validation deployment
> used for conformance and performance testing. They describe what that
> deployment did on a given day from a given machine. They are not a capacity
> statement, not a performance guarantee, and not a service level objective or
> agreement. Nothing here commits Overmesh or its operators to any level of
> availability, latency or throughput.

## What a client-observed campaign is

A client-observed campaign measures two real client tools — the Azure CLI and
AzCopy — from one operator machine, on one real network, against a direct
Storage endpoint and against the Overmesh Gateway. It answers one question:
what did a person with a laptop actually experience?

It is not the isolated performance baseline. The isolated campaigns under
`harness/artifacts/live/` run from a dedicated benchmark VM inside the same
region, with a fixed SKU, a certified matrix and deterministic request
budgets. They are comparable across commits and they gate releases.

A corporate laptop is none of those things. Its DNS, proxy, VPN concentrator,
Wi-Fi radio and TLS interception middlebox are all outside the measurement and
all free to change between two adjacent seconds. A campaign run there can show
what the tools did; it cannot certify what the code costs.

## Why the two kinds of evidence never mix

Client-observed bundles declare
`apiVersion: performance.overmesh.io/client-observed/v1`.

`harness/environments/azure/performance/validate_performance_evidence.py` and
`harness/environments/azure/performance/compare_live_performance.py` both
accept `performance.overmesh.io/v1` only. Passing a client-observed bundle to
either one fails closed with an explicit refusal. There is no flag that
overrides it.

The bundles themselves carry the refusal too:

```json
"scope": {
  "role": "client-observed",
  "usableAsBaseline": false,
  "usableAsGate": false,
  "comparableWithIsolatedCampaigns": false
}
```

They may not contain `baselineEligible`, `nonRegression`, `comparisons`,
`repeatability`, `resolution`, `p50SpreadRatio`, any percentile field, or any
other gating construct. The builder refuses to publish a document that does.

Never place a client-observed figure in the same table as an isolated
baseline figure. They do not measure the same thing.

## The protocol

- **Contract.** `harness/performance/client-observed-v1.toml`.
- **Runner.**
  `harness/environments/azure/performance/client_observed_campaign.py`.
- **Driver.** `harness/environments/azure/validate-client-observed.sh`.

The matrix is eight operation families, each measured against both the direct
Storage endpoint and the Gateway, for sixteen measurement families in total:

| Tool | Operation | Shape | Size |
| --- | --- | --- | --- |
| Azure CLI | upload | single blob | 1 MiB |
| Azure CLI | download | single blob | 1 MiB |
| Azure CLI | upload | single blob | 100 MiB |
| Azure CLI | download | single blob | 100 MiB |
| AzCopy | upload | single file | 100 MiB |
| AzCopy | download | single file | 100 MiB |
| AzCopy | upload | directory | 500 × 200 KiB |
| AzCopy | download | directory | 500 × 200 KiB |

Direct and Gateway are interleaved operation by operation, so the two targets
of one operation are always adjacent in time. The leading target alternates so
neither is systematically first.

Each family runs five times. The bundle publishes the five observations and
their minimum, median and maximum. It publishes no percentile, no spread
ratio, no stability classification and no regression verdict, because five
observations from an uncontrolled network cannot support one.

The directory families report aggregate wall time and aggregate throughput.
They do not report per-blob latency: AzCopy schedules its own concurrency, so
a per-blob number taken from outside would describe the scheduler rather than
either storage path.

## What is still required

Server-side attribution is not relaxed. Every measured client operation is
issued under a deterministic blob prefix,

```text
perf/client-observed/{runId}/{operation}/{target}
```

and every retained case must report `unattributedRequests: 0` and attribute
all five client operations. The bundle records how many structural requests
each tool produced and how many backend requests the server made per client
operation. Those counts are recorded, not gated: the isolated request budgets
belong to the isolated contract and are not applied here.

### The paths a case actually touches

The bundle publishes the exact remote paths that were measured, not a prefix
the measured requests never reached:

```text
upload   perf/client-observed/{runId}/{operation}/{target}/run/{NN}[/{name}]
download perf/client-observed/{runId}/{operation}/{target}/source[/{name}]
```

An upload measures five distinct paths, one per run. A download measures one
path, five times. Both live under the published attribution prefix, and the
builder refuses a bundle whose declared `measuredPaths` are not exactly the
paths the contract and run identifier produce.

A download needs its source to exist first, so the campaign writes that source
before measurement. That seed write necessarily touches the same blob the
download later reads — there is no honest way to seed a different path — so it
is excluded by time rather than hidden under another prefix:

- the campaign publishes a `setupWindow` and a `measurementWindow`, and the
  builder refuses a campaign whose seed writes have not finished before the
  measurement window opens;
- every measurement publishes its own `measurementWindow`, which must fall
  inside the campaign measurement window and start after seeding ended;
- the telemetry collector must declare the same window and the same measured
  paths, and must set `setupWritesExcluded` for every download case;
- each measurement publishes its `setupWrites` count, paths, and the reason
  they are excluded.

The bundle therefore states plainly that the seed write happened, where it
happened, and which window kept it out of the measured counts.

## Remote cleanup

The campaign tracks every prefix it writes, on both the direct and the Gateway
target, and removes them after the last measured operation and outside every
measured window — including when the campaign itself failed part way through.
Cleanup uses the same Entra login as the campaign; it never mints or accepts a
SAS.

Cleanup traffic reaches the same paths the campaign just measured, so it has
to be told apart from the last case. Timestamps carry second resolution, so
the runner waits for the second to turn over and only then opens a
`cleanupWindow` whose `startedAt` is strictly greater than
`measurementWindow.finishedAt`. The bundle publishes that window together with
the prefixes it covered:

```json
"cleanupWindow": {"startedAt": "...", "finishedAt": "..."},
"cleanup": {
  "excludedBy": "campaign-cleanup-window",
  "prefixes": {"direct": ["..."], "gateway": ["..."]}
}
```

The builder and the validator both refuse a bundle whose cleanup window opens
at or before the measurement window closes, whose declared prefixes are not
exactly the prefixes the contract and run identifier produce on both targets,
or any case window that reaches into the cleanup window. A collector therefore
cannot fold a deletion into the last measured case, and the removal of the
last directory-download prefix is accounted for like every other.

If a prefix cannot be removed, the runner prints one actionable line naming
the target, the prefix and the container. When the campaign itself succeeded,
an incomplete cleanup fails the run. When the campaign had already failed, the
original failure is the one that propagates: cleanup never masks it.

## Required client context

A campaign refuses to run, build or publish unless the operator declares the
machine it ran on. The context is supplied through the environment and is
never invented by the tooling:

| Field | Environment variable |
| --- | --- |
| `country` | `OVERMESH_CLIENT_OBSERVED_COUNTRY` |
| `connection` | `OVERMESH_CLIENT_OBSERVED_CONNECTION` |
| `corporateProxy` | `OVERMESH_CLIENT_OBSERVED_CORPORATE_PROXY` |
| `vpn` | `OVERMESH_CLIENT_OBSERVED_VPN` |
| `os` | `OVERMESH_CLIENT_OBSERVED_OS` |
| `note` | `OVERMESH_CLIENT_OBSERVED_NOTE` |

`OVERMESH_CLIENT_OBSERVED_ISOLATED_ENVIRONMENT` must be `false`, and the
retained `clientContext.isolatedEnvironment` is always `false`. A campaign
that claims isolation is rejected.

## Working directories

A campaign owns three sibling directories under
`.harness/client-observed/{runId}/`:

| Directory | Owner | Lifetime |
| --- | --- | --- |
| `staging/` | the runner | created and deleted by every run |
| `artifacts/` | the operator | never touched by the runner |
| `azure-cli-config/` | the driver | created before login, removed by its trap |

Staged payloads, download destinations, and AzCopy logs and job plans live in
`staging/`. The raw client result and the server telemetry file live in
`artifacts/`, so a telemetry file collected before or during the campaign
survives staging cleanup.

The Azure CLI profile is a sibling of `staging/`, never a child of it: the
runner deletes `staging/` while the campaign is still logged in, so a profile
kept there would disappear underneath the commands still to run. The runner
refuses to dispatch a single command unless `AZURE_CONFIG_DIR` names an
existing directory outside `staging/`, and it never removes that directory —
only the driver's trap does.

The runner refuses to write its raw result into `staging/`, and refuses to
write it anywhere under `harness/artifacts/`.

Server telemetry is read from
`.harness/client-observed/{runId}/artifacts/client-observed-telemetry.json`.
Set `OVERMESH_CLIENT_OBSERVED_TELEMETRY` when the collector writes it
elsewhere. The driver stops before publication, with an explicit message, if
that file is absent.

## Credentials

The campaign uses a dedicated service principal. Its credentials arrive
through the process environment only, and no credential is ever created by
this tooling. The runner selects the least exposing credential the
environment already supplies, and the bundle records that choice as
`campaign.credentialMode`:

| Mode | Environment | Exposure on the operator host |
| --- | --- | --- |
| `certificate` | `AZURE_CLIENT_CERTIFICATE_PATH` | a file path only |
| `workload-identity` | `AZURE_FEDERATED_TOKEN_FILE` | a short-lived token |
| `client-secret` | `AZURE_CLIENT_SECRET` | the secret itself |

AzCopy authenticates entirely from environment variables, so no AzCopy
invocation ever carries credential material on its command line.

The Azure CLI is different, and the honest statement is this: `az login`
cannot read `--password` from standard input. Its prompt refuses to run
without a TTY, so piping the secret fails rather than being consumed. In
`client-secret` mode the secret is therefore visible in that process's argv,
to any local user who can list processes, for as long as the login runs.
Prefer `certificate` mode on any host that is not exclusively yours; it passes
a path instead of a secret and nothing sensitive reaches argv. In
`workload-identity` mode the federated assertion reaches argv, but it is short
lived.

No credential value is ever written to a file by this tooling, and none is
ever written to evidence. The Azure CLI does persist its own token cache: the
driver points `AZURE_CONFIG_DIR` at a private `azure-cli-config/` directory
beside `staging/`, created with `umask 077` and mode `700`, and a shell trap
runs `az account clear` and `az logout` and deletes that directory on every
exit path, including interruption. It sits beside `staging/` rather than
inside it because the runner deletes `staging/` during the campaign. The
operator's own `~/.azure` profile is never read or written.

## Retention

Retained bundles must not contain a secret, a tenant, an account, a user
principal name, a raw endpoint, a token, a SAS fragment, a local path, an IP
address, or a transfer identifier or log location emitted by AzCopy. Endpoints
appear only as `endpoint-<16 hex>` fingerprints. The builder reuses the
redact-before-sign canonicalisation in
`harness/environments/azure/build-live-evidence.py` and then refuses to
publish if any forbidden field, pattern or redaction pseudonym survives.

Bundles are written under this directory only. Publishing to
`harness/artifacts/live/` is rejected, because that tree holds certified
isolated evidence.

## Status

No client-observed campaign has been retained yet. This directory documents
the protocol so that the first campaign has somewhere honest to land. Figures
appear in `docs/WHY_OVERMESH.md` only once a bundle exists here.
