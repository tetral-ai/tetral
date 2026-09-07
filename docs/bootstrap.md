# Bootstrap Tetral from zero

Tetral needs one deployment-owned workspace row before Auth can register the
bootstrap API key. Before starting workloads, the repository-owned PostgreSQL
preparation command constructs the current schema, creates the separate migration
and serving roles, and applies their exact grants. Seed the workspace before
starting workloads; every service then only verifies database readiness.

## 1. Choose the workspace ID

Choose a short lowercase identifier using `[a-z0-9-]`, at most 20 characters:

```bash
export TETRAL_WORKSPACE_ID=acme
export TETRAL_WORKSPACE_NAME="Acme"
```

`tetral-bootstrap` rejects empty IDs, IDs longer than 128 bytes, `/`, and all
whitespace. It warns above 20 characters because longer IDs consume the
63-character sandbox snapshot-name budget and can reduce the artifact hash
space.

## 2. Generate the local secret material

Generate a strong bootstrap API token, the internal-principal signing pair, the
vault encryption key, and the Runtime binding-token HMAC key locally:

```bash
openssl rand -base64 48

openssl genpkey -algorithm ed25519 -out ip.pem
openssl pkey -in ip.pem -outform DER         | tail -c 32 | base64 -w0
openssl pkey -in ip.pem -pubout -outform DER | tail -c 32 | base64 -w0

openssl rand -hex 32
openssl rand -base64 32
```

The two Ed25519 outputs are the base64-encoded raw 32-byte private seed and
raw 32-byte public key. `tail -c 32` is positional on the OpenSSL DER layout;
do not store the complete PKCS8 or SPKI DER output in these Secret keys.

The bootstrap API token must contain at least 32 bytes after trimming. The
vault key is exactly 64 hexadecimal characters (32 bytes). The binding-token
HMAC key accepts 32 through 4096 bytes.

Prepare `TETRAL_WEB_API_KEYS` separately as a non-empty JSON string array of
Jina provider keys, for example `["jina_key_one","jina_key_two"]`. These are
provider credentials, not generated random HMAC material.

Delete `ip.pem` after placing its two derived values into the Secret manager
used for the installation.

## 3. Create every in-cluster Secret

Create the following 15 Secrets in `tetral-system` before installation. Secret
names may be changed through Helm values, but their keys are fixed by the
workloads.

| Secret | Required keys |
| --- | --- |
| `api-database` | `url` |
| `api-secrets` | `engine-vault-key` |
| `auth-bootstrap` | `engine-api-key` |
| `auth-database` | `url` |
| `auth-internal-principal` | `private_key_b64`, `public_key_b64` |
| `gateway-web-blob` | `TETRAL_BLOB_ENDPOINT`, `TETRAL_BLOB_REGION`, `TETRAL_BLOB_BUCKET`, `TETRAL_BLOB_ACCESS_KEY`, `TETRAL_BLOB_SECRET_KEY` |
| `gateway-web-keypool` | `TETRAL_WEB_API_KEYS` |
| `queue-database` | `url` |
| `runtime-binding-token` | `hmac-key` |
| `sandbox-blob` | `TETRAL_BLOB_ACCESS_KEY`, `TETRAL_BLOB_SECRET_KEY` |
| `sandbox-daytona` | `DAYTONA_API_KEY` |
| `sandbox-r2-parent` | `TETRAL_R2_PARENT_API_TOKEN`, `TETRAL_R2_PARENT_ACCESS_KEY` |
| `tetral-blob` | `endpoint`, `region`, `bucket`, `access-key`, `secret-key` |
| `tetral-database` | `bridge-url`, `cleanup-url`, `gateway-url`, `git-proxy-url`, `TETRAL_POSTGRES_DSN` |
| `tetral-event-stream-database` | `url` |

When `edge.enabled=true`, also create the TLS Secret selected by
`edge.tlsSecretName` (`git-proxy-tls` by default) with `tls.crt` and `tls.key`.

### `sandbox-r2-parent` takes two different kinds of credential

This Secret is the one place where two credential types meet, and supplying
the wrong one for `TETRAL_R2_PARENT_API_TOKEN` fails only later, when a
session first asks for an execution environment.

`TETRAL_R2_PARENT_ACCESS_KEY` is the **access key id** of an R2 API token
(the S3-compatible kind, created under R2 → Manage API Tokens). It is the
same id the platform uses to read and write the bucket, and only the id is
needed here — not its secret.

`TETRAL_R2_PARENT_API_TOKEN` is a **Cloudflare account API token**, created
under My Profile → API Tokens → Create Token → Custom token (no template
covers R2). It needs exactly one permission — Account → Workers R2 Storage →
Edit — scoped to the account that owns the bucket. An R2 API token does not
work here and Cloudflare rejects it as invalid.

The sandbox service presents the account token together with the parent
access key id to Cloudflare's temporary-credentials API, which returns a
short-lived, **object-read-only** credential scoped to one bucket. Each
execution environment receives its own; nothing long-lived and nothing
writable ever reaches a sandbox. Edit permission is required to mint a
credential; it does not widen what the minted credential can do.

Verify the token before installing:

```
curl -s -H "Authorization: Bearer $TOKEN" \
  https://api.cloudflare.com/client/v4/user/tokens/verify
```

Five Secrets have no checked-in example template:
`gateway-web-blob`, `gateway-web-keypool`, `runtime-binding-token`,
`tetral-blob`, and `tetral-event-stream-database`. Create them directly with
`kubectl create secret generic` or through the cluster's Secret controller.
The event-stream database is covered by
`tetral-event-stream-database/url` and the public half of
`auth-internal-principal`; it has no separate template.

The blob key casing is intentionally documented as it exists:
`tetral-blob` uses lowercase keys while `gateway-web-blob` uses uppercase
`TETRAL_BLOB_*` keys for the same logical settings. Unifying this surface is a
registered follow-up, not part of bootstrap.

Before installing the platform, run `tetral-db-prepare` from the exact source or
image revision being installed. Give it an administrative connection only for
this one-shot operation, and pipe one protected JSON declaration on stdin. The
declaration must contain exactly the workload keys in `database/roles.json`,
plus `migration`; every value supplies an operator-chosen `name` and
`password`. Do not put the JSON or administrative DSN in the repository,
command arguments, shell history, or Kubernetes manifest.

`TETRAL_DATABASE_ADMIN_URL` must connect as a PostgreSQL superuser
(`rolsuper=true`). A `CREATEROLE` administrator or migration-role membership
alone does not satisfy the current installer's requirements. The command checks
this privilege before changing schema or roles; a failed check stops preparation
with a safe error message.

```bash
export TETRAL_DATABASE_ADMIN_URL
go run ./cmd/tetral-db-prepare \
  < /secure/path/tetral-postgresql-roles.json
```

The idempotent command applies all pending versions, revokes public database/schema
access, assigns catalog ownership to the migration role, and grants each
serving role only its declared operations. Use the resulting role DSNs in the
Secret inventory above; `api-database/url` is the API serving role. Keep the
schema-owner credential out of serving Secrets. Runtime workloads reject
superuser or BYPASSRLS credentials before readiness. The administrative
credential is not a serving credential and must not be placed in a workload
Secret.

To run preparation inside Kubernetes using the release image, provision a
separate operator-managed `database-preparation` Secret with key `url` containing
the administrative DSN. The following one-shot Pod receives that Secret only;
the role JSON is streamed over stdin. Keep the completed Pod available for
`kubectl logs` and the cluster's Pod log collector, if configured:

```bash
kubectl -n tetral-system run tetral-db-prepare \
  -i --restart=Never \
  --image=ghcr.io/tetral-ai/tetral@sha256:<tetral-image-digest> \
  --override-type=strategic \
  --overrides='{
    "apiVersion": "v1",
    "spec": {
      "containers": [{
        "name": "tetral-db-prepare",
        "env": [{
          "name": "TETRAL_DATABASE_ADMIN_URL",
          "valueFrom": {
            "secretKeyRef": {"name": "database-preparation", "key": "url"}
          }
        }]
      }]
    }
  }' \
  --command -- /usr/local/bin/tetral-db-prepare \
  < /secure/path/tetral-postgresql-roles.json
```

Inspect `kubectl -n tetral-system logs tetral-db-prepare` before deleting the
completed Pod with `kubectl -n tetral-system delete pod tetral-db-prepare`;
delete it before reusing the same name for another attempt.

Proceed only after a zero exit. A failure after migration may leave that schema
version committed; inspect the structured logs and rerun the same command after
correcting the cause. Neither invocation deploys services or resets data. For
existing installations, use the [upgrade sequence](../deploy/helm/tetral/README.md#upgrade-and-rollback)
before invoking this command; do not replay from-zero bootstrap as an upgrade.

## 4. Seed the workspace

Use the same immutable Tetral image digest recorded by the selected GitHub
Release. Reference the API role's
DSN directly from `api-database/url` so it never becomes a literal command-line
argument or Pod-spec value, and remove the one-shot pod when it exits:

```bash
kubectl -n tetral-system run tetral-bootstrap \
  --rm -i --restart=Never \
  --image=ghcr.io/tetral-ai/tetral@sha256:<tetral-image-digest> \
  --override-type=strategic \
  --overrides='{
    "apiVersion": "v1",
    "spec": {
      "containers": [{
        "name": "tetral-bootstrap",
        "env": [{
          "name": "TETRAL_DATABASE_URL",
          "valueFrom": {
            "secretKeyRef": {"name": "api-database", "key": "url"}
          }
        }]
      }]
    }
  }' \
  --command -- /usr/local/bin/tetral-bootstrap \
  --workspace-id "${TETRAL_WORKSPACE_ID}" \
  --name "${TETRAL_WORKSPACE_NAME}"
```

The command reports either `created` or `already present`; rerunning it is
safe. This seeds a workspace row, not an API key.

## 5. Install the platform

Set Helm's `bootstrapWorkspaceID` to the seeded ID, or set the corresponding
environment value in the raw manifests, and install Tetral. Every database
consumer verifies the schema and its serving role before becoming ready.
Auth finds the workspace and registers the bootstrap API key from
`auth-bootstrap/engine-api-key`.

See the [Helm chart instructions](../deploy/helm/tetral/README.md) for the
remaining cluster prerequisites and install command.

## 6. Register the sandbox snapshot with Daytona

Tool execution runs inside Daytona-managed sandboxes. The sandbox service
hands Daytona the environment's artifact reference verbatim as the snapshot
name (`internal/sandbox/driver/provider.go`). The release chart renders the
stable, numbered lookup name from `image.registry`, `/sandbox:`, and the
released platform version. A snapshot named
`ghcr.io/tetral-ai/sandbox:<platform version>` must exist in the Daytona
organization that owns `sandbox-daytona/DAYTONA_API_KEY` before the first
tool runs. This lookup name is distinct from the immutable sandbox image
digest from which Daytona builds the snapshot.

Nothing earlier verifies this. An environment with no custom packages never
touches Daytona at admission: it is marked ready with the configured default
reference as-is (`internal/environment/postgresql_store.go`,
`createEnvironmentArtifactAdmission`). A missing snapshot therefore fails
neither installation, bootstrap, session creation, nor environment
readiness — it surfaces only when the first tool call cannot create its
sandbox. On a fresh install where sessions answer but every command
execution fails, check this step first.

Environments *with* custom packages are different and need no manual
snapshot: the platform derives a deterministic snapshot per package set and
creates it in Daytona automatically, building `FROM` the sandbox image — for
those, Daytona must be able to pull the image itself (see the registry note
below).

Register the snapshot in the Daytona dashboard (Snapshots → Create), or
through the API. Set the snapshot name to the stable numbered lookup name and
set the image to the exact sandbox digest recorded by the selected release.
Set **4 vCPU, 8 GiB memory, and 10 GiB disk** (Large-equivalent); Sandboxes
inherit their snapshot's resources. Use the Tetral image, which contains the
required helper, rather than Daytona's stock `daytona-large` snapshot. Custom
package snapshots use the same fixed resources in the artifact builder.
See [Daytona snapshot resources](https://www.daytona.io/docs/en/snapshots/).
The 10 GiB allocation does not guarantee that an arbitrary repository workload fits.

This allocation counts against the organization's regional resource quotas
and reduces the number of Sandboxes that can run concurrently. For example,
with 10 vCPU / 20 GiB RAM / 30 GiB disk available, 4/8/10 allows at most two
running Sandboxes; other resource use can lower that further. Confirm the
actual regional limits and available headroom in the
[Daytona dashboard](https://app.daytona.io/dashboard/limits) before rollout;
Session admission itself does not allocate a Sandbox.

```bash
SNAPSHOT_NAME="ghcr.io/tetral-ai/sandbox:<platform version>"
SOURCE_IMAGE="ghcr.io/tetral-ai/sandbox@sha256:<sandbox-image-digest>"
curl -sS -X POST https://app.daytona.io/api/snapshots \
  -H "Authorization: Bearer ${DAYTONA_API_KEY}" \
  -H "Content-Type: application/json" \
  -d "{\"name\": \"${SNAPSHOT_NAME}\", \"imageName\": \"${SOURCE_IMAGE}\", \"cpu\": 4, \"memory\": 8, \"disk\": 10}"
```

Then poll `GET https://app.daytona.io/api/snapshots` until the entry whose
`name` equals `SNAPSHOT_NAME` reports `state: "active"`; creation takes up to
a few minutes. A snapshot that reaches an error state, or never appears,
means Daytona could not pull the image — if the image is not publicly
pullable, register the registry credential with Daytona first (one-time per
organization, in the dashboard or via `POST /api/docker-registry`).

Every platform version needs its own numbered snapshot name. Register that
name from the matching release digest before the sandbox service rolls to the
new version.

Changing the default reference does not rewrite artifacts already stored for
existing Environments or resize existing Sandboxes. In particular, a new
Session using an old Environment can still inherit its old snapshot's size.
Create a new Environment to adopt the new defaults; changing only networking
can reuse the old package snapshot. Existing-instance migration is separate.
In-flight builds can still adopt their already-created snapshots, preserving
the allocation that was chosen before the upgrade.

## 7. Add a model provider key

After Auth is healthy, add the first model credential with
[`services/gateway/scripts/platform-key.ts`](../services/gateway/scripts/platform-key.ts).
That script is the maintained provider-key workflow; bootstrap does not
duplicate it.

## Registered follow-ups

- Unify the lowercase `tetral-blob` and uppercase `gateway-web-blob` Secret
  key spellings.
- Converge the remaining inline workspace inserts in tests onto
  `workspace.Seeder`; test support now uses the production seeder, but the
  broader fixture cleanup is intentionally separate.
