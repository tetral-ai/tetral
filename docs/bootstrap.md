# Bootstrap Tetral from zero

Tetral needs one deployment-owned workspace row before Auth can register the
bootstrap API key. Before starting workloads, the repository-owned PostgreSQL
installer constructs the current schema, creates the separate migration and
serving roles, and applies their exact grants. The API then performs an
idempotent migration check under the migration owner. Auth crash-loops until
the workspace seed exists and then recovers through its normal restart backoff.

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
| `api-database` | `url`, `migration-url` |
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

Before installing the platform, run the role installer from the exact source or
image revision being installed. Give it an administrative connection only for
this one-shot operation, and pipe one protected JSON declaration on stdin. The
declaration must contain exactly the workload keys in `database/roles.json`,
plus `migration`; every value supplies an operator-chosen `name` and
`password`. Do not put the JSON or administrative DSN in the repository,
command arguments, shell history, or Kubernetes manifest.

```bash
export TETRAL_DATABASE_ADMIN_URL
go run ./services/api/cmd/tetral-postgresql-roles \
  < /secure/path/tetral-postgresql-roles.json
```

The idempotent command constructs Version 1, revokes public database/schema
access, assigns catalog ownership to the migration role, and grants each
serving role only its declared operations. Use the resulting role DSNs in the
Secret inventory above; `api-database/url` is the API serving role and
`api-database/migration-url` is the migration owner. Runtime workloads reject
superuser or BYPASSRLS credentials before readiness. The administrative
credential is not a serving credential and must not be placed in a workload
Secret.

## 4. Install the platform

Set Helm's `bootstrapWorkspaceID` to the chosen ID, or set the corresponding
environment value in the raw manifests, and install Tetral. The API verifies
and idempotently migrates through the dedicated owner, then starts through its
restricted serving role. Auth fails its workspace lookup and crash-loops at
this point by design.

See the [Helm chart instructions](../deploy/helm/tetral/README.md) for the
remaining cluster prerequisites and install command.

## 5. Seed the workspace

Use the same Tetral image version as the installation. Reference the API role's
DSN directly from `api-database/url` so it never becomes a literal command-line
argument or Pod-spec value, and remove the one-shot pod when it exits:

```bash
kubectl -n tetral-system run tetral-bootstrap \
  --rm -i --restart=Never \
  --image=ghcr.io/tetral-ai/tetral:0.1.0-alpha \
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
safe. Auth then finds the row, registers the bootstrap API key from
`auth-bootstrap/engine-api-key`, and self-heals within its restart backoff.

## 6. Add a model provider key

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
