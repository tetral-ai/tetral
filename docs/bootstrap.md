# Bootstrap Tetral from zero

Tetral needs one deployment-owned workspace row before Auth can register the
bootstrap API key. The API service owns migrations, so a fresh installation
must start far enough for API to create the schema before the workspace can be
seeded. Auth crash-loops until that seed exists and then recovers through its
normal restart backoff.

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

Create the following 17 Secrets in `tetral-system` before installation. Secret
names may be changed through Helm values, but their keys are fixed by the
workloads.

| Secret | Required keys |
| --- | --- |
| `api-database` | `url` |
| `api-secrets` | `engine-vault-key` |
| `auth-bootstrap` | `engine-api-key` |
| `auth-database` | `url` |
| `auth-internal-principal` | `private_key_b64`, `public_key_b64` |
| `bridge-daytona` | `DAYTONA_API_KEY` |
| `gateway-web-blob` | `TETRAL_BLOB_ENDPOINT`, `TETRAL_BLOB_REGION`, `TETRAL_BLOB_BUCKET`, `TETRAL_BLOB_ACCESS_KEY`, `TETRAL_BLOB_SECRET_KEY` |
| `gateway-web-keypool` | `TETRAL_WEB_API_KEYS` |
| `queue-database` | `url` |
| `runtime-binding-token` | `hmac-key` |
| `sandbox-blob` | `TETRAL_BLOB_ACCESS_KEY`, `TETRAL_BLOB_SECRET_KEY` |
| `sandbox-daytona` | `DAYTONA_API_KEY` |
| `sandbox-internal-grpc` | `token` |
| `sandbox-r2-parent` | `TETRAL_R2_PARENT_API_TOKEN`, `TETRAL_R2_PARENT_ACCESS_KEY` |
| `tetral-blob` | `endpoint`, `region`, `bucket`, `access-key`, `secret-key` |
| `tetral-database` | `bridge-url`, `cleanup-url`, `gateway-url`, `git-proxy-url`, `TETRAL_POSTGRES_DSN` |
| `tetral-event-stream-database` | `url` |

When `edge.enabled=true`, also create the TLS Secret selected by
`edge.tlsSecretName` (`git-proxy-tls` by default) with `tls.crt` and `tls.key`.

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

Use restricted database roles for every workload. The API role can run the
seed because API owns the `workspaces` table it created by running migrations.
Tetral does not define a production `GRANT` for another role and operators
must not use the PostgreSQL superuser for bootstrap.

## 4. Install the platform

Set Helm's `bootstrapWorkspaceID` to the chosen ID, or set the corresponding
environment value in the raw manifests, and install Tetral. The API starts and
runs migrations. Auth fails its workspace lookup and crash-loops at this point
by design.

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
