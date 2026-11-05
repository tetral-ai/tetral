# Integrating the Tetral CLI into an app

This guide is for every app that embeds the Tetral CLI, the developer's own
and third parties' alike. It follows one example throughout: the app Acme,
its user Alice and her Mac. Values in angle brackets are placeholders; no
example holds a real key.

## 1. Roles and steps

Acme runs `tetral` on Alice's machine. Acme's backend acts as a Controller
with the workspace API key: the key stays on the backend, never ships in the
app and never reaches Alice's machine. The steps, in order:

| Step | Who | What |
|---|---|---|
| 1 | Acme's build | Ships `tetral` in the app |
| 2 | Acme's backend | Registers the machine and gets its bootstrap |
| 3 | Acme | Installs the bootstrap with `tetral configure` |
| 4 | Acme, on a Mac | Enables drives with Alice's consent |
| 5 | Acme | Starts the App Server with `tetral server run` |
| 6 | The developer | Lists the machine in a Session's computers |
| 7 | Engine and the CLI | Prepare the Session on the machine |
| 8 | Engine and the CLI | Run the Session's tool calls on the machine |
| 9 | Acme | Reads the App Server's state through the local API |
| 10 | Acme | Upgrades, replaces the configuration or removes the machine |

Engine and the CLI carry out steps 6 to 8 with no action of Acme's. The
example program [`cli/examples/app-integration.sh`](../examples/app-integration.sh)
runs steps 2 to 5, step 9 and the stop (section 14).

## 2. Commands and results

Acme keeps one state root per app account inside its data directory, for
example `<app data>/tetral/<account id>/`. Every command except
`tetral --version` needs the root's absolute path in `TETRAL_HOME` and
`--app acme`; switching Alice's Acme account switches the root, and never
touches another account's root. `tetral --version` needs neither and writes
`{"cli_version":"<version>"}`.

Each command writes one JSON object to stdout and its records to stderr;
`server run` also writes them to `$TETRAL_HOME/logs/` (section 13). It exits
0 with its result, or 1 with

```json
{"error": {"code": "<code>", "message": "<message>"}}
```

Acme branches on `code` and shows `message`. The failures every command can
return:

| `code` | Meaning | Acme's action |
|---|---|---|
| `invalid_arguments` | A fault in Acme's call: `TETRAL_HOME` missing or relative, `--app` missing, an unknown command or option, or an invalid value | Fix the call |
| `app_mismatch` | The root was configured for another app | Fix the call |
| `io_failed` | Reading or writing the root failed; `message` names the path and the system error | Show `message` |
| `unsafe_control_path` naming the socket directory (section 9) | Another user or program controls that directory | Show `message`; Alice removes the directory |
| `unsafe_control_path` naming a path in the state root | The root is damaged | Delete the root and install a new credential for the same instance in a new root (steps 2 and 3) |
| `invalid_configuration` returned by `configure` itself | The root is damaged | As above |
| `invalid_configuration` from any other command | The root lacks a complete configuration | Install a new credential for the instance (steps 2 and 3) |

Deleting a state root, here and at steps 2, 5 and 10, also discards the drive
changes in it that are not yet saved to Engine.

## 3. Step 1: ship `tetral`

Acme's build downloads, for each platform it supports,
`tetral-<target>.tar.gz` and `SHA256SUMS` from the GitHub Release
`cli-v<version>` of `tetral-ai/tetral`. The targets are
`x86_64-unknown-linux-musl`, `aarch64-unknown-linux-musl`,
`x86_64-apple-darwin` and `aarch64-apple-darwin`. It checks the archives it
downloaded, in the directory that holds them:

```sh
sha256sum --ignore-missing -c SHA256SUMS        # on Linux
shasum -a 256 --ignore-missing -c SHA256SUMS    # on macOS
```

Each downloaded archive must print `OK`, and the command must exit 0; the
targets Acme does not ship are passed over. An archive whose digest differs
is not used.

Acme places `tetral` and `THIRD-PARTY-NOTICES` in its bundle. On Linux it
places `bwrap`, `COPYING`, `COPYING.libcap` and `COPYING.openssl` from the
same archive beside them, since
`tetral` runs tools through that `bwrap` when the machine has no usable
system one. Acme signs its bundle and has a bundle identifier, and this one
app launches every `tetral` command. There is no installer, no Tetral
signature and no self-update. The version matrix in
[`cli/README.md`](../README.md#versions) names what each release speaks.

On Ubuntu 24.04, where `kernel.apparmor_restrict_unprivileged_userns` is `1`,
`bwrap` cannot create its user namespaces unless an AppArmor profile allows
`tetral` to. Acme's installation writes, as root and once for the path
pattern that covers every version directory it installs `tetral` into, the
profile `/etc/apparmor.d/acme-tetral`:

```text
abi <abi/4.0>,
include <tunables/global>

profile acme-tetral /opt/acme/*/tetral flags=(unconfined) {
  userns,
}
```

and loads it before the first `server run`:

```sh
apparmor_parser -r /etc/apparmor.d/acme-tetral
```

Every `bwrap` that this `tetral` starts can then create its namespaces;
`tetral` stays otherwise unconfined, and no other program gains the
permission. AppArmor attaches the profile when `tetral` starts, so a
`server run` that started before the profile was loaded keeps the
restriction until Acme stops it and starts it again (step 5). Without the profile every tool call on that machine is rejected
`sandbox_unavailable` with the message `bwrap cannot create user namespaces
on this machine; on Ubuntu, install the AppArmor profile that allows them for
tetral`.

## 4. Step 2: register the machine

After Alice signs in, Acme asks its backend to register her machine. The
backend sends:

```http
POST <Engine URL>/v1/instances
X-Api-Key: <workspace API key>
Idempotency-Key: <a new value of at most 255 bytes>
Content-Type: application/json

{"name": "<display label of 1 to 256 characters>", "credential": {}}
```

`credential.expires_at`, a future RFC 3339 UTC time at most Engine's maximum
credential lifetime ahead (30 days by default), is optional; without it the
credential lasts that maximum.

`201` returns `{instance, credential}`: the instance object and the
credential object, which holds the one-time `connection_key`:

```json
{
  "instance": {"id": "<instance id>", "name": "Alice's Mac", "...": "..."},
  "credential": {
    "id": "<credential id>",
    "connection_key": "<connection key>",
    "expires_at": "<RFC 3339 UTC time>",
    "...": "..."
  }
}
```

The backend stores `instance.id` with Alice's account and builds the
bootstrap from the response:

```json
{
  "server_url": "<Engine URL>",
  "instance_id": "<instance.id>",
  "credential_id": "<credential.id>",
  "connection_key": "<credential.connection_key>",
  "expires_at": "<credential.expires_at>"
}
```

It sends the bootstrap to Acme over its authenticated channel, and neither
logs it nor stores the key.

Errors come in the envelope
`{"type": "error", "error": {"type": "<type>", "message": "<message>"}, "request_id": "<id>"}`:

| Status and `error.type` | Meaning | The backend's action |
|---|---|---|
| `400 invalid_request_error` | The message names `Idempotency-Key`, `name`, `credential` or `expires_at` | Fix that part of the request |
| `401 authentication_error` | The API key is missing, invalid or revoked, or a connection key was sent in its place | Fix its credential |
| `409 idempotency_conflict_error` | The `Idempotency-Key` was used before with another body | Use a new key for a new registration |
| `413 request_too_large` | The body exceeds the limit the message names | Send only the body above |
| A `5xx`, or no response | The outcome is unknown | Repeat the request with the same `Idempotency-Key` and body |

The repeated request returns `201` with the same IDs and
`connection_key: null`. The backend then revokes that credential with
`DELETE /v1/instances/{instance_id}/credentials/{credential_id}` (`204`) and
issues a new one as below; it never registers a second instance for the
machine.

A new credential for the same instance:

```http
POST <Engine URL>/v1/instances/{instance_id}/credentials
X-Api-Key: <workspace API key>
Idempotency-Key: <a new value>
Content-Type: application/json

{"delivery": "response"}
```

returns `201` with the credential object holding `connection_key`, from
which the backend builds the bootstrap as above. Its errors besides those
above:

| Status and `error.type` | Meaning | The backend's action |
|---|---|---|
| `409 credential_limit_error` | Two credentials are neither revoked nor expired | Revoke the one whose `in_use` is `false` in `GET /v1/instances/{instance_id}/credentials`, then issue again |
| `403 instance_unavailable_error` or `404 not_found_error` | The instance is deregistered or unknown | Acme deletes the state root, which discards the drive changes in it not yet saved to Engine, and registers again |

## 5. Step 3: install the bootstrap

```sh
printf '%s' "$bootstrap" | TETRAL_HOME="$root" tetral configure --app acme --from-stdin
```

The bootstrap, at most 64 KiB, comes on stdin and nowhere else: never in an
argument, a file Acme keeps or an environment variable. The command needs no
terminal and asks nothing. It creates the root with mode `0700` and writes
`{}`. Errors besides those of every command:

| `code` | Meaning | Acme's action |
|---|---|---|
| `invalid_bootstrap` | stdin is not one complete, valid bootstrap | Ask the backend for a new credential |
| `server_running` | An App Server owns the root | Stop it first (step 10), then configure |

Replacing a configuration revokes nothing at Engine.

## 6. Step 4: enable drives on a Mac

Acme tells Alice that it will enable drives and continues only after she
agrees. After step 3 and before step 5, while no App Server owns the root, it
runs:

```sh
TETRAL_HOME="$root" tetral drives enable --app acme
```

macOS asks Alice for network-volume access, naming Acme. The command waits
for her answer with no time limit; meanwhile Acme shows that macOS is
waiting, with a cancel action that sends SIGINT or SIGTERM to the command.
`{}` means drives mount from the next `server run`. Acme launches this
command and the App Server alike, so that macOS keeps attributing the access
to Acme. Errors besides those of every command:

| `code` | Meaning | Acme's action |
|---|---|---|
| `drives_access_denied` | Alice denied the access | Point to System Settings > Privacy & Security > Files and Folders; once she has turned access on, offer this step again |
| `cancelled` | Acme cancelled while macOS waited | Nothing is recorded; drives stay disabled |
| `drives_mount_failed` | The test drive could not be served or mounted; `message` is `NFS server: <error>`, `mount point: <error>` or `mount: exit status <n>` | Show `message`; drives stay disabled |
| `server_running` | An App Server owns the root | Stop it first, as for enabling later |

Enabling later stops the App Server, which ends the Mac's running commands,
so Acme does it only while the Mac is idle, checked as step 10 states: a
stop (step 10), this step, then step 5. On Linux the step is not needed: the
command writes `{}` and changes nothing.

On macOS a program of another account on the same Mac can read and change a
running Session's drives through the CLI's local NFS server, so Acme runs the
CLI only on a Mac that no one else logs in to.

## 7. Step 5: start the App Server

```sh
TETRAL_HOME="$root" tetral server run --app acme
```

Acme runs it, without `--idle-exit`, as a child process that it keeps running
while Alice is signed in, whether or not a Session uses the machine. It reads
the child's stderr or redirects it to its own log: records it does not read
in time are dropped. SIGINT and SIGTERM stop the App Server as
`POST /v1/shutdown` does; at its exit it writes the `GET /v1/status` object
as of that moment and exits 0. Errors besides those of every command:
`server_running`, an App Server that already owns the root, which Acme uses
through the local API.

Acme then reads `GET /v1/status` (section 9) until `connection_state` is
`online`. The App Server retries network failures, timeouts and `5xx`
answers by itself. `blocked` lasts until the App Server restarts, and Acme
acts on `last_error.code`:

| `last_error.code` | Meaning | Acme's action |
|---|---|---|
| `credential_expired`, `credential_revoked`, `credential_invalid` | The credential no longer works | Get a new credential (step 2) and replace the configuration (step 10). A `credential_expired` that a new credential does not clear means the machine's clock is wrong: once Alice corrects it, restart the App Server |
| `instance_unavailable` | The instance is deregistered | Delete the state root and register again; the drive changes in the root not yet saved to Engine are lost |
| `cli_upgrade_required` | This CLI is below Engine's minimum version | Upgrade (step 10) |
| `connection_conflict` | Another App Server holds this instance's live connection, which happens only when the root was copied, for example to a new computer | Tell Alice that this computer's identity is in use elsewhere; once she agrees, stop the App Server, delete the root, which discards its drive changes not yet saved to Engine, and register this computer as a new instance (steps 2 to 5), revoking no credential, so the other App Server keeps working. The developer then lists the new instance for this computer |
| Any other code | | Show it and offer to restart the App Server |

Right after the App Server restarts, it can stay `disconnected` with
`last_error.code` `connection_conflict` for up to 45 seconds while Engine
still holds the connection of the App Server that ran before; it retries by
itself, and only a `connection_conflict` with `blocked` calls for the action
above.

`cleanup_pending` in `last_error` while `online` means that the processes of
an earlier App Server that ended abnormally are not yet known to be gone, and
no command runs until they are. When it does not clear, Alice restarts the
computer; the next start then counts every earlier process as gone.

## 8. Steps 6 to 8: Sessions on the machine

The developer lists the machine in a Session's `computers` as
`{instance_id, working_directory}`, with an optional `networking` of
`{"type": "blocked"}` (the default) or `{"type": "unrestricted"}`, beside at
least one cloud computer, or adds it to an idle Session with
`POST /v1/sessions/{session_id}/computers`. The instance need not be online.

Acme takes `working_directory` from the system folder picker, so the folder
exists. When Alice has picked none, for example in a conversation outside any
project, Acme creates `<root>/<YYYY-MM-DD>/<conversation name>` under a root
she chose once in Acme's settings, outside folders that a cloud-storage
client syncs (such as `Documents` synced by iCloud or OneDrive), and passes
that: neither Engine nor the CLI has a default working directory for a user
computer. The CLI checks the folder again when Engine prepares the Session
there, and Engine then routes the Session's tool calls for that computer to
the machine.

A failed preparation reaches the developer as `computer_preparation_error`
with its `code`. For `cwd_not_found`, `cwd_not_directory`,
`cwd_inaccessible`, `cwd_forbidden` and `cwd_check_timeout`, Acme asks Alice
to pick the folder again; the developer, while the Session is idle, removes
the computer and, once its removal completes, adds it with the new folder.

## 9. Step 9: the local API

The App Server serves HTTP/JSON on a Unix socket of mode `0600`, in a
directory of mode `0700` outside the state root, and opens no TCP listener.
Acme derives the socket's path from the state root:

```sh
# Linux
sock="/tmp/tetral-sock-$(id -u)/$(printf '%s' "$(cd "$TETRAL_HOME" && pwd -P)" | sha256sum | cut -c1-16).sock"
# macOS
sock="/private/tmp/tetral-sock-$(id -u)/$(printf '%s' "$(cd "$TETRAL_HOME" && pwd -P)" | shasum -a 256 | cut -c1-16).sock"

curl --unix-socket "$sock" http://localhost/v1/status
```

A missing socket, or one that refuses the connection, means that no App
Server owns the root.

**`GET /v1/status`**

```json
{
  "server_state": "running",
  "connection_state": "online",
  "instance_id": "<instance id>",
  "connection_id": "<connection id>",
  "credential_id": "<credential id>",
  "expires_at": "<RFC 3339 UTC time>",
  "cli_version": "<version>",
  "protocol_version": 1,
  "last_error": null,
  "cua_driver": null
}
```

- `server_state`: `running` or `stopping`.
- `connection_state`: `disconnected`, `connecting`, `registering`, `online`,
  `resuming`, `blocked`, `stopping` or `stopped`.
- `instance_id`, `credential_id`, `expires_at`: the instance, the
  credential in use and that credential's expiry.
- `connection_id`: `null` without a connection.
- `cli_version`: this CLI's version.
- `protocol_version`: the version of the connection protocol that
  [`protocol/connection-v1.schema.json`](../../protocol/connection-v1.schema.json)
  describes.
- `last_error`: `null` or `{code, message}`; it never holds a key.
- `cua_driver`: `null` unless `config.toml` records Computer Use enabled
  (section 10); otherwise `{version, tested_version}`. `version` is what the
  recorded driver's `--version` reported when `cua` last started, `null`
  before that or when it could not be read; `tested_version` is the
  cua-driver version this CLI release was tested with. A `version` read as
  other than `tested_version`, `null` included, does not stop Computer Use;
  the App Server then writes the warning record `mcp.driver_version_untested`.

A running App Server and an online connection are different facts.

**`GET /v1/sessions`**

```json
{"sessions": [{"session_id": "<session id>", "binding_id": "<binding id>", "connection_id": "<connection id>", "cwd": "/Users/alice/projects/site", "state": "prepared", "error": null}]}
```

`cwd` is the canonical working directory or `null`. `state` is `staged`,
`preparing`, `prepared_pending_ack`, `prepared`, `failed`, `closing` or
`unavailable`. `error` is the latest preparation error or `null`.

**`POST /v1/computer-use`** takes the body `{"enabled": <boolean>,
"driver": <absolute path or null>}`, which `tetral computer-use` sends.
`driver` is the driver's absolute path when `enabled` is `true`; otherwise
the App Server ignores it and keeps the recorded one.

```sh
curl --unix-socket "$sock" -X POST -d '{"enabled":true,"driver":"/Applications/CuaDriver.app/Contents/MacOS/cua-driver"}' http://localhost/v1/computer-use
```

| Answer | Meaning |
|---|---|
| `200` with `{}` | The change is recorded and applied |
| `400` with `{"code": "invalid_arguments", "message": "<message>"}` | The body is not one JSON object with exactly these two members; nothing changed |
| `500` with `{"code": "driver_failed", "message": "<message>"}` | `<driver> mcp` did not start; the setting is recorded |
| `500` with `{"code": "invalid_configuration", "message": "<message>"}` or `{"code": "io_failed", "message": "<message>"}` | `config.toml` does not parse, or could not be read or written; nothing changed |

**`POST /v1/shutdown`** answers `202` with `{}`. The App Server then stops
within one five-second deadline shared by everything it waits for, closes
its listener, removes the socket while it still holds the root's lock, and
releases the lock. A child it could not confirm as exited is not reported
as exited.

**`GET /v1/metrics`** answers the Prometheus text format 0.0.4 of exactly
these families:

| Family | Meaning |
|---|---|
| `tetral_connection_state{state}` | Gauge: `1` for the current `connection_state`, `0` for the seven others |
| `tetral_reconnects_total{kind}` | Counter of reconnections: `resume`, the same connection on a new stream, or `new`, a new connection after the first |
| `tetral_startup_phase_seconds{phase}` | Gauge: the duration of each startup phase of this process, `config`, `connect` and `ready` |
| `tetral_tasks_in_progress` | Gauge: running tool tasks |
| `tetral_tasks_queued` | Gauge: admitted tool calls waiting to start |
| `tetral_tasks_oldest_queued_seconds` | Gauge: the wait of the oldest of them |
| `tetral_tasks_queue_wait_seconds` | Histogram: how long tool calls waited to start |
| `tetral_tasks_rejected_total{cause}` | Counter of tool calls rejected; `cause` is `resource_exhausted` |
| `tetral_probes_in_progress`, `tetral_probes_queued`, `tetral_probes_oldest_queued_seconds`, `tetral_probes_queue_wait_seconds` | The same for working-directory checks |
| `tetral_probes_limit` | Gauge: the checks that run at once, 8 |
| `tetral_probes_rejected_total{cause}` | Counter of checks rejected; `cause` is `timeout` |
| `tetral_uploads_in_progress`, `tetral_uploads_queued`, `tetral_uploads_oldest_queued_seconds`, `tetral_uploads_queue_wait_seconds`, `tetral_uploads_limit` | The same for drive uploads, 8 at once |
| `tetral_uploads_rejected_total{cause}` | Counter of uploads Engine refused; `cause` is `rate_limited`, a `429` |
| `tetral_downloads_in_progress`, `tetral_downloads_queued`, `tetral_downloads_oldest_queued_seconds`, `tetral_downloads_queue_wait_seconds`, `tetral_downloads_limit` | The same for drive downloads, 8 at once |
| `tetral_downloads_rejected_total{cause}` | Counter of downloads Engine refused; `cause` is `rate_limited`, a `429` |
| `tetral_log_write_failures_total{sink}` | Counter of records that `stderr` or the log `file` could not take |

An excerpt:

```text
# TYPE tetral_tasks_in_progress gauge
tetral_tasks_in_progress 0
# TYPE tetral_tasks_queued gauge
tetral_tasks_queued 0
```

`tetral server status --app acme` writes the `GET /v1/status` object;
`tetral server stop --app acme` sends `POST /v1/shutdown` and writes `{}`.
Both fail with `server_not_running` when the socket is absent or refuses the
connection, and with `server_not_responding` when a complete answer does not
arrive within five seconds. A successful stop confirms that the App Server
accepted it; it does not wait for the App Server to release the root.

## 10. Computer Use

Computer Use is the local MCP server `cua`. It runs the official, unmodified
cua-driver release that [`cli/UPSTREAM.md`](../UPSTREAM.md) pins; Tetral
does not build, rebrand or patch it. Installing the driver authorizes
nothing: Acme explains the risks and Alice turns Computer Use on herself.
Acme takes these steps in this order.

1. **Install the tested release.** From the GitHub release
   `cua-driver-rs-v<version>` of `trycua/cua`, with `<version>` the one
   `cli/UPSTREAM.md` names, Acme downloads the asset for the platform and
   `SHA256SUMS` into one directory and checks the asset there, as for
   `tetral` in step 1. On a Mac the asset is
   `cua-driver-rs-<version>-darwin-universal.tar.gz`:

   ```sh
   shasum -a 256 --ignore-missing -c SHA256SUMS
   ```

   prints `cua-driver-rs-<version>-darwin-universal.tar.gz: OK` and exits 0;
   an asset whose digest differs is not installed. Acme extracts it and
   places its `CuaDriver.app` as `/Applications/CuaDriver.app`. One
   `/Applications/CuaDriver.app` serves every program on the Mac that uses
   cua-driver, so when one is already there Acme tells Alice that other
   programs on her Mac use it and replaces it only once she agrees;
   otherwise it keeps the one there. The driver executable, `<driver>`
   below, is
   `/Applications/CuaDriver.app/Contents/MacOS/cua-driver`. On Linux the
   asset is the release's Linux build, and `<driver>` is its `cua-driver`.

2. **Explain the risks.** With Computer Use on, an agent reads Alice's
   screen and types and clicks in her other apps as she would. Sessions
   share the one desktop: the driver decides whether their actions wait for
   each other, run at once or are refused, and no Session gets the desktop
   to itself.
   What a call has already done stays after a cancel, a timeout, a disable
   or a disconnection: nothing is undone or replayed.

3. **Let Alice turn Computer Use on.** Acme continues only after Alice
   explicitly enables it.

4. **Choose the driver's permission mode.** The driver fixes its permission
   mode, its risk acknowledgement and its policy files when its runtime
   starts; without a choice the mode is the driver's default, `standard`.
   The CLI runs the driver only as `<driver> --version` and
   `<driver> mcp`, with no other argument, and never relaxes the mode.
   - On Linux the runtime is each `<driver> mcp`, which inherits the
     environment of `tetral server run`: Acme sets
     `CUA_DRIVER_PERMISSION_MODE` and the related `CUA_DRIVER_*` variables
     there.
   - On a Mac the runtime is `CuaDriver.app`, whose environment is not
     Acme's. A `<driver> mcp` that finds it not running starts it with the
     default mode, so for another mode Acme starts it first, before each
     `tetral server run`, through LaunchServices as the driver starts itself:

     ```sh
     open -n -g /Applications/CuaDriver.app --args serve --permission-mode <mode>
     ```

     with the related flags that `<driver> serve --help` lists. Running
     `<driver> serve` as Acme's own child instead would make macOS attribute
     the driver's grants to Acme. A running `CuaDriver.app` keeps its mode;
     changing it takes `<driver> stop` and a new start.

5. **Check and request the grants on a Mac.** On a Mac the bindings'
   `<driver> mcp` processes all reach `CuaDriver.app`, which macOS starts as
   a standalone app. The App Server neither starts nor stops it and needs no
   Accessibility or Screen Recording grant of its own; stopping `cua` stops
   only its `<driver> mcp` processes. The grants belong to `CuaDriver.app`,
   so macOS names CuaDriver in its prompts, not Acme and not `tetral`, and
   Alice grants them once per Mac: no later Session asks again. Acme reads
   the grants without prompting:

   ```sh
   /Applications/CuaDriver.app/Contents/MacOS/cua-driver permissions status --json
   ```

   Both are granted when the answer holds `"accessibility": true` and
   `"screen_recording": true`. Any other answer, including
   `{"daemon_running": false, "status": "unknown", ...}` while
   `CuaDriver.app` is not running, means Acme asks Alice to approve them and
   runs:

   ```sh
   /Applications/CuaDriver.app/Contents/MacOS/cua-driver permissions grant
   ```

   which opens `CuaDriver.app` through macOS, so the prompts name
   CuaDriver. Alice approves both in System Settings > Privacy & Security,
   and Acme reads the status again. A grant Alice denies or later revokes
   makes the driver's calls fail with the driver's own errors, which reach
   the agent unchanged. On Linux the driver needs a desktop session reachable
   through the environment of `tetral server run`; without one, the driver
   itself shows it, through a failed start, a shorter tool list or call
   errors.

6. **Enable Computer Use.**

   ```sh
   TETRAL_HOME="$root" tetral computer-use enable --app acme --driver /Applications/CuaDriver.app/Contents/MacOS/cua-driver
   ```

   `--driver` takes the driver's absolute path. The command writes `{}`
   whether or not an App Server runs; a running App Server starts
   `<driver> mcp` and reports the driver's tools.
   Once its App Server answers `GET /v1/status` (section 9) with Computer
   Use enabled, after this command or after a `tetral server run`, Acme
   waits 5 seconds, then reads `cua_driver` and tells Alice when `version`
   differs from `tested_version`: another program's install may have
   replaced the tested release, and Computer Use keeps running with it. A
   `version` still `null` then means the driver's version could not be read.
   `tetral computer-use disable --app acme` turns it off again and writes
   `{}`: a running `cua` call ends `cancelled`, and later calls are rejected
   until it is enabled again. Errors besides those of every command:

   | `code` | Meaning | Acme's action |
   |---|---|---|
   | `driver_failed` | `<driver> mcp` did not start; the recorded setting stays | Show `message` and check the driver's installation |
   | `server_running` | The root's lock stayed held for 10 seconds with no App Server answering | Retry once the App Server has started or stopped |

## 11. Step 10: upgrades, configuration replacement and removal

**Upgrade.** An upgrade comes through Acme's own update channel. Before it,
Acme may download, check and install the complete new release into a
separate version directory while the old App Server keeps running; it keeps
the old executable and helpers unchanged. On Linux each version directory
holds `tetral`, `THIRD-PARTY-NOTICES`, `bwrap`, `COPYING`, `COPYING.libcap`
and `COPYING.openssl` from the same archive. The switch happens while the machine is idle, which
Acme checks right before the stop: `GET /v1/metrics` (section 9) shows
`tetral_tasks_in_progress` and `tetral_tasks_queued` at 0; otherwise Acme
waits and checks again. A call that reaches the machine between the check
and the stop ends, and the agent gets its outcome as unknown. Acme then:

1. stops the App Server with `tetral server stop` or `POST /v1/shutdown`;
2. waits for `server run` to exit;
3. runs step 5 from the new version directory on the same state root.

Commands running on the machine end; Engine prepares its Sessions again, and
each Session's drive changes not yet saved stay and are saved after the
start. Pointing an outside `current` link at the new directory while the old
App Server runs does not change the executable that App Server uses; Acme
removes the old directory only after every App Server using it has exited.
Acme owns version selection and cleanup: the CLI never updates itself and
copies no executable at start. The backend reads `cli_version` and
`cli_update_available` on `GET /v1/instances/{instance_id}`; a CLI below
Engine's minimum stops at `cli_upgrade_required`.

**Configuration replacement**, for a new credential: Acme stops the App
Server, runs step 3 with the new bootstrap and runs step 5 again at once.

**Uninstall.** Acme runs `tetral server stop` first, where
`server_not_running` means that none runs, then removes the binary.

**Removal.** When Alice removes the device, the backend deregisters the
instance with `DELETE /v1/instances/{instance_id}`, which answers `204`, also
when repeated, and removes it from every Session; Acme deletes the state
root, which discards the drive changes in it not yet saved to Engine.
Alice's project files stay.

## 12. A Mac without drives

When Alice declines step 4 or it fails, the Mac connects and runs commands
without drives: it mounts none, `TETRAL_DRIVES` is unset in its commands,
and the developer gets one `drive_mount_error`, with the drive's `drive_id`
and the machine's `computer_id`, for each drive a Session binds, at that
Session's preparation on the Mac. Enabling later is step 4 while the Mac is
idle, between a stop and a new start.

## 13. Diagnostics

- `TETRAL_LOG_LEVEL` in the environment of a command sets the lowest record
  level written: `debug`, `info` (the default), `warn` or `error`; any other
  value, the empty one included, fails the command with `invalid_arguments`.
- `server run` writes its records to `$TETRAL_HOME/logs/tetral.log` as well
  as to stderr; the directory has mode `0700` and each file `0600`. When a
  record would make the file exceed 10 MiB, `tetral.log.1` becomes
  `tetral.log.2`, `tetral.log` becomes `tetral.log.1`, and a new
  `tetral.log` starts; at most these three files exist.
- `tetral diag --app acme --output <path>` writes a `tar.gz` bundle, mode
  `0600`, to `<path>`, an absolute path where no file exists, whether or not
  an App Server runs, and writes `{}`. The bundle holds exactly:

  | Entry | Content |
  |---|---|
  | `logs/` | Copies of the log files |
  | `config.toml` | The root's `config.toml`; the credential file is never included |
  | `system.json` | `{cli_version, os, os_version, arch}` |
  | `status.json`, `metrics.txt` | The answers of `GET /v1/status` and `GET /v1/metrics` when an App Server answers within five seconds; otherwise `status.json` is `{"server_state": "not_running"}` and `metrics.txt` is absent |

  Its own failures:

  | `code` | Meaning | Acme's action |
  |---|---|---|
  | `invalid_arguments` | `<path>` is relative or a file exists there | Choose another path |
  | `io_failed` naming a bundle entry | That entry holds a key, and no bundle is written | Show `message`; Alice's support looks at the entry |

  Acme sends the bundle to its own support only after Alice agrees. Engine
  never receives it.

## 14. Example program

[`cli/examples/app-integration.sh`](../examples/app-integration.sh) is a
POSIX `sh` script that performs Acme's side of steps 2 to 5, step 9 and the
stop. It stands in for the backend in step 2 and for Alice's answer in step
4. It runs as `sh app-integration.sh <command>` on Linux and macOS with
`curl` and `jq` on `PATH`, and reads:

| Variable | Value | Commands |
|---|---|---|
| `TETRAL_HOME` | The state root, an absolute path | All |
| `TETRAL_APP` | The app identifier passed as `--app`, nonempty | `register`, `start` |
| `TETRAL_BIN` | The absolute path of an executable `tetral` | `register`, `start` |
| `TETRAL_BASE_URL` | Engine's base URL, which becomes the bootstrap's `server_url` | `register` |
| `TETRAL_API_KEY` | The workspace API key | `register` |
| `TETRAL_ENABLE_DRIVES` | Alice's answer in step 4: `1` agreed; unset or `0` declined | `register` |

| Command | What it does |
|---|---|
| `register` | Fails with `already_configured` when the root already holds a `config.toml`. Registers the machine under the name `uname -n` prints, installs the bootstrap with `tetral configure`, then, on a Mac with `TETRAL_ENABLE_DRIVES=1`, runs `tetral drives enable`, passing on a SIGINT or SIGTERM the script receives meanwhile. Prints the `instance` object, `configure`'s `{}`, then `drives enable`'s `{}` when it ran |
| `start` | Replaces itself with `tetral server run`; the caller runs it in the background and ends it with `stop`, SIGINT or SIGTERM. Prints the `server run` object at its exit |
| `wait-online` | Reads `GET /v1/status` once a second for at most 120 seconds. Prints the `online` status object; `blocked` fails with `last_error.code`; at 120 seconds it fails with `server_not_running` when the socket never answered, otherwise with `not_online` |
| `status` | Prints `GET /v1/status`, then `GET /v1/sessions` |
| `stop` | Sends `POST /v1/shutdown`, then checks once a second, for at most 10 seconds, that the socket is gone. Prints `{}`; fails with `stop_timeout` when the socket stays |

Each step's object goes to stdout, compact and one per line, and nothing
else does. On a failure the script writes the failing step's object, writes
`app-integration.sh: <code>` to stderr and exits 1. The object is a `tetral`
command's error with its `code`, Engine's API error envelope with its
`error.type`, the `blocked` status object with its `last_error.code`, or the
script's own `{"error": {"code": "<code>", "message": "<message>"}}`:

| `code` | Failure |
|---|---|
| `invalid_arguments` | An unknown command, or a variable the command reads missing or invalid, named in `message` |
| `missing_prerequisite` | `curl` or `jq` is not on `PATH` |
| `already_configured` | `register` with a `config.toml` in the root |
| `request_failed` | No HTTP response from Engine (`message` `curl: exit status <n>`), or an answer other than `201` that is not an API error envelope (`message` `HTTP <status>`) |
| `server_not_running` | A local API request could not connect |
| `not_online` | `wait-online` reached 120 seconds with the socket answering |
| `stop_timeout` | The socket stayed 10 seconds after the `202` |

The script unsets `TETRAL_API_KEY` at its start, so no program it runs
inherits it, and gives the key to `curl` on stdin. The registration's
response, which holds the connection key, reaches only `jq` and
`tetral configure`, through pipes. No key appears in the output, in a file
or in a process argument. A failure after the registration leaves the
instance registered: the printed `instance` object names its `id`, and the
developer deregisters it with `DELETE /v1/instances/{instance_id}`.

One complete run on Linux:

```console
$ export TETRAL_HOME=/home/alice/.local/share/acme/tetral/acct_1 TETRAL_APP=acme \
    TETRAL_BIN=/opt/acme/2.4.0/tetral TETRAL_BASE_URL=https://engine.example.com \
    TETRAL_API_KEY=<workspace API key>
$ sh app-integration.sh register
{"id":"<instance id>","name":"alice-laptop","...":"..."}
{}
$ sh app-integration.sh start &
$ sh app-integration.sh wait-online
{"server_state":"running","connection_state":"online","instance_id":"<instance id>","connection_id":"<connection id>","credential_id":"<credential id>","expires_at":"<RFC 3339 UTC time>","cli_version":"<version>","protocol_version":1,"last_error":null,"cua_driver":null}
$ sh app-integration.sh status
{"server_state":"running","connection_state":"online","instance_id":"<instance id>","connection_id":"<connection id>","credential_id":"<credential id>","expires_at":"<RFC 3339 UTC time>","cli_version":"<version>","protocol_version":1,"last_error":null,"cua_driver":null}
{"sessions":[]}
$ sh app-integration.sh stop
{}
```

The background `start` then exits 0 and prints the App Server's last status
object.
