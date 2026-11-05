# Upstream sources

Every outside source that the CLI builds from or runs, with the version it
is pinned to. The CLI carries no local change to any of them. No source
licensed under the AGPL enters the workspace.

| Source | Origin | Pinned version | Licence | Local changes | Last checked |
|---|---|---|---|---|---|
| Codex: the library crates `codex-utils-pty`, `codex-sandboxing`, `codex-linux-sandbox`, `codex-protocol`, `codex-utils-path-uri` and `codex-utils-absolute-path`, Git dependencies in `Cargo.toml` | https://github.com/openai/codex | `rust-v0.164.0` (commit `6aaac694803004a63c6f393d153c96d22521a6b9`) | Apache-2.0 | none | 2026-10-24 |
| `codex-bwrap`: the bundled `bwrap` of the Linux archives, built from Codex's vendored bubblewrap source and shipped as a separate program with bubblewrap's `COPYING` | https://github.com/openai/codex | `rust-v0.164.0` (commit `6aaac694803004a63c6f393d153c96d22521a6b9`) | LGPL-2.0-or-later | none | 2026-10-24 |
| libcap: the static library that the Linux archives' `bwrap` links, built by `scripts/package.sh` from the release tarball, which it checks against the pinned SHA-256 before building anything, and shipped with its `License` as `COPYING.libcap` | https://mirrors.edge.kernel.org/pub/linux/libs/security/linux-privs/libcap2/ | `2.75` (`libcap-2.75.tar.xz`, SHA-256 `de4e7e064c9ba451d5234dd46e897d7c71c96a9ebf9a0c445bc04f4742d83632`) | BSD-3-Clause OR GPL-2.0-only | none | 2026-11-04 |
| OpenSSL: the static library that the Linux archives' `tetral` links through `native-tls` of the Codex crates, built by `scripts/package.sh` from the release tarball, which it checks against the pinned SHA-256 before building anything, and shipped with its `LICENSE.txt` as `COPYING.openssl` | https://github.com/openssl/openssl/releases/download/openssl-3.6.4/ | `3.6.4` (`openssl-3.6.4.tar.gz`, SHA-256 `9bffaa1ad1e07b354c21bd3324ec02fa15579f45a7d0494b3e74bc449b7333ef`) | Apache-2.0 | none | 2026-11-04 |
| `tokio-tungstenite` and `tungstenite`: the forks that Codex's own `Cargo.toml` patches in at the pinned tag, which the Codex crates build against, through `[patch.crates-io]` in `Cargo.toml` | https://github.com/openai-oss-forks/tokio-tungstenite and https://github.com/openai-oss-forks/tungstenite-rs | `0e5b2d73aa18dd9f0a50ee9ff199d5aef7594186` (`tokio-tungstenite` 0.28.0) and `4fffad30fe373adbdcffab9545e9e9bf4f2fc19f` (`tungstenite` 0.27.0) | MIT; MIT OR Apache-2.0 | none | 2026-10-25 |
| crates.io libraries | https://crates.io | The version that `Cargo.lock` records once a crate uses the library; `rmcp` also by its exact requirement `=3.5.1` | Each library's own | none | 2026-11-04 |
| Microsoft MXC: `mxc-sdk`, a transitive Git dependency of Codex | https://github.com/microsoft/mxc | `mxc-sdk` 1.0.0, commit `b21120168d70befe12356411423266bfef168446` | MIT (`LICENSE.md`) | none | 2026-10-27 |
| cua-driver: the official Rust driver `cua-driver-rs` that Computer Use runs as `<driver> mcp`, from its stable channel only: a tag `cua-driver-rs-v<major>.<minor>.<patch>` whatever GitHub's prerelease flag says, never a `nightly-cua-driver-rs-v…` tag. Its assets are `cua-driver-rs-<version>-darwin-universal.tar.gz`, holding `CuaDriver.app`, on macOS and the glibc build `cua-driver-rs-<version>-linux-<x86_64\|arm64>-binary.tar.gz` on Linux; each is checked against the release's `SHA256SUMS` before it is extracted, and one whose SHA-256 differs is not installed | https://github.com/trycua/cua | `cua-driver-rs-v0.38.0` (commit `85e2b1318bd77ccf3cfa88ccdf875a4bb7eaed19`) | MIT | none: used as released, never built, rebranded or patched | 2026-11-04 |

## Raising a source

Each raise is one change that sets its rows' pinned version and `Last
checked` date; `Local changes` stays `none` for every source. A raise whose
gate fails is not made. Every command below runs in `cli/`.

### Codex

The CLI's owner raises Codex by hand on the first working day of each month,
to the newest stable Codex release tag `rust-v<major>.<minor>.<patch>`, never
one with a suffix such as `-alpha.1`. One change:

1. moves the six Codex crates in `Cargo.toml` to the new tag, and
   `[patch.crates-io]` to the `tokio-tungstenite` and `tungstenite`
   revisions that `codex-rs/Cargo.toml` patches at that tag;
2. sets `rust-toolchain.toml` to the `channel` of
   `codex-rs/rust-toolchain.toml` at that tag;
3. runs `cargo build`, which records the new revisions in `Cargo.lock`;
4. sets the Codex, `codex-bwrap`, fork and `mxc-sdk` rows to what
   `Cargo.lock` now records;
5. removes each `deny.toml` ignore entry whose advisory the new tag fixes:
   `cargo deny check advisories licenses sources` warns that the entry
   matches nothing;
6. sets `LIBCAP_VERSION` and `LIBCAP_SHA256` in `scripts/package.sh`, and
   the libcap row, to the libcap version and tarball SHA-256 that the new
   tag's musl release builds of `bwrap` use, which
   `.github/scripts/install-musl-build-tools.sh` of the Codex repository
   names at that tag, and `OPENSSL_VERSION` and `OPENSSL_SHA256`, and the
   OpenSSL row, to the version and tarball SHA-256 that
   `.github/scripts/install-musl-openssl.sh` names at that tag.

Gate, on Linux and on macOS: `cargo build`, `cargo test -p execution` (the
policy translation table and the isolation launch), and `cargo test -p
tetral-tests --test isolation_linux` on Linux or `--test isolation_macos` on
macOS, whose restricted processes show every `sandbox_policy` rule still
enforced; on macOS also `cargo build --target x86_64-apple-darwin`; on a
machine of each Linux target, `scripts/package.sh <target>` and the archive
test that [Releases](README.md#releases) describes. The pull request's CLI
Verification then runs the whole suite. When the gate fails, the old tag
stays, and a change that sets only the `Last checked` date of the Codex,
`codex-bwrap`, libcap and OpenSSL rows records the attempt.

Only a change in Codex's permission types changes the translation layer,
`crates/execution/src/policy/translate.rs`.

### cua-driver

To a later stable tag `cua-driver-rs-v<major>.<minor>.<patch>` only, with
the new release's assets checked against its `SHA256SUMS`. Gate:

- `cargo test -p tetral-tests --test cua_driver --
  --ignored the_releases_tool_list_report_equals_a_direct_listing_after_an_unattended_start`
  with `TETRAL_TEST_CUA_DRIVER` naming the new release's `cua-driver`. It
  writes the report's `server_info` and `tools` into its acceptance record,
  and the change replaces `tests/snapshots/cua-driver-tools.json` with
  them. The file's diff, every added, removed and changed tool, is reviewed
  tool by tool;
- `each_binding_has_its_own_driver_and_a_close_ends_only_its_own` of the
  same file, on Linux and on macOS;
- on a Mac without the grants, a person follows
  [Computer Use](docs/app-integration.md#10-computer-use) and sees that the
  Accessibility and Screen Recording prompts name CuaDriver.

### crates.io libraries

`cargo update`, or `cargo update -p <crate>` to raise one crate, such as one
that an advisory names. Gate: `cargo test` on Linux and on macOS, which runs
the MCP, drive and connection tests. `rmcp` stays at `=3.5.1`: the MCP proxy
contract fixes it, so raising it is a contract change, not a raise.

### Advisories, licences and sources

`cargo deny check advisories licenses sources` checks every dependency
against the RustSec advisories, the licence list of `deny.toml`, which holds
no AGPL licence, and its `[sources]`, which admit crates.io and only the
Codex crates' Git sources, so that any other Git source fails the check. A
finding fails the check; a vulnerable crate is raised as a crates.io
library. An advisory that only a Codex raise can fix, because its crate
reaches the CLI only through the Codex crates at the pinned tag, is listed
by its ID in `deny.toml`'s `[advisories] ignore` with that reason, and the
monthly Codex raise removes the entry once the new tag fixes it.

**CLI Dependency Check** runs the check in continuous integration;
[Continuous integration](README.md#continuous-integration) says when, and
`.github/workflows/cli-dependency-check.yml` pins the cargo-deny version to
install for a local run.
