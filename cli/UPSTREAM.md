# Upstream sources

Every outside source that the CLI builds from or runs, with the version it
is pinned to. The CLI carries no local change to any of them. No source
licensed under the AGPL enters the workspace.

| Source | Origin | Pinned version | Licence | Local changes | Last checked |
|---|---|---|---|---|---|
| Codex: the library crates `codex-utils-pty`, `codex-sandboxing`, `codex-linux-sandbox`, `codex-protocol`, `codex-utils-path-uri` and `codex-utils-absolute-path`, Git dependencies in `Cargo.toml` | https://github.com/openai/codex | `rust-v0.164.0` (commit `6aaac694803004a63c6f393d153c96d22521a6b9`) | Apache-2.0 | none | 2026-10-24 |
| `codex-bwrap`: the bundled `bwrap` of the Linux archives, built from Codex's vendored bubblewrap source and shipped as a separate program with bubblewrap's `COPYING` | https://github.com/openai/codex | `rust-v0.164.0` (commit `6aaac694803004a63c6f393d153c96d22521a6b9`) | LGPL-2.0-or-later | none | 2026-10-24 |
| `tokio-tungstenite` and `tungstenite`: the forks that Codex's own `Cargo.toml` patches in at the pinned tag, which the Codex crates build against, through `[patch.crates-io]` in `Cargo.toml` | https://github.com/openai-oss-forks/tokio-tungstenite and https://github.com/openai-oss-forks/tungstenite-rs | `0e5b2d73aa18dd9f0a50ee9ff199d5aef7594186` (`tokio-tungstenite` 0.28.0) and `4fffad30fe373adbdcffab9545e9e9bf4f2fc19f` (`tungstenite` 0.27.0) | MIT; MIT OR Apache-2.0 | none | 2026-10-25 |
| crates.io libraries | https://crates.io | The version that `Cargo.lock` records once a crate uses the library; `rmcp` also by its exact requirement `=3.5.1` | Each library's own | none | 2026-11-04 |
| Microsoft MXC: `mxc-sdk`, a transitive Git dependency of Codex | https://github.com/microsoft/mxc | `mxc-sdk` 1.0.0, commit `b21120168d70befe12356411423266bfef168446` | MIT (`LICENSE.md`) | none | 2026-10-27 |
| cua-driver: the official Rust driver `cua-driver-rs` that Computer Use runs as `<driver> mcp`, from its stable channel only: a tag `cua-driver-rs-v<major>.<minor>.<patch>` whatever GitHub's prerelease flag says, never a `nightly-cua-driver-rs-v…` tag. Its assets are `cua-driver-rs-<version>-darwin-universal.tar.gz`, holding `CuaDriver.app`, on macOS and the glibc build `cua-driver-rs-<version>-linux-<x86_64\|arm64>-binary.tar.gz` on Linux; each is checked against the release's `SHA256SUMS` before it is extracted, and one whose SHA-256 differs is not installed | https://github.com/trycua/cua | `cua-driver-rs-v0.38.0` (commit `85e2b1318bd77ccf3cfa88ccdf875a4bb7eaed19`) | MIT | none: used as released, never built, rebranded or patched | 2026-11-04 |
