# Supplemental release notices

The CLI release owner maintains these verbatim source files. `scripts/package.sh`
checks their SHA-256 pins before appending them to `THIRD-PARTY-NOTICES`; that
script owns the enforced pins.

| File | Original source and revision | SHA-256 |
|---|---|---|
| `codex-NOTICE` | [Codex root NOTICE](https://github.com/openai/codex/blob/6aaac694803004a63c6f393d153c96d22521a6b9/NOTICE), commit `6aaac694803004a63c6f393d153c96d22521a6b9` (`rust-v0.164.0`), matching `Cargo.lock` | `9d71575ecfd9a843fc1677b0efb08053c6ba9fd686a0de1a6f5382fd3c220915` |
| `moka-NOTICE` | `NOTICE` from [moka 0.12.16](https://static.crates.io/crates/moka/moka-0.12.16.crate), package SHA-256 `4293f18e7567a1caf3c584855554377025c65e0aa445344d04171f5ad63d19b9` matching `Cargo.lock` | `f6d3f84b39d597c19748c6b50a04abe1634edb3c0b15392daa217ec44119f652` |
| `musl-COPYRIGHT` | [musl COPYRIGHT](https://git.musl-libc.org/cgit/musl/plain/COPYRIGHT?h=v1.2.5), tag `v1.2.5` | `f9bc4423732350eb0b3f7ed7e91d530298476f8fec0c6c427a1c04ade22655af` |

The Codex notice is reproduced as the upstream project's statement on every
target, including its original nonbreaking spaces and Ratatui attribution. That
statement does not add Ratatui to Tetral's target-resolved crate inventory.
The Moka notice is included on every target and preserves the attribution of
its Apache-only files ported from Java Caffeine.
The musl notice is included only in Linux archives, covering both `tetral` and
`bwrap`. Its complete component attributions, permissions and exceptions are
preserved. These records cover the selected sources, not an exhaustive legal
audit of every dependency.

## Runtime provenance

The Linux runtime is **musl 1.2.5 as distributed with Rust 1.99.1**. Rust release
1.99.1 is source commit `58cf8f9c35b355199727627d07ae34a4e2846d67`; its distribution
manifest is dated 2026-10-15. At that source revision:

- The [x86_64 musl target](https://github.com/rust-lang/rust/blob/58cf8f9c35b355199727627d07ae34a4e2846d67/compiler/rustc_target/src/spec/targets/x86_64_unknown_linux_musl.rs)
  and [aarch64 musl target](https://github.com/rust-lang/rust/blob/58cf8f9c35b355199727627d07ae34a4e2846d67/compiler/rustc_target/src/spec/targets/aarch64_unknown_linux_musl.rs)
  identify musl 1.2.5 and default to static CRT.
- [linux_musl options](https://github.com/rust-lang/rust/blob/58cf8f9c35b355199727627d07ae34a4e2846d67/compiler/rustc_target/src/spec/base/linux_musl.rs)
  select `InferredForMusl`; [`self_contained_components`](https://github.com/rust-lang/rust/blob/58cf8f9c35b355199727627d07ae34a4e2846d67/compiler/rustc_codegen_ssa/src/back/link.rs)
  enables the self-contained components for this static case. The supported
  packaging path therefore selects the Rust target's `lib/self-contained/libc.a`.
- [musl-toolchain.sh](https://github.com/rust-lang/rust/blob/58cf8f9c35b355199727627d07ae34a4e2846d67/src/ci/docker/scripts/musl-toolchain.sh)
  selects musl 1.2.5 through musl-cross-make commit
  `3635262e4524c991552789af6f36211a335a77b3` and adds distribution patches.
  [musl.sh](https://github.com/rust-lang/rust/blob/58cf8f9c35b355199727627d07ae34a4e2846d67/src/ci/docker/scripts/musl.sh)
  names the same base version and distribution fixes. These patches change
  implementation sources, not `COPYRIGHT`; the runtime is not described as an
  unpatched upstream build.

The pinned Codex [`bwrap/build.rs`](https://github.com/openai/codex/blob/6aaac694803004a63c6f393d153c96d22521a6b9/codex-rs/bwrap/build.rs)
compiles C objects into `standalone_bwrap`, then Rust links the `bwrap` wrapper.
`CC_<target>` selects C compilation and headers, not a separate final musl
runtime. The source/configuration inspection above and the packaging Rust-release
check establish the supported source selection; they are not a final-link trace.
Changes to the toolchain, target or link configuration must revisit this record.

## Refresh and acceptance

Follow [Raising Codex](../UPSTREAM.md#codex) when changing its tag or Rust release.
When updating Moka, retrieve the locked crate archive, verify its `Cargo.lock`
checksum, and refresh its complete `NOTICE`, version and digest together.
Inspect linked dependencies for applicable upstream notices when raising them.
For Codex and musl:

1. Retrieve root `NOTICE` from the new exact locked Codex revision, and `COPYRIGHT`
   from the musl version selected by the new Rust target/distribution sources.
   Inspect distribution patches for changes to that notice.
2. Copy complete source bytes without rewriting attribution or line endings.
   Update this provenance, the source table and the corresponding revision,
   version and SHA-256 constants in `scripts/package.sh` in the same change.
3. Build native archives and run release-archive acceptance on them.
   It checks crate coverage, complete licence bodies independently of `about.hbs`,
   and these exact supplemental bodies and source labels in the extracted file.

When changing the validator, check that removing each supplemental body from
an otherwise valid archive fails acceptance, then verify the original archive
passes. These controls validate the checker; ordinary runs check the delivered
archive.
