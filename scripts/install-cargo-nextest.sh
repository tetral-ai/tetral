#!/usr/bin/env bash
# Installs cargo-nextest 0.9.151, which runs the CLI's tests one process per
# test with a time limit for each, from the upstream release archive of the
# running system once its SHA-256 matches, into Cargo's bin directory.
set -euo pipefail

version=0.9.151
case "$(uname -s)-$(uname -m)" in
  Linux-x86_64)
    target=x86_64-unknown-linux-gnu
    sha256=1d4ce302b820c41f4b89c5fb9bce45f0f0c94882c1cf08fc360caf08be6e7df4
    ;;
  Darwin-*)
    target=universal-apple-darwin
    sha256=d1d7d025d4bfc9fb154cd76aea3949bd78534829a0589abefa6e5dd4edb135dd
    ;;
  *)
    echo "no cargo-nextest archive is pinned for $(uname -s) on $(uname -m)" >&2
    exit 1
    ;;
esac
archive=cargo-nextest-$version-$target.tar.gz

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
curl -fsSL -o "$work/$archive" \
  "https://github.com/nextest-rs/nextest/releases/download/cargo-nextest-$version/$archive"
(cd "$work" && printf '%s  %s\n' "$sha256" "$archive" | shasum -a 256 -c -)

bin=${CARGO_HOME:-$HOME/.cargo}/bin
mkdir -p "$bin"
tar -xzf "$work/$archive" -C "$bin" cargo-nextest
"$bin/cargo-nextest" nextest --version
