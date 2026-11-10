#!/usr/bin/env bash
# Installs bubblewrap 0.13.0 as /usr/bin/bwrap, where the Linux CLI tests find
# the host's bwrap, built from the upstream release tarball once its SHA-256
# matches. Releases before 0.13.0 remount each submount from a snapshot of the
# mount table, and a launch fails when a mount goes away in between, as a
# drive mount of a concurrent test does; 0.13.0 applies the flags with
# mount_setattr. The build is reproducible, so the digest of /usr/bin/bwrap in
# the build cache keys is the same in each workflow that runs this script.
set -euo pipefail

version=0.13.0
sha256=4734237473c0e5d695e4e9034a34e43b2dbf5164655bd13fa59ae376b2b7a765
tarball=bubblewrap-$version.tar.xz

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
curl -fsSL -o "$work/$tarball" \
  "https://github.com/containers/bubblewrap/releases/download/v$version/$tarball"
(cd "$work" && printf '%s  %s\n' "$sha256" "$tarball" | sha256sum -c -)

sudo apt-get update
sudo apt-get install -y --no-install-recommends meson ninja-build pkg-config libcap-dev
tar -xJf "$work/$tarball" -C "$work"
meson setup --buildtype=release -Dman=disabled -Dtests=false \
  "$work/build" "$work/bubblewrap-$version"
meson compile -C "$work/build"
sudo install -m 0755 "$work/build/bwrap" /usr/bin/bwrap
/usr/bin/bwrap --version
