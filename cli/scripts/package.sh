#!/bin/sh
# Builds the release archive of one target on a machine of that target:
#
#   cli/scripts/package.sh <target>
#
# writes cli/target/dist/tetral-<target>.tar.gz and prints its SHA-256. Every
# archive holds `tetral` and `THIRD-PARTY-NOTICES`, the licences of the Rust
# crates built into that `tetral`, which cargo-about writes from
# cli/about.hbs and cli/about.toml, plus the pinned Codex and Moka NOTICEs
# and, on Linux, musl COPYRIGHT from cli/notices/. A Linux archive also
# holds `bwrap`, built from the pinned Codex tag against a static libcap
# built from the pinned tarball, its licence `COPYING` and libcap's licence
# `COPYING.libcap`; the
# `tetral` in it links a static OpenSSL built from the pinned tarball, whose
# licence is `COPYING.openssl`, and accepts only that `bwrap`. cli/README.md
# lists each target's build prerequisites.
set -eu

# libcap for the Linux `bwrap`: the version and tarball digest that the
# Codex release builds of `bwrap` for musl use (cli/UPSTREAM.md).
LIBCAP_VERSION=2.75
LIBCAP_SHA256=de4e7e064c9ba451d5234dd46e897d7c71c96a9ebf9a0c445bc04f4742d83632
LIBCAP_ORIGIN=https://mirrors.edge.kernel.org/pub/linux/libs/security/linux-privs/libcap2

# OpenSSL for the Linux `tetral`, which links it through the Codex crates:
# the version and tarball digest that the Codex release builds for musl use
# (cli/UPSTREAM.md).
OPENSSL_VERSION=3.6.4
OPENSSL_SHA256=9bffaa1ad1e07b354c21bd3324ec02fa15579f45a7d0494b3e74bc449b7333ef
OPENSSL_ORIGIN=https://github.com/openssl/openssl/releases/download

# cargo-about, which writes the Rust licence bodies (cli/UPSTREAM.md).
CARGO_ABOUT_VERSION=0.9.3

# Verbatim source notices and the runtime distribution they cover. Refresh
# these together with the source records in cli/notices/README.md.
CODEX_NOTICE_REVISION=6aaac694803004a63c6f393d153c96d22521a6b9
CODEX_NOTICE_SHA256=9d71575ecfd9a843fc1677b0efb08053c6ba9fd686a0de1a6f5382fd3c220915
MOKA_NOTICE_VERSION=0.12.16
MOKA_NOTICE_SHA256=f6d3f84b39d597c19748c6b50a04abe1634edb3c0b15392daa217ec44119f652
MUSL_RUST_RELEASE=1.99.1
MUSL_VERSION=1.2.5
MUSL_COPYRIGHT_SHA256=f9bc4423732350eb0b3f7ed7e91d530298476f8fec0c6c427a1c04ade22655af

fail() {
    echo "package.sh: $*" >&2
    exit 1
}

[ "$#" -eq 1 ] || fail "usage: package.sh <target>"
target=$1
cli=$(cd "$(dirname "$0")/.." && pwd)
dist=$cli/target/dist
archive=$dist/tetral-$target.tar.gz

case $target in
    x86_64-unknown-linux-musl | aarch64-unknown-linux-musl) linux=1 ;;
    x86_64-apple-darwin | aarch64-apple-darwin) linux=0 ;;
    *) fail "unknown target $target" ;;
esac

sha256() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | cut -d ' ' -f 1
    else
        shasum -a 256 "$1" | cut -d ' ' -f 1
    fi
}

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
trap 'exit 1' INT TERM
stage=$work/stage
mkdir "$stage"
rm -f "$archive"

# Resolve the source identity once for both the Linux clone check and the
# supplemental Codex notice on every target.
pin=$(sed -n 's/^codex-linux-sandbox = { git = "\([^"]*\)", tag = "\([^"]*\)".*/\1 \2/p' "$cli/Cargo.toml")
origin=${pin% *}
tag=${pin#* }
[ -n "$pin" ] && [ "$origin" != "$pin" ] || fail "no Codex git dependency in cli/Cargo.toml"
locked=$(sed -n "s|^source = \"git+$origin?tag=$tag#\([0-9a-f]*\)\"\$|\1|p" "$cli/Cargo.lock" | sort -u)
[ "$(printf '%s\n' "$locked" | wc -l)" -eq 1 ] && [ -n "$locked" ] ||
    fail "cli/Cargo.lock records no single commit for $origin at $tag"

if [ "$linux" = 1 ]; then
    arch=${target%%-*}
    if command -v "$arch-linux-musl-gcc" >/dev/null 2>&1; then
        musl_gcc=$arch-linux-musl-gcc
    else
        musl_gcc=musl-gcc
    fi

    # The libcap and OpenSSL tarballs, checked before anything is built.
    tarball=libcap-$LIBCAP_VERSION.tar.xz
    curl -fsSL -o "$work/$tarball" "$LIBCAP_ORIGIN/$tarball" ||
        fail "download of $tarball failed"
    (cd "$work" && printf '%s  %s\n' "$LIBCAP_SHA256" "$tarball" | sha256sum -c -) >&2 ||
        fail "$tarball does not match LIBCAP_SHA256"
    openssl_tarball=openssl-$OPENSSL_VERSION.tar.gz
    curl -fsSL -o "$work/$openssl_tarball" "$OPENSSL_ORIGIN/openssl-$OPENSSL_VERSION/$openssl_tarball" ||
        fail "download of $openssl_tarball failed"
    (cd "$work" && printf '%s  %s\n' "$OPENSSL_SHA256" "$openssl_tarball" | sha256sum -c -) >&2 ||
        fail "$openssl_tarball does not match OPENSSL_SHA256"

    # The Codex source of `bwrap` at the tag cli/Cargo.toml pins, checked
    # against the commit cli/Cargo.lock records for it.
    git clone -q --depth 1 --branch "$tag" "$origin" "$work/codex" >&2
    cloned=$(git -C "$work/codex" rev-parse HEAD)
    [ "$cloned" = "$locked" ] ||
        fail "$tag of $origin is at $cloned, but cli/Cargo.lock records $locked"
fi

# No Cargo invocation precedes Linux tarball and clone-versus-lock checks.
about=$(cd "$cli" && cargo about --version 2>/dev/null) || about=
[ "$about" = "cargo-about $CARGO_ABOUT_VERSION" ] ||
    fail "needs cargo-about $CARGO_ABOUT_VERSION${about:+, not $about}: cargo install --locked --features cli cargo-about --version $CARGO_ABOUT_VERSION"

[ "$locked" = "$CODEX_NOTICE_REVISION" ] ||
    fail "Codex notice revision $CODEX_NOTICE_REVISION does not match cli/Cargo.lock $locked"
[ "$(sha256 "$cli/notices/codex-NOTICE")" = "$CODEX_NOTICE_SHA256" ] ||
    fail "codex-NOTICE does not match CODEX_NOTICE_SHA256"
moka_locked=$(sed -n '/^name = "moka"$/ { n; s/^version = "\([^"]*\)"$/\1/p; }' "$cli/Cargo.lock")
[ "$moka_locked" = "$MOKA_NOTICE_VERSION" ] ||
    fail "Moka notice version $MOKA_NOTICE_VERSION does not match cli/Cargo.lock $moka_locked"
[ "$(sha256 "$cli/notices/moka-NOTICE")" = "$MOKA_NOTICE_SHA256" ] ||
    fail "moka-NOTICE does not match MOKA_NOTICE_SHA256"
if [ "$linux" = 1 ]; then
    channel=$(sed -n 's/^channel = "\([^"]*\)"$/\1/p' "$cli/rust-toolchain.toml")
    release=$(cd "$cli" && rustc --version --verbose | sed -n 's/^release: //p')
    [ "$release" = "$MUSL_RUST_RELEASE" ] && [ "$channel" = "$MUSL_RUST_RELEASE" ] ||
        fail "musl notice needs Rust $MUSL_RUST_RELEASE; toolchain channel is $channel, rustc release is $release"
    [ "$(sha256 "$cli/notices/musl-COPYRIGHT")" = "$MUSL_COPYRIGHT_SHA256" ] ||
        fail "musl-COPYRIGHT does not match MUSL_COPYRIGHT_SHA256"
fi

# A crate whose licence cannot be identified, or is not accepted, fails here.
(
    cd "$cli"
    cargo about generate --locked --fail --target "$target" -m crates/tetral/Cargo.toml \
        -c about.toml -o "$stage/THIRD-PARTY-NOTICES" about.hbs >&2
) || fail "cargo about generate failed for $target"

# Keep explanatory framing outside each verbatim source body.
{
    printf '\n================================================================================\n'
    printf "Upstream project's NOTICE, reproduced verbatim\n"
    printf 'Source: %s/blob/%s/NOTICE\nRevision: %s\n' "$origin" "$CODEX_NOTICE_REVISION" "$CODEX_NOTICE_REVISION"
    printf '%s\n' '----- BEGIN CODEX NOTICE -----'
    cat "$cli/notices/codex-NOTICE"
    printf '%s\n' '----- END CODEX NOTICE -----'
} >>"$stage/THIRD-PARTY-NOTICES"
{
    printf '\n================================================================================\n'
    printf 'Moka %s NOTICE, reproduced verbatim\n' "$MOKA_NOTICE_VERSION"
    printf 'Source: https://static.crates.io/crates/moka/moka-%s.crate (NOTICE)\n' "$MOKA_NOTICE_VERSION"
    printf '%s\n' '----- BEGIN MOKA NOTICE -----'
    cat "$cli/notices/moka-NOTICE"
    printf '%s\n' '----- END MOKA NOTICE -----'
} >>"$stage/THIRD-PARTY-NOTICES"
if [ "$linux" = 1 ]; then
    {
        printf '\n================================================================================\n'
        printf 'musl %s as distributed with Rust %s\n' "$MUSL_VERSION" "$MUSL_RUST_RELEASE"
        printf 'Executables: tetral and bwrap\n'
        printf 'Source: https://git.musl-libc.org/cgit/musl/plain/COPYRIGHT?h=v%s\n' "$MUSL_VERSION"
        printf '%s\n' '----- BEGIN MUSL COPYRIGHT -----'
        cat "$cli/notices/musl-COPYRIGHT"
        printf '%s\n' '----- END MUSL COPYRIGHT -----'
    } >>"$stage/THIRD-PARTY-NOTICES"
fi
chmod 0644 "$stage/THIRD-PARTY-NOTICES"

if [ "$linux" = 1 ]; then
    # A static libcap for the target, found through pkg-config.
    tar -xJf "$work/$tarball" -C "$work"
    source=$work/libcap-$LIBCAP_VERSION
    prefix=$work/libcap
    make -C "$source/libcap" CC="$musl_gcc" AR=ar RANLIB=ranlib >&2
    mkdir -p "$prefix/lib/pkgconfig" "$prefix/include/linux" "$prefix/include/sys"
    cp "$source/libcap/libcap.a" "$prefix/lib/"
    cp "$source/libcap/include/uapi/linux/capability.h" "$prefix/include/linux/"
    cp "$source/libcap/include/sys/capability.h" "$prefix/include/sys/"
    cat >"$prefix/lib/pkgconfig/libcap.pc" <<EOF
prefix=$prefix
libdir=\${prefix}/lib
includedir=\${prefix}/include

Name: libcap
Description: libcap
Version: $LIBCAP_VERSION
Libs: -L\${libdir} -lcap
Cflags: -I\${includedir}
EOF

    # The target's C sources include Linux kernel headers, which the musl
    # compiler does not search: the build machine's, after musl's own.
    headers=$work/linux-headers
    mkdir "$headers"
    ln -s /usr/include/linux /usr/include/asm-generic "$headers/"
    ln -s "/usr/include/$arch-linux-gnu/asm" "$headers/asm"
    variable=$(printf '%s' "$target" | tr '-' '_')
    export "CC_$variable=$musl_gcc" "CFLAGS_$variable=-idirafter $headers"

    # A static OpenSSL for the target, configured as the Codex release
    # builds configure it.
    tar -xzf "$work/$openssl_tarball" -C "$work"
    openssl=$work/openssl
    (
        cd "$work/openssl-$OPENSSL_VERSION"
        CC=$musl_gcc perl ./Configure "linux-$arch" "-idirafter$headers" \
            --prefix="$openssl" --openssldir=/usr/local/ssl --libdir=lib \
            no-shared no-module no-tests no-comp no-zlib no-zlib-dynamic no-ssl3 \
            no-md2 no-rc5 no-weak-ssl-ciphers no-camellia no-idea no-seed \
            no-engine no-async -DOPENSSL_NO_SECURE_MEMORY
        make -j"$(nproc)" build_libs
        make install_dev
    ) >&2

    # `bwrap`, then `tetral` with its digest built in. A Codex release tag
    # sets the workspace's version in Cargo.toml without rewriting its
    # Cargo.lock; as Codex's own release build does, `cargo update
    # --workspace` rewrites only the workspace's own entries, so every
    # dependency stays as locked.
    (
        cd "$cli"
        cargo update --workspace --manifest-path "$work/codex/codex-rs/Cargo.toml" >&2
        export PKG_CONFIG_ALLOW_CROSS=1
        export "PKG_CONFIG_PATH_$variable=$prefix/lib/pkgconfig"
        export "PKG_CONFIG_LIBDIR_$variable=$prefix/lib/pkgconfig"
        cargo build --manifest-path "$work/codex/codex-rs/Cargo.toml" --release --locked \
            -p codex-bwrap --target "$target" >&2
    )
    bwrap=$work/codex/codex-rs/target/$target/release/bwrap
    digest=$(sha256 "$bwrap")
    upper=$(printf '%s' "$variable" | tr '[:lower:]' '[:upper:]')
    (
        cd "$cli"
        export "${upper}_OPENSSL_DIR=$openssl" "${upper}_OPENSSL_STATIC=1"
        CODEX_BWRAP_SHA256=$digest cargo build --release --locked -p tetral --target "$target" >&2
    )

    install -m 0755 "$cli/target/$target/release/tetral" "$stage/tetral"
    install -m 0755 "$bwrap" "$stage/bwrap"
    install -m 0644 "$work/codex/codex-rs/vendor/bubblewrap/COPYING" "$stage/COPYING"
    install -m 0644 "$source/License" "$stage/COPYING.libcap"
    install -m 0644 "$work/openssl-$OPENSSL_VERSION/LICENSE.txt" "$stage/COPYING.openssl"
    entries="tetral THIRD-PARTY-NOTICES bwrap COPYING COPYING.libcap COPYING.openssl"
else
    (cd "$cli" && cargo build --release --locked -p tetral --target "$target" >&2)
    install -m 0755 "$cli/target/$target/release/tetral" "$stage/tetral"
    entries="tetral THIRD-PARTY-NOTICES"
fi

mkdir -p "$dist"
# shellcheck disable=SC2086 # the entries are fixed names without spaces
tar -C "$stage" -czf "$archive" $entries
sha256 "$archive"
