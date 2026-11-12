#!/usr/bin/env bash
# Runs the Helper's root-only proofs in an isolated Linux container stage.
#
# The proofs are the stage's RUN steps, so the build's exit status is the
# result and the stage's image is never used. The cacheonly output keeps the
# build from exporting that image: the nonce makes every run's image unique,
# and an exported, untagged image per run accumulates on the host.
set -euo pipefail

engine_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

docker build --target helper-privilege-test \
  --build-arg "TETRAL_HELPER_PRIVILEGE_TEST_NONCE=$(date +%s%N)" \
  --output type=cacheonly \
  --file "$engine_root/Dockerfile.sandbox" "$engine_root"
