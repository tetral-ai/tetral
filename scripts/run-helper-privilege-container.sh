#!/usr/bin/env bash
# Runs the Helper's root-only proofs in an isolated Linux container stage.
set -euo pipefail

engine_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

docker build --target helper-privilege-test \
  --build-arg "TETRAL_HELPER_PRIVILEGE_TEST_NONCE=$(date +%s%N)" \
  --file "$engine_root/Dockerfile.sandbox" "$engine_root"
