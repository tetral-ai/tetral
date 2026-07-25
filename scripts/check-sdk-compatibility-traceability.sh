#!/usr/bin/env bash
# Verifies that every tracked SDK compatibility row has a registered test.

set -euo pipefail

engine_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$engine_root"

if [[ -z "${TETRAL_ENGINE_SDK_ROOT:-}" ]]; then
  for candidate in "$engine_root/../tetral-sdk-typescript" "$engine_root/../../tetral-sdk-typescript"; do
    if [[ -f "$candidate/tests/compatibility/compat-cases.json" ]]; then
      export TETRAL_ENGINE_SDK_ROOT="$candidate"
      break
    fi
  done
fi

go test ./integration/static -run '^TestSDKCompatibilityRowsEqualRegistry$' -count=1
