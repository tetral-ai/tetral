#!/usr/bin/env bash
set -euo pipefail

test "$(git rev-parse HEAD)" = "$WORKFLOW_SHA"
test "$WORKFLOW_REF" = refs/heads/main
test "$WORKFLOW_SHA" = "$(git rev-parse origin/main)"
git cat-file -e "$SOURCE_COMMIT^{commit}"
if [[ "$MODE" = candidate ]]; then
  test "$SOURCE_COMMIT" = "$WORKFLOW_SHA"
fi
version_json="$(go run ./internal/release/cmd/tetral-release validate-version --version "$VERSION")"
artifact_version="$(jq -r .artifact <<<"$version_json")"
test "$artifact_version" != "dev"
echo "artifact_version=$artifact_version" >> "$GITHUB_OUTPUT"
echo "candidate_tag=candidate-$artifact_version" >> "$GITHUB_OUTPUT"
