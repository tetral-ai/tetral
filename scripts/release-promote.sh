#!/usr/bin/env bash
set -euo pipefail

: "${VERSION:?artifact version is required}"
: "${GIT_VERSION:?Git version is required}"
: "${SOURCE_COMMIT:?source commit is required}"
: "${CANDIDATE_DIGEST:?candidate digest is required}"
: "${EVIDENCE_DIGEST:?evidence digest is required}"
: "${WORKFLOW_SHA:?workflow commit is required}"
: "${RUN_ID:?workflow run is required}"
: "${RUN_ATTEMPT:?workflow attempt is required}"
: "${ACTOR_ID:?approving actor is required}"
: "${GITHUB_REPOSITORY:?GitHub repository is required}"
: "${RELEASE_METADATA_REPOSITORY:?metadata repository is required}"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

git fetch --force origin main
target_sequence="${VERSION##*.}"
while IFS= read -r existing; do
  [[ "$existing" =~ ^v0\.1\.0-alpha\.[0-9]+$ ]] || continue
  test "${existing##*.}" -le "$target_sequence" || { echo "a newer release version already exists" >&2; exit 1; }
done < <(git tag -l 'v0.1.0-alpha.*')
while IFS= read -r tag; do
  [[ "$tag" =~ ^reservation-0\.1\.0-alpha\.[0-9]+$ ]] || continue
  test "${tag##*.}" -le "$target_sequence" || { echo "a newer release reservation already exists" >&2; exit 1; }
done < <(oras repo tags "$RELEASE_METADATA_REPOSITORY" --format json | jq -r '.tags[]')

# Discover once, retaining the original canonical records for publication.
./scripts/release-state.sh "$VERSION" "$work/facts.json" "$work"
test "$(jq -r .candidate_digest "$work/facts.json")" = "$CANDIDATE_DIGEST"
test "$(jq -r .rehearsal_digest "$work/facts.json")" = "$EVIDENCE_DIGEST"
jq -e --arg source "$SOURCE_COMMIT" --arg git "$GIT_VERSION" --arg version "$VERSION" \
  '.source_commit == $source and .version.git == $git and .version.artifact == $version' "$work/candidate.json" >/dev/null
state="$(go run ./internal/release/cmd/tetral-release state --facts "$work/facts.json" --now "$(date -u +%FT%TZ)" | jq -r .state)"
case "$state" in rehearsed|authorized|partially_promoted|released) ;; *) echo "release state $state cannot be promoted" >&2; exit 1 ;; esac

# The protected environment confirms this invocation. Persist a new authorization
# only after validating it against the accepted Candidate and rehearsal evidence.
deployment_id="$(./scripts/release-github-deployment.sh "$WORKFLOW_SHA" "$RUN_ID" "$ACTOR_ID")"
authorization_digest="$(jq -r '.authorization_digest // empty' "$work/facts.json")"
if [[ -z "$authorization_digest" ]]; then
  jq -n --arg version "$GIT_VERSION" --arg artifact "$VERSION" --argjson sequence "${VERSION##*.}" --arg source "$SOURCE_COMMIT" --arg candidate "$CANDIDATE_DIGEST" --arg evidence "$EVIDENCE_DIGEST" --argjson run_id "$RUN_ID" --argjson attempt "$RUN_ATTEMPT" --argjson deployment "$deployment_id" --arg now "$(date -u +%FT%TZ)" \
    '{schema:"tetral.release-authorization/v1",version:{git:$version,artifact:$artifact,sequence:$sequence},source_commit:$source,candidate_digest:$candidate,evidence_digest:$evidence,workflow_run_id:$run_id,workflow_run_attempt:$attempt,deployment_id:$deployment,authorized_at:$now}' > "$work/authorization.json"
  go run ./internal/release/cmd/tetral-release validate-authorization --candidate "$work/candidate.json" --candidate-digest "$CANDIDATE_DIGEST" --evidence-digest "$EVIDENCE_DIGEST" --authorization "$work/authorization.json" >/dev/null
  authorization_digest="$(./scripts/release-oci-record.sh publish authorization "$work/authorization.json" "$RELEASE_METADATA_REPOSITORY:authorization-$VERSION")"
  ./scripts/release-oci-record.sh fetch authorization "$RELEASE_METADATA_REPOSITORY@$authorization_digest" "$work/authorization.json" >/dev/null
  jq --slurpfile authorization "$work/authorization.json" --arg digest "$authorization_digest" \
    '. + {authorization:$authorization[0],authorization_digest:$digest}' "$work/facts.json" > "$work/authorized.json"
  mv "$work/authorized.json" "$work/facts.json"
fi

# Initial discovery rejected conflicting targets. Reuse matching targets and
# publish only missing ones; the final read verifies the actual remote results.
for image in tetral gateway agent-runtime sandbox; do
  if [[ -z "$(jq -r --arg image "$image" '.final.images[$image] // empty' "$work/facts.json")" ]]; then
    digest="$(jq -r --arg image "$image" '.images[$image].top_level_digest' "$work/candidate.json")"
    docker buildx imagetools create --tag "ghcr.io/tetral-ai/$image:$VERSION" "ghcr.io/tetral-ai/$image@$digest"
  fi
done

if [[ -z "$(jq -r '.final.chart_manifest // empty' "$work/facts.json")" ]]; then
  chart_manifest="$(jq -r .chart.candidate_manifest_digest "$work/candidate.json")"
  chart_digest="$(jq -r .chart.package_sha256 "$work/candidate.json")"
  ./scripts/release-oci-record.sh fetch helm-candidate "$RELEASE_METADATA_REPOSITORY@$chart_manifest" "$work/tetral-$VERSION.tgz" >/dev/null
  test "sha256:$(sha256sum "$work/tetral-$VERSION.tgz" | awk '{print $1}')" = "$chart_digest"
  helm push "$work/tetral-$VERSION.tgz" oci://ghcr.io/tetral-ai/charts
fi

if [[ -z "$(jq -r '.final.git_tag_commit // empty' "$work/facts.json")" ]]; then
  annotated="$(gh api --method POST "repos/$GITHUB_REPOSITORY/git/tags" -f tag="$GIT_VERSION" -f message="Tetral $GIT_VERSION" -f object="$(jq -r .source_commit "$work/candidate.json")" -f type=commit --jq .sha)"
  gh api --method POST "repos/$GITHUB_REPOSITORY/git/refs" -f ref="refs/tags/$GIT_VERSION" -f sha="$annotated" >/dev/null
fi

# A killed `gh release create` can leave a draft or some uploaded assets.
# Create the draft explicitly, fill missing assets, then publish it.
if [[ "$(jq -r '.final.github_release == null' "$work/facts.json")" = true ]]; then
  gh release create "$GIT_VERSION" --repo "$GITHUB_REPOSITORY" --verify-tag --draft --prerelease --title "Tetral $GIT_VERSION"
fi
for record in candidate rehearsal authorization; do
  asset="$record.json"
  [[ "$record" != rehearsal ]] || asset=evidence.json
  if [[ -z "$(jq -r --arg asset "$asset" '.final.github_release_assets[$asset] // empty' "$work/facts.json")" ]]; then
    # GitHub uses the local basename as the attachment name.
    [[ "$record" != rehearsal ]] || cp "$work/rehearsal.json" "$work/evidence.json"
    gh release upload "$GIT_VERSION" "$work/$asset" --repo "$GITHUB_REPOSITORY"
  fi
done
if [[ "$(jq -r '.final.github_release == null or .final.github_release.draft' "$work/facts.json")" = true ]]; then
  gh release edit "$GIT_VERSION" --repo "$GITHUB_REPOSITORY" --draft=false --prerelease
fi

# Verify published targets once, including anonymous registry access for users.
# Reuse immutable records already validated above.
oras logout ghcr.io
./scripts/release-final-state.sh "$VERSION" "$work/final.json"
jq --slurpfile final "$work/final.json" '.final = $final[0]' "$work/facts.json" > "$work/completed.json"
test "$(go run ./internal/release/cmd/tetral-release state --facts "$work/completed.json" --now "$(date -u +%FT%TZ)" | jq -r .state)" = released
if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
  {
    echo "Released: https://github.com/$GITHUB_REPOSITORY/releases/tag/$GIT_VERSION"
    echo "Candidate Manifest: $CANDIDATE_DIGEST"
    echo "Rehearsal Evidence: $EVIDENCE_DIGEST"
    echo "Authorization: $authorization_digest"
  } >> "$GITHUB_STEP_SUMMARY"
fi
