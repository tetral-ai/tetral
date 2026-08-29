#!/usr/bin/env bash
set -euo pipefail

source_commit="${1:?source commit is required}"
run_id="${2:?workflow run ID is required}"
expected_approver_id="${3:-}"
: "${GH_TOKEN:?GitHub job token is required}"
: "${GITHUB_REPOSITORY:?GitHub repository is required}"

if [[ -n "$expected_approver_id" ]]; then
  approvals="$(gh api "repos/$GITHUB_REPOSITORY/actions/runs/$run_id/approvals")"
  count="$(jq --argjson actor "$expected_approver_id" '[.[] | select(.state=="approved" and .user.id==$actor and any(.environments[]; .name=="release"))] | length' <<<"$approvals")"
  [[ "$count" == 1 ]] || {
    echo "current release run does not have one exact Owner approval" >&2
    exit 1
  }
fi

matches=()
while IFS= read -r deployment_id; do
  latest="$(gh api "repos/$GITHUB_REPOSITORY/deployments/$deployment_id/statuses?per_page=1")"
  log_url="$(jq -r '.[0].log_url // ""' <<<"$latest")"
  if [[ "$log_url" == "https://github.com/$GITHUB_REPOSITORY/actions/runs/$run_id/"* ]]; then
    matches+=("$deployment_id")
  fi
done < <(gh api --paginate --slurp "repos/$GITHUB_REPOSITORY/deployments?environment=release&ref=$source_commit&per_page=100" | jq -r --arg source "$source_commit" '.[][] | select(.sha==$source) | .id')

if ((${#matches[@]} != 1)); then
  echo "current workflow run does not own exactly one release deployment" >&2
  exit 1
fi
printf '%s\n' "${matches[0]}"
