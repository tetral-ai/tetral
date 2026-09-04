#!/usr/bin/env bash
set -euo pipefail

: "${GH_TOKEN:?GitHub job token is required}"
: "${GITHUB_REPOSITORY:?GitHub repository identity is required}"
: "${GITHUB_ACTOR_ID:?GitHub actor database ID is required}"
if [[ "$GITHUB_REPOSITORY" != tetral-ai/tetral ]]; then
  echo "release authority is limited to tetral-ai/tetral" >&2
  exit 1
fi

environment="$(gh api "repos/$GITHUB_REPOSITORY/environments/release")"
if [[ "$(jq -r '.can_admins_bypass' <<<"$environment")" != false ]] ||
   [[ "$(jq -r '.deployment_branch_policy.custom_branch_policies' <<<"$environment")" != true ]] ||
   [[ "$(jq -r '.deployment_branch_policy.protected_branches' <<<"$environment")" != false ]]; then
  echo "release environment does not enforce the approved custom-branch policy" >&2
  exit 1
fi

reviewers="$(jq -c '[.protection_rules[] | select(.type=="required_reviewers") | .reviewers[]] | sort_by(.type,.reviewer.id)' <<<"$environment")"
if [[ "$(jq 'length' <<<"$reviewers")" != 1 ]] ||
   [[ "$(jq -r '.[0].type' <<<"$reviewers")" != User ]] ||
   [[ "$(jq -r '.[0].reviewer.id' <<<"$reviewers")" != "$GITHUB_ACTOR_ID" ]] ||
   [[ "$(jq -r '[.protection_rules[] | select(.type=="required_reviewers") | .prevent_self_review] | unique == [false]' <<<"$environment")" != true ]]; then
  echo "release environment must have exactly the authenticated Owner reviewer and allow explicit self-approval" >&2
  exit 1
fi

branches="$(gh api --paginate --slurp "repos/$GITHUB_REPOSITORY/environments/release/deployment-branch-policies?per_page=100" | jq '{branch_policies: [.[].branch_policies[]]}')"
if [[ "$(jq -r '[.branch_policies[].name] | sort | join(",")' <<<"$branches")" != main ]]; then
  echo "release environment must permit exactly main" >&2
  exit 1
fi

secret_names="$(gh api --paginate --slurp "repos/$GITHUB_REPOSITORY/environments/release/secrets?per_page=100" | jq '[.[].secrets[].name] | unique | sort')"
variable_names="$(gh api --paginate --slurp "repos/$GITHUB_REPOSITORY/environments/release/variables?per_page=100" | jq '[.[].variables[].name] | unique | sort')"
for retired in DAYTONA_API_KEY DAYTONA_API_URL DAYTONA_TARGET; do
  if jq -e --arg name "$retired" 'index($name) != null' <<<"$secret_names" >/dev/null ||
     jq -e --arg name "$retired" 'index($name) != null' <<<"$variable_names" >/dev/null; then
    echo "retired release credential name $retired remains configured" >&2
    exit 1
  fi
done
