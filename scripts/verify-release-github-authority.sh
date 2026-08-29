#!/usr/bin/env bash
set -euo pipefail

: "${GH_TOKEN:?GitHub job token is required}"
: "${GITHUB_REPOSITORY:?GitHub repository identity is required}"
: "${GITHUB_ACTOR_ID:?GitHub actor database ID is required}"
: "${DECLARED_PACKAGE_PERMISSION:?Calling job package permission is required}"

if [[ "$DECLARED_PACKAGE_PERMISSION" != write ]]; then
  echo "release mutations require an explicitly declared packages: write job permission" >&2
  exit 1
fi

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

repository_id="$(gh api "repos/$GITHUB_REPOSITORY" --jq .id)"
package_names=(tetral gateway agent-runtime sandbox tetral-release-metadata charts/tetral)
packages="$(gh api --paginate --slurp 'orgs/tetral-ai/packages?package_type=container&per_page=100' | jq 'add')"
mkdir -p .release-preflight
identities=.release-preflight/packages.jsonl
: > "$identities"

missing=()
for name in "${package_names[@]}"; do
  package="$(jq -c --arg name "$name" '.[] | select(.name==$name)' <<<"$packages")"
  if [[ -z "$package" ]]; then
    missing+=("$name")
    jq -cn --arg name "$name" '{found:false,name:$name,organization:"tetral-ai",visibility:"",linked_repository_id:0,actions_repository_ids:[],existing_references:[],creation_authorized:true}' >> "$identities"
    continue
  fi
  visibility="$(jq -r .visibility <<<"$package")"
  linked="$(jq -r '.repository.id // 0' <<<"$package")"
  encoded="$(jq -rn --arg value "$name" '$value|@uri')"
  access="$(gh api --paginate --slurp "orgs/tetral-ai/packages/container/$encoded/repositories?per_page=100" | jq 'add')"
  versions="$(gh api --paginate --slurp "orgs/tetral-ai/packages/container/$encoded/versions?per_page=100" | jq 'add')"
  actions_ids="$(jq -c '[.[].id] | sort' <<<"$access")"
  references="$(jq -c '[.[].metadata.container.tags[]] | unique | sort' <<<"$versions")"
  jq -cn --arg name "$name" --arg visibility "$visibility" --argjson linked "$linked" --argjson actions "$actions_ids" --argjson references "$references" '{found:true,name:$name,organization:"tetral-ai",visibility:$visibility,linked_repository_id:$linked,actions_repository_ids:$actions,existing_references:$references,creation_authorized:false}' >> "$identities"
done

if ((${#missing[@]})); then
  expected="$(printf '%s\n' "${missing[@]}" | sort | jq -Rsc 'split("\n")[:-1]' | sha256sum | awk '{print "sha256:"$1}')"
  if [[ "${PACKAGE_CREATION_AUTHORIZATION_DIGEST:-}" != "$expected" ]]; then
    echo "not-found package targets require exact authorization digest $expected" >&2
    exit 1
  fi
fi

jq -s --argjson repository_id "$repository_id" --arg permission "$DECLARED_PACKAGE_PERMISSION" '{schema:"tetral.release-package-preflight/v1",organization:"tetral-ai",repository_id:$repository_id,require_write:true,declared_job_permission:$permission,readback_complete:true,packages:.}' "$identities" > .release-preflight/packages.json
go run ./internal/release/cmd/tetral-release package-preflight --input .release-preflight/packages.json >/dev/null
