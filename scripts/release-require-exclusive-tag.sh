#!/usr/bin/env bash
set -euo pipefail

# The candidate digest must be held by its expected tag and by no other tag of
# the repository, so deleting it removes nothing another tag still names.
repository="${1:?repository is required}"
digest="${2:?digest is required}"
expected_tag="${3:?expected tag is required}"

found=false
while IFS= read -r tag; do
  current="$(oras manifest fetch --format go-template --template '{{ .digest }}' "$repository:$tag")"
  if test "$current" = "$digest"; then
    test "$tag" = "$expected_tag" || {
      echo "$repository@$digest is also retained by tag $tag" >&2
      exit 1
    }
    found=true
  fi
done < <(oras repo tags "$repository" --format json | jq -r '.tags[]')
test "$found" = true || {
  echo "$repository:$expected_tag does not own the candidate digest" >&2
  exit 1
}
