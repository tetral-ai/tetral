// Package githubrepo owns the single folding comparison that decides when two
// github.com repository URLs name the same repository.
//
// OWNS:
//   - ComparisonKey: the github-repository sameness key. It case-sensitively
//     trims one trailing lowercase ".git" and then lowercases the owner and
//     repository segments; an uppercase ".GIT" is left in place, so it does not
//     fold to the same key as its ".git" form.
//   - The whole-repo rule that ComparisonKey is applied at exactly two sites and
//     is forbidden for every other repository-identity comparison. ComparisonKey
//     is the create-time uniqueness gate for duplicate github resources.
//
// STATE MACHINE (session-create admission dedup, internal/session/service.go):
// a per-request set is keyed by ComparisonKey(canonicalURL).
//
//	no-key      -> key-present   writer: github_repository admission loop; the
//	                             first resource in the request whose URL folds
//	                             to this key claims it.
//	key-present -> rejected      writer: same admission loop; a later resource
//	                             in the request whose URL folds to an
//	                             already-claimed key fails with the
//	                             "duplicate GitHub repository URL" error.
//
// STATE MACHINE (per-request proxy row selection, services/git-proxy/
// credential.go): the mounted session_github_repository_resources row is chosen
// by folding gr.url in SQL to ComparisonKey(owner/repo) with a LIMIT 2 probe.
//
//	0 rows  -> unmounted                     (policy relays anonymously)
//	1 row   -> resolved to that row's write-only token
//	>1 rows -> errRepositoryIdentityAmbiguous
//
// The admission gate above exists to keep this selection single-valued: the
// proxy resolves the per-repo token by folded URL only and cannot disambiguate
// two mounts that fold together yet carry independent tokens, so any pair that
// would fold to one key is rejected at create time rather than left ambiguous
// here.
//
// INVARIANTS:
//   - Only the two sites above fold. Stored and echoed repository URLs, the
//     default "/workspace/<repo>" mount name, and the sandbox clone remote all
//     keep their original case; folding never mutates a persisted or emitted
//     URL. The database stores gr.url verbatim and folds only inside the
//     selection query.
//   - The sandbox same-origin re-clone guard compares remote.origin.url to the
//     admitted URL byte-for-byte (only a trailing ".git" stripped), never
//     through ComparisonKey, so it stays fold-free.
//
// UPDATE-WITH: internal/session/service.go (admission dedup set),
// services/git-proxy/credential.go (proxy row-selection fold),
// internal/sandbox/driver/github_repository.go (fold-free same-origin guard).
package githubrepo

import "strings"

const canonicalURLPrefix = "https://github.com/"

// ComparisonKey preserves the case-sensitive .git trim before folding the
// admission-constrained ASCII owner and repository segments.
func ComparisonKey(canonicalURL string) string {
	ownerRepo := strings.TrimPrefix(canonicalURL, canonicalURLPrefix)
	ownerRepo = strings.TrimSuffix(ownerRepo, ".git")
	return canonicalURLPrefix + strings.ToLower(ownerRepo)
}
