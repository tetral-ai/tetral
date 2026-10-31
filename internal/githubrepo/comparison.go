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
//     is the uniqueness gate for the repositories of one computer.
//
// STATE MACHINE (computer admission dedup, internal/session/computers.go):
// one set per computer, keyed by ComparisonKey(canonicalURL).
//
//	no-key      -> key-present   writer: prepareRepositories; the first
//	                             repository of the computer whose URL folds
//	                             to this key claims it.
//	key-present -> rejected      writer: the same loop; a later repository of
//	                             that computer whose URL folds to an
//	                             already-claimed key fails with the
//	                             "duplicate GitHub repository URL" error.
//
// STATE MACHINE (per-request proxy row selection, services/git-proxy/
// credential.go): among the rows of the ticket's computer (a computer ticket)
// or of its Session (a Session ticket), the mounted
// session_github_repository_resources row is chosen by folding gr.url in SQL
// to ComparisonKey(owner/repo) with a LIMIT 2 probe.
//
//	0 rows  -> unmounted                     (policy relays anonymously)
//	1 row   -> resolved to that row's write-only token
//	>1 rows -> errRepositoryIdentityAmbiguous
//
// The admission gate above keeps a computer ticket's selection single-valued:
// the proxy resolves the per-repo token by folded URL only and cannot
// disambiguate two mounts that fold together yet carry independent tokens, so
// one computer never holds two such repositories. Two computers of one
// Session may; a Session ticket then finds both and fails closed.
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
// UPDATE-WITH: internal/session/computers.go (admission dedup set),
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
