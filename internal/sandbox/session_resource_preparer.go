package sandbox

import "context"

type SessionResourcePreparer interface {
	PrepareSessionResources(context.Context, SandboxSetup, ProviderHandle) (ResourceSetup, error)
}

// SessionResourceCleanupCoordinator sequences the removal of a deletion-pending
// session resource so a crash-and-retry can never destroy a successor that has
// already taken over the same mount path. A deletion-pending resource
// (delete_requested_at set, detached_at not yet written) is a CLEANUP owner, not
// an active collision owner: it is excluded from the resolved-path collision set,
// so a new active resource MAY claim its exact resolved path. That is safe only
// because detach is per-resource and strictly ordered:
//
//	step  action                                                    durable effect
//	----  --------------------------------------------------------  --------------------------------
//	1     remove: rm -rf <mount_path> in the sandbox (memory,       none yet; row stays deletion-pending
//	      GitHub, and file deletions share this rm -rf shape)
//	2     on the removal ACK: commit detached_at                     row leaves deletion-pending
//	3     successor materializes at that path                        runs only after step 2 commits
//
// A removal failure keeps the row deletion-pending and BLOCKS the successor's
// materialization (steps 2 and 3 never run). A pending owner reserves only its
// EXACT path, not its subtree; ancestor/descendant subtree replacement is out of
// scope. Implemented by sessionPreparationResourceCleanupCoordinator in
// session_prepare.go (CheckSessionPreparationResourceCleanup -> remove ->
// DetachSessionPreparationResource).
//
// UPDATE-WITH: memory_projection.go and github_preparation.go removal call sites.
type SessionResourceCleanupCoordinator interface {
	CleanupSessionResource(context.Context, string, func(context.Context) error) error
}

type SessionResourcePreparationCompensator interface {
	CompensateSessionResourcePreparation(context.Context, SandboxSetup, ProviderHandle) error
}
