package session

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/computer"
	"github.com/tetral-ai/tetral/internal/githubrepo"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// ComputerRequest is one computer entry of Session creation or of the add
// route; Repositories are its github_repository items, which Entry only
// counts.
type ComputerRequest struct {
	Entry        computer.Entry
	Repositories []ResourceRequest
}

// Idempotency is a request's Idempotency-Key scope and its fingerprint
// (computer.Fingerprint); Canonical is false when the body could not be
// fingerprinted.
type Idempotency struct {
	Scope       instance.IdempotencyScope
	Fingerprint [sha256.Size]byte
	Canonical   bool
}

// replay answers a request whose record exists: the recorded result for an
// equal fingerprint, IdempotencyConflictError for any other request.
func (i *Idempotency) replay(ctx context.Context, tx Transaction) (computer.IdempotencyRecord, bool, error) {
	if i == nil {
		return computer.IdempotencyRecord{}, false, nil
	}
	record, found, err := tx.FindComputerIdempotencyRecord(ctx, i.Scope)
	if err != nil || !found {
		return computer.IdempotencyRecord{}, false, err
	}
	if !i.Canonical || record.Fingerprint != i.Fingerprint {
		return computer.IdempotencyRecord{}, true, &instance.IdempotencyConflictError{}
	}
	return record, true, nil
}

// idempotentTx runs fn in one runtime mutation transaction of the Session, or
// one Workspace transaction without a Session, and runs it once more when a
// concurrent identical request committed the same idempotency record first;
// the second run finds that record.
func (s *Service) idempotentTx(ctx context.Context, ws workspace.ID, sessionID string, fn func(Transaction) error) error {
	run := func() error {
		if sessionID == "" {
			return s.store.WithWorkspaceTxAndCleanup(ctx, ws, fn, nil)
		}
		return s.store.WithRuntimeMutationTx(ctx, ws, sessionID, fn)
	}
	err := run()
	if computer.IsIdempotencyRace(err) {
		err = run()
	}
	return err
}

// checkedComputers are computer requests that passed their checks, with
// their repositories prepared, ready to store.
type checkedComputers struct {
	checked      []computer.Checked
	repositories [][]*Resource
}

// checkComputersTx checks requests and their repositories without writing.
func (s *Service) checkComputersTx(ctx context.Context, tx Transaction, sessionID string, requests []ComputerRequest) (checkedComputers, error) {
	entries := make([]computer.Entry, len(requests))
	for i, request := range requests {
		entries[i] = request.Entry
		entries[i].Repositories = len(request.Repositories)
	}
	checked, err := tx.CheckComputerEntries(ctx, sessionID, entries)
	if err != nil {
		return checkedComputers{}, err
	}
	repositories := make([][]*Resource, len(requests))
	for i, request := range requests {
		if repositories[i], err = s.prepareRepositories(checked[i].WorkingDirectory, request.Repositories); err != nil {
			return checkedComputers{}, err
		}
		// Sessions carry no drive bindings yet, so the size counts none.
		if err := computer.CheckPrepareSize(sessionID, checked[i], preparedRepositories(repositories[i]), nil); err != nil {
			return checkedComputers{}, err
		}
	}
	return checkedComputers{checked: checked, repositories: repositories}, nil
}

// preparedRepositories are resolved repository resources as a
// session.prepare names them.
func preparedRepositories(resources []*Resource) []computer.Repository {
	repositories := make([]computer.Repository, 0, len(resources))
	for _, resource := range resources {
		repository := resource.GitHubRepository
		prepared := computer.Repository{URL: repository.URL, Path: repository.MountPath}
		switch repository.CheckoutType {
		case "branch":
			prepared.Checkout = &computer.Checkout{Type: "branch", Name: repository.CheckoutRef}
		case "commit":
			prepared.Checkout = &computer.Checkout{Type: "commit", SHA: repository.CheckoutRef}
		}
		if repository.GitIdentity != nil {
			prepared.GitIdentity = &computer.GitIdentity{Name: repository.GitIdentity.Name, Email: repository.GitIdentity.Email}
		}
		repositories = append(repositories, prepared)
	}
	return repositories
}

// storeComputersTx stores each checked computer and its repositories and
// returns the new computer IDs in request order.
func storeComputersTx(ctx context.Context, tx Transaction, ws workspace.ID, sessionID string, computers checkedComputers, now time.Time) ([]string, error) {
	ids, err := tx.AddComputers(ctx, sessionID, computers.checked)
	if err != nil {
		return nil, err
	}
	for i, computerID := range ids {
		for _, resource := range computers.repositories[i] {
			resource.SessionID, resource.WorkspaceID = sessionID, ws
			resource.CreatedAt, resource.UpdatedAt = now, now
			resource.GitHubRepository.ComputerID = computerID
			if err := tx.CreateResource(ctx, resource); err != nil {
				return nil, err
			}
		}
	}
	return ids, nil
}

// prepareRepositories checks one computer's repository items and resolves
// each clone path: mount_path, or <working_directory>/<repository name>.
func (s *Service) prepareRepositories(workingDirectory string, requests []ResourceRequest) ([]*Resource, error) {
	paths, urls := map[string]bool{}, map[string]bool{}
	prepared := make([]*Resource, 0, len(requests))
	for _, request := range requests {
		if request.Type != string(ResourceTypeGitHubRepository) {
			return nil, &ValidationError{Message: "a computer's repositories are github_repository items"}
		}
		if err := validateResourceRequestTypeClosure(request); err != nil {
			return nil, err
		}
		canonicalURL, name, err := validateGitHubRepositoryURL(request.GitHubURL)
		if err != nil {
			return nil, err
		}
		key := githubrepo.ComparisonKey(canonicalURL)
		if urls[key] {
			return nil, &ValidationError{Message: "duplicate GitHub repository URL"}
		}
		urls[key] = true
		if request.AuthorizationToken == "" {
			return nil, &ValidationError{Message: "authorization_token is required"}
		}
		checkout, err := validateCheckout(request.Checkout)
		if err != nil {
			return nil, err
		}
		gitIdentity, err := validateGitIdentity(request.GitIdentity)
		if err != nil {
			return nil, err
		}
		path := strings.TrimSuffix(workingDirectory, "/") + "/" + name
		var requested *string
		if request.MountPath != nil {
			if !strings.HasPrefix(*request.MountPath, "/") || strings.ContainsRune(*request.MountPath, 0) {
				return nil, &ValidationError{Message: "repository mount_path must be an absolute path without U+0000"}
			}
			mountPath := *request.MountPath
			path, requested = mountPath, &mountPath
		}
		if paths[path] {
			return nil, &ValidationError{Message: fmt.Sprintf("two repositories of one computer share the path %q", path)}
		}
		paths[path] = true
		checkoutType, checkoutRef := "", ""
		if checkout != nil {
			checkoutType, checkoutRef = checkout.Type, checkout.Name
			if checkout.Type == "commit" {
				checkoutRef = checkout.SHA
			}
		}
		if s.encryptor == nil {
			return nil, fmt.Errorf("session resource encryptor is required")
		}
		encryptedToken, err := s.encryptor.Encrypt([]byte(request.AuthorizationToken))
		if err != nil {
			return nil, err
		}
		prepared = append(prepared, &Resource{
			ID:   s.resourceIDStrategy(),
			Type: ResourceTypeGitHubRepository,
			GitHubRepository: &GitHubRepositoryResource{
				URL:                         canonicalURL,
				MountPath:                   path,
				RequestedMountPath:          requested,
				CheckoutType:                checkoutType,
				CheckoutRef:                 checkoutRef,
				GitIdentity:                 gitIdentity,
				AuthorizationTokenEncrypted: encryptedToken,
			},
		})
	}
	return prepared, nil
}

// AddComputer runs POST /v1/sessions/{session_id}/computers: one entry added
// to an idle Session, deduplicated by idempotency. A replay returns the
// recorded computer.
func (s *Service) AddComputer(ctx context.Context, ws workspace.ID, sessionID string, request ComputerRequest, idempotency Idempotency) (*computer.Object, error) {
	var computerID string
	err := s.idempotentTx(ctx, ws, sessionID, func(tx Transaction) error {
		computerID = ""
		record, found, err := idempotency.replay(ctx, tx)
		if found || err != nil {
			computerID = record.ComputerID
			return err
		}
		if !idempotency.Canonical {
			return &ValidationError{Message: "request body must be one JSON object without repeated keys"}
		}
		if _, err := s.lockMutableSession(ctx, tx, sessionID); err != nil {
			return err
		}
		checked, err := s.checkComputersTx(ctx, tx, sessionID, []ComputerRequest{request})
		if err != nil {
			return err
		}
		now := s.clock().UTC()
		ids, err := storeComputersTx(ctx, tx, ws, sessionID, checked, now)
		if err != nil {
			return err
		}
		computerID = ids[0]
		if err := tx.WriteMachineMessage(ctx, sessionID, computerID, computer.ChangeAdded, now); err != nil {
			return err
		}
		return tx.SaveComputerIdempotencyRecord(ctx, idempotency.Scope, computer.IdempotencyRecord{Fingerprint: idempotency.Fingerprint, SessionID: sessionID, ComputerID: computerID})
	})
	if err != nil {
		return nil, err
	}
	return s.GetComputer(ctx, ws, sessionID, computerID)
}

// GetComputer returns the computer object the read route serves.
func (s *Service) GetComputer(ctx context.Context, ws workspace.ID, sessionID, computerID string) (*computer.Object, error) {
	var object computer.Object
	if err := s.store.WithWorkspaceTx(ctx, ws, func(tx Transaction) error {
		if _, err := tx.GetSession(ctx, sessionID); err != nil {
			return err
		}
		var err error
		object, err = tx.GetComputer(ctx, sessionID, computerID)
		return err
	}); err != nil {
		return nil, err
	}
	return &object, nil
}

// createComputers is the computer list of a creation request: its computers,
// or the one cloud computer of the top-level environment_id carrying the
// github_repository items of resources, which it removes from the returned
// resources.
func createComputers(request CreateRequest) ([]ComputerRequest, []ResourceRequest, error) {
	if request.ComputersPresent {
		if request.EnvironmentID != "" {
			return nil, nil, &ValidationError{Message: "computers and the top-level environment_id cannot both be given"}
		}
		if len(request.Resources) > 0 {
			return nil, nil, &ValidationError{Message: "resources cannot be given with computers; list repositories on their computer"}
		}
		return request.Computers, nil, nil
	}
	if request.EnvironmentID == "" {
		return nil, nil, &ValidationError{Message: "computers or environment_id is required"}
	}
	environmentID := request.EnvironmentID
	one := ComputerRequest{Entry: computer.Entry{EnvironmentID: &environmentID}}
	var resources []ResourceRequest
	for _, resource := range request.Resources {
		if resource.Type == string(ResourceTypeGitHubRepository) {
			one.Repositories = append(one.Repositories, resource)
			continue
		}
		resources = append(resources, resource)
	}
	return []ComputerRequest{one}, resources, nil
}

// firstCloudEnvironment is the Environment of the first cloud computer of
// requests, or "" when none is a cloud computer.
func firstCloudEnvironment(requests []ComputerRequest) string {
	for _, request := range requests {
		if request.Entry.EnvironmentID != nil {
			return *request.Entry.EnvironmentID
		}
	}
	return ""
}

// sessionEnvironmentID is the Session object's environment_id: the
// Environment of its first attached cloud computer, or "" without one.
func sessionEnvironmentID(computers []computer.Object) string {
	for _, object := range computers {
		if object.Kind == computer.KindCloud && object.EnvironmentID != nil {
			return *object.EnvironmentID
		}
	}
	return ""
}

// computerObjects is computers as the Session object lists them: never null.
func computerObjects(computers []computer.Object) []computer.Object {
	if computers == nil {
		return []computer.Object{}
	}
	return computers
}

// rejectSandboxMountOverlap refuses a one-computer creation whose repository
// paths overlap its file or memory_store mounts, which Sandbox Service mounts
// in the same sandbox as the repositories.
func rejectSandboxMountOverlap(prepared []preparedResource, repositories []ResourceRequest) error {
	stored := make([]*Resource, len(prepared))
	for i, resource := range prepared {
		stored[i] = resource.stored
	}
	mounts := resourceMounts(stored)
	for _, repository := range repositories {
		_, name, err := validateGitHubRepositoryURL(repository.GitHubURL)
		if err != nil {
			return err
		}
		path := computer.DefaultCloudWorkingDirectory + "/" + name
		if repository.MountPath != nil {
			path = *repository.MountPath
		}
		if err := rejectMountOverlap(mounts, path); err != nil {
			return err
		}
	}
	return nil
}
