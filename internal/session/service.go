// Package session pins each session's tetral_agent_toolset family and enforces it
// across the session lifecycle; this file (service.go) holds that admission logic.
//
// OWNS:
//   - The family lock: a session's tetral_agent_toolset family, fixed at create from
//     the agent version and unchangeable by any later tools update.
//
// STATE MACHINE (the toolset family across one session):
//
//	create   Create resolves the agent version and requires its effective config to
//	         carry exactly one tetral_agent_toolset (agent.TetralToolsetFamily); the
//	         resolved Tools become the session's installed_tools_json, pinning the
//	         family. Zero or duplicate tetral_agent_toolset -> 400.
//	update   resolveRuntimeAgentConfigPatch treats a tools patch as a full replace but
//	         requires the replacement to keep exactly one tetral_agent_toolset of the
//	         pinned family (mismatch -> 400); mcp_toolset entries in the same array
//	         replace freely.
//
// INVARIANTS:
//   - Cardinality (exactly one tetral_agent_toolset; zero or duplicate rejected) is
//     enforced by package internal/agent (TetralToolsetFamily,
//     ValidateAndCanonicalizeConfig); this package adds the cross-update family lock
//     that the agent layer alone does not express.
//   - The pinned family never changes for the life of the session.
//
// UPDATE-WITH:
//   - internal/agent (TetralToolsetFamily, ValidateAndCanonicalizeConfig).
//   - service.go (Create, resolveRuntimeAgentConfigPatch).
package session

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/storage"

	"github.com/tetral-ai/tetral/internal/agent"
	"github.com/tetral-ai/tetral/internal/files"
	"github.com/tetral-ai/tetral/internal/githubrepo"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/workspace"
)

type Service struct {
	agents       AgentReader
	environments EnvironmentReader
	files        FileIdentityService
	memories     MemoryStoreReader
	vaults       VaultReferenceValidator
	store        Store
	encryptor    Encryptor
	clock        func() time.Time

	sessionIDStrategy  func() string
	threadIDStrategy   func() string
	resourceIDStrategy func() string
	fileIDStrategy     func() string
}

type Option func(*Service)

func WithClock(clock func() time.Time) Option {
	return func(s *Service) { s.clock = clock }
}

func NewService(agents AgentReader, environments EnvironmentReader, fileIdentities FileIdentityService, memories MemoryStoreReader, vaults VaultReferenceValidator, store Store, encryptor Encryptor, options ...Option) *Service {
	service := &Service{
		agents:             agents,
		environments:       environments,
		files:              fileIdentities,
		memories:           memories,
		vaults:             vaults,
		store:              store,
		encryptor:          encryptor,
		clock:              func() time.Time { return storage.Now() },
		sessionIDStrategy:  func() string { return id.New("sesn_") },
		threadIDStrategy:   func() string { return id.New("thread_") },
		resourceIDStrategy: func() string { return id.New("sesrsc_") },
		fileIDStrategy:     func() string { return id.New(files.IDPrefix) },
	}
	for _, option := range options {
		option(service)
	}
	return service
}

func (s *Service) Create(ctx context.Context, ws workspace.ID, request CreateRequest) (*Response, error) {
	if ws == "" {
		return nil, &ValidationError{Message: "workspace_id is required"}
	}
	if request.Agent.ID == "" {
		return nil, &ValidationError{Message: "agent is required"}
	}
	if request.EnvironmentID == "" {
		return nil, &ValidationError{Message: "environment_id is required"}
	}
	if err := validateMetadata(request.Metadata); err != nil {
		return nil, err
	}
	resolvedAgent, err := s.resolveAgentForCreate(ctx, ws, request.Agent)
	if err != nil {
		return nil, err
	}
	if _, err := agent.TetralToolsetFamily(resolvedAgent.Tools); err != nil {
		return nil, &ValidationError{Message: "agent must declare exactly one tetral_agent_toolset entry; update the agent"}
	}
	env, err := s.environments.Get(ctx, ws, request.EnvironmentID)
	if err != nil {
		return nil, err
	}
	if env.ArchivedAt != nil {
		return nil, &ValidationError{Message: "environment is archived"}
	}
	if len(request.VaultIDs) > 0 {
		if err := s.vaults.ValidateVaultReferences(ctx, ws, request.VaultIDs); err != nil {
			return nil, err
		}
	}
	createProviderSelector, err := validateProviderSelectorsForAgent(resolvedAgent, request.Providers, request.VaultIDs)
	if err != nil {
		return nil, err
	}
	prepared, err := s.prepareCreateResources(ctx, ws, request.Resources)
	if err != nil {
		return nil, err
	}
	now := s.clock().UTC()
	sessionID := s.sessionIDStrategy()
	threadID := s.threadIDStrategy()
	preparationAttemptID := id.New("prep_")
	sandboxID := id.New("sandbox_")
	stored := &Session{
		ID:               sessionID,
		Type:             "session",
		WorkspaceID:      ws,
		Title:            request.Title,
		Metadata:         cloneMetadata(request.Metadata),
		Status:           StatusIdle,
		ConfigGeneration: 1,
		ApprovalMode:     ApprovalMode(resolvedAgent.ApprovalMode),
		RuntimeAgentConfig: cloneRuntimeAgentConfigPointer(&RuntimeAgentConfig{
			Tools:      cloneRawMessages([]json.RawMessage(resolvedAgent.Tools)),
			MCPServers: cloneRawMessages([]json.RawMessage(resolvedAgent.MCPServers)),
		}),
		AgentID:        resolvedAgent.ID,
		AgentVersionID: resolvedAgent.VersionID,
		AgentVersion:   resolvedAgent.Version,
		EnvironmentID:  request.EnvironmentID,
		VaultIDs:       append([]string(nil), request.VaultIDs...),
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if err := s.store.WithWorkspaceTxAndCleanup(ctx, ws, func(tx Transaction) error {
		if err := tx.CreateSession(ctx, stored); err != nil {
			return err
		}
		if err := tx.CreatePrimaryThread(ctx, &Thread{
			ID:          threadID,
			WorkspaceID: ws,
			SessionID:   sessionID,
			Status:      ThreadStatusIdle,
			CreatedAt:   now,
			UpdatedAt:   now,
		}); err != nil {
			return err
		}
		if createProviderSelector != nil {
			if err := s.upsertSessionProviderAuth(ctx, tx, sessionID, request.VaultIDs, *createProviderSelector, now); err != nil {
				return err
			}
		}
		for _, resource := range prepared {
			resource.stored.SessionID = sessionID
			resource.stored.WorkspaceID = ws
			resource.stored.CreatedAt = now
			resource.stored.UpdatedAt = now
			if resource.fileSourceID != "" {
				metadata, err := s.files.CreateSessionFileIdentity(ctx, filesTx{tx: tx}, ws, files.SessionFileIdentityRequest{
					SourceFileID:  resource.fileSourceID,
					SessionID:     sessionID,
					SessionFileID: resource.stored.File.FileID,
					CreatedAt:     now,
				})
				if err != nil {
					return err
				}
				if resource.stored.File.MountPath == "" {
					resource.stored.File.MountPath = "/mnt/session/uploads/" + metadata.ID
				}
			}
			if err := tx.CreateResource(ctx, resource.stored); err != nil {
				return err
			}
		}
		return tx.CreateSessionPreparation(ctx, SessionPreparationAdmission{
			SessionID:            sessionID,
			EnvironmentID:        request.EnvironmentID,
			PreparationAttemptID: preparationAttemptID,
			SandboxID:            sandboxID,
			CreatedAt:            now,
		})
	}, nil); err != nil {
		return nil, err
	}
	return s.Get(ctx, ws, sessionID)
}

func (s *Service) Get(ctx context.Context, ws workspace.ID, sessionID string) (*Response, error) {
	stored, err := s.store.Get(ctx, ws, sessionID)
	if err != nil {
		return nil, err
	}
	return s.assemble(ctx, ws, stored)
}

func (s *Service) List(ctx context.Context, ws workspace.ID, options ListOptions) (*ListResult, error) {
	result, err := s.store.List(ctx, ws, options)
	if err != nil {
		return nil, err
	}
	responses := make([]*Response, 0, len(result.Data))
	for _, item := range result.Data {
		response, err := s.assemble(ctx, ws, item)
		if err != nil {
			return nil, err
		}
		responses = append(responses, response)
	}
	return &ListResult{Data: responses, NextPage: result.NextPage}, nil
}

func (s *Service) ListThreads(ctx context.Context, ws workspace.ID, sessionID string, options ThreadListOptions) (*ThreadListResult, error) {
	result, err := s.store.ListThreads(ctx, ws, sessionID, options)
	if err != nil {
		return nil, err
	}
	responses, err := s.assembleThreads(ctx, ws, sessionID, result.Data)
	if err != nil {
		return nil, err
	}
	return &ThreadListResult{Data: responses, NextPage: result.NextPage}, nil
}

func (s *Service) GetThread(ctx context.Context, ws workspace.ID, sessionID string, threadID string) (*ThreadResponse, error) {
	thread, err := s.store.GetThread(ctx, ws, sessionID, threadID)
	if err != nil {
		return nil, err
	}
	sess, err := s.store.Get(ctx, ws, sessionID)
	if err != nil {
		return nil, err
	}
	agentSnapshot, err := s.agents.GetVersion(ctx, ws, sess.AgentID, sess.AgentVersion)
	if err != nil {
		return nil, err
	}
	return assembleThreadResponse(thread, agentSnapshot, sess.RuntimeAgentConfig, sess.ApprovalMode)
}

func (s *Service) ArchiveThread(ctx context.Context, ws workspace.ID, sessionID string, threadID string) (*ThreadResponse, error) {
	thread, err := s.store.ArchiveThread(ctx, ws, sessionID, threadID, s.clock().UTC())
	if err != nil {
		return nil, err
	}
	sess, err := s.store.Get(ctx, ws, sessionID)
	if err != nil {
		return nil, err
	}
	agentSnapshot, err := s.agents.GetVersion(ctx, ws, sess.AgentID, sess.AgentVersion)
	if err != nil {
		return nil, err
	}
	return assembleThreadResponse(thread, agentSnapshot, sess.RuntimeAgentConfig, sess.ApprovalMode)
}

func (s *Service) Update(ctx context.Context, ws workspace.ID, sessionID string, request UpdateRequest) (*Response, error) {
	var updated *Session
	if err := s.store.WithWorkspaceTx(ctx, ws, func(tx Transaction) error {
		current, err := s.lockMutableSession(ctx, tx, sessionID)
		if err != nil {
			return err
		}
		metadataBase := current.Metadata
		if request.MetadataPresent && request.MetadataPatch == nil {
			metadataBase = nil
		}
		if err := validateMetadataPatch(metadataBase, request.MetadataPatch); err != nil {
			return err
		}
		if request.ApprovalMode != nil {
			if err := validateSessionApprovalMode(*request.ApprovalMode); err != nil {
				return err
			}
		}
		runtimeAgentConfig, err := s.resolveRuntimeAgentConfigPatch(ctx, ws, current, request)
		if err != nil {
			return err
		}
		if !updateRequestChangesSession(current, request, runtimeAgentConfig) {
			updated = current
			return nil
		}
		updated, err = tx.UpdateSession(ctx, sessionID, UpdateSession{
			Title:              request.Title,
			TitlePresent:       request.TitlePresent,
			MetadataPatch:      request.MetadataPatch,
			MetadataPresent:    request.MetadataPresent,
			ApprovalMode:       request.ApprovalMode,
			RuntimeAgentConfig: runtimeAgentConfig,
			UpdatedAt:          s.clock().UTC(),
		})
		return err
	}); err != nil {
		return nil, err
	}
	return s.assemble(ctx, ws, updated)
}

func updateRequestChangesSession(current *Session, request UpdateRequest, runtimeAgentConfig *RuntimeAgentConfig) bool {
	if runtimeAgentConfig != nil {
		return true
	}
	titlePresent := request.TitlePresent || request.Title != nil
	if titlePresent {
		if request.Title == nil {
			if current.Title != nil {
				return true
			}
		} else if current.Title == nil || *current.Title != *request.Title {
			return true
		}
	}
	if request.ApprovalMode != nil && current.ApprovalMode != *request.ApprovalMode {
		return true
	}
	metadataPresent := request.MetadataPresent || request.MetadataPatch != nil
	if metadataPresent && request.MetadataPatch == nil && len(current.Metadata) > 0 {
		return true
	}
	for key, value := range request.MetadataPatch {
		currentValue, exists := current.Metadata[key]
		if value == nil {
			if exists {
				return true
			}
			continue
		}
		if !exists || currentValue != *value {
			return true
		}
	}
	return false
}

func (s *Service) resolveRuntimeAgentConfigPatch(ctx context.Context, ws workspace.ID, current *Session, request UpdateRequest) (*RuntimeAgentConfig, error) {
	if request.ToolsPatch == nil && request.MCPServersPatch == nil {
		return nil, nil
	}
	agentSnapshot, err := s.agents.GetVersion(ctx, ws, current.AgentID, current.AgentVersion)
	if err != nil {
		return nil, err
	}
	base := effectiveRuntimeAgentConfig(agentSnapshot, current.RuntimeAgentConfig)
	pinnedFamily, err := agent.TetralToolsetFamily(agent.RawArray(base.Tools))
	if err != nil {
		return nil, err
	}
	next := normalizeRuntimeAgentConfig(base)
	if request.ToolsPatch != nil {
		next.Tools = cloneRawMessages([]json.RawMessage(*request.ToolsPatch))
	}
	if request.MCPServersPatch != nil {
		next.MCPServers = cloneRawMessages([]json.RawMessage(*request.MCPServersPatch))
	}
	configForValidation := agentSnapshot.Normalize()
	configForValidation.Tools = cloneRawArray(agent.RawArray(next.Tools))
	configForValidation.MCPServers = cloneRawArray(agent.RawArray(next.MCPServers))
	canonical, err := agent.ValidateAndCanonicalizeConfig(configForValidation)
	if err != nil {
		return nil, err
	}
	requestedFamily, err := agent.TetralToolsetFamily(canonical.Tools)
	if err != nil {
		return nil, err
	}
	if requestedFamily != pinnedFamily {
		familyIndex := 0
		for index, raw := range canonical.Tools {
			var entry struct {
				Type string `json:"type"`
			}
			if json.Unmarshal(raw, &entry) == nil && entry.Type == "tetral_agent_toolset" {
				familyIndex = index
				break
			}
		}
		return nil, &ValidationError{Message: fmt.Sprintf("tools[%d].family must match the session's pinned family", familyIndex)}
	}
	canonicalRuntimeConfig := normalizeRuntimeAgentConfig(RuntimeAgentConfig{
		Tools:      cloneRawMessages([]json.RawMessage(canonical.Tools)),
		MCPServers: cloneRawMessages([]json.RawMessage(canonical.MCPServers)),
	})
	if runtimeAgentConfigEqual(base, canonicalRuntimeConfig) {
		return nil, nil
	}
	return &canonicalRuntimeConfig, nil
}

func runtimeAgentConfigFromAgentSnapshot(snapshot *agent.Agent) RuntimeAgentConfig {
	if snapshot == nil {
		return RuntimeAgentConfig{}
	}
	return normalizeRuntimeAgentConfig(RuntimeAgentConfig{
		Tools:      cloneRawMessages([]json.RawMessage(snapshot.Tools)),
		MCPServers: cloneRawMessages([]json.RawMessage(snapshot.MCPServers)),
	})
}

func effectiveRuntimeAgentConfig(snapshot *agent.Agent, override *RuntimeAgentConfig) RuntimeAgentConfig {
	if override != nil {
		return normalizeRuntimeAgentConfig(*override)
	}
	return runtimeAgentConfigFromAgentSnapshot(snapshot)
}

func overlayAgentRuntimeConfig(snapshot *agent.Agent, override *RuntimeAgentConfig) *agent.Agent {
	if snapshot == nil {
		return nil
	}
	overlaid := *snapshot
	overlaid.AgentConfig = snapshot.Normalize()
	config := effectiveRuntimeAgentConfig(snapshot, override)
	overlaid.Tools = cloneRawArray(agent.RawArray(config.Tools))
	overlaid.MCPServers = cloneRawArray(agent.RawArray(config.MCPServers))
	overlaid.Skills = cloneRawArray(snapshot.Skills)
	if snapshot.Metadata != nil {
		overlaid.Metadata = agent.StringMap{}
		for key, value := range snapshot.Metadata {
			overlaid.Metadata[key] = value
		}
	}
	return &overlaid
}

func validateProviderSelectorsForAgent(agentSnapshot *agent.Agent, providers ProviderSelectors, vaultIDs []string) (*ProviderCredentialSelectorForAdmission, error) {
	if len(providers) == 0 {
		return nil, nil
	}
	if len(providers) != 1 {
		return nil, &ValidationError{Message: "providers must contain exactly one provider"}
	}
	if len(vaultIDs) == 0 {
		return nil, &ValidationError{Message: "providers requires at least one vault_id"}
	}
	providerID, err := providerIDFromAgentModel(agentSnapshot)
	if err != nil {
		return nil, err
	}
	for selectorProviderID, selector := range providers {
		if selectorProviderID == "" {
			return nil, &ValidationError{Message: "providers provider_id is required"}
		}
		if selectorProviderID != providerID {
			return nil, &ValidationError{Message: "providers provider_id must match the agent model provider"}
		}
		if selector.CredentialID == "" {
			return nil, &ValidationError{Message: "providers credential_id is required"}
		}
		return &ProviderCredentialSelectorForAdmission{
			ProviderID:   selectorProviderID,
			CredentialID: selector.CredentialID,
		}, nil
	}
	return nil, nil
}

type ProviderCredentialSelectorForAdmission struct {
	ProviderID   string
	CredentialID string
}

func providerIDFromAgentModel(agentSnapshot *agent.Agent) (string, error) {
	if agentSnapshot == nil {
		return "", &ValidationError{Message: "agent is required"}
	}
	providerID, _, ok := strings.Cut(agentSnapshot.Model, "/")
	if !ok || providerID == "" {
		return "", &ValidationError{Message: "agent model must include a provider"}
	}
	return providerID, nil
}

func (s *Service) upsertSessionProviderAuth(ctx context.Context, tx Transaction, sessionID string, boundVaultIDs []string, selector ProviderCredentialSelectorForAdmission, now time.Time) error {
	credential, err := tx.GetProviderCredentialForAdmission(ctx, selector.CredentialID, boundVaultIDs)
	if err != nil {
		return err
	}
	if credential.Archived {
		return providerCredentialInaccessibleError()
	}
	if credential.Revoked {
		return providerCredentialInaccessibleError()
	}
	if credential.AuthType != "provider_api_key" && credential.AuthType != "provider_oauth" {
		return providerCredentialInaccessibleError()
	}
	if credential.ProviderID != selector.ProviderID {
		return providerCredentialInaccessibleError()
	}
	if !stringSliceContains(boundVaultIDs, credential.VaultID) {
		return providerCredentialInaccessibleError()
	}
	return tx.UpsertSessionProviderAuth(ctx, SessionProviderAuthAdmission{
		SessionID:    sessionID,
		ProviderID:   selector.ProviderID,
		VaultID:      credential.VaultID,
		CredentialID: credential.ID,
		AccessMode:   credential.AccessMode,
		UpdatedAt:    now,
	})
}

func providerCredentialInaccessibleError() *PermissionError {
	return &PermissionError{Message: "provider credential is inaccessible"}
}

func stringSliceContains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func (s *Service) Archive(ctx context.Context, ws workspace.ID, sessionID string) (*Response, error) {
	var archived *Session
	if err := s.store.WithWorkspaceTx(ctx, ws, func(tx Transaction) error {
		var err error
		archived, err = tx.ArchiveSession(ctx, sessionID, s.clock().UTC())
		return err
	}); err != nil {
		return nil, err
	}
	return s.assemble(ctx, ws, archived)
}

func (s *Service) Delete(ctx context.Context, ws workspace.ID, sessionID string) (*DeleteResponse, error) {
	if err := s.store.WithWorkspaceTx(ctx, ws, func(tx Transaction) error {
		stored, err := tx.LockSessionForDelete(ctx, sessionID)
		if err != nil {
			return err
		}
		for _, resource := range stored.Resources {
			if resource.File != nil {
				if err := s.files.TombstoneSessionFileIdentity(ctx, filesTx{tx: tx}, ws, sessionID, resource.File.FileID); err != nil {
					return err
				}
			}
		}
		return tx.DeleteSession(ctx, sessionID)
	}); err != nil {
		return nil, err
	}
	return &DeleteResponse{ID: sessionID, Type: "session_deleted"}, nil
}

func (s *Service) AddResource(ctx context.Context, ws workspace.ID, sessionID string, request ResourceRequest) (*ResourceResponse, error) {
	if err := validateResourceRequestTypeClosure(request); err != nil {
		return nil, err
	}
	if request.Type != string(ResourceTypeFile) {
		return nil, &ValidationError{Message: "only file resources can be added after session creation"}
	}
	if request.FileID == "" {
		return nil, &ValidationError{Message: "file_id is required"}
	}
	normalizedMountPath := ""
	if request.MountPath != nil {
		var err error
		normalizedMountPath, err = normalizeFileMountPath(*request.MountPath)
		if err != nil {
			return nil, err
		}
	}
	var response *ResourceResponse
	err := s.store.WithRuntimeMutationLock(ctx, ws, sessionID, func() error {
		var err error
		response, err = s.addResourceLocked(ctx, ws, sessionID, request, normalizedMountPath)
		return err
	})
	return response, err
}

func (s *Service) addResourceLocked(ctx context.Context, ws workspace.ID, sessionID string, request ResourceRequest, normalizedMountPath string) (*ResourceResponse, error) {
	resourceID := s.resourceIDStrategy()
	sessionFileID := s.fileIDStrategy()
	mountPath := "/mnt/session/uploads/" + sessionFileID
	if request.MountPath != nil {
		mountPath = normalizedMountPath
	}
	now := s.clock().UTC()
	resource := &Resource{
		ID:          resourceID,
		SessionID:   sessionID,
		WorkspaceID: ws,
		Type:        ResourceTypeFile,
		CreatedAt:   now,
		UpdatedAt:   now,
		File: &FileResource{
			SourceFileID: request.FileID,
			FileID:       sessionFileID,
			MountPath:    mountPath,
		},
	}
	if err := s.store.WithWorkspaceTx(ctx, ws, func(tx Transaction) error {
		storedSession, err := s.lockMutableSession(ctx, tx, sessionID)
		if err != nil {
			return err
		}
		if activeFileResourceCount(storedSession.Resources) >= maxFileResources {
			return &ValidationError{Message: "too many file resources"}
		}
		if err := rejectMountOverlap(resourceMounts(storedSession.Resources), mountPath); err != nil {
			return err
		}
		if _, err := s.files.CreateSessionFileIdentity(ctx, filesTx{tx: tx}, ws, files.SessionFileIdentityRequest{
			SourceFileID:  request.FileID,
			SessionID:     sessionID,
			SessionFileID: sessionFileID,
			CreatedAt:     now,
		}); err != nil {
			return err
		}
		if err := tx.CreateResource(ctx, resource); err != nil {
			return err
		}
		return tx.PrepareSessionResourceMutation(ctx, sessionID, now)
	}); err != nil {
		return nil, err
	}
	return resourceResponse(resource), nil
}

func (s *Service) ListResources(ctx context.Context, ws workspace.ID, sessionID string, options ResourceListOptions) (*ResourceListResult, error) {
	result, err := s.store.ListResources(ctx, ws, sessionID, options)
	if err != nil {
		return nil, err
	}
	responses := make([]*ResourceResponse, 0, len(result.Data))
	for _, resource := range result.Data {
		responses = append(responses, resourceResponse(resource))
	}
	return &ResourceListResult{Data: responses, NextPage: result.NextPage}, nil
}

func (s *Service) GetResource(ctx context.Context, ws workspace.ID, sessionID string, resourceID string) (*ResourceResponse, error) {
	var resource *Resource
	if err := s.store.WithWorkspaceTx(ctx, ws, func(tx Transaction) error {
		if _, err := tx.GetSession(ctx, sessionID); err != nil {
			return err
		}
		var err error
		resource, err = tx.GetResource(ctx, sessionID, resourceID)
		return err
	}); err != nil {
		return nil, err
	}
	return resourceResponse(resource), nil
}

func (s *Service) DeleteResource(ctx context.Context, ws workspace.ID, sessionID string, resourceID string) (*ResourceDeleteResponse, error) {
	var response *ResourceDeleteResponse
	err := s.store.WithRuntimeMutationLock(ctx, ws, sessionID, func() error {
		var err error
		response, err = s.deleteResourceLocked(ctx, ws, sessionID, resourceID)
		return err
	})
	return response, err
}

func (s *Service) deleteResourceLocked(ctx context.Context, ws workspace.ID, sessionID string, resourceID string) (*ResourceDeleteResponse, error) {
	now := s.clock().UTC()
	if err := s.store.WithWorkspaceTx(ctx, ws, func(tx Transaction) error {
		if _, err := s.lockMutableSession(ctx, tx, sessionID); err != nil {
			return err
		}
		deleted, err := tx.RequestResourceDelete(ctx, sessionID, resourceID, now)
		if err != nil {
			return err
		}
		if deleted.DetachedAt != nil {
			if deleted.File != nil {
				if err := s.files.TombstoneSessionFileIdentity(ctx, filesTx{tx: tx}, ws, sessionID, deleted.File.FileID); err != nil {
					return err
				}
			}
			return nil
		}
		return tx.PrepareSessionResourceMutation(ctx, sessionID, now)
	}); err != nil {
		return nil, err
	}
	return &ResourceDeleteResponse{ID: resourceID, Type: "session_resource_deleted"}, nil
}

func (s *Service) lockMutableSession(ctx context.Context, tx Transaction, sessionID string) (*Session, error) {
	if err := tx.RequireSessionUsableForMutation(ctx, sessionID); err != nil {
		return nil, err
	}
	storedSession, err := tx.LockSession(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	if storedSession.ArchivedAt != nil {
		return nil, &ConflictError{Message: "session is archived", InvalidRequest: true}
	}
	return storedSession, nil
}

func (s *Service) resolveAgentForCreate(ctx context.Context, ws workspace.ID, ref AgentReference) (*agent.Agent, error) {
	var resolved *agent.Agent
	var err error
	if ref.Version == nil {
		resolved, err = s.agents.Get(ctx, ws, ref.ID)
	} else if *ref.Version <= 0 {
		return nil, &ValidationError{Message: "agent version is invalid"}
	} else {
		resolved, err = s.agents.GetVersion(ctx, ws, ref.ID, *ref.Version)
	}
	if err != nil {
		return nil, err
	}
	if resolved.ArchivedAt != nil {
		return nil, &ValidationError{Message: "agent is archived"}
	}
	return resolved, nil
}

type preparedResource struct {
	stored       *Resource
	fileSourceID string
}

func (s *Service) prepareCreateResources(ctx context.Context, ws workspace.ID, requests []ResourceRequest) ([]preparedResource, error) {
	fileCount := 0
	memoryCount := 0
	mounts := []string{}
	gitHubRepositoryURLs := map[string]struct{}{}
	prepared := make([]preparedResource, 0, len(requests))
	for _, request := range requests {
		if err := validateResourceRequestTypeClosure(request); err != nil {
			return nil, err
		}
		switch request.Type {
		case string(ResourceTypeFile):
			fileCount++
			if fileCount > maxFileResources {
				return nil, &ValidationError{Message: "too many file resources"}
			}
			if request.FileID == "" {
				return nil, &ValidationError{Message: "file_id is required"}
			}
			resourceID := s.resourceIDStrategy()
			sessionFileID := s.fileIDStrategy()
			mount := "/mnt/session/uploads/" + sessionFileID
			if request.MountPath != nil {
				var err error
				mount, err = normalizeFileMountPath(*request.MountPath)
				if err != nil {
					return nil, err
				}
			}
			if err := rejectMountOverlap(mounts, mount); err != nil {
				return nil, err
			}
			if err := s.files.ValidateSessionFileSource(ctx, ws, request.FileID); err != nil {
				return nil, err
			}
			mounts = append(mounts, mount)
			prepared = append(prepared, preparedResource{
				fileSourceID: request.FileID,
				stored: &Resource{
					ID:   resourceID,
					Type: ResourceTypeFile,
					File: &FileResource{SourceFileID: request.FileID, FileID: sessionFileID, MountPath: mount},
				},
			})
		case string(ResourceTypeMemoryStore):
			memoryCount++
			if memoryCount > maxMemoryResources {
				return nil, &ValidationError{Message: "too many memory_store resources"}
			}
			if request.MemoryStoreID == "" {
				return nil, &ValidationError{Message: "memory_store_id is required"}
			}
			store, err := s.memories.GetStore(ctx, ws, request.MemoryStoreID)
			if err != nil {
				return nil, err
			}
			if store.ArchivedAt != nil {
				return nil, &ValidationError{Message: "memory_store is archived"}
			}
			if len([]rune(request.Instructions)) > maxMemoryInstructions {
				return nil, &ValidationError{Message: "memory_store instructions are too long"}
			}
			access := request.Access
			if access == "" {
				access = "read_only"
			}
			if access != "read_write" && access != "read_only" {
				return nil, &ValidationError{Message: "memory_store access is invalid"}
			}
			slug := memorySlug(store.Name, request.MemoryStoreID)
			mount := "/mnt/memory/" + slug
			if err := rejectMountOverlap(mounts, mount); err != nil {
				mount = "/mnt/memory/" + slug + "-" + request.MemoryStoreID
				if err := rejectMountOverlap(mounts, mount); err != nil {
					return nil, err
				}
			}
			mounts = append(mounts, mount)
			prepared = append(prepared, preparedResource{stored: &Resource{
				ID:   s.resourceIDStrategy(),
				Type: ResourceTypeMemoryStore,
				MemoryStore: &MemoryStoreResource{
					MemoryStoreID: request.MemoryStoreID,
					Access:        access,
					Instructions:  request.Instructions,
					Name:          store.Name,
					Description:   store.Description,
					MountPath:     mount,
				},
			}})
		case string(ResourceTypeGitHubRepository):
			if request.AuthorizationToken == "" {
				return nil, &ValidationError{Message: "authorization_token is required"}
			}
			canonicalURL, repo, err := validateGitHubRepositoryURL(request.GitHubURL)
			if err != nil {
				return nil, err
			}
			comparisonKey := githubrepo.ComparisonKey(canonicalURL)
			if _, exists := gitHubRepositoryURLs[comparisonKey]; exists {
				return nil, &ValidationError{Message: "duplicate GitHub repository URL"}
			}
			gitHubRepositoryURLs[comparisonKey] = struct{}{}
			mount := "/workspace/" + repo
			if request.MountPath != nil {
				mount, err = normalizeGitHubMountPath(*request.MountPath)
				if err != nil {
					return nil, err
				}
			}
			if err := rejectMountOverlap(mounts, mount); err != nil {
				return nil, err
			}
			checkout, err := validateCheckout(request.Checkout)
			if err != nil {
				return nil, err
			}
			checkoutType := ""
			checkoutRef := ""
			if checkout != nil {
				checkoutType = checkout.Type
				switch checkout.Type {
				case "branch":
					checkoutRef = checkout.Name
				case "commit":
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
			mounts = append(mounts, mount)
			prepared = append(prepared, preparedResource{stored: &Resource{
				ID:   s.resourceIDStrategy(),
				Type: ResourceTypeGitHubRepository,
				GitHubRepository: &GitHubRepositoryResource{
					URL:                         canonicalURL,
					MountPath:                   mount,
					CheckoutType:                checkoutType,
					CheckoutRef:                 checkoutRef,
					AuthorizationTokenEncrypted: encryptedToken,
				},
			}})
		case "skills":
			return nil, &ValidationError{Message: "skills is not a session resource"}
		default:
			return nil, &ValidationError{Message: "invalid session resource type"}
		}
	}
	return prepared, nil
}

func (s *Service) UpdateResource(ctx context.Context, ws workspace.ID, sessionID string, resourceID string, token string) (*ResourceResponse, error) {
	if token == "" {
		return nil, &ValidationError{Message: "authorization_token is required"}
	}
	var resource *Resource
	if err := s.store.WithWorkspaceTx(ctx, ws, func(tx Transaction) error {
		storedSession, err := tx.LockSession(ctx, sessionID)
		if err != nil {
			return err
		}
		if storedSession.LifecycleState == LifecycleStateDeleted {
			return &NotFoundError{Message: "session not found"}
		}
		if storedSession.ArchivedAt != nil ||
			storedSession.LifecycleState == LifecycleStateArchiving ||
			storedSession.LifecycleState == LifecycleStateArchived {
			return &ConflictError{Message: "session is archived", InvalidRequest: true}
		}
		current, err := tx.GetResource(ctx, sessionID, resourceID)
		if err != nil {
			return err
		}
		if current.Type != ResourceTypeGitHubRepository {
			return &ValidationError{Message: "only github_repository authorization_token can be updated"}
		}
		if s.encryptor == nil {
			return fmt.Errorf("session resource encryptor is required")
		}
		encrypted, err := s.encryptor.Encrypt([]byte(token))
		if err != nil {
			return err
		}
		resource, err = tx.UpdateGitHubRepositoryToken(ctx, sessionID, resourceID, encrypted, s.clock().UTC())
		return err
	}); err != nil {
		return nil, err
	}
	return resourceResponse(resource), nil
}

func (s *Service) assemble(ctx context.Context, ws workspace.ID, stored *Session) (*Response, error) {
	resolvedAgent, err := s.agents.GetVersion(ctx, ws, stored.AgentID, stored.AgentVersion)
	if err != nil {
		return nil, err
	}
	resolvedAgent = overlayAgentRuntimeConfig(resolvedAgent, stored.RuntimeAgentConfig)
	resolvedAgent.ApprovalMode = agent.ApprovalMode(stored.ApprovalMode)
	providers, err := s.store.ListSessionProviderAuth(ctx, ws, stored.ID)
	if err != nil {
		return nil, err
	}
	resources := make([]*ResourceResponse, 0, len(stored.Resources))
	for _, resource := range stored.Resources {
		resources = append(resources, resourceResponse(resource))
	}
	return &Response{
		ID:                 stored.ID,
		Type:               stored.Type,
		Title:              stored.Title,
		Status:             stored.Status,
		ArchivedAt:         stored.ArchivedAt,
		Agent:              sessionAgentResponse(resolvedAgent),
		EnvironmentID:      stored.EnvironmentID,
		VaultIDs:           cloneVaultIDs(stored.VaultIDs),
		Providers:          providers,
		Metadata:           cloneMetadata(stored.Metadata),
		Resources:          resources,
		OutcomeEvaluations: agent.RawArray{},
		Usage:              stored.Usage,
		Stats:              sessionStats(stored, s.clock().UTC()),
		CreatedAt:          stored.CreatedAt,
		UpdatedAt:          stored.UpdatedAt,
	}, nil
}

func sessionStats(stored *Session, now time.Time) SessionStats {
	activeSeconds := stored.ActiveSecondsTotal
	if stored.RunningSince != nil && stored.Status == StatusRunning {
		activeSeconds += nonNegativeSeconds(now.Sub(*stored.RunningSince))
	}
	durationEnd := now
	if stored.Status == StatusTerminated && stored.TerminatedAt != nil {
		durationEnd = *stored.TerminatedAt
	}
	return SessionStats{
		ActiveSeconds:   activeSeconds,
		DurationSeconds: nonNegativeSeconds(durationEnd.Sub(stored.CreatedAt)),
	}
}

func nonNegativeSeconds(duration time.Duration) float64 {
	if duration < 0 {
		return 0
	}
	return duration.Seconds()
}

func sessionAgentResponse(snapshot *agent.Agent) *SessionAgentResponse {
	if snapshot == nil {
		return nil
	}
	return &SessionAgentResponse{
		ID:           snapshot.ID,
		Type:         snapshot.Type,
		Version:      snapshot.Version,
		Name:         snapshot.Name,
		Model:        agent.ModelConfig{ID: snapshot.Model},
		ApprovalMode: snapshot.ApprovalMode,
		System:       cloneStringPointer(snapshot.System),
		Description:  cloneStringPointer(snapshot.Description),
		Tools:        cloneRawArray(snapshot.Tools),
		MCPServers:   cloneRawArray(snapshot.MCPServers),
		Skills:       cloneRawArray(snapshot.Skills),
		Multiagent:   nil,
	}
}

func (s *Service) assembleThreads(ctx context.Context, ws workspace.ID, sessionID string, threads []*Thread) ([]*ThreadResponse, error) {
	if len(threads) == 0 {
		return []*ThreadResponse{}, nil
	}
	sess, err := s.store.Get(ctx, ws, sessionID)
	if err != nil {
		return nil, err
	}
	agentSnapshot, err := s.agents.GetVersion(ctx, ws, sess.AgentID, sess.AgentVersion)
	if err != nil {
		return nil, err
	}
	responses := make([]*ThreadResponse, 0, len(threads))
	for _, thread := range threads {
		response, err := assembleThreadResponse(thread, agentSnapshot, sess.RuntimeAgentConfig, sess.ApprovalMode)
		if err != nil {
			return nil, err
		}
		responses = append(responses, response)
	}
	return responses, nil
}

func assembleThreadResponse(thread *Thread, agentSnapshot *agent.Agent, runtimeAgentConfig *RuntimeAgentConfig, approvalMode ApprovalMode) (*ThreadResponse, error) {
	status, err := publicThreadStatus(thread.Status)
	if err != nil {
		return nil, err
	}
	return &ThreadResponse{
		ID:             thread.ID,
		Type:           "session_thread",
		SessionID:      thread.SessionID,
		ParentThreadID: cloneStringPointer(thread.ParentThreadID),
		Agent:          threadAgentResponse(agentSnapshot, runtimeAgentConfig, approvalMode),
		Status:         status,
		Usage: ThreadUsage{
			InputTokens:          thread.Usage.InputTokens,
			OutputTokens:         thread.Usage.OutputTokens,
			CacheCreation:        thread.Usage.CacheCreation,
			CacheReadInputTokens: thread.Usage.CacheReadInputTokens,
		},
		ArchivedAt: cloneTimePointer(thread.ArchivedAt),
		CreatedAt:  thread.CreatedAt,
		UpdatedAt:  thread.UpdatedAt,
	}, nil
}

func threadAgentResponse(agentSnapshot *agent.Agent, runtimeAgentConfig *RuntimeAgentConfig, approvalMode ApprovalMode) ThreadAgentResponse {
	model := ThreadAgentModelConfig{ID: agentSnapshot.Model}
	effectiveConfig := effectiveRuntimeAgentConfig(agentSnapshot, runtimeAgentConfig)
	return ThreadAgentResponse{
		ID:           agentSnapshot.ID,
		Type:         "agent",
		Version:      agentSnapshot.Version,
		Name:         agentSnapshot.Name,
		Model:        model,
		ApprovalMode: agent.ApprovalMode(approvalMode),
		System:       cloneStringPointer(agentSnapshot.System),
		Description:  cloneStringPointer(agentSnapshot.Description),
		Tools:        cloneRawArray(agent.RawArray(effectiveConfig.Tools)),
		MCPServers:   cloneRawArray(agent.RawArray(effectiveConfig.MCPServers)),
		Skills:       cloneRawArray(agentSnapshot.Skills),
	}
}

func cloneRawArray(values agent.RawArray) agent.RawArray {
	cloned := make(agent.RawArray, len(values))
	copy(cloned, values)
	return cloned
}

func cloneStringPointer(value *string) *string {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneTimePointer(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneVaultIDs(vaultIDs []string) []string {
	cloned := make([]string, len(vaultIDs))
	copy(cloned, vaultIDs)
	return cloned
}

func resourceResponse(resource *Resource) *ResourceResponse {
	response := &ResourceResponse{
		ID:        resource.ID,
		Type:      string(resource.Type),
		CreatedAt: resource.CreatedAt,
		UpdatedAt: resource.UpdatedAt,
	}
	switch resource.Type {
	case ResourceTypeFile:
		response.FileID = resource.File.FileID
		response.MountPath = resource.File.MountPath
	case ResourceTypeMemoryStore:
		response.MemoryStoreID = resource.MemoryStore.MemoryStoreID
		response.Access = resource.MemoryStore.Access
		response.Instructions = resource.MemoryStore.Instructions
		response.Name = resource.MemoryStore.Name
		response.Description = resource.MemoryStore.Description
		response.MountPath = resource.MemoryStore.MountPath
	case ResourceTypeGitHubRepository:
		response.URL = resource.GitHubRepository.URL
		response.MountPath = resource.GitHubRepository.MountPath
		if resource.GitHubRepository.CheckoutType != "" {
			response.CheckoutType = resource.GitHubRepository.CheckoutType
			response.CheckoutRef = resource.GitHubRepository.CheckoutRef
		}
	}
	return response
}

func cloneMetadata(metadata map[string]string) map[string]string {
	out := map[string]string{}
	for key, value := range metadata {
		out[key] = value
	}
	return out
}

func activeFileResourceCount(resources []*Resource) int {
	count := 0
	for _, resource := range resources {
		if resource.Type == ResourceTypeFile && resource.DetachedAt == nil && resource.DeleteRequestedAt == nil {
			count++
		}
	}
	return count
}

func resourceMounts(resources []*Resource) []string {
	mounts := []string{}
	for _, resource := range resources {
		if resource.DetachedAt != nil || resource.DeleteRequestedAt != nil {
			continue
		}
		switch resource.Type {
		case ResourceTypeFile:
			mounts = append(mounts, resource.File.MountPath)
		case ResourceTypeMemoryStore:
			mounts = append(mounts, resource.MemoryStore.MountPath)
		case ResourceTypeGitHubRepository:
			mounts = append(mounts, resource.GitHubRepository.MountPath)
		}
	}
	return mounts
}

type filesTx struct {
	tx Transaction
}

func (t filesTx) ExecContext(ctx context.Context, query string, args ...any) (interface {
	RowsAffected() (int64, error)
}, error) {
	return t.tx.Exec(ctx, query, args...)
}

func (t filesTx) Query(ctx context.Context, query string, args ...any) (files.Rows, error) {
	return t.tx.QueryRows(ctx, query, args...)
}

func (t filesTx) QueryRow(ctx context.Context, query string, args ...any) files.Row {
	return t.tx.QueryRowScanner(ctx, query, args...)
}
