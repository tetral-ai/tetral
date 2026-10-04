package auth

import "slices"

// Operation is a stable semantic action. Route aliases share an operation;
// reshaping a URL does not change the meaning of a durable key's ceiling.
type Operation string

const (
	WorkspaceFullAccess                            = "workspace_full_access"
	PolicyVersion                        int64     = 1
	OperationAgentVersionsList           Operation = "agent_versions.list"
	OperationAgentsArchive               Operation = "agents.archive"
	OperationAgentsCreate                Operation = "agents.create"
	OperationAgentsList                  Operation = "agents.list"
	OperationAgentsRead                  Operation = "agents.read"
	OperationAgentsUpdate                Operation = "agents.update"
	OperationAPIKeysCreate               Operation = "api_keys.create"
	OperationAPIKeysDelete               Operation = "api_keys.delete"
	OperationAPIKeysList                 Operation = "api_keys.list" //nolint:gosec // G101: registered operation name, not a credential.
	OperationCredentialsArchive          Operation = "credentials.archive"
	OperationCredentialsCreate           Operation = "credentials.create"
	OperationCredentialsDelete           Operation = "credentials.delete"
	OperationCredentialsList             Operation = "credentials.list"
	OperationCredentialsRead             Operation = "credentials.read"
	OperationCredentialsUpdate           Operation = "credentials.update"
	OperationCredentialsValidateMcpOauth Operation = "credentials.validate_mcp_oauth"
	OperationEnvironmentsArchive         Operation = "environments.archive"
	OperationEnvironmentsCreate          Operation = "environments.create"
	OperationEnvironmentsDelete          Operation = "environments.delete"
	OperationEnvironmentsList            Operation = "environments.list"
	OperationEnvironmentsRead            Operation = "environments.read"
	OperationEnvironmentsUpdate          Operation = "environments.update"
	OperationFilesContentRead            Operation = "files.content.read"
	OperationFilesCreate                 Operation = "files.create"
	OperationFilesDelete                 Operation = "files.delete"
	OperationFilesList                   Operation = "files.list"
	OperationFilesRead                   Operation = "files.read"
	OperationMemoriesCreate              Operation = "memories.create"
	OperationMemoriesDelete              Operation = "memories.delete"
	OperationMemoriesList                Operation = "memories.list"
	OperationMemoriesRead                Operation = "memories.read"
	OperationMemoriesUpdate              Operation = "memories.update"
	OperationMemoryStoresArchive         Operation = "memory_stores.archive"
	OperationMemoryStoresCreate          Operation = "memory_stores.create"
	OperationMemoryStoresDelete          Operation = "memory_stores.delete"
	OperationMemoryStoresList            Operation = "memory_stores.list"
	OperationMemoryStoresRead            Operation = "memory_stores.read"
	OperationMemoryStoresUpdate          Operation = "memory_stores.update"
	OperationMemoryVersionsList          Operation = "memory_versions.list"
	OperationMemoryVersionsRead          Operation = "memory_versions.read"
	OperationMemoryVersionsRedact        Operation = "memory_versions.redact"
	OperationModelsList                  Operation = "models.list"
	OperationModelsRead                  Operation = "models.read"
	OperationSessionEventsCreate         Operation = "session_events.create"
	OperationSessionEventsList           Operation = "session_events.list"
	OperationSessionEventsStream         Operation = "session_events.stream"
	OperationSessionResourcesCreate      Operation = "session_resources.create"
	OperationSessionResourcesDelete      Operation = "session_resources.delete"
	OperationSessionResourcesList        Operation = "session_resources.list"
	OperationSessionResourcesRead        Operation = "session_resources.read"
	OperationSessionResourcesUpdate      Operation = "session_resources.update"
	OperationSessionThreadsArchive       Operation = "session_threads.archive"
	OperationSessionThreadsList          Operation = "session_threads.list"
	OperationSessionThreadsRead          Operation = "session_threads.read"
	OperationSessionThreadsStream        Operation = "session_threads.stream"
	OperationSessionsArchive             Operation = "sessions.archive"
	OperationSessionsCreate              Operation = "sessions.create"
	OperationSessionsDelete              Operation = "sessions.delete"
	OperationSessionsList                Operation = "sessions.list"
	OperationSessionsRead                Operation = "sessions.read"
	OperationSessionsUpdate              Operation = "sessions.update"
	OperationSkillVersionsContentRead    Operation = "skill_versions.content.read"
	OperationSkillVersionsCreate         Operation = "skill_versions.create"
	OperationSkillVersionsDelete         Operation = "skill_versions.delete"
	OperationSkillVersionsList           Operation = "skill_versions.list"
	OperationSkillVersionsRead           Operation = "skill_versions.read"
	OperationSkillsCreate                Operation = "skills.create"
	OperationSkillsDelete                Operation = "skills.delete"
	OperationSkillsList                  Operation = "skills.list"
	OperationSkillsRead                  Operation = "skills.read"
	OperationThreadEventsList            Operation = "thread_events.list"
	OperationVaultsArchive               Operation = "vaults.archive"
	OperationVaultsCreate                Operation = "vaults.create"
	OperationVaultsDelete                Operation = "vaults.delete"
	OperationVaultsList                  Operation = "vaults.list"
	OperationVaultsRead                  Operation = "vaults.read"
	OperationVaultsUpdate                Operation = "vaults.update"
)

var registeredOperations = []Operation{
	OperationAgentVersionsList,
	OperationAgentsArchive,
	OperationAgentsCreate,
	OperationAgentsList,
	OperationAgentsRead,
	OperationAgentsUpdate,
	OperationAPIKeysCreate,
	OperationAPIKeysDelete,
	OperationAPIKeysList,
	OperationCredentialsArchive,
	OperationCredentialsCreate,
	OperationCredentialsDelete,
	OperationCredentialsList,
	OperationCredentialsRead,
	OperationCredentialsUpdate,
	OperationCredentialsValidateMcpOauth,
	OperationEnvironmentsArchive,
	OperationEnvironmentsCreate,
	OperationEnvironmentsDelete,
	OperationEnvironmentsList,
	OperationEnvironmentsRead,
	OperationEnvironmentsUpdate,
	OperationFilesContentRead,
	OperationFilesCreate,
	OperationFilesDelete,
	OperationFilesList,
	OperationFilesRead,
	OperationMemoriesCreate,
	OperationMemoriesDelete,
	OperationMemoriesList,
	OperationMemoriesRead,
	OperationMemoriesUpdate,
	OperationMemoryStoresArchive,
	OperationMemoryStoresCreate,
	OperationMemoryStoresDelete,
	OperationMemoryStoresList,
	OperationMemoryStoresRead,
	OperationMemoryStoresUpdate,
	OperationMemoryVersionsList,
	OperationMemoryVersionsRead,
	OperationMemoryVersionsRedact,
	OperationModelsList,
	OperationModelsRead,
	OperationSessionEventsCreate,
	OperationSessionEventsList,
	OperationSessionEventsStream,
	OperationSessionResourcesCreate,
	OperationSessionResourcesDelete,
	OperationSessionResourcesList,
	OperationSessionResourcesRead,
	OperationSessionResourcesUpdate,
	OperationSessionThreadsArchive,
	OperationSessionThreadsList,
	OperationSessionThreadsRead,
	OperationSessionThreadsStream,
	OperationSessionsArchive,
	OperationSessionsCreate,
	OperationSessionsDelete,
	OperationSessionsList,
	OperationSessionsRead,
	OperationSessionsUpdate,
	OperationSkillVersionsContentRead,
	OperationSkillVersionsCreate,
	OperationSkillVersionsDelete,
	OperationSkillVersionsList,
	OperationSkillVersionsRead,
	OperationSkillsCreate,
	OperationSkillsDelete,
	OperationSkillsList,
	OperationSkillsRead,
	OperationThreadEventsList,
	OperationVaultsArchive,
	OperationVaultsCreate,
	OperationVaultsDelete,
	OperationVaultsList,
	OperationVaultsRead,
	OperationVaultsUpdate,
}

// OperationRoute records an explicitly classified public registration. Owners
// still enforce the operation after obtaining trusted resource facts.
type OperationRoute struct {
	Method    string
	Pattern   string
	Operation Operation
}

var operationRoutes = []OperationRoute{
	{"DELETE", "/v1/api_keys/{api_key_id}", OperationAPIKeysDelete},
	{"DELETE", "/v1/environments/{environment_id}", OperationEnvironmentsDelete},
	{"DELETE", "/v1/files/{file_id}", OperationFilesDelete},
	{"DELETE", "/v1/memory_stores/{memory_store_id}", OperationMemoryStoresDelete},
	{"DELETE", "/v1/memory_stores/{memory_store_id}/memories/{memory_id}", OperationMemoriesDelete},
	{"DELETE", "/v1/sessions/{session_id}", OperationSessionsDelete},
	{"DELETE", "/v1/sessions/{session_id}/resources/{resource_id}", OperationSessionResourcesDelete},
	{"DELETE", "/v1/skills/{skill_id}", OperationSkillsDelete},
	{"DELETE", "/v1/skills/{skill_id}/versions/{version}", OperationSkillVersionsDelete},
	{"DELETE", "/v1/vaults/{vault_id}", OperationVaultsDelete},
	{"DELETE", "/v1/vaults/{vault_id}/credentials/{credential_id}", OperationCredentialsDelete},
	{"GET", "/v1/agents", OperationAgentsList},
	{"GET", "/v1/agents/{agent_id}", OperationAgentsRead},
	{"GET", "/v1/agents/{agent_id}/versions", OperationAgentVersionsList},
	{"GET", "/v1/api_keys", OperationAPIKeysList},
	{"GET", "/v1/environments", OperationEnvironmentsList},
	{"GET", "/v1/environments/{environment_id}", OperationEnvironmentsRead},
	{"GET", "/v1/files", OperationFilesList},
	{"GET", "/v1/files/{file_id}", OperationFilesRead},
	{"GET", "/v1/files/{file_id}/content", OperationFilesContentRead},
	{"GET", "/v1/memory_stores", OperationMemoryStoresList},
	{"GET", "/v1/memory_stores/{memory_store_id}", OperationMemoryStoresRead},
	{"GET", "/v1/memory_stores/{memory_store_id}/memories", OperationMemoriesList},
	{"GET", "/v1/memory_stores/{memory_store_id}/memories/{memory_id}", OperationMemoriesRead},
	{"GET", "/v1/memory_stores/{memory_store_id}/memory_versions", OperationMemoryVersionsList},
	{"GET", "/v1/memory_stores/{memory_store_id}/memory_versions/{memory_version_id}", OperationMemoryVersionsRead},
	{"GET", "/v1/models", OperationModelsList},
	{"GET", "/v1/models/{model_id}", OperationModelsRead},
	{"GET", "/v1/sessions", OperationSessionsList},
	{"GET", "/v1/sessions/{session_id}", OperationSessionsRead},
	{"GET", "/v1/sessions/{session_id}/events", OperationSessionEventsList},
	{"GET", "/v1/sessions/{session_id}/events/stream", OperationSessionEventsStream},
	{"GET", "/v1/sessions/{session_id}/resources", OperationSessionResourcesList},
	{"GET", "/v1/sessions/{session_id}/resources/{resource_id}", OperationSessionResourcesRead},
	{"GET", "/v1/sessions/{session_id}/threads", OperationSessionThreadsList},
	{"GET", "/v1/sessions/{session_id}/threads/{thread_id}", OperationSessionThreadsRead},
	{"GET", "/v1/sessions/{session_id}/threads/{thread_id}/events", OperationThreadEventsList},
	{"GET", "/v1/sessions/{session_id}/threads/{thread_id}/stream", OperationSessionThreadsStream},
	{"GET", "/v1/skills", OperationSkillsList},
	{"GET", "/v1/skills/{skill_id}", OperationSkillsRead},
	{"GET", "/v1/skills/{skill_id}/versions", OperationSkillVersionsList},
	{"GET", "/v1/skills/{skill_id}/versions/{version}", OperationSkillVersionsRead},
	{"GET", "/v1/skills/{skill_id}/versions/{version}/content", OperationSkillVersionsContentRead},
	{"GET", "/v1/vaults", OperationVaultsList},
	{"GET", "/v1/vaults/{vault_id}", OperationVaultsRead},
	{"GET", "/v1/vaults/{vault_id}/credentials", OperationCredentialsList},
	{"GET", "/v1/vaults/{vault_id}/credentials/{credential_id}", OperationCredentialsRead},
	{"POST", "/v1/agents", OperationAgentsCreate},
	{"POST", "/v1/agents/{agent_id}", OperationAgentsUpdate},
	{"POST", "/v1/agents/{agent_id}/archive", OperationAgentsArchive},
	{"POST", "/v1/api_keys", OperationAPIKeysCreate},
	{"POST", "/v1/environments", OperationEnvironmentsCreate},
	{"POST", "/v1/environments/{environment_id}", OperationEnvironmentsUpdate},
	{"POST", "/v1/environments/{environment_id}/archive", OperationEnvironmentsArchive},
	{"POST", "/v1/files", OperationFilesCreate},
	{"POST", "/v1/memory_stores", OperationMemoryStoresCreate},
	{"POST", "/v1/memory_stores/{memory_store_id}", OperationMemoryStoresUpdate},
	{"POST", "/v1/memory_stores/{memory_store_id}/archive", OperationMemoryStoresArchive},
	{"POST", "/v1/memory_stores/{memory_store_id}/memories", OperationMemoriesCreate},
	{"POST", "/v1/memory_stores/{memory_store_id}/memories/{memory_id}", OperationMemoriesUpdate},
	{"POST", "/v1/memory_stores/{memory_store_id}/memory_versions/{memory_version_id}/redact", OperationMemoryVersionsRedact},
	{"POST", "/v1/sessions", OperationSessionsCreate},
	{"POST", "/v1/sessions/{session_id}", OperationSessionsUpdate},
	{"POST", "/v1/sessions/{session_id}/archive", OperationSessionsArchive},
	{"POST", "/v1/sessions/{session_id}/events", OperationSessionEventsCreate},
	{"POST", "/v1/sessions/{session_id}/resources", OperationSessionResourcesCreate},
	{"POST", "/v1/sessions/{session_id}/resources/{resource_id}", OperationSessionResourcesUpdate},
	{"POST", "/v1/sessions/{session_id}/threads/{thread_id}/archive", OperationSessionThreadsArchive},
	{"POST", "/v1/skills", OperationSkillsCreate},
	{"POST", "/v1/skills/{skill_id}/versions", OperationSkillVersionsCreate},
	{"POST", "/v1/vaults", OperationVaultsCreate},
	{"POST", "/v1/vaults/{vault_id}", OperationVaultsUpdate},
	{"POST", "/v1/vaults/{vault_id}/archive", OperationVaultsArchive},
	{"POST", "/v1/vaults/{vault_id}/credentials", OperationCredentialsCreate},
	{"POST", "/v1/vaults/{vault_id}/credentials/{credential_id}", OperationCredentialsUpdate},
	{"POST", "/v1/vaults/{vault_id}/credentials/{credential_id}/archive", OperationCredentialsArchive},
	{"POST", "/v1/vaults/{vault_id}/credentials/{credential_id}/mcp_oauth_validate", OperationCredentialsValidateMcpOauth},
}

func RegisteredOperations() []Operation           { return slices.Clone(registeredOperations) }
func RegisteredOperationRoutes() []OperationRoute { return slices.Clone(operationRoutes) }
func IsRegisteredOperation(operation Operation) bool {
	return slices.Contains(registeredOperations, operation)
}
func OperationForRoute(method, pattern string) (Operation, bool) {
	for _, route := range operationRoutes {
		if route.Method == method && route.Pattern == pattern {
			return route.Operation, true
		}
	}
	return "", false
}

// RoleOperations resolves the only assignable product role. Restricted scopes
// remain valid admission snapshots, but are not an assignable product role.
func RoleOperations(role string) ([]Operation, error) {
	if role != WorkspaceFullAccess {
		return nil, &ValidationError{Message: "unsupported workspace role"}
	}
	return RegisteredOperations(), nil
}
