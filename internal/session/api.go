package session

import (
	"context"
	"encoding/json"
	"time"

	"github.com/tetral-ai/tetral/internal/agent"
	"github.com/tetral-ai/tetral/internal/environment"
	"github.com/tetral-ai/tetral/internal/files"
	"github.com/tetral-ai/tetral/internal/memory"
	"github.com/tetral-ai/tetral/internal/workspace"
)

type AgentReader interface {
	Get(context.Context, workspace.ID, string) (*agent.Agent, error)
	GetVersion(context.Context, workspace.ID, string, int) (*agent.Agent, error)
}

type EnvironmentReader interface {
	Get(context.Context, workspace.ID, string) (*environment.Environment, error)
}

type FileIdentityService interface {
	ValidateSessionFileSource(context.Context, workspace.ID, string) error
	CreateSessionFileIdentity(context.Context, files.SessionTransaction, workspace.ID, files.SessionFileIdentityRequest) (*files.FileMetadata, error)
	TombstoneSessionFileIdentity(context.Context, files.SessionTransaction, workspace.ID, string, string) error
}

type MemoryStoreReader interface {
	GetStore(context.Context, workspace.ID, string) (*memory.Store, error)
}

type VaultReferenceValidator interface {
	ValidateVaultReferences(context.Context, workspace.ID, []string) error
}

type Encryptor interface {
	Encrypt([]byte) ([]byte, error)
}

type AgentReference struct {
	ID      string
	Version *int
}

type CreateRequest struct {
	Agent         AgentReference
	EnvironmentID string
	Title         *string
	Metadata      map[string]string
	Resources     []ResourceRequest
	VaultIDs      []string
	Providers     ProviderSelectors
}

type ProviderSelectors map[string]ProviderCredentialSelector

type ProviderCredentialSelector struct {
	CredentialID string `json:"credential_id"`
}

type ResourceRequest struct {
	Type string

	FileID    string
	MountPath *string

	MemoryStoreID string
	Access        string
	Instructions  string

	GitHubURL          string
	AuthorizationToken string
	Checkout           *GitHubCheckout
}

type GitHubCheckout struct {
	Type string `json:"type"`
	Name string `json:"name,omitempty"`
	SHA  string `json:"sha,omitempty"`
}

type UpdateRequest struct {
	Title           *string
	TitlePresent    bool
	MetadataPatch   map[string]*string
	MetadataPresent bool
	ApprovalMode    *ApprovalMode
	ToolsPatch      *agent.RawArray
	MCPServersPatch *agent.RawArray
}

type DeleteResponse struct {
	ID   string `json:"id"`
	Type string `json:"type"`
}

type ResourceDeleteResponse struct {
	ID   string `json:"id"`
	Type string `json:"type"`
}

type ListResult struct {
	Data     []*Response `json:"data"`
	NextPage *string     `json:"next_page"`
}

type ResourceListResult struct {
	Data     []*ResourceResponse `json:"data"`
	NextPage *string             `json:"next_page"`
}

type ThreadListResult struct {
	Data     []*ThreadResponse `json:"data"`
	NextPage *string           `json:"next_page"`
}

type Response struct {
	ID                 string                `json:"id"`
	Type               string                `json:"type"`
	DeploymentID       *string               `json:"deployment_id"`
	Title              *string               `json:"title"`
	Status             Status                `json:"status"`
	ArchivedAt         *time.Time            `json:"archived_at"`
	Agent              *SessionAgentResponse `json:"agent"`
	EnvironmentID      string                `json:"environment_id"`
	VaultIDs           []string              `json:"vault_ids"`
	Providers          ProviderSelectors     `json:"providers"`
	Metadata           map[string]string     `json:"metadata"`
	Resources          []*ResourceResponse   `json:"resources"`
	OutcomeEvaluations agent.RawArray        `json:"outcome_evaluations"`
	Usage              Usage                 `json:"usage"`
	Stats              SessionStats          `json:"stats"`
	CreatedAt          time.Time             `json:"created_at"`
	UpdatedAt          time.Time             `json:"updated_at"`
}

type SessionStats struct {
	ActiveSeconds   float64 `json:"active_seconds"`
	DurationSeconds float64 `json:"duration_seconds"`
}

type SessionAgentResponse struct {
	ID           string             `json:"id"`
	Type         string             `json:"type"`
	Version      int                `json:"version"`
	Name         string             `json:"name"`
	Model        agent.ModelConfig  `json:"model"`
	ApprovalMode agent.ApprovalMode `json:"approval_mode"`
	System       *string            `json:"system"`
	Description  *string            `json:"description"`
	Tools        agent.RawArray     `json:"tools"`
	MCPServers   agent.RawArray     `json:"mcp_servers"`
	Skills       agent.RawArray     `json:"skills"`
	Multiagent   *json.RawMessage   `json:"multiagent"`
}

func (value SessionAgentResponse) MarshalJSON() ([]byte, error) {
	tools, err := agent.ProjectToolsetsForResponse(value.Tools)
	if err != nil {
		return nil, err
	}
	type response struct {
		ID           string             `json:"id"`
		Type         string             `json:"type"`
		Version      int                `json:"version"`
		Name         string             `json:"name"`
		Model        agent.ModelConfig  `json:"model"`
		ApprovalMode agent.ApprovalMode `json:"approval_mode"`
		System       *string            `json:"system"`
		Description  *string            `json:"description"`
		Tools        agent.RawArray     `json:"tools"`
		MCPServers   agent.RawArray     `json:"mcp_servers"`
		Skills       agent.RawArray     `json:"skills"`
		Multiagent   *json.RawMessage   `json:"multiagent"`
	}
	return json.Marshal(response{
		ID: value.ID, Type: value.Type, Version: value.Version, Name: value.Name,
		Model: value.Model, ApprovalMode: value.ApprovalMode, System: value.System,
		Description: value.Description, Tools: tools, MCPServers: value.MCPServers,
		Skills: value.Skills, Multiagent: value.Multiagent,
	})
}

type ThreadResponse struct {
	ID             string              `json:"id"`
	Type           string              `json:"type"`
	SessionID      string              `json:"session_id"`
	ParentThreadID *string             `json:"parent_thread_id"`
	Agent          ThreadAgentResponse `json:"agent"`
	Status         Status              `json:"status"`
	Usage          ThreadUsage         `json:"usage"`
	Stats          *ThreadStats        `json:"stats"`
	ArchivedAt     *time.Time          `json:"archived_at"`
	CreatedAt      time.Time           `json:"created_at"`
	UpdatedAt      time.Time           `json:"updated_at"`
}

type ThreadUsage struct {
	InputTokens          int64              `json:"input_tokens"`
	OutputTokens         int64              `json:"output_tokens"`
	CacheCreation        UsageCacheCreation `json:"cache_creation"`
	CacheReadInputTokens int64              `json:"cache_read_input_tokens"`
}

type ThreadStats struct {
	ActiveSeconds   *float64 `json:"active_seconds,omitempty"`
	DurationSeconds *float64 `json:"duration_seconds,omitempty"`
	StartupSeconds  *float64 `json:"startup_seconds,omitempty"`
}

type ThreadAgentResponse struct {
	ID           string                 `json:"id"`
	Type         string                 `json:"type"`
	Version      int                    `json:"version"`
	Name         string                 `json:"name"`
	Model        ThreadAgentModelConfig `json:"model"`
	ApprovalMode agent.ApprovalMode     `json:"approval_mode"`
	System       *string                `json:"system"`
	Description  *string                `json:"description"`
	Tools        agent.RawArray         `json:"tools"`
	MCPServers   agent.RawArray         `json:"mcp_servers"`
	Skills       agent.RawArray         `json:"skills"`
}

type ThreadAgentModelConfig struct {
	ID    string `json:"id"`
	Speed string `json:"speed,omitempty"`
}

type ResourceResponse struct {
	ID            string    `json:"id"`
	Type          string    `json:"type"`
	FileID        string    `json:"file_id,omitempty"`
	MemoryStoreID string    `json:"memory_store_id,omitempty"`
	Access        string    `json:"access,omitempty"`
	Instructions  string    `json:"instructions,omitempty"`
	Name          string    `json:"name,omitempty"`
	Description   string    `json:"description,omitempty"`
	URL           string    `json:"url,omitempty"`
	MountPath     string    `json:"mount_path"`
	CheckoutType  string    `json:"-"`
	CheckoutRef   string    `json:"-"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

func (value ResourceResponse) MarshalJSON() ([]byte, error) {
	type response struct {
		ID            string          `json:"id"`
		Type          string          `json:"type"`
		FileID        string          `json:"file_id,omitempty"`
		MemoryStoreID string          `json:"memory_store_id,omitempty"`
		Access        string          `json:"access,omitempty"`
		Instructions  string          `json:"instructions,omitempty"`
		Name          string          `json:"name,omitempty"`
		Description   string          `json:"description,omitempty"`
		URL           string          `json:"url,omitempty"`
		MountPath     string          `json:"mount_path"`
		Checkout      *GitHubCheckout `json:"checkout,omitempty"`
		CreatedAt     time.Time       `json:"created_at"`
		UpdatedAt     time.Time       `json:"updated_at"`
	}
	var checkout *GitHubCheckout
	if value.CheckoutType != "" || value.CheckoutRef != "" {
		checkout = &GitHubCheckout{Type: value.CheckoutType}
		switch value.CheckoutType {
		case "commit":
			checkout.SHA = value.CheckoutRef
		default:
			checkout.Name = value.CheckoutRef
		}
	}
	return json.Marshal(response{
		ID: value.ID, Type: value.Type, FileID: value.FileID,
		MemoryStoreID: value.MemoryStoreID, Access: value.Access,
		Instructions: value.Instructions, Name: value.Name,
		Description: value.Description, URL: value.URL, MountPath: value.MountPath,
		Checkout: checkout, CreatedAt: value.CreatedAt, UpdatedAt: value.UpdatedAt,
	})
}
