// Package environment provides Environment template CRUD operations.
package environment

import (
	"encoding/json"
	"time"
)

// Environment represents an environment resource.
type Environment struct {
	ID                string            `json:"id"`
	Type              string            `json:"type"`
	Name              string            `json:"name"`
	Description       string            `json:"description"`
	Config            EnvironmentConfig `json:"config"`
	Metadata          StringMap         `json:"metadata"`
	CurrentGeneration int64             `json:"-"`
	CreatedAt         time.Time         `json:"created_at"`
	UpdatedAt         time.Time         `json:"updated_at"`
	ArchivedAt        *time.Time        `json:"archived_at"`
}

// EnvironmentTemplate is the runtime-agnostic template resolved at
// session creation time for future container materialization.
type EnvironmentTemplate struct {
	ID       string
	Config   EnvironmentConfig
	Metadata StringMap
}

// DeleteResult is returned when an Environment is hard-deleted.
type DeleteResult struct {
	ID   string `json:"id"`
	Type string `json:"type"`
}

// ListOptions controls Anthropic-compatible Environment list
// pagination.
type ListOptions struct {
	Limit           int
	Page            string
	IncludeArchived bool
}

// ListResult is the Environment-specific list result. NextPage is nil
// when no additional page exists.
type ListResult struct {
	Data     []*Environment
	NextPage *string
}

// EnvironmentConfig is the normalized Anthropic-compatible cloud
// container template.
type EnvironmentConfig struct {
	Type       string            `json:"type"`
	Networking *NetworkingConfig `json:"networking"`
	Packages   PackageMap        `json:"packages"`
}

// MarshalJSON emits the complete normalized config shape even when a
// caller constructs a zero-value request DTO for HTTP tests/clients.
func (c EnvironmentConfig) MarshalJSON() ([]byte, error) {
	normalized, err := NormalizeEnvironmentConfig(c)
	if err != nil {
		return nil, err
	}
	type configAlias struct {
		Type       string            `json:"type"`
		Networking *NetworkingConfig `json:"networking"`
		Packages   PackageMap        `json:"packages"`
	}
	return json.Marshal(configAlias(normalized))
}

// NetworkingConfig controls network access for the environment.
type NetworkingConfig struct {
	Type             string `json:"type"`
	NetworkAllowList string `json:"network_allow_list,omitempty"`
}

// MarshalJSON emits exactly the public networking shape for the active
// networking mode.
func (n NetworkingConfig) MarshalJSON() ([]byte, error) {
	if n.Type == "cidr_allow_list" {
		type cidrNetworking struct {
			Type             string `json:"type"`
			NetworkAllowList string `json:"network_allow_list"`
		}
		return json.Marshal(cidrNetworking{
			Type:             "cidr_allow_list",
			NetworkAllowList: n.NetworkAllowList,
		})
	}
	type simpleNetworking struct {
		Type string `json:"type"`
	}
	if n.Type == "blocked" {
		return json.Marshal(simpleNetworking{Type: "blocked"})
	}
	return json.Marshal(simpleNetworking{Type: "unrestricted"})
}

// PackageMap is keyed by Anthropic package manager name.
type PackageMap map[string][]string

// MarshalJSON emits the complete six-manager public SDK shape.
func (m PackageMap) MarshalJSON() ([]byte, error) {
	values := func(manager string) []string {
		items := m[manager]
		if items == nil {
			return []string{}
		}
		return items
	}
	return json.Marshal(struct {
		APT   []string `json:"apt"`
		Cargo []string `json:"cargo"`
		Gem   []string `json:"gem"`
		Go    []string `json:"go"`
		NPM   []string `json:"npm"`
		PIP   []string `json:"pip"`
	}{
		APT: values("apt"), Cargo: values("cargo"), Gem: values("gem"),
		Go: values("go"), NPM: values("npm"), PIP: values("pip"),
	})
}

// StringMap is public non-secret string metadata.
type StringMap map[string]string

// MarshalJSON keeps empty metadata maps as `{}` instead of `null`.
func (m StringMap) MarshalJSON() ([]byte, error) {
	if m == nil {
		return []byte("{}"), nil
	}
	type alias map[string]string
	return json.Marshal(alias(m))
}

// CreateEnvironmentRequest is the input for creating a new environment.
type CreateEnvironmentRequest struct {
	Name        string            `json:"name"`
	Description string            `json:"description"`
	Config      EnvironmentConfig `json:"config"`
	Metadata    StringMap         `json:"metadata"`
}

// EnvironmentPatch is the partial update shape for an Environment.
type EnvironmentPatch struct {
	raw map[string]json.RawMessage
}

// NotFoundError indicates the environment was not found.
type NotFoundError struct {
	Message string
}

func (e *NotFoundError) Error() string { return e.Message }

// ValidationError indicates invalid input.
type ValidationError struct {
	Message string
}

func (e *ValidationError) Error() string { return e.Message }

// ConflictError indicates a version conflict or name uniqueness violation.
type ConflictError struct {
	Message string
}

func (e *ConflictError) Error() string { return e.Message }
