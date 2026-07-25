package memory

import "encoding/json"

const (
	ViewBasic = "basic"
	ViewFull  = "full"

	MemoryOrderByPath      = "path"
	MemoryOrderByCreatedAt = "created_at"
	MemoryOrderByUpdatedAt = "updated_at"
	SortAscending          = "asc"
	SortDescending         = "desc"

	ActorAPI     = "api_actor"
	ActorSession = "session_actor"
	ActorUser    = "user_actor"

	OperationCreated  = "created"
	OperationModified = "modified"
	OperationDeleted  = "deleted"

	PreconditionContentSHA256 = "content_sha256"
)

type Store struct {
	ID          string            `json:"id"`
	Type        string            `json:"type"`
	Name        string            `json:"name"`
	Description string            `json:"description"`
	Metadata    map[string]string `json:"metadata"`
	CreatedAt   string            `json:"created_at"`
	UpdatedAt   string            `json:"updated_at"`
	ArchivedAt  *string           `json:"archived_at"`
}

func (value Store) MarshalJSON() ([]byte, error) {
	type response struct {
		ID          string            `json:"id"`
		Type        string            `json:"type"`
		Name        *string           `json:"name"`
		Description string            `json:"description"`
		Metadata    map[string]string `json:"metadata"`
		CreatedAt   string            `json:"created_at"`
		UpdatedAt   string            `json:"updated_at"`
		ArchivedAt  *string           `json:"archived_at"`
	}
	var name *string
	if value.Name != "" {
		nameValue := value.Name
		name = &nameValue
	}
	metadata := value.Metadata
	if metadata == nil {
		metadata = map[string]string{}
	}
	return json.Marshal(response{
		ID: value.ID, Type: value.Type, Name: name, Description: value.Description,
		Metadata: metadata, CreatedAt: value.CreatedAt, UpdatedAt: value.UpdatedAt,
		ArchivedAt: value.ArchivedAt,
	})
}

type CreateStoreRequest struct {
	Name        string            `json:"name"`
	Description string            `json:"description"`
	Metadata    map[string]string `json:"metadata"`
}

type StorePatch struct {
	Name            *string
	NameClear       bool
	Description     *string
	MetadataPatch   map[string]*string
	MetadataPresent bool
	MutablePresent  bool
}

func (p StorePatch) Materialize(current Store) (Store, error) {
	next := current
	if p.NameClear {
		next.Name = ""
	} else if p.Name != nil {
		name, err := normalizeStoreName(*p.Name)
		if err != nil {
			return Store{}, err
		}
		next.Name = name
	}
	if p.Description != nil {
		description, err := normalizeStoreDescription(*p.Description)
		if err != nil {
			return Store{}, err
		}
		next.Description = description
	}
	if p.MetadataPresent {
		metadata := map[string]string{}
		if p.MetadataPatch != nil {
			metadata = cloneMetadata(current.Metadata)
		}
		for key, value := range p.MetadataPatch {
			if value == nil {
				delete(metadata, key)
				continue
			}
			metadata[key] = *value
		}
		normalized, err := normalizeMetadata(metadata)
		if err != nil {
			return Store{}, err
		}
		next.Metadata = normalized
	}
	return next, nil
}

func cloneMetadata(metadata map[string]string) map[string]string {
	cloned := make(map[string]string, len(metadata))
	for key, value := range metadata {
		cloned[key] = value
	}
	return cloned
}

type ListStoresOptions struct {
	Limit           int
	LimitSet        bool
	Page            string
	IncludeArchived bool
	CreatedAtGTE    string
	CreatedAtLTE    string
}

type StoreListResult struct {
	Data     []*Store `json:"data"`
	NextPage *string  `json:"next_page"`
}

type Actor struct {
	Type      string `json:"type"`
	APIKeyID  string `json:"api_key_id,omitempty"`
	SessionID string `json:"session_id,omitempty"`
	UserID    string `json:"user_id,omitempty"`
}

type CreateMemoryRequest struct {
	Path       string
	Content    string
	ContentSet bool
	View       string
}

type MemoryPrecondition struct {
	Type          string
	ContentSHA256 string
}

type UpdateMemoryRequest struct {
	Path         *string
	Content      string
	ContentSet   bool
	Precondition *MemoryPrecondition
	View         string
}

type ListMemoriesOptions struct {
	Limit      int
	LimitSet   bool
	Page       string
	PathPrefix string
	Depth      int
	DepthSet   bool
	View       string
	OrderBy    string
	Order      string
}

type MemoryPrefix struct {
	Type string `json:"type"`
	Path string `json:"path"`
}

type MemoryListEntry struct {
	Memory *Memory
	Prefix *MemoryPrefix
}

func (e MemoryListEntry) MarshalJSON() ([]byte, error) {
	if e.Memory != nil {
		return json.Marshal(e.Memory)
	}
	if e.Prefix != nil {
		return json.Marshal(e.Prefix)
	}
	return []byte("null"), nil
}

func (e MemoryListEntry) Type() string {
	if e.Memory != nil {
		return e.Memory.Type
	}
	if e.Prefix != nil {
		return e.Prefix.Type
	}
	return ""
}

func (e MemoryListEntry) Path() string {
	if e.Memory != nil {
		return e.Memory.Path
	}
	if e.Prefix != nil {
		return e.Prefix.Path
	}
	return ""
}

type MemoryListResult struct {
	Data     []MemoryListEntry `json:"data"`
	NextPage *string           `json:"next_page"`
}

type ListMemoryVersionsOptions struct {
	Limit        int
	LimitSet     bool
	Page         string
	MemoryID     string
	Operation    string
	SessionID    string
	APIKeyID     string
	CreatedAtGTE string
	CreatedAtLTE string
	View         string
}

type MemoryVersionListResult struct {
	Data     []*MemoryVersion `json:"data"`
	NextPage *string          `json:"next_page"`
}

// Memory is a stored memory record. A Memory belongs to exactly one store,
// named by MemoryStoreID, and that binding is never defaulted: public API writes
// take the store id from the request, and runtime tool writes resolve it from
// the session's single read_write memory-store binding. A session with no
// writable binding is rejected (memory_store_not_configured); one with more than
// one is rejected as ambiguous (memory_store_ambiguous). The store is projected
// read-only into the sandbox at /mnt/memory, so that runtime projection path is
// not a writable memory path: ValidatePath rejects memory paths under
// /mnt/memory, and memories change only through the store id, never by writing
// files under the projection.
//
// UPDATE-WITH: internal/memory/path.go (ValidatePath /mnt/memory rejection);
// services/bridge/bridge_api_tools.go (resolveWritableMemoryStoreTx);
// internal/sandbox/driver (read-only /mnt/memory projection).
type Memory struct {
	ID               string  `json:"id"`
	Type             string  `json:"type"`
	MemoryStoreID    string  `json:"memory_store_id"`
	MemoryVersionID  string  `json:"memory_version_id"`
	Path             string  `json:"path"`
	Content          *string `json:"content,omitempty"`
	ContentSHA256    string  `json:"content_sha256"`
	ContentSizeBytes int64   `json:"content_size_bytes"`
	CreatedAt        string  `json:"created_at"`
	UpdatedAt        string  `json:"updated_at"`
}

type MemoryVersion struct {
	ID               string  `json:"id"`
	Type             string  `json:"type"`
	MemoryStoreID    string  `json:"memory_store_id"`
	MemoryID         string  `json:"memory_id"`
	Operation        string  `json:"operation"`
	Path             *string `json:"path"`
	Content          *string `json:"content"`
	ContentSHA256    *string `json:"content_sha256"`
	ContentSizeBytes *int64  `json:"content_size_bytes"`
	CreatedAt        string  `json:"created_at"`
	CreatedBy        Actor   `json:"created_by"`
	RedactedAt       *string `json:"redacted_at"`
	RedactedBy       *Actor  `json:"redacted_by"`
}

type DeleteMemoryResult struct {
	ID   string `json:"id"`
	Type string `json:"type"`
}
