package memory

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"
)

const (
	storeNameMaxRunes        = 255
	storeDescriptionMaxRunes = 1024
	storeMetadataMaxKeys     = 16
	storeMetadataKeyMaxRunes = 64
	storeMetadataValMaxRunes = 512
	memoryContentMaxBytes    = 102400
)

func DecodeCreateMemoryRequest(body []byte) (CreateMemoryRequest, error) {
	var raw map[string]json.RawMessage
	if err := decodeJSONObject(body, &raw); err != nil {
		return CreateMemoryRequest{}, err
	}
	for key := range raw {
		switch key {
		case "path", "content":
		default:
			return CreateMemoryRequest{}, &ValidationError{Message: "unknown field " + key}
		}
	}
	pathRaw, ok := raw["path"]
	if !ok {
		return CreateMemoryRequest{}, &ValidationError{Message: "path is required"}
	}
	var path string
	if err := json.Unmarshal(pathRaw, &path); err != nil {
		return CreateMemoryRequest{}, &ValidationError{Message: "path must be a string"}
	}
	if err := ValidatePath(path); err != nil {
		return CreateMemoryRequest{}, err
	}
	contentRaw, ok := raw["content"]
	if !ok {
		return CreateMemoryRequest{}, &ValidationError{Message: "content is required"}
	}
	if string(contentRaw) == "null" {
		return CreateMemoryRequest{}, &ValidationError{Message: "content must be a string"}
	}
	var content string
	if err := json.Unmarshal(contentRaw, &content); err != nil {
		return CreateMemoryRequest{}, &ValidationError{Message: "content must be a string"}
	}
	if len([]byte(content)) > memoryContentMaxBytes {
		return CreateMemoryRequest{}, &ValidationError{Message: "content must be at most 102400 bytes"}
	}
	return CreateMemoryRequest{Path: path, Content: content, ContentSet: true, View: ViewFull}, nil
}

func DecodeUpdateMemoryRequest(body []byte) (UpdateMemoryRequest, error) {
	var raw map[string]json.RawMessage
	if err := decodeJSONObject(body, &raw); err != nil {
		return UpdateMemoryRequest{}, err
	}
	var request UpdateMemoryRequest
	for key, value := range raw {
		switch key {
		case "path":
			if string(value) == "null" {
				return UpdateMemoryRequest{}, &ValidationError{Message: "path must be a string"}
			}
			var path string
			if err := json.Unmarshal(value, &path); err != nil {
				return UpdateMemoryRequest{}, &ValidationError{Message: "path must be a string"}
			}
			if err := ValidatePath(path); err != nil {
				return UpdateMemoryRequest{}, err
			}
			request.Path = &path
		case "content":
			if string(value) == "null" {
				return UpdateMemoryRequest{}, &ValidationError{Message: "content must be a string"}
			}
			var content string
			if err := json.Unmarshal(value, &content); err != nil {
				return UpdateMemoryRequest{}, &ValidationError{Message: "content must be a string"}
			}
			if len([]byte(content)) > memoryContentMaxBytes {
				return UpdateMemoryRequest{}, &ValidationError{Message: "content must be at most 102400 bytes"}
			}
			request.Content = content
			request.ContentSet = true
		case "precondition":
			precondition, err := decodeMemoryPrecondition(value)
			if err != nil {
				return UpdateMemoryRequest{}, err
			}
			request.Precondition = precondition
		default:
			return UpdateMemoryRequest{}, &ValidationError{Message: "unknown field " + key}
		}
	}
	return request, nil
}

func decodeMemoryPrecondition(raw json.RawMessage) (*MemoryPrecondition, error) {
	if string(raw) == "null" {
		return nil, &ValidationError{Message: "precondition must be an object"}
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, &ValidationError{Message: "precondition must be an object"}
	}
	if fields == nil {
		return nil, &ValidationError{Message: "precondition must be an object"}
	}
	for key := range fields {
		switch key {
		case "type", "content_sha256":
		default:
			return nil, &ValidationError{Message: "unknown field precondition." + key}
		}
	}
	typeRaw, ok := fields["type"]
	if !ok {
		return nil, &ValidationError{Message: "precondition.type is required"}
	}
	var preconditionType string
	if err := json.Unmarshal(typeRaw, &preconditionType); err != nil {
		return nil, &ValidationError{Message: "precondition.type must be a string"}
	}
	if preconditionType != PreconditionContentSHA256 {
		return nil, &ValidationError{Message: "precondition type must be content_sha256"}
	}
	hashRaw, ok := fields["content_sha256"]
	if !ok {
		return nil, &ValidationError{Message: "precondition.content_sha256 is required"}
	}
	var hash string
	if err := json.Unmarshal(hashRaw, &hash); err != nil {
		return nil, &ValidationError{Message: "precondition.content_sha256 must be a string"}
	}
	if err := validateSHA256Hex(hash); err != nil {
		return nil, err
	}
	return &MemoryPrecondition{Type: preconditionType, ContentSHA256: hash}, nil
}

func DecodeCreateStoreRequest(body []byte) (CreateStoreRequest, error) {
	var raw map[string]json.RawMessage
	if err := decodeJSONObject(body, &raw); err != nil {
		return CreateStoreRequest{}, err
	}
	for key := range raw {
		switch key {
		case "name", "description", "metadata":
		default:
			return CreateStoreRequest{}, &ValidationError{Message: "unknown field " + key}
		}
	}
	var request CreateStoreRequest
	nameRaw, ok := raw["name"]
	if !ok {
		return CreateStoreRequest{}, &ValidationError{Message: "name is required"}
	}
	var name string
	if err := json.Unmarshal(nameRaw, &name); err != nil {
		return CreateStoreRequest{}, &ValidationError{Message: "name must be a string"}
	}
	normalizedName, err := normalizeStoreName(name)
	if err != nil {
		return CreateStoreRequest{}, err
	}
	request.Name = normalizedName
	if descriptionRaw, ok := raw["description"]; ok {
		var description string
		if err := json.Unmarshal(descriptionRaw, &description); err != nil {
			return CreateStoreRequest{}, &ValidationError{Message: "description must be a string"}
		}
		normalizedDescription, err := normalizeStoreDescription(description)
		if err != nil {
			return CreateStoreRequest{}, err
		}
		request.Description = normalizedDescription
	}
	if metadataRaw, ok := raw["metadata"]; ok {
		metadata, err := decodeMetadata(metadataRaw)
		if err != nil {
			return CreateStoreRequest{}, err
		}
		request.Metadata = metadata
	}
	if request.Metadata == nil {
		request.Metadata = map[string]string{}
	}
	return request, nil
}

func DecodeUpdateStoreRequest(body []byte) (StorePatch, error) {
	var raw map[string]json.RawMessage
	if err := decodeJSONObject(body, &raw); err != nil {
		return StorePatch{}, err
	}
	var patch StorePatch
	for key, value := range raw {
		switch key {
		case "name":
			if isJSONNull(value) {
				patch.NameClear = true
				patch.MutablePresent = true
				continue
			}
			var name string
			if err := json.Unmarshal(value, &name); err != nil {
				return StorePatch{}, &ValidationError{Message: "name must be a string"}
			}
			patch.Name = &name
			patch.MutablePresent = true
		case "description":
			if isJSONNull(value) {
				description := ""
				patch.Description = &description
			} else {
				var description string
				if err := json.Unmarshal(value, &description); err != nil {
					return StorePatch{}, &ValidationError{Message: "description must be a string"}
				}
				patch.Description = &description
			}
			patch.MutablePresent = true
		case "metadata":
			if isJSONNull(value) {
				patch.MetadataPatch = nil
			} else {
				metadata, err := decodeMetadataPatch(value)
				if err != nil {
					return StorePatch{}, err
				}
				patch.MetadataPatch = metadata
			}
			patch.MetadataPresent = true
			patch.MutablePresent = true
		default:
			return StorePatch{}, &ValidationError{Message: "unknown field " + key}
		}
	}
	if !patch.MutablePresent {
		return StorePatch{}, &ValidationError{Message: "at least one mutable field is required"}
	}
	return patch, nil
}

func decodeJSONObject(body []byte, target *map[string]json.RawMessage) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return &ValidationError{Message: "invalid request body"}
	}
	if len(*target) == 0 && strings.TrimSpace(string(body)) == "" {
		return &ValidationError{Message: "request body is required"}
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != io.EOF {
		return &ValidationError{Message: "request body must contain exactly one JSON value"}
	}
	return nil
}

func normalizeStoreName(name string) (string, error) {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return "", &ValidationError{Message: "name is required"}
	}
	if utf8.RuneCountInString(trimmed) > storeNameMaxRunes {
		return "", &ValidationError{Message: fmt.Sprintf("name must be at most %d characters", storeNameMaxRunes)}
	}
	return trimmed, nil
}

func normalizeStoreDescription(description string) (string, error) {
	if utf8.RuneCountInString(description) > storeDescriptionMaxRunes {
		return "", &ValidationError{Message: fmt.Sprintf("description must be at most %d characters", storeDescriptionMaxRunes)}
	}
	return description, nil
}

func decodeMetadata(raw json.RawMessage) (map[string]string, error) {
	if isJSONNull(raw) {
		return nil, &ValidationError{Message: "metadata must be an object"}
	}
	var generic map[string]any
	if err := json.Unmarshal(raw, &generic); err != nil {
		return nil, &ValidationError{Message: "metadata must be an object"}
	}
	if generic == nil {
		return nil, &ValidationError{Message: "metadata must be an object"}
	}
	metadata := make(map[string]string, len(generic))
	for key, value := range generic {
		stringValue, ok := value.(string)
		if !ok {
			return nil, &ValidationError{Message: "metadata values must be strings"}
		}
		metadata[key] = stringValue
	}
	return normalizeMetadata(metadata)
}

func decodeMetadataPatch(raw json.RawMessage) (map[string]*string, error) {
	if isJSONNull(raw) {
		return nil, &ValidationError{Message: "metadata must be an object"}
	}
	var generic map[string]json.RawMessage
	if err := json.Unmarshal(raw, &generic); err != nil {
		return nil, &ValidationError{Message: "metadata must be an object"}
	}
	if generic == nil {
		return nil, &ValidationError{Message: "metadata must be an object"}
	}
	metadata := make(map[string]*string, len(generic))
	for key, value := range generic {
		if isJSONNull(value) {
			metadata[key] = nil
			continue
		}
		var stringValue string
		if err := json.Unmarshal(value, &stringValue); err != nil {
			return nil, &ValidationError{Message: "metadata values must be strings or null"}
		}
		metadata[key] = &stringValue
	}
	return metadata, nil
}

func normalizeMetadata(metadata map[string]string) (map[string]string, error) {
	if len(metadata) > storeMetadataMaxKeys {
		return nil, &ValidationError{Message: "metadata must contain at most 16 keys"}
	}
	normalized := make(map[string]string, len(metadata))
	for key, value := range metadata {
		if utf8.RuneCountInString(key) > storeMetadataKeyMaxRunes {
			return nil, &ValidationError{Message: "metadata keys must be at most 64 characters"}
		}
		if utf8.RuneCountInString(value) > storeMetadataValMaxRunes {
			return nil, &ValidationError{Message: "metadata values must be at most 512 characters"}
		}
		normalized[key] = value
	}
	return normalized, nil
}

func isJSONNull(raw json.RawMessage) bool {
	return bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}
