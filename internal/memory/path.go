package memory

import (
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/tetral-ai/tetral/internal/pathvalidation"
)

func ValidatePath(path string) error {
	if path == "" {
		return &ValidationError{Message: "path is required"}
	}
	if !strings.HasPrefix(path, "/") {
		return &ValidationError{Message: "path must start with /"}
	}
	if path == "/" {
		return &ValidationError{Message: "path must identify a memory"}
	}
	if path == "/mnt/memory" || strings.HasPrefix(path, "/mnt/memory/") {
		return &ValidationError{Message: "path must not use runtime projection path"}
	}
	if len(path) > 1024 {
		return &ValidationError{Message: "path must be at most 1024 bytes"}
	}
	if !utf8.ValidString(path) {
		return &ValidationError{Message: "path must be valid UTF-8"}
	}
	if !pathvalidation.IsNFC(path) {
		return &ValidationError{Message: "path must be NFC normalized"}
	}
	for _, r := range path {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			return &ValidationError{Message: "path must not contain control or format characters"}
		}
	}
	for _, segment := range strings.Split(path[1:], "/") {
		if segment == "" {
			return &ValidationError{Message: "path must not contain empty segments"}
		}
		if segment == "." || segment == ".." {
			return &ValidationError{Message: "path must not contain . or .. segments"}
		}
	}
	return nil
}

func ValidatePathPrefix(pathPrefix string) error {
	if pathPrefix == "" {
		return nil
	}
	if !strings.HasPrefix(pathPrefix, "/") {
		return &ValidationError{Message: "path_prefix must start with /"}
	}
	if len(pathPrefix) > 1024 {
		return &ValidationError{Message: "path_prefix must be at most 1024 bytes"}
	}
	if !utf8.ValidString(pathPrefix) {
		return &ValidationError{Message: "path_prefix must be valid UTF-8"}
	}
	if !pathvalidation.IsNFC(pathPrefix) {
		return &ValidationError{Message: "path_prefix must be NFC normalized"}
	}
	for _, r := range pathPrefix {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			return &ValidationError{Message: "path_prefix must not contain control or format characters"}
		}
	}
	if pathPrefix == "/" {
		return nil
	}
	segments := strings.Split(pathPrefix[1:], "/")
	for i, segment := range segments {
		if segment == "" {
			if i == len(segments)-1 && strings.HasSuffix(pathPrefix, "/") {
				continue
			}
			return &ValidationError{Message: "path_prefix must not contain empty segments"}
		}
		if segment == "." || segment == ".." {
			return &ValidationError{Message: "path_prefix must not contain . or .. segments"}
		}
	}
	return nil
}
