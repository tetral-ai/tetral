package session

import (
	"net/url"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/tetral-ai/tetral/internal/pathvalidation"
)

const (
	maxMetadataPairs      = 16
	maxMetadataKeyRunes   = 64
	maxMetadataValueRunes = 512
	// maxFileResources bounds the number of distinct, still-attached file
	// resources a session may hold; it caps resource entries, not total bytes.
	// Enforced at both entry points that attach files: session create
	// (prepareCreateResources counts requested file entries) and resources.add
	// (addResourceLocked compares activeFileResourceCount, which excludes
	// detached and delete-requested rows), so the (N+1)th file is rejected 400.
	// UPDATE-WITH: service.go (prepareCreateResources, addResourceLocked,
	// activeFileResourceCount).
	maxFileResources      = 100
	maxMemoryResources    = 8
	maxMemoryInstructions = 4096
	maxGitBranchNameBytes = 255
)

var (
	fullHexSHA                    = regexp.MustCompile(`\A[0-9a-fA-F]{40}\z`)
	gitHubRepositorySegmentRegexp = regexp.MustCompile(`\A[A-Za-z0-9][A-Za-z0-9._-]*\z`)
)

func validateMetadata(metadata map[string]string) error {
	if len(metadata) > maxMetadataPairs {
		return &ValidationError{Message: "metadata has too many entries"}
	}
	for key, value := range metadata {
		if key == "" || len([]rune(key)) > maxMetadataKeyRunes {
			return &ValidationError{Message: "metadata key is invalid"}
		}
		if len([]rune(value)) > maxMetadataValueRunes {
			return &ValidationError{Message: "metadata value is invalid"}
		}
	}
	return nil
}

func validateMetadataPatch(current map[string]string, patch map[string]*string) error {
	next := map[string]string{}
	for key, value := range current {
		next[key] = value
	}
	for key, value := range patch {
		if value == nil {
			delete(next, key)
			continue
		}
		next[key] = *value
	}
	return validateMetadata(next)
}

func validateResourceRequestTypeClosure(request ResourceRequest) error {
	switch request.Type {
	case string(ResourceTypeFile):
		if request.MemoryStoreID != "" || request.Access != "" || request.Instructions != "" || request.GitHubURL != "" || request.AuthorizationToken != "" || request.Checkout != nil {
			return resourceFieldNotAllowedForTypeError(request.Type)
		}
	case string(ResourceTypeMemoryStore):
		if request.FileID != "" || request.MountPath != nil || request.GitHubURL != "" || request.AuthorizationToken != "" || request.Checkout != nil {
			return resourceFieldNotAllowedForTypeError(request.Type)
		}
	case string(ResourceTypeGitHubRepository):
		if request.FileID != "" || request.MemoryStoreID != "" || request.Access != "" || request.Instructions != "" {
			return resourceFieldNotAllowedForTypeError(request.Type)
		}
	}
	return nil
}

func resourceFieldNotAllowedForTypeError(resourceType string) error {
	return &ValidationError{
		Message: "resource field is not allowed for type",
		Detail:  "resource type " + resourceType + " received a field outside its allowed shape",
	}
}

func normalizeMountPath(raw string, roots ...string) (string, error) {
	if raw == "" || !utf8.ValidString(raw) {
		return "", &ValidationError{Message: "mount_path is invalid"}
	}
	normalized := pathvalidation.NormalizeNFC(raw)
	if normalized != raw {
		return "", &ValidationError{Message: "mount_path must be NFC-normalized"}
	}
	if !strings.HasPrefix(normalized, "/") {
		return "", &ValidationError{Message: "mount_path must be absolute"}
	}
	if strings.Contains(normalized, "\x00") {
		return "", &ValidationError{Message: "mount_path is invalid"}
	}
	parts := strings.Split(normalized, "/")
	for _, part := range parts[1:] {
		if part == "" || part == "." || part == ".." {
			return "", &ValidationError{Message: "mount_path is invalid"}
		}
		for _, r := range part {
			if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
				return "", &ValidationError{Message: "mount_path is invalid"}
			}
		}
	}
	for _, root := range roots {
		if normalized == root || strings.HasPrefix(normalized, root+"/") {
			return normalized, nil
		}
	}
	if len(roots) == 0 {
		return normalized, nil
	}
	return "", &ValidationError{Message: "mount_path root is invalid"}
}

func normalizeFileMountPath(raw string) (string, error) {
	normalized, err := normalizeMountPath(raw)
	if err != nil {
		return "", err
	}
	if len(strings.Split(normalized, "/")) < 3 {
		return "", &ValidationError{Message: "mount_path must have a non-root parent directory"}
	}
	return normalized, nil
}

func normalizeGitHubMountPath(raw string) (string, error) {
	normalized, err := normalizeMountPath(raw)
	if err != nil {
		return "", err
	}
	if normalized == "/workspace" || mountPathOverlapsAny(normalized, reservedMountSubtrees()) {
		return "", &ValidationError{Message: "mount_path overlaps a reserved path"}
	}
	if !strings.HasPrefix(normalized, "/workspace/") {
		return "", &ValidationError{Message: "mount_path root is invalid"}
	}
	return normalized, nil
}

func rejectMountOverlap(existing []string, candidate string) error {
	for _, current := range existing {
		if current == candidate ||
			strings.HasPrefix(current, candidate+"/") ||
			strings.HasPrefix(candidate, current+"/") {
			return &ValidationError{Message: "mount_path overlaps another resource"}
		}
	}
	return nil
}

func reservedMountSubtrees() []string {
	return []string{
		"/mnt/tetral/r2",
		"/tmp/tetral-runtime",
		"/dev/shm/tetral-runtime",
		"/tmp/tetral/session-prepare",
		"/mnt/memory",
		"/skills",
		"/mnt/session/outputs",
	}
}

func mountPathOverlapsAny(candidate string, reserved []string) bool {
	for _, current := range reserved {
		if candidate == current ||
			strings.HasPrefix(candidate, current+"/") ||
			strings.HasPrefix(current, candidate+"/") {
			return true
		}
	}
	return false
}

func validateGitHubRepositoryURL(raw string) (string, string, error) {
	if raw == "" || !utf8.ValidString(raw) {
		return "", "", &ValidationError{Message: "invalid GitHub repository URL"}
	}
	for _, r := range raw {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			return "", "", &ValidationError{Message: "invalid GitHub repository URL"}
		}
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", "", &ValidationError{Message: "invalid GitHub repository URL"}
	}
	if parsed.Scheme != "https" || parsed.Host != "github.com" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", "", &ValidationError{Message: "invalid GitHub repository URL"}
	}
	parts := strings.Split(strings.Trim(parsed.EscapedPath(), "/"), "/")
	if len(parts) != 2 {
		return "", "", &ValidationError{Message: "invalid GitHub repository URL"}
	}
	owner, err := url.PathUnescape(parts[0])
	if err != nil || owner == "" || strings.Contains(owner, ".") && (owner == "." || owner == "..") {
		return "", "", &ValidationError{Message: "invalid GitHub repository URL"}
	}
	repo, err := url.PathUnescape(parts[1])
	if err != nil || repo == "" {
		return "", "", &ValidationError{Message: "invalid GitHub repository URL"}
	}
	repo = strings.TrimSuffix(repo, ".git")
	if repo == "" || !safeGitHubPathComponent(owner) || !safeGitHubPathComponent(repo) {
		return "", "", &ValidationError{Message: "invalid GitHub repository URL"}
	}
	return "https://github.com/" + owner + "/" + repo, repo, nil
}

func safeGitHubPathComponent(value string) bool {
	if !gitHubRepositorySegmentRegexp.MatchString(value) || strings.Contains(value, "/") {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) || unicode.IsSpace(r) || strings.ContainsRune("?#&=:@\\", r) {
			return false
		}
	}
	return true
}

func validateCheckout(checkout *GitHubCheckout) (*GitHubCheckout, error) {
	if checkout == nil {
		return nil, nil
	}
	switch checkout.Type {
	case "branch":
		if checkout.SHA != "" || !safeGitBranchName(checkout.Name) {
			return nil, &ValidationError{Message: "checkout is invalid"}
		}
		return &GitHubCheckout{Type: "branch", Name: checkout.Name}, nil
	case "commit":
		if checkout.Name != "" || !fullHexSHA.MatchString(checkout.SHA) {
			return nil, &ValidationError{Message: "checkout is invalid"}
		}
		return &GitHubCheckout{Type: "commit", SHA: strings.ToLower(checkout.SHA)}, nil
	default:
		return nil, &ValidationError{Message: "checkout is invalid"}
	}
}

func safeGitBranchName(name string) bool {
	if name == "" || len(name) > maxGitBranchNameBytes || !utf8.ValidString(name) {
		return false
	}
	if name == "@" || name == "HEAD" || strings.HasPrefix(name, "-") || strings.HasPrefix(name, "refs/") {
		return false
	}
	if strings.HasPrefix(name, "/") || strings.HasSuffix(name, "/") || strings.Contains(name, "//") {
		return false
	}
	if strings.Contains(name, "..") || strings.Contains(name, "@{") || strings.HasSuffix(name, ".") {
		return false
	}
	if strings.ContainsAny(name, " ~^:?*[\\") {
		return false
	}
	for _, r := range name {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) || unicode.IsSpace(r) {
			return false
		}
	}
	for _, part := range strings.Split(name, "/") {
		if part == "" || part == "." || part == ".." || strings.HasPrefix(part, ".") || strings.HasSuffix(part, ".lock") {
			return false
		}
	}
	return true
}

func memorySlug(name string, fallback string) string {
	lower := strings.ToLower(pathvalidation.NormalizeNFC(name))
	var builder strings.Builder
	lastDash := false
	for _, r := range lower {
		switch {
		case r >= 'a' && r <= 'z':
			builder.WriteRune(r)
			lastDash = false
		case r >= '0' && r <= '9':
			builder.WriteRune(r)
			lastDash = false
		default:
			if !lastDash {
				builder.WriteByte('-')
				lastDash = true
			}
		}
	}
	slug := strings.Trim(builder.String(), "-")
	if slug == "" {
		slug = fallback
	}
	return slug
}
