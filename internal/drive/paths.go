package drive

import (
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/cases"

	"github.com/tetral-ai/tetral/internal/pathvalidation"
)

// The path rule messages, in the order a write checks them. Rules 7 to 9
// compare fold keys against the drive's current files (commit.go).
const (
	ruleRelative       = "path must be relative, without a leading /"
	ruleLength         = "path must be at most 1024 bytes"
	ruleNFC            = "path must be valid UTF-8 in NFC"
	ruleCharacters     = "path must not contain control or format characters"
	ruleSegments       = "path must not contain empty, . or .. segments"
	ruleSegmentLength  = "each path segment must be at most 255 bytes"
	ruleFoldCollision  = "another file's path equals this path under case folding"
	ruleFileAtFolder   = "a file exists at a folder of this path"
	ruleFilesBelowPath = "files exist below this path"
)

// CheckPath returns the message of the first of rules 1 to 6 the path breaks,
// and "" when it follows them. These rules need no drive state.
func CheckPath(path string) string {
	switch {
	case strings.HasPrefix(path, "/"):
		return ruleRelative
	case len(path) > 1024:
		return ruleLength
	case !utf8.ValidString(path) || !pathvalidation.IsNFC(path) || !pathvalidation.IsStreamSafe(path):
		return ruleNFC
	}
	for _, r := range path {
		if unicode.Is(unicode.Cc, r) || unicode.Is(unicode.Cf, r) {
			return ruleCharacters
		}
	}
	for _, segment := range strings.Split(path, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return ruleSegments
		}
	}
	for _, segment := range strings.Split(path, "/") {
		if len(segment) > 255 {
			return ruleSegmentLength
		}
	}
	return ""
}

// storable reports whether Postgres text can hold path: valid UTF-8 without
// NUL. Every other path breaks a lexical rule, so no version is ever stored at
// it and a lookup can answer without a query.
func storable(path string) bool {
	return utf8.ValidString(path) && !strings.ContainsRune(path, 0)
}

// FoldKey is the path under full Unicode case folding, so `Straße.md` and
// `STRASSE.md` share one key.
func FoldKey(path string) string {
	return pathvalidation.NormalizeNFC(cases.Fold().String(path))
}

// folderFoldKeys are the fold keys of every folder above path.
func folderFoldKeys(path string) []string {
	var keys []string
	for i := strings.IndexByte(path, '/'); i >= 0; {
		keys = append(keys, FoldKey(path[:i]))
		next := strings.IndexByte(path[i+1:], '/')
		if next < 0 {
			break
		}
		i += next + 1
	}
	return keys
}

// likePrefix escapes value for a LIKE pattern that matches it as a prefix.
func likePrefix(value string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(value) + "%"
}
