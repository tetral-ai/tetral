// Package gitidentity defines the declared Git commit identity contract shared
// by Session admission and Sandbox durable snapshot validation.
package gitidentity

import (
	"errors"
	"strings"
	"unicode"
	"unicode/utf8"
)

var (
	ErrInvalidName  = errors.New("git identity name is invalid")
	ErrInvalidEmail = errors.New("git identity email is invalid")
)

// Validate accepts a complete identity that Git can represent without changing
// its values. Callers handle optional identities before calling Validate.
// Errors identify the field without echoing untrusted input.
func Validate(name, email string) error {
	if name == "" || !utf8.ValidString(name) || len(name) > 256 {
		return ErrInvalidName
	}
	if email == "" || !utf8.ValidString(email) || len(email) > 254 {
		return ErrInvalidEmail
	}
	for _, r := range name {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			return ErrInvalidName
		}
	}
	if strings.TrimSpace(name) != name || needsSanitization(name) {
		return ErrInvalidName
	}
	for _, r := range email {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) || unicode.IsSpace(r) {
			return ErrInvalidEmail
		}
	}
	if needsSanitization(email) || strings.Count(email, "@") != 1 || strings.HasPrefix(email, "@") || strings.HasSuffix(email, "@") {
		return ErrInvalidEmail
	}
	return nil
}

// Git's ident.c removes angle brackets anywhere and trims these ASCII bytes
// from both ends (crud / strbuf_addstr_without_crud). Internal punctuation,
// periods, and non-ASCII names remain representable and must not be stripped.
func needsSanitization(value string) bool {
	const trimmed = " \t\n\r\v\f,:;<>\"\\'"
	return strings.ContainsAny(value, "<>") || strings.Trim(value, trimmed) != value
}
