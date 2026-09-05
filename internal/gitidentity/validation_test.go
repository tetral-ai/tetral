package gitidentity

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestValidate(t *testing.T) {
	for _, tc := range []struct {
		label string
		name  string
		email string
		want  error
	}{
		{"ordinary", "Example Automation", "example-automation@users.noreply.github.com", nil},
		{"unicode", "山田 太郎", "taro@example.co.jp", nil},
		{"minimal email", "Bot", "a@b", nil},
		{"periods preserved", ".山田 O'Brien.", "a.b+bot@example.test.", nil},
		{"internal punctuation", "A, B: C; D\"E\\F", "o'brien@example.test", nil},
		{"name at limit", strings.Repeat("n", MaxNameBytes), "bot@example.test", nil},
		{"name over limit", strings.Repeat("n", MaxNameBytes+1), "bot@example.test", ErrInvalidName},
		// Fixed Unicode fixtures independently pin the documented 256/254-byte contract.
		{"unicode name at byte limit", strings.Repeat("界", 85) + "n", "bot@example.test", nil},
		{"unicode name over byte limit", strings.Repeat("界", 85) + "nn", "bot@example.test", ErrInvalidName},
		{"email at limit", "Bot", strings.Repeat("e", MaxEmailBytes-2) + "@b", nil},
		{"email over limit", "Bot", strings.Repeat("e", MaxEmailBytes-1) + "@b", ErrInvalidEmail},
		{"unicode email at byte limit", "Bot", strings.Repeat("界", 84) + "@b", nil},
		{"unicode email over byte limit", "Bot", strings.Repeat("界", 84) + "n@b", ErrInvalidEmail},
		{"empty name", "", "bot@example.test", ErrInvalidName},
		{"empty email", "Bot", "", ErrInvalidEmail},
		{"invalid UTF-8 name", "Bot\xff", "bot@example.test", ErrInvalidName},
		{"invalid UTF-8 email", "Bot", "bot\xff@example.test", ErrInvalidEmail},
		{"newline in name", "Bot\nCo-Authored-By: x", "bot@example.test", ErrInvalidName},
		{"format character in name", "Bot\u200b", "bot@example.test", ErrInvalidName},
		{"surrounding space in name", "Bot ", "bot@example.test", ErrInvalidName},
		{"surrounding unicode space in name", "\u00a0Bot", "bot@example.test", ErrInvalidName},
		{"newline in email", "Bot", "bot\n@example.test", ErrInvalidEmail},
		{"format character in email", "Bot", "bot\u200b@example.test", ErrInvalidEmail},
		{"space in email", "Bot", "b ot@example.test", ErrInvalidEmail},
		{"unicode space in email", "Bot", "b\u00a0ot@example.test", ErrInvalidEmail},
		{"missing at", "Bot", "bot.example.test", ErrInvalidEmail},
		{"double at", "Bot", "a@b@example.test", ErrInvalidEmail},
		{"empty local part", "Bot", "@example.test", ErrInvalidEmail},
		{"empty domain", "Bot", "bot@", ErrInvalidEmail},
		{"only angle brackets", "<>", "bot@example.test", ErrInvalidName},
		{"angle brackets in name", "Alice <Automation>", "bot@example.test", ErrInvalidName},
		{"angle brackets around email", "Bot", "<bot@example.test>", ErrInvalidEmail},
		{"angle bracket inside email", "Bot", "bo<t@example.test", ErrInvalidEmail},
		{"trailing angle bracket in email", "Bot", "bot@example.test>", ErrInvalidEmail},
	} {
		t.Run(tc.label, func(t *testing.T) {
			if err := Validate(tc.name, tc.email); !errors.Is(err, tc.want) {
				t.Fatalf("Validate = %v; want %v", err, tc.want)
			}
		})
	}
}

func TestValidateRejectsGitTrimmedPunctuation(t *testing.T) {
	for _, punctuation := range []string{",", ":", ";", "\"", "\\", "'"} {
		for _, field := range []string{"name", "email"} {
			for _, leading := range []bool{true, false} {
				t.Run(fmt.Sprintf("%s/%q/leading=%t", field, punctuation, leading), func(t *testing.T) {
					name, email := "Bot", "bot@example.test"
					value, want := &name, ErrInvalidName
					if field == "email" {
						value, want = &email, ErrInvalidEmail
					}
					if leading {
						*value = punctuation + *value
					} else {
						*value += punctuation
					}
					if err := Validate(name, email); !errors.Is(err, want) {
						t.Fatalf("Validate = %v; want %v", err, want)
					}
				})
			}
		}
	}
}
