package wire

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"unicode"

	"golang.org/x/text/cases"

	"github.com/tetral-ai/tetral/internal/pathvalidation"
)

// want is the error of a value that breaks its own rule.
func want(holds bool, what string) error {
	if holds {
		return nil
	}
	return fmt.Errorf("want %s", what)
}

// oneOf is the rule of an enumerated string.
func oneOf(value string, allowed ...string) error {
	return want(slices.Contains(allowed, value), "one of "+strings.Join(allowed, ", "))
}

// ID is an identity: a nonempty string without U+0000, compared and sent
// back but never read for meaning.
type ID string

func (id *ID) check() error {
	return want(*id != "" && !strings.ContainsRune(string(*id), 0), "a nonempty identity without U+0000")
}

// SafeID is an identity that may name a directory: no `/`, and neither `.`
// nor `..`. `session_id` and `drive_id` are safe identities.
type SafeID string

func (id *SafeID) check() error {
	text := string(*id)
	return want(text != "" && !strings.ContainsAny(text, "\x00/") && text != "." && text != "..",
		"an identity without U+0000 or '/' that is not '.' or '..'")
}

// NonEmpty is a string with at least one byte.
type NonEmpty string

func (text *NonEmpty) check() error { return want(*text != "", "a nonempty string") }

// AbsolutePath is a path on the machine: it starts with `/` and holds no
// U+0000.
type AbsolutePath string

func (path *AbsolutePath) check() error {
	return want(strings.HasPrefix(string(*path), "/") && !strings.ContainsRune(string(*path), 0), "an absolute path")
}

// Sha256 is a SHA-256 digest in 64 lowercase hexadecimal characters.
type Sha256 string

func (digest *Sha256) check() error {
	return want(lowerHex(string(*digest), 64), "a lowercase SHA-256")
}

// CommitSha is a Git commit in 40 lowercase hexadecimal characters.
type CommitSha string

func (sha *CommitSha) check() error {
	return want(lowerHex(string(*sha), 40), "a lowercase commit SHA")
}

func lowerHex(text string, length int) bool {
	return len(text) == length && strings.Trim(text, "0123456789abcdef") == ""
}

// Drive path limits.
const (
	DrivePathMaxBytes    = 1024
	DriveSegmentMaxBytes = 255
)

// DrivePath is a file's path relative to a drive's root: NFC text of at most
// 1,024 UTF-8 bytes whose `/`-separated segments are nonempty, at most 255
// bytes, never `.` or `..`, and hold no control or format character.
type DrivePath string

func (path *DrivePath) check() error { return drivePathRule(string(*path)) }

// ExcludePath is a drive path, or a directory's when it ends with `/`.
type ExcludePath string

func (path *ExcludePath) check() error {
	if len(*path) > DrivePathMaxBytes {
		return want(false, "a drive path of at most 1,024 bytes")
	}
	return drivePathRule(strings.TrimSuffix(string(*path), "/"))
}

func drivePathRule(path string) error {
	if len(path) > DrivePathMaxBytes {
		return want(false, "a drive path of at most 1,024 bytes")
	}
	for segment := range strings.SplitSeq(path, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return want(false, "drive path segments that are not empty, '.' or '..'")
		}
		if len(segment) > DriveSegmentMaxBytes {
			return want(false, "drive path segments of at most 255 bytes")
		}
	}
	if strings.IndexFunc(path, func(r rune) bool { return unicode.In(r, unicode.Cc, unicode.Cf) }) >= 0 {
		return want(false, "a drive path without control or format characters")
	}
	return want(pathvalidation.IsNFC(path), "a drive path in NFC")
}

// caseFolded is the key under which two paths that differ only in case are
// equal, by Unicode full case folding.
func caseFolded(path string) string { return cases.Fold().String(path) }

// Time is an RFC 3339 time in UTC, `YYYY-MM-DDTHH:MM:SS[.fraction]Z`, naming
// a time that exists: month 1 to 12, a day of that month, hour at most 23,
// minute at most 59 and second at most 60, for a leap second.
type Time string

func (t *Time) check() error { return want(validUTC(string(*t)), "an RFC 3339 UTC time") }

func validUTC(text string) bool {
	if len(text) < 20 || text[4] != '-' || text[7] != '-' || text[10] != 'T' || text[13] != ':' || text[16] != ':' {
		return false
	}
	number := func(from, to int) (int, bool) {
		value := 0
		for _, digit := range []byte(text[from:to]) {
			if digit < '0' || digit > '9' {
				return 0, false
			}
			value = value*10 + int(digit-'0')
		}
		return value, true
	}
	year, okYear := number(0, 4)
	month, okMonth := number(5, 7)
	day, okDay := number(8, 10)
	hour, okHour := number(11, 13)
	minute, okMinute := number(14, 16)
	second, okSecond := number(17, 19)
	if !okYear || !okMonth || !okDay || !okHour || !okMinute || !okSecond {
		return false
	}
	rest := text[19:]
	switch {
	case rest == "Z":
	case len(rest) > 2 && rest[0] == '.' && rest[len(rest)-1] == 'Z':
		if _, ok := number(20, len(text)-1); !ok {
			return false
		}
	default:
		return false
	}
	return month >= 1 && month <= 12 && day >= 1 && day <= daysIn(year, month) &&
		hour <= 23 && minute <= 59 && second <= 60
}

func daysIn(year, month int) int {
	switch month {
	case 2:
		if year%4 == 0 && (year%100 != 0 || year%400 == 0) {
			return 29
		}
		return 28
	case 4, 6, 9, 11:
		return 30
	}
	return 31
}

// Decimal is a nonnegative decimal integer within the signed 64-bit range, as
// a query or header value carries it.
type Decimal string

func (text *Decimal) check() error {
	_, ok := decimal(string(*text))
	return want(ok, "a nonnegative decimal integer")
}

// Value is the integer a Decimal holds.
func (text Decimal) Value() int64 {
	value, _ := decimal(string(text))
	return value
}

func decimal(text string) (int64, bool) {
	if text == "" || (len(text) > 1 && text[0] == '0') || strings.Trim(text, "0123456789") != "" {
		return 0, false
	}
	value, err := strconv.ParseInt(text, 10, 64)
	return value, err == nil
}
