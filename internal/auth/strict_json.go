package auth

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"unicode"
)

// DecodeStrictJSON rejects duplicate fields, excessive nesting, unknown fields,
// and trailing values. Security selectors never depend on parser last-wins rules.
func DecodeStrictJSON(input []byte, target any) error {
	scanner := json.NewDecoder(bytes.NewReader(input))
	scanner.UseNumber()
	if err := scanJSONValue(scanner, 0); err != nil {
		return &ValidationError{Message: "invalid JSON object"}
	}
	if _, err := scanner.Token(); !errors.Is(err, io.EOF) {
		return &ValidationError{Message: "invalid JSON object"}
	}
	decoder := json.NewDecoder(bytes.NewReader(input))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return &ValidationError{Message: "invalid JSON object"}
	}
	return nil
}

func scanJSONValue(decoder *json.Decoder, depth int) error {
	if depth > 16 {
		return errors.New("JSON nesting limit")
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		seen := map[string]bool{}
		for decoder.More() {
			name, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := name.(string)
			folded := foldJSONField(key)
			if !ok || seen[folded] {
				return errors.New("duplicate JSON field")
			}
			seen[folded] = true
			if err := scanJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
	case '[':
		for decoder.More() {
			if err := scanJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
	default:
		return errors.New("unexpected JSON delimiter")
	}
	_, err = decoder.Token()
	return err
}

// encoding/json accepts Unicode case-equivalent struct field names. Reject
// aliases in the same object using the same simple-fold equivalence, including
// escaped names, Kelvin K, long S, and Greek sigma variants.
func foldJSONField(name string) string {
	return strings.Map(func(value rune) rune {
		if value < unicode.MaxASCII+1 {
			if value >= 'a' && value <= 'z' {
				return value - 'a' + 'A'
			}
			return value
		}
		for {
			next := unicode.SimpleFold(value)
			if next <= value {
				return next
			}
			value = next
		}
	}, name)
}
