package auth

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
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
			if !ok || seen[key] {
				return errors.New("duplicate JSON field")
			}
			seen[key] = true
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
