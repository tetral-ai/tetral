package runtimecontrol

import (
	"encoding/json"
	"strings"
)

func MarshalJSON(value any) (string, error) {
	body, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func MarshalDataJSON(value any) (string, error) {
	var body strings.Builder
	encoder := json.NewEncoder(&body)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return "", err
	}
	return strings.TrimSuffix(body.String(), "\n"), nil
}
