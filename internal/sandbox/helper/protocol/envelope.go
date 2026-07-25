package protocol

import (
	"encoding/json"
	"errors"
	"strings"
	"unicode/utf8"
)

type Envelope struct {
	SchemaVersion int             `json:"schema_version,omitempty"`
	Tool          string          `json:"tool,omitempty"`
	Status        string          `json:"status,omitempty"`
	Truncated     *bool           `json:"truncated,omitempty"`
	Error         *ToolError      `json:"error"`
	Result        json.RawMessage `json:"result,omitempty"`
	Metrics       *Metrics        `json:"metrics,omitempty"`
}

// MaxEnvelopeBytes is the 256 KiB self-cap the whole result envelope is held
// to. MarshalEnvelope meets it by progressively trimming the bounded result
// fields (the string-rune / array-item ladder) rather than exceeding it. Two
// envelopes are exempt and pass allowOversize = true so MarshalEnvelope skips
// this cap: view_image and Read media, both of which carry base64 media the
// transport has no size limit on. The idle-output capture envelope is also
// exempt from this cap, but by a different mechanism: it is a separate
// protocol.CaptureEnvelope encoded straight to stdout and never routed through
// MarshalEnvelope, so this cap — a property of Envelope marshaling — never
// applies to it at all.
//
// maxErrorMessageBytes (4 KiB) and maxErrorDetailBytes (32 KiB) bound
// error.message and error.detail. Ordering is load-bearing: withBoundedError
// trims these two error fields FIRST, and only then is the whole envelope fit
// to MaxEnvelopeBytes — so the byte budget below always sees already-bounded
// error fields.
//
// UPDATE-WITH:
//   - internal/sandbox/helper/internal/cli/cli.go (allowOversize call sites)
const (
	MaxEnvelopeBytes     = 256 * 1024
	maxErrorMessageBytes = 4096
	maxErrorDetailBytes  = 32 * 1024
)

func (e Envelope) ResultBytes() []byte {
	return []byte(e.Result)
}

func MarshalEnvelope(envelope Envelope, allowOversize bool) ([]byte, error) {
	envelope = withBoundedError(envelope)
	body, err := json.Marshal(envelope)
	if err != nil || allowOversize || len(body) <= MaxEnvelopeBytes {
		return body, err
	}
	fitted := envelope
	fitted.Truncated = boolPtr(true)
	if len(fitted.Result) > 0 {
		var value any
		if err := json.Unmarshal(fitted.Result, &value); err == nil {
			for _, limit := range []struct {
				stringRunes int
				arrayItems  int
			}{
				{8192, 100},
				{4096, 50},
				{1024, 20},
				{256, 10},
				{64, 3},
			} {
				fitValue := fitJSONValue(value, limit.stringRunes, limit.arrayItems)
				if root, ok := fitValue.(map[string]any); ok {
					root["truncated"] = true
				}
				encoded, err := json.Marshal(fitValue)
				if err != nil {
					continue
				}
				fitted.Result = encoded
				body, err = json.Marshal(fitted)
				if err != nil {
					return nil, err
				}
				if len(body) <= MaxEnvelopeBytes {
					return body, nil
				}
			}
		}
	}
	fitted.Result = json.RawMessage(`{"truncated":true}`)
	body, err = json.Marshal(fitted)
	if err != nil {
		return nil, err
	}
	if len(body) > MaxEnvelopeBytes {
		return nil, errors.New("helper envelope exceeds 256 KiB")
	}
	return body, nil
}

func withBoundedError(envelope Envelope) Envelope {
	if envelope.Error == nil {
		return envelope
	}
	toolError := *envelope.Error
	toolError.Message = trimUTF8Bytes(toolError.Message, maxErrorMessageBytes)
	toolError.Detail = boundedErrorDetail(toolError.Detail)
	envelope.Error = &toolError
	return envelope
}

func boundedErrorDetail(detail map[string]any) map[string]any {
	if len(detail) == 0 {
		return detail
	}
	if encoded, err := json.Marshal(detail); err == nil && len(encoded) <= maxErrorDetailBytes {
		return detail
	}
	for _, limit := range []struct {
		stringRunes int
		arrayItems  int
	}{
		{4096, 50},
		{1024, 20},
		{256, 10},
		{64, 3},
	} {
		fitted, ok := fitJSONValue(detail, limit.stringRunes, limit.arrayItems).(map[string]any)
		if !ok {
			continue
		}
		fitted["truncated"] = true
		if encoded, err := json.Marshal(fitted); err == nil && len(encoded) <= maxErrorDetailBytes {
			return fitted
		}
	}
	return map[string]any{"truncated": true}
}

func fitJSONValue(value any, stringRunes int, arrayItems int) any {
	switch typed := value.(type) {
	case string:
		return trimRunes(typed, stringRunes)
	case []any:
		limit := len(typed)
		if limit > arrayItems {
			limit = arrayItems
		}
		out := make([]any, 0, limit)
		for index := 0; index < limit; index++ {
			out = append(out, fitJSONValue(typed[index], stringRunes, arrayItems))
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, child := range typed {
			out[key] = fitJSONValue(child, stringRunes, arrayItems)
		}
		return out
	default:
		return value
	}
}

func trimRunes(value string, maxRunes int) string {
	if maxRunes <= 0 {
		return ""
	}
	runes := []rune(value)
	if len(runes) <= maxRunes {
		return value
	}
	if maxRunes < 32 {
		return string(runes[:maxRunes])
	}
	seam := "\n[... truncated ...]\n"
	head := (maxRunes - len([]rune(seam))) / 2
	if head < 1 {
		head = 1
	}
	tail := maxRunes - len([]rune(seam)) - head
	if tail < 1 {
		tail = 1
	}
	return string(runes[:head]) + seam + string(runes[len(runes)-tail:])
}

func trimUTF8Bytes(value string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	value = strings.ToValidUTF8(value, "\uFFFD")
	if len(value) <= maxBytes {
		return value
	}
	seam := "\n[... truncated ...]\n"
	if maxBytes <= len(seam) {
		end := maxBytes
		for end > 0 && !utf8.RuneStart(value[end]) {
			end--
		}
		return value[:end]
	}
	remaining := maxBytes - len(seam)
	headEnd := remaining / 2
	for headEnd > 0 && !utf8.RuneStart(value[headEnd]) {
		headEnd--
	}
	tailStart := len(value) - (remaining - headEnd)
	for tailStart < len(value) && !utf8.RuneStart(value[tailStart]) {
		tailStart++
	}
	return value[:headEnd] + seam + value[tailStart:]
}
