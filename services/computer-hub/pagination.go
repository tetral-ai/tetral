package computerhub

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/tetral-ai/tetral/internal/httpapi"
)

// The list page size when limit is absent, and the largest page; a larger
// limit is treated as maxPageLimit.
const (
	defaultPageLimit = 20
	maxPageLimit     = 100
)

// pageOrder is the one order every hub list uses: ascending storage
// sequence, which is creation order.
const pageOrder = "storage_sequence"

// pageBinding is what a page token is valid for. A token verifies only for
// the binding it was issued under, so a token of one list, workspace or
// filter is refused by every other.
type pageBinding struct {
	WorkspaceID string            `json:"workspace_id"`
	Route       string            `json:"route"`
	Filters     map[string]string `json:"filters"`
	Order       string            `json:"order"`
}

// listPage is one parsed list request: the page size and the storage
// sequence after which the page starts (0 for the first page).
type listPage struct {
	limit int
	after int64
}

type pageTokenPayload struct {
	After int64 `json:"after"`
}

type pageToken struct {
	Payload   pageTokenPayload `json:"payload"`
	Signature string           `json:"signature"`
}

// listResponse is the body of every hub list.
type listResponse[T any] struct {
	Data     []T     `json:"data"`
	NextPage *string `json:"next_page"`
}

// parseListPage reads limit and page. limit is absent (defaultPageLimit) or a
// positive decimal integer given once; page is absent, empty or a token that
// verifies under key for binding.
func parseListPage(r *http.Request, key []byte, binding pageBinding) (listPage, error) {
	query := r.URL.Query()
	page := listPage{limit: defaultPageLimit}
	switch limits := query["limit"]; len(limits) {
	case 0:
	case 1:
		limit, ok := parsePageLimit(limits[0])
		if !ok {
			return listPage{}, &httpapi.ValidationError{Message: "limit must be a positive integer"}
		}
		page.limit = limit
	default:
		return listPage{}, &httpapi.ValidationError{Message: "limit must be given at most once"}
	}
	switch pages := query["page"]; len(pages) {
	case 0:
	case 1:
		if pages[0] == "" {
			break
		}
		after, ok := verifyPageToken(key, binding, pages[0])
		if !ok {
			return listPage{}, &httpapi.ValidationError{Message: "page is not a page token of this list"}
		}
		page.after = after
	default:
		return listPage{}, &httpapi.ValidationError{Message: "page must be given at most once"}
	}
	return page, nil
}

func parsePageLimit(raw string) (int, bool) {
	if raw == "" || strings.Trim(raw, "0123456789") != "" {
		return 0, false
	}
	digits := strings.TrimLeft(raw, "0")
	if digits == "" {
		return 0, false
	}
	if len(digits) > len(strconv.Itoa(maxPageLimit)) {
		return maxPageLimit, true
	}
	limit, _ := strconv.Atoi(digits)
	return min(limit, maxPageLimit), true
}

// nextPage returns the token of the page after the item at storage sequence
// last, or nil when more is false.
func nextPage(key []byte, binding pageBinding, last int64, more bool) *string {
	if !more {
		return nil
	}
	payload := pageTokenPayload{After: last}
	raw, _ := json.Marshal(pageToken{Payload: payload, Signature: signPage(key, binding, payload)})
	token := base64.RawURLEncoding.EncodeToString(raw)
	return &token
}

func verifyPageToken(key []byte, binding pageBinding, raw string) (int64, bool) {
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return 0, false
	}
	var token pageToken
	if json.Unmarshal(decoded, &token) != nil || token.Payload.After <= 0 {
		return 0, false
	}
	if !hmac.Equal([]byte(token.Signature), []byte(signPage(key, binding, token.Payload))) {
		return 0, false
	}
	return token.Payload.After, true
}

func signPage(key []byte, binding pageBinding, payload pageTokenPayload) string {
	signed, _ := json.Marshal(struct {
		Binding pageBinding      `json:"binding"`
		Payload pageTokenPayload `json:"payload"`
	}{binding, payload})
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(signed)
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}
