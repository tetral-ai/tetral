package webconnector

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"strings"
	"time"

	providergatewayv1 "github.com/tetral-ai/tetral/services/gateway/gen/tetral/provider_gateway/v1"
)

type BindingVerifier struct {
	key []byte
	now func() time.Time
}

type bindingPayload struct {
	Version           int    `json:"v"`
	WorkspaceID       string `json:"workspace_id"`
	SessionID         string `json:"session_id"`
	SessionThreadID   string `json:"session_thread_id"`
	BindingID         string `json:"binding_id"`
	BindingGeneration int64  `json:"binding_generation"`
	RuntimePodUID     string `json:"runtime_pod_uid"`
	ExpiresAtUnix     int64  `json:"exp"`
}

func NewBindingVerifier(key []byte, now func() time.Time) *BindingVerifier {
	if now == nil {
		now = time.Now
	}
	return &BindingVerifier{key: append([]byte(nil), key...), now: now}
}

func (v *BindingVerifier) Verify(request *providergatewayv1.RunWebRequest, podUID string) bool {
	if v == nil || len(v.key) < 32 || request == nil || podUID == "" {
		return false
	}
	parts := strings.Split(request.GetRuntimeBindingToken(), ".")
	if len(parts) != 3 || parts[0] != "rtbt_v1" {
		return false
	}
	mac := hmac.New(sha256.New, v.key)
	_, _ = mac.Write([]byte(parts[1]))
	expected := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	if len(parts[2]) != len(expected) || subtle.ConstantTimeCompare([]byte(parts[2]), []byte(expected)) != 1 {
		return false
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return false
	}
	var payload bindingPayload
	if json.Unmarshal(raw, &payload) != nil || payload.Version != 1 || payload.ExpiresAtUnix <= v.now().Unix() {
		return false
	}
	return payload.WorkspaceID == request.GetWorkspaceId() && payload.SessionID == request.GetSessionId() &&
		payload.SessionThreadID == request.GetSessionThreadId() && payload.BindingID == request.GetBindingId() &&
		payload.BindingGeneration == request.GetBindingGeneration() && payload.RuntimePodUID == podUID
}
