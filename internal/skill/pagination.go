package skill

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"

	"github.com/tetral-ai/tetral/internal/workspace"
)

const (
	skillPageTokenVersion = 1
	skillPageKindSkills   = "skills"
	skillPageKindVersions = "skill_versions"
)

type skillPageTokenPayload struct {
	Version      int    `json:"version"`
	Kind         string `json:"kind"`
	Workspace    string `json:"workspace"`
	Source       string `json:"source,omitempty"`
	SkillID      string `json:"skill_id,omitempty"`
	Sequence     int64  `json:"sequence"`
	VersionValue string `json:"version_value,omitempty"`
}

type signedPageToken struct {
	Payload   skillPageTokenPayload `json:"payload"`
	Signature string                `json:"signature"`
}

func (s *Service) encodeSkillPageToken(ws workspace.ID, source string, sequence int64, skillID string) (string, error) {
	return s.encodePageToken(skillPageTokenPayload{
		Version:   skillPageTokenVersion,
		Kind:      skillPageKindSkills,
		Workspace: string(ws),
		Source:    source,
		Sequence:  sequence,
		SkillID:   skillID,
	})
}

func (s *Service) decodeSkillPageToken(raw string, ws workspace.ID, source string) (skillPageTokenPayload, error) {
	payload, err := s.decodePageToken(raw)
	if err != nil {
		return skillPageTokenPayload{}, err
	}
	if payload.Kind != skillPageKindSkills || payload.Workspace != string(ws) || payload.Source != source || payload.SkillID == "" || payload.Sequence <= 0 {
		return skillPageTokenPayload{}, &ValidationError{Message: "invalid page token"}
	}
	return payload, nil
}

func (s *Service) encodeVersionPageToken(ws workspace.ID, skillID string, sequence int64, version string) (string, error) {
	return s.encodePageToken(skillPageTokenPayload{
		Version:      skillPageTokenVersion,
		Kind:         skillPageKindVersions,
		Workspace:    string(ws),
		SkillID:      skillID,
		Sequence:     sequence,
		VersionValue: version,
	})
}

func (s *Service) decodeVersionPageToken(raw string, ws workspace.ID, skillID string) (skillPageTokenPayload, error) {
	payload, err := s.decodePageToken(raw)
	if err != nil {
		return skillPageTokenPayload{}, err
	}
	if payload.Kind != skillPageKindVersions || payload.Workspace != string(ws) || payload.SkillID != skillID || payload.VersionValue == "" || payload.Sequence <= 0 {
		return skillPageTokenPayload{}, &ValidationError{Message: "invalid page token"}
	}
	return payload, nil
}

func (s *Service) encodePageToken(payload skillPageTokenPayload) (string, error) {
	if len(s.pageTokenSecret) != 32 {
		return "", &ValidationError{Message: "invalid page token"}
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, s.pageTokenSecret)
	_, _ = mac.Write(body)
	token := signedPageToken{
		Payload:   payload,
		Signature: base64.RawURLEncoding.EncodeToString(mac.Sum(nil)),
	}
	raw, err := json.Marshal(token)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func (s *Service) decodePageToken(raw string) (skillPageTokenPayload, error) {
	if len(s.pageTokenSecret) != 32 {
		return skillPageTokenPayload{}, &ValidationError{Message: "invalid page token"}
	}
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return skillPageTokenPayload{}, &ValidationError{Message: "invalid page token"}
	}
	var token signedPageToken
	if err := json.Unmarshal(decoded, &token); err != nil {
		return skillPageTokenPayload{}, &ValidationError{Message: "invalid page token"}
	}
	if token.Payload.Version != skillPageTokenVersion {
		return skillPageTokenPayload{}, &ValidationError{Message: "invalid page token"}
	}
	body, err := json.Marshal(token.Payload)
	if err != nil {
		return skillPageTokenPayload{}, err
	}
	wantMac := hmac.New(sha256.New, s.pageTokenSecret)
	_, _ = wantMac.Write(body)
	gotSignature, err := base64.RawURLEncoding.DecodeString(token.Signature)
	if err != nil {
		return skillPageTokenPayload{}, &ValidationError{Message: "invalid page token"}
	}
	if !hmac.Equal(gotSignature, wantMac.Sum(nil)) {
		return skillPageTokenPayload{}, &ValidationError{Message: "invalid page token"}
	}
	return token.Payload, nil
}
