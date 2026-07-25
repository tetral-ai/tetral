package agent

import (
	"encoding/json"
)

// SkillReference is the runtime-agnostic projection of one
// `skills[]` entry. Agent input validation has already normalized the
// caller's bytes into `{"type":"custom","skill_id":"...","version":"..."}`
// by the time SkillReference values are constructed.
type SkillReference struct {
	SkillID string
	Version string
}

// decodeSkillReferences projects the canonical `{skill_id, version}`
// objects into the runtime-agnostic SkillReference slice the Skill
// Service resolver consumes. Empty input returns nil so the resolver
// can skip the workspace-lock fast path for skills-free Agents.
func decodeSkillReferences(skills RawArray) ([]SkillReference, error) {
	if len(skills) == 0 {
		return nil, nil
	}
	refs := make([]SkillReference, 0, len(skills))
	for i, raw := range skills {
		var entry struct {
			SkillID string `json:"skill_id"`
			Version string `json:"version"`
		}
		if err := jsonUnmarshal(raw, &entry); err != nil {
			return nil, &ValidationError{Message: "skills[" + itoa(i) + "] is not a valid object"}
		}
		refs = append(refs, SkillReference{SkillID: entry.SkillID, Version: entry.Version})
	}
	return refs, nil
}

// jsonUnmarshal is a tiny indirection so the unmarshal call site can
// later be swapped for a streaming decoder if needed without a churn
// in surrounding code. The implementation is the standard library
// json.Unmarshal.
var jsonUnmarshal = json.Unmarshal

// itoa is a stdlib-free integer formatter used by error messages so
// this leaf helper does not pull strconv into the package's import
// graph for a single decimal digit conversion.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	negative := n < 0
	if negative {
		n = -n
	}
	var buf [20]byte
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	if negative {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
