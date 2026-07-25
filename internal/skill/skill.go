// Package skill defines the Skill resource registry: workspace-scoped
// metadata, package validation, and store workflow for custom Skill
// packages. The package owns:
//
//   - Skill / SkillVersion envelope types (skill.go)
//   - Strict package validator and safe server-owned staging
//     (package.go, staging.go)
//   - SKILL.md YAML frontmatter parser confined to this package
//     (frontmatter.go)
//   - Typed errors mapped through the centralized HTTP error layer
//     (errors.go)
//
// Skills cannot be referenced by an Agent until they are uploaded to this
// registry. Upload handling must never trust client paths, and persisted
// packages are normalized zip blobs rather than the original multipart shape.
package skill

import "time"

// IDPrefix is the stable prefix every Anthropic custom Skill
// identifier uses.
const IDPrefix = "skill_"

// VersionIDPrefix is the stable prefix every SkillVersion object id
// uses. The Version field remains a separate opaque timestamp string.
const VersionIDPrefix = "skill_version_"

// Skill is the workspace-scoped Anthropic parent resource returned by
// the replacement `/v1/skills` JSON endpoints.
type Skill struct {
	ID            string    `json:"id"`
	Type          string    `json:"type"`
	Source        string    `json:"source"`
	DisplayTitle  *string   `json:"display_title"`
	LatestVersion *string   `json:"latest_version"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`

	storageSequence int64
}

// SkillVersion is the immutable Anthropic SkillVersion resource.
type SkillVersion struct {
	ID          string    `json:"id"`
	Type        string    `json:"type"`
	SkillID     string    `json:"skill_id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	Directory   string    `json:"directory"`
	Version     string    `json:"version"`
	CreatedAt   time.Time `json:"created_at"`

	storageSequence int64
}

// SkillListResult is the Anthropic list envelope for parent Skills.
type SkillListResult struct {
	Data     []*Skill `json:"data"`
	HasMore  bool     `json:"has_more"`
	NextPage *string  `json:"next_page"`
}

// SkillVersionListResult is the Anthropic list envelope for
// SkillVersion resources.
type SkillVersionListResult struct {
	Data     []*SkillVersion `json:"data"`
	HasMore  bool            `json:"has_more"`
	NextPage *string         `json:"next_page"`
}

// DeleteResponse is the standard JSON envelope returned by Skill and
// Skill-version delete endpoints. The `Type` field uses the standard
// Tetral `"<resource>_deleted"` shape.
type DeleteResponse struct {
	ID   string `json:"id"`
	Type string `json:"type"`
}
