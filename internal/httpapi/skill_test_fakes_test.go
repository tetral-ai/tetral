package httpapi_test

import (
	"bytes"
	"context"
	"io"
	"sync"
	"time"

	"github.com/tetral-ai/tetral/internal/skill"
	"github.com/tetral-ai/tetral/internal/workspace"
)

type fakeSkillStore struct {
	mu       sync.Mutex
	calls    []string
	skillRow *skill.Skill
	version  *skill.SkillVersion
	content  []byte
	closed   bool
}

func newRouterTestSkillStore() *fakeSkillStore {
	latest := "1"
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return &fakeSkillStore{
		skillRow: &skill.Skill{
			ID:            "skill_x",
			Type:          "skill",
			Source:        "custom",
			LatestVersion: &latest,
			CreatedAt:     now,
			UpdatedAt:     now,
		},
		version: &skill.SkillVersion{
			ID:          "skill_version_x",
			Type:        "skill_version",
			SkillID:     "skill_x",
			Name:        "router-test",
			Description: "stub",
			Directory:   "router-test",
			Version:     "1",
			CreatedAt:   now,
		},
		content: []byte("router-test-durable-zip"),
	}
}

func (f *fakeSkillStore) reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = nil
}

func (f *fakeSkillStore) wasCalled(name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.calls {
		if c == name {
			return true
		}
	}
	return false
}

func (f *fakeSkillStore) record(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, name)
}

func (f *fakeSkillStore) CreateSkill(_ context.Context, _ workspace.ID, _ skill.CreateSkillInput) (*skill.Skill, error) {
	f.record("create_skill")
	return f.skillRow, nil
}

func (f *fakeSkillStore) CreateVersion(_ context.Context, _ workspace.ID, _ string, _ skill.CreateVersionInput) (*skill.SkillVersion, error) {
	f.record("create_version")
	return f.version, nil
}

func (f *fakeSkillStore) GetSkill(_ context.Context, _ workspace.ID, _ string) (*skill.Skill, error) {
	f.record("get_skill")
	return f.skillRow, nil
}

func (f *fakeSkillStore) GetVersion(_ context.Context, _ workspace.ID, _, _ string) (*skill.SkillVersion, error) {
	f.record("get_version")
	return f.version, nil
}

func (f *fakeSkillStore) OpenVersionContent(_ context.Context, _ workspace.ID, _, _ string) (io.ReadCloser, error) {
	f.record("open_version_content")
	return &trackingSkillContentReader{Reader: bytes.NewReader(f.content), onClose: func() { f.closed = true }}, nil
}

func (f *fakeSkillStore) ListSkills(_ context.Context, _ workspace.ID, _ skill.ListSkillsOptions) (skill.SkillListResult, error) {
	f.record("list_skills")
	return skill.SkillListResult{Data: []*skill.Skill{}, HasMore: false}, nil
}

func (f *fakeSkillStore) ListVersions(_ context.Context, _ workspace.ID, _ string, _ skill.ListVersionsOptions) (skill.SkillVersionListResult, error) {
	f.record("list_versions")
	return skill.SkillVersionListResult{Data: []*skill.SkillVersion{}, HasMore: false}, nil
}

func (f *fakeSkillStore) DeleteSkill(_ context.Context, _ workspace.ID, _ string) error {
	f.record("delete_skill")
	return nil
}

func (f *fakeSkillStore) DeleteVersion(_ context.Context, _ workspace.ID, _, _ string) error {
	f.record("delete_version")
	return nil
}

type trackingSkillContentReader struct {
	*bytes.Reader
	onClose func()
}

func (r *trackingSkillContentReader) Close() error {
	if r.onClose != nil {
		r.onClose()
	}
	return nil
}
