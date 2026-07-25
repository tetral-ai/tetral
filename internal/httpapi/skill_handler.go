package httpapi

import (
	"context"
	"errors"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"unicode/utf8"

	"github.com/go-chi/chi/v5"

	"github.com/tetral-ai/tetral/internal/skill"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const (
	skillUploadCap               = 31 * 1024 * 1024
	displayTitleFieldByteCap     = 1024*4 + 1
	defaultSkillListLimit        = 20
	maxSkillListLimit            = 100
	defaultSkillVersionListLimit = 20
	maxSkillVersionListLimit     = 1000
)

type SkillService interface {
	CreateSkill(ctx context.Context, ws workspace.ID, input skill.CreateSkillInput) (*skill.Skill, error)
	CreateVersion(ctx context.Context, ws workspace.ID, skillID string, input skill.CreateVersionInput) (*skill.SkillVersion, error)
	GetSkill(ctx context.Context, ws workspace.ID, skillID string) (*skill.Skill, error)
	ListSkills(ctx context.Context, ws workspace.ID, options skill.ListSkillsOptions) (skill.SkillListResult, error)
	DeleteSkill(ctx context.Context, ws workspace.ID, skillID string) error
	GetVersion(ctx context.Context, ws workspace.ID, skillID, version string) (*skill.SkillVersion, error)
	OpenVersionContent(ctx context.Context, ws workspace.ID, skillID, version string) (io.ReadCloser, error)
	ListVersions(ctx context.Context, ws workspace.ID, skillID string, options skill.ListVersionsOptions) (skill.SkillVersionListResult, error)
	DeleteVersion(ctx context.Context, ws workspace.ID, skillID, version string) error
}

type SkillHandler struct {
	service  SkillService
	stageDir string
}

func NewSkillHandler(service SkillService, stageDir string) *SkillHandler {
	return &SkillHandler{service: service, stageDir: stageDir}
}

func (h *SkillHandler) createSkill(w http.ResponseWriter, r *http.Request) {
	if _, err := requireBetaQuery(r); err != nil {
		writeError(w, r, err)
		return
	}
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	parsed, err := h.parseUploadMultipart(w, r, true)
	if err != nil {
		writeError(w, r, err)
		return
	}
	defer func() { _ = skill.CleanupStagedUploadParts(parsed.files) }()
	created, err := h.service.CreateSkill(r.Context(), ws, skill.CreateSkillInput{
		DisplayTitle: parsed.displayTitle,
		Files:        parsed.files,
	})
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, created)
}

func (h *SkillHandler) createVersion(w http.ResponseWriter, r *http.Request) {
	if _, err := requireBetaQuery(r); err != nil {
		writeError(w, r, err)
		return
	}
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	parsed, err := h.parseUploadMultipart(w, r, false)
	if err != nil {
		writeError(w, r, err)
		return
	}
	defer func() { _ = skill.CleanupStagedUploadParts(parsed.files) }()
	created, err := h.service.CreateVersion(r.Context(), ws, chi.URLParam(r, "skill_id"), skill.CreateVersionInput{Files: parsed.files})
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, created)
}

type parsedSkillUpload struct {
	displayTitle *string
	files        []skill.StagedUploadPart
}

func (h *SkillHandler) parseUploadMultipart(w http.ResponseWriter, r *http.Request, allowDisplayTitle bool) (*parsedSkillUpload, error) {
	r.Body = http.MaxBytesReader(w, r.Body, skillUploadCap)
	mr, err := r.MultipartReader()
	if err != nil {
		return nil, &skill.ValidationError{Message: "request must be multipart/form-data"}
	}
	var budget skill.UploadBudget
	parsed := &parsedSkillUpload{}
	cleanupOnError := func() {
		_ = skill.CleanupStagedUploadParts(parsed.files)
		parsed.files = nil
	}
	var displayTitleSet bool
	for {
		part, err := mr.NextPart()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			cleanupOnError()
			if isMaxBytesError(err) {
				return nil, &skill.RequestTooLargeError{Message: "request body too large"}
			}
			return nil, &skill.ValidationError{Message: "multipart body is malformed"}
		}
		switch part.FormName() {
		case "display_title":
			if !allowDisplayTitle {
				cleanupOnError()
				return nil, &skill.ValidationError{Message: "version upload must not include display_title"}
			}
			if _, ok := multipartFilename(part); ok {
				cleanupOnError()
				return nil, &skill.ValidationError{Message: "display_title must be a text field"}
			}
			if displayTitleSet {
				cleanupOnError()
				return nil, &skill.ValidationError{Message: "request contains multiple display_title fields"}
			}
			title, err := readDisplayTitle(part)
			if err != nil {
				cleanupOnError()
				return nil, err
			}
			displayTitleSet = true
			parsed.displayTitle = &title
		case "files[]":
			filename, ok := multipartFilename(part)
			if !ok {
				cleanupOnError()
				return nil, &skill.ValidationError{Message: "files[] must be file parts"}
			}
			staged, err := skill.StageUploadPart(r.Context(), skillMaxBytesMappingReader{reader: part}, h.stageDir, filename, &budget)
			if err != nil {
				cleanupOnError()
				return nil, err
			}
			parsed.files = append(parsed.files, staged)
		default:
			cleanupOnError()
			return nil, &skill.ValidationError{Message: "request contains an unexpected form part"}
		}
	}
	if len(parsed.files) == 0 {
		cleanupOnError()
		return nil, &skill.ValidationError{Message: "request is missing files[]"}
	}
	return parsed, nil
}

func readDisplayTitle(part *multipart.Part) (string, error) {
	body, err := io.ReadAll(io.LimitReader(skillMaxBytesMappingReader{reader: part}, displayTitleFieldByteCap+1))
	if err != nil {
		var tooLarge *skill.RequestTooLargeError
		if errors.As(err, &tooLarge) {
			return "", tooLarge
		}
		return "", &skill.ValidationError{Message: "display_title is malformed"}
	}
	if len(body) > displayTitleFieldByteCap {
		return "", &skill.ValidationError{Message: "display_title must be at most 1024 characters"}
	}
	title := string(body)
	if !utf8.ValidString(title) {
		return "", &skill.ValidationError{Message: "display_title must be valid UTF-8"}
	}
	return title, nil
}

func multipartFilename(part *multipart.Part) (string, bool) {
	_, params, err := mime.ParseMediaType(part.Header.Get("Content-Disposition"))
	if err != nil {
		return "", false
	}
	filename, ok := params["filename"]
	return filename, ok
}

type skillMaxBytesMappingReader struct {
	reader io.Reader
}

func (r skillMaxBytesMappingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if err != nil && isMaxBytesError(err) {
		return n, &skill.RequestTooLargeError{Message: "request body too large"}
	}
	return n, err
}

func (h *SkillHandler) getSkill(w http.ResponseWriter, r *http.Request) {
	if _, err := requireBetaQuery(r); err != nil {
		writeError(w, r, err)
		return
	}
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	got, err := h.service.GetSkill(r.Context(), ws, chi.URLParam(r, "skill_id"))
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, got)
}

func (h *SkillHandler) getVersion(w http.ResponseWriter, r *http.Request) {
	if _, err := requireBetaQuery(r); err != nil {
		writeError(w, r, err)
		return
	}
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	version := chi.URLParam(r, "version")
	if version == "" {
		writeError(w, r, &skill.NotFoundError{Message: "skill version not found"})
		return
	}
	got, err := h.service.GetVersion(r.Context(), ws, chi.URLParam(r, "skill_id"), version)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, got)
}

func (h *SkillHandler) getVersionContent(w http.ResponseWriter, r *http.Request) {
	if _, err := requireBetaQuery(r); err != nil {
		writeError(w, r, err)
		return
	}
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	version := chi.URLParam(r, "version")
	if version == "" {
		writeError(w, r, &skill.NotFoundError{Message: "skill version not found"})
		return
	}
	content, err := h.service.OpenVersionContent(r.Context(), ws, chi.URLParam(r, "skill_id"), version)
	if err != nil {
		writeError(w, r, err)
		return
	}
	defer func() { _ = content.Close() }()
	w.Header().Set("Content-Type", "application/zip")
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, content)
}

func (h *SkillHandler) listSkills(w http.ResponseWriter, r *http.Request) {
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	options, err := parseSkillListOptions(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	result, err := h.service.ListSkills(r.Context(), ws, options)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *SkillHandler) listVersions(w http.ResponseWriter, r *http.Request) {
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	options, err := parseSkillVersionListOptions(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	result, err := h.service.ListVersions(r.Context(), ws, chi.URLParam(r, "skill_id"), options)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func parseSkillListOptions(r *http.Request) (skill.ListSkillsOptions, error) {
	values, err := parseStrictSkillQuery(r, []string{"limit", "page", "source"})
	if err != nil {
		return skill.ListSkillsOptions{}, err
	}
	options := skill.ListSkillsOptions{Limit: defaultSkillListLimit}
	if rawValues, ok := values["limit"]; ok {
		limit, err := parseSkillLimit(rawValues[0], maxSkillListLimit)
		if err != nil {
			return skill.ListSkillsOptions{}, err
		}
		options.Limit = limit
	}
	if rawValues, ok := values["page"]; ok {
		options.Page = rawValues[0]
		if options.Page == "" {
			return skill.ListSkillsOptions{}, &skill.ValidationError{Message: "page must be a non-empty token"}
		}
	}
	if rawValues, ok := values["source"]; ok {
		options.Source = rawValues[0]
		if options.Source == "" {
			return skill.ListSkillsOptions{}, &skill.ValidationError{Message: "source must be custom"}
		}
	}
	return options, nil
}

func parseSkillVersionListOptions(r *http.Request) (skill.ListVersionsOptions, error) {
	values, err := parseStrictSkillQuery(r, []string{"limit", "page"})
	if err != nil {
		return skill.ListVersionsOptions{}, err
	}
	options := skill.ListVersionsOptions{Limit: defaultSkillVersionListLimit}
	if rawValues, ok := values["limit"]; ok {
		limit, err := parseSkillLimit(rawValues[0], maxSkillVersionListLimit)
		if err != nil {
			return skill.ListVersionsOptions{}, err
		}
		options.Limit = limit
	}
	if rawValues, ok := values["page"]; ok {
		options.Page = rawValues[0]
		if options.Page == "" {
			return skill.ListVersionsOptions{}, &skill.ValidationError{Message: "page must be a non-empty token"}
		}
	}
	return options, nil
}

func parseStrictSkillQuery(r *http.Request, allowed []string) (url.Values, error) {
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return nil, &skill.ValidationError{Message: "invalid query string"}
	}
	allowedSet := make(map[string]struct{}, len(allowed)+1)
	allowedSet["beta"] = struct{}{}
	for _, key := range allowed {
		allowedSet[key] = struct{}{}
	}
	for key, rawValues := range values {
		if len(rawValues) != 1 {
			return nil, &skill.ValidationError{Message: "duplicate query parameter " + key}
		}
		if rawValues[0] == "" {
			return nil, &skill.ValidationError{Message: "empty query parameter " + key}
		}
		if _, ok := allowedSet[key]; !ok {
			return nil, &skill.ValidationError{Message: "unknown query parameter " + key}
		}
	}
	if rawValues, ok := values["beta"]; !ok || rawValues[0] != "true" {
		return nil, &skill.ValidationError{Message: "beta must be true"}
	}
	delete(values, "beta")
	return values, nil
}

func parseSkillLimit(raw string, maxLimit int) (int, error) {
	limit, err := strconv.Atoi(raw)
	if err != nil || limit <= 0 {
		return 0, &skill.ValidationError{Message: "limit must be a positive integer"}
	}
	if limit > maxLimit {
		return 0, &skill.ValidationError{Message: "limit exceeds the maximum allowed value"}
	}
	return limit, nil
}

func (h *SkillHandler) deleteSkill(w http.ResponseWriter, r *http.Request) {
	if _, err := requireBetaQuery(r); err != nil {
		writeError(w, r, err)
		return
	}
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	skillID := chi.URLParam(r, "skill_id")
	if err := h.service.DeleteSkill(r.Context(), ws, skillID); err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, skill.DeleteResponse{ID: skillID, Type: "skill_deleted"})
}

func (h *SkillHandler) deleteVersion(w http.ResponseWriter, r *http.Request) {
	if _, err := requireBetaQuery(r); err != nil {
		writeError(w, r, err)
		return
	}
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	version := chi.URLParam(r, "version")
	if version == "" {
		writeError(w, r, &skill.NotFoundError{Message: "skill version not found"})
		return
	}
	if err := h.service.DeleteVersion(r.Context(), ws, chi.URLParam(r, "skill_id"), version); err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, skill.DeleteResponse{ID: version, Type: "skill_version_deleted"})
}

func isMaxBytesError(err error) bool {
	var maxErr *http.MaxBytesError
	return errors.As(err, &maxErr)
}
