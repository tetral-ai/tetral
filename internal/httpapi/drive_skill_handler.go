package httpapi

import (
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/tetral-ai/tetral/internal/drive"
)

// driveSkillUploadByteCap bounds a Skill upload's whole multipart body: an
// archive of drive.MaxSkillArchiveBytes plus the multipart framing.
const driveSkillUploadByteCap = 32_505_856

type driveSkillObject struct {
	Name        string  `json:"name"`
	Description *string `json:"description"`
	Error       *string `json:"error"`
}

func newDriveSkillObject(s drive.Skill) driveSkillObject {
	return driveSkillObject{Name: s.Name, Description: s.Description, Error: s.Error}
}

// uploadSkill publishes the archive part. The drive is resolved before the
// handler runs; the budget is taken after the declared-length check and
// before any byte of the body is read.
func (h *DriveHandler) uploadSkill(w http.ResponseWriter, r *http.Request) {
	if err := requireDriveQuery(r, nil); err != nil {
		writeError(w, r, err)
		return
	}
	ws, writer, err := drivePrincipal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if r.ContentLength > driveSkillUploadByteCap {
		writeError(w, r, driveSkillUploadTooLarge())
		return
	}
	if err := h.budget.Take(r.Context(), ws, r.ContentLength); err != nil {
		writeDriveBudgetError(w, r, err)
		return
	}
	archive, err := h.readSkillUpload(w, r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	defer archive.Remove()
	skill, err := h.drives.PublishSkill(r.Context(), ws, chi.URLParam(r, "drive_id"), writer, archive)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, newDriveSkillObject(skill))
}

// readSkillUpload stages the one part, archive, of the multipart body.
func (h *DriveHandler) readSkillUpload(w http.ResponseWriter, r *http.Request) (*drive.SkillArchive, error) {
	mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "multipart/form-data" || params["boundary"] == "" {
		return nil, &ValidationError{Message: "the body must be multipart/form-data"}
	}
	reader := multipart.NewReader(http.MaxBytesReader(w, r.Body, driveSkillUploadByteCap), params["boundary"])
	part, err := reader.NextPart()
	switch {
	case errors.Is(err, io.EOF):
		return nil, &ValidationError{Message: "multipart part archive is required"}
	case err != nil:
		return nil, driveSkillReadError(err)
	case part.FormName() != "archive":
		return nil, &ValidationError{Message: "the body must hold exactly one multipart part, archive"}
	}
	archive, err := h.drives.StageSkillArchive(part)
	if err != nil {
		return nil, driveSkillReadError(err)
	}
	if _, err := reader.NextPart(); !errors.Is(err, io.EOF) {
		archive.Remove()
		if err != nil {
			return nil, driveSkillReadError(err)
		}
		return nil, &ValidationError{Message: "the body must hold exactly one multipart part, archive"}
	}
	return archive, nil
}

func driveSkillUploadTooLarge() error {
	return &RequestTooLargeError{Message: fmt.Sprintf("request body exceeds the limit of %d bytes", driveSkillUploadByteCap)}
}

func driveSkillReadError(err error) error {
	var tooLarge *http.MaxBytesError
	switch {
	case errors.As(err, &tooLarge):
		return driveSkillUploadTooLarge()
	case errors.Is(err, drive.ErrSkillArchiveTooLarge):
		return err
	}
	return &ValidationError{Message: "the multipart body could not be read"}
}

func (h *DriveHandler) listSkills(w http.ResponseWriter, r *http.Request) {
	if err := requireDriveQuery(r, []string{"limit", "page"}); err != nil {
		writeError(w, r, err)
		return
	}
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	driveID := chi.URLParam(r, "drive_id")
	binding := drivePageBinding{WorkspaceID: string(ws), Route: "drives.skills.list", DriveID: driveID}
	limit, after, err := h.parsePage(r, binding)
	if err != nil {
		writeError(w, r, err)
		return
	}
	skills, more, err := h.drives.Skills(r.Context(), ws, driveID, after, limit)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if !authorizeReadResource(w, r, ws, "drive", driveID) {
		return
	}
	page := drivePage[driveSkillObject]{Data: make([]driveSkillObject, 0, len(skills))}
	for _, s := range skills {
		page.Data = append(page.Data, newDriveSkillObject(s))
	}
	if more {
		page.NextPage = h.nextPage(binding, skills[len(skills)-1].Name)
	}
	writeJSON(w, http.StatusOK, page)
}

func (h *DriveHandler) getSkill(w http.ResponseWriter, r *http.Request) {
	if err := requireDriveQuery(r, nil); err != nil {
		writeError(w, r, err)
		return
	}
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	driveID := chi.URLParam(r, "drive_id")
	skill, err := h.drives.Skill(r.Context(), ws, driveID, chi.URLParam(r, "name"))
	if err != nil {
		writeError(w, r, err)
		return
	}
	if !authorizeReadResource(w, r, ws, "drive", driveID) {
		return
	}
	writeJSON(w, http.StatusOK, newDriveSkillObject(skill))
}

func (h *DriveHandler) deleteSkill(w http.ResponseWriter, r *http.Request) {
	if err := requireDriveQuery(r, nil); err != nil {
		writeError(w, r, err)
		return
	}
	ws, writer, err := drivePrincipal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if err := h.drives.DeleteSkill(r.Context(), ws, chi.URLParam(r, "drive_id"), chi.URLParam(r, "name"), writer); err != nil {
		writeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
