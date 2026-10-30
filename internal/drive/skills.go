package drive

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/drive/skillpackage"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// MaxSkillArchiveBytes bounds a published Skill archive.
const MaxSkillArchiveBytes int64 = 30_000_000

var ErrSkillArchiveTooLarge = fmt.Errorf("archive exceeds the limit of %d bytes", MaxSkillArchiveBytes)

// Skill is an entry of a drive's Skills folder: a folder there that holds a
// current SKILL.md directly. Description is set when its frontmatter is
// valid, Error to the first failing rule otherwise.
type Skill struct {
	Name        string
	Description *string
	Error       *string
}

// SkillArchive is a Skill archive staged in the service's data directory.
type SkillArchive struct {
	staged *stagedBody
}

// Remove deletes the staged archive.
func (a *SkillArchive) Remove() { a.staged.remove() }

// StageSkillArchive copies an archive of at most MaxSkillArchiveBytes to a
// server-named file, or returns ErrSkillArchiveTooLarge.
func (s *Service) StageSkillArchive(archive io.Reader) (*SkillArchive, error) {
	file, err := os.CreateTemp(s.stageDir, "skill-*")
	if err != nil {
		return nil, fmt.Errorf("drive: stage skill archive: %w", err)
	}
	staged := &stagedBody{path: file.Name()}
	written, copyErr := io.Copy(file, io.LimitReader(archive, MaxSkillArchiveBytes+1))
	closeErr := file.Close()
	switch {
	case copyErr != nil:
		staged.remove()
		return nil, copyErr
	case closeErr != nil:
		staged.remove()
		return nil, closeErr
	case written > MaxSkillArchiveBytes:
		staged.remove()
		return nil, ErrSkillArchiveTooLarge
	}
	staged.size = written
	return &SkillArchive{staged: staged}, nil
}

// PublishSkill checks a staged archive and writes it to
// <skills_path><name>/ in one all_or_nothing commit, deleting the folder's
// files the archive lacks. A broken archive rule is a
// *skillpackage.ValidationError and changes nothing.
func (s *Service) PublishSkill(ctx context.Context, ws workspace.ID, driveID string, writer Writer, archive *SkillArchive) (Skill, error) {
	d, err := s.Get(ctx, ws, driveID)
	if err != nil {
		return Skill{}, err
	}
	file, err := archive.staged.open()
	if err != nil {
		return Skill{}, err
	}
	defer func() { _ = file.Close() }()
	pkg, err := skillpackage.Read(ctx, file, archive.staged.size)
	if err != nil {
		return Skill{}, err
	}
	folder := d.SkillsPath + pkg.Name + "/"
	changes := make([]Change, 0, len(pkg.Files))
	written := make(map[string]bool, len(pkg.Files))
	for _, f := range pkg.Files {
		content, err := s.putSkillFile(ctx, ws, driveID, f)
		if err != nil {
			return Skill{}, err
		}
		changes = append(changes, Change{Path: folder + f.Path, Base: Base{Kind: BaseAny}, ContentSHA256: content.SHA256, Executable: f.Executable})
		written[folder+f.Path] = true
	}
	current, err := s.filesUnder(ctx, ws, driveID, folder)
	if err != nil {
		return Skill{}, err
	}
	for _, f := range current {
		if !written[f.Path] {
			changes = append(changes, Change{Path: f.Path, Delete: true, Base: Base{Kind: BaseAny}})
		}
	}
	if _, err := s.Commit(ctx, ws, driveID, writer, ModeAllOrNothing, changes); err != nil {
		return Skill{}, err
	}
	return Skill{Name: pkg.Name, Description: &pkg.Description}, nil
}

func (s *Service) putSkillFile(ctx context.Context, ws workspace.ID, driveID string, f skillpackage.File) (Content, error) {
	body, err := f.Open()
	if err != nil {
		return Content{}, err
	}
	defer func() { _ = body.Close() }()
	return s.PutContent(ctx, ws, driveID, body, f.SizeBytes, "")
}

// filesUnder returns every current file whose path starts with folder.
func (s *Service) filesUnder(ctx context.Context, ws workspace.ID, driveID, folder string) ([]File, error) {
	var all []File
	for after := ""; ; {
		files, more, err := s.ListFiles(ctx, ws, driveID, FileQuery{After: after, Prefix: folder, Limit: 1000})
		if err != nil {
			return nil, err
		}
		all = append(all, files...)
		if !more {
			return all, nil
		}
		after = files[len(files)-1].Path
	}
}

// Skills returns at most limit entries with a name above after, in UTF-8
// byte order of name, and whether more exist.
func (s *Service) Skills(ctx context.Context, ws workspace.ID, driveID, after string, limit int) ([]Skill, bool, error) {
	type found struct{ name, sha256 string }
	var rows []found
	err := s.read(ctx, ws, driveID, "drive.skills_list", func(tx *dbconnect.Tx) error {
		// name is the one segment between skills_path and /SKILL.md.
		result, err := tx.Query(ctx, `SELECT name, f.content_sha256 FROM drives d
			JOIN drive_files f ON f.workspace_id=d.workspace_id AND f.drive_id=d.drive_id
			CROSS JOIN LATERAL (SELECT substr(f.path, length(d.skills_path)+1, greatest(0, length(f.path)-length(d.skills_path)-length('/SKILL.md'))) COLLATE "C" AS name) n
			WHERE d.workspace_id=$1 AND d.drive_id=$2 AND left(f.path, length(d.skills_path))=d.skills_path AND right(f.path, length('/SKILL.md'))='/SKILL.md'
				AND name<>'' AND strpos(name, '/')=0 AND name>$3
			ORDER BY name LIMIT $4`, string(ws), driveID, after, limit+1)
		if err != nil {
			return err
		}
		defer func() { _ = result.Close() }()
		for result.Next() {
			var row found
			if err := result.Scan(&row.name, &row.sha256); err != nil {
				return err
			}
			rows = append(rows, row)
		}
		return result.Err()
	})
	if err != nil {
		return nil, false, err
	}
	more := len(rows) > limit
	rows = rows[:min(len(rows), limit)]
	skills := make([]Skill, 0, len(rows))
	for _, row := range rows {
		skill, err := s.skillEntry(ctx, ws, driveID, row.name, row.sha256)
		if err != nil {
			return nil, false, err
		}
		skills = append(skills, skill)
	}
	return skills, more, nil
}

// Skill returns the entry of the folder name, or ErrNotFound when it holds no
// current SKILL.md.
func (s *Service) Skill(ctx context.Context, ws workspace.ID, driveID, name string) (Skill, error) {
	skillMD, err := s.skillMD(ctx, ws, driveID, name)
	if err != nil {
		return Skill{}, err
	}
	return s.skillEntry(ctx, ws, driveID, name, skillMD.ContentSHA256)
}

// DeleteSkill deletes every current file under the folder name in one
// all_or_nothing commit, or returns ErrNotFound when it holds no current
// SKILL.md.
func (s *Service) DeleteSkill(ctx context.Context, ws workspace.ID, driveID, name string, writer Writer) error {
	skillMD, err := s.skillMD(ctx, ws, driveID, name)
	if err != nil {
		return err
	}
	files, err := s.filesUnder(ctx, ws, driveID, strings.TrimSuffix(skillMD.Path, skillpackage.SkillMD))
	if err != nil {
		return err
	}
	changes := make([]Change, 0, len(files))
	for _, f := range files {
		changes = append(changes, Change{Path: f.Path, Delete: true, Base: Base{Kind: BaseAny}})
	}
	_, err = s.Commit(ctx, ws, driveID, writer, ModeAllOrNothing, changes)
	return err
}

func (s *Service) skillMD(ctx context.Context, ws workspace.ID, driveID, name string) (File, error) {
	d, err := s.Get(ctx, ws, driveID)
	if err != nil {
		return File{}, err
	}
	if name == "" || strings.Contains(name, "/") {
		return File{}, ErrNotFound
	}
	return s.CurrentFile(ctx, ws, driveID, d.SkillsPath+name+"/"+skillpackage.SkillMD)
}

func (s *Service) skillEntry(ctx context.Context, ws workspace.ID, driveID, name, sha256 string) (Skill, error) {
	body, _, err := s.OpenContent(ctx, ws, driveID, sha256)
	if err != nil {
		return Skill{}, err
	}
	defer func() { _ = body.Close() }()
	frontmatter, err := skillpackage.ReadFrontmatter(body)
	var invalid *skillpackage.ValidationError
	switch {
	case errors.As(err, &invalid):
		return Skill{Name: name, Error: &invalid.Message}, nil
	case err != nil:
		return Skill{}, err
	}
	return Skill{Name: name, Description: &frontmatter.Description}, nil
}
