// Package skillpackage checks a Skill archive published to a drive: a zip
// with one root folder holding SKILL.md. It is the only checker of a drive
// Skill's SKILL.md; every failed rule is a *ValidationError naming the rule.
package skillpackage

import (
	"archive/zip"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"strings"
	"unicode/utf8"
)

const (
	MaxEntries       = 1000
	MaxEntryBytes    = 10_485_760
	MaxExpandedBytes = 209_715_200
	SkillMD          = "SKILL.md"
)

const (
	zipCreatorFAT    = 0
	zipCreatorUnix   = 3
	zipCreatorNTFS   = 11
	zipCreatorVFAT   = 14
	zipCreatorMacOSX = 19

	zipUnixModeType    = 0xf000
	zipUnixModeRegular = 0x8000
	zipUnixModeDir     = 0x4000
	zipUnixModeSetuid  = 0x0800
	zipUnixModeSetgid  = 0x0400
	zipUnixModeSticky  = 0x0200
	zipUnixOwnerExec   = 0o100

	zipMSDOSDir      = 0x10
	zipMSDOSReadOnly = 0x01
)

// ValidationError is a broken archive rule.
type ValidationError struct {
	Message string
}

func (e *ValidationError) Error() string { return e.Message }

// Package is a checked archive: its SKILL.md frontmatter and its regular
// files, by path below the root folder.
type Package struct {
	Frontmatter
	Files []File
}

// File is a regular entry of a checked archive.
type File struct {
	Path       string
	Executable bool
	SizeBytes  int64
	entry      *zip.File
}

// Open returns the entry's content, SizeBytes long.
func (f File) Open() (io.ReadCloser, error) { return f.entry.Open() }

// Read checks the archive of the given size. It reads every entry once to
// measure what it expands to, so a declared size never counts.
func Read(ctx context.Context, archive io.ReaderAt, size int64) (*Package, error) {
	// An insecure entry name comes with a usable reader; checkPath gives its
	// reason.
	reader, err := zip.NewReader(archive, size)
	if err != nil && !errors.Is(err, zip.ErrInsecurePath) {
		return nil, &ValidationError{Message: "the archive is not a valid zip archive"}
	}
	if len(reader.File) > MaxEntries {
		return nil, &ValidationError{Message: fmt.Sprintf("the archive has more than %d entries", MaxEntries)}
	}
	tree := packageTree{files: map[string]bool{}, dirs: map[string]bool{}, explicitDirs: map[string]bool{}}
	pkg := &Package{}
	var expanded int64
	var skillMD *zip.File
	for _, entry := range reader.File {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		clean, isDir, err := checkEntry(entry)
		if err != nil {
			return nil, err
		}
		if err := tree.add(clean, isDir); err != nil {
			return nil, err
		}
		if isDir {
			continue
		}
		written, err := measure(entry)
		if err != nil {
			return nil, err
		}
		if expanded += written; expanded > MaxExpandedBytes {
			return nil, &ValidationError{Message: fmt.Sprintf("the archive expands to more than %d bytes", MaxExpandedBytes)}
		}
		rest := strings.TrimPrefix(clean, tree.root+"/")
		if rest == SkillMD {
			skillMD = entry
		}
		pkg.Files = append(pkg.Files, File{Path: rest, Executable: ownerExecutable(entry), SizeBytes: written, entry: entry})
	}
	if skillMD == nil {
		return nil, &ValidationError{Message: "the archive's root folder must hold SKILL.md"}
	}
	body, err := skillMD.Open()
	if err != nil {
		return nil, &ValidationError{Message: "an archive entry is not readable"}
	}
	defer func() { _ = body.Close() }()
	if pkg.Frontmatter, err = ReadFrontmatter(body); err != nil {
		var invalid *ValidationError
		if !errors.As(err, &invalid) {
			return nil, &ValidationError{Message: "an archive entry is not readable"}
		}
		return nil, err
	}
	return pkg, nil
}

// measure reads an entry to its end, refusing it past MaxEntryBytes.
func measure(entry *zip.File) (int64, error) {
	body, err := entry.Open()
	if err != nil {
		return 0, &ValidationError{Message: "an archive entry is not readable"}
	}
	defer func() { _ = body.Close() }()
	written, err := io.Copy(io.Discard, io.LimitReader(body, MaxEntryBytes+1))
	switch {
	case err != nil:
		return 0, &ValidationError{Message: "an archive entry is not readable"}
	case written > MaxEntryBytes:
		return 0, &ValidationError{Message: fmt.Sprintf("an archive entry expands to more than %d bytes", MaxEntryBytes)}
	}
	return written, nil
}

// ownerExecutable reports the owner execute bit of an entry's Unix mode; an
// entry without a Unix mode is not executable.
func ownerExecutable(entry *zip.File) bool {
	creator := entry.CreatorVersion >> 8
	return (creator == zipCreatorUnix || creator == zipCreatorMacOSX) && (entry.ExternalAttrs>>16)&zipUnixOwnerExec != 0
}

// checkEntry returns the entry's clean path and whether it is a directory.
func checkEntry(entry *zip.File) (string, bool, error) {
	if entry.NonUTF8 || entry.Comment != "" || len(entry.Extra) != 0 {
		return "", false, &ValidationError{Message: "archive entry names must be UTF-8, without comments or extra fields"}
	}
	mode := entry.Mode()
	isDir := strings.HasSuffix(entry.Name, "/") || mode.IsDir()
	if err := checkExternalAttrs(entry, isDir); err != nil {
		return "", false, err
	}
	clean, err := checkPath(entry.Name, isDir)
	if err != nil {
		return "", false, err
	}
	if err := checkMode(mode, isDir); err != nil {
		return "", false, err
	}
	return clean, isDir, nil
}

func checkMode(mode fs.FileMode, isDir bool) error {
	typeBits := mode.Type()
	if isDir {
		typeBits &^= fs.ModeDir
	}
	switch {
	case typeBits != 0:
		return errEntryType
	case mode&^(fs.ModeType|0o777) != 0:
		return errSpecialBits
	}
	return nil
}

var (
	errEntryType   = &ValidationError{Message: "archive entries must be regular files or directories, not links or special files"}
	errSpecialBits = &ValidationError{Message: "archive entries must not carry setuid, setgid or sticky bits"}
	errMetadata    = &ValidationError{Message: "archive entry metadata is not supported"}
)

func checkExternalAttrs(entry *zip.File, isDir bool) error {
	if entry.ExternalAttrs == 0 {
		return nil
	}
	switch entry.CreatorVersion >> 8 {
	case zipCreatorUnix, zipCreatorMacOSX:
		return checkUnixAttrs(entry.ExternalAttrs, isDir)
	case zipCreatorFAT, zipCreatorNTFS, zipCreatorVFAT:
		return checkMSDOSAttrs(entry.ExternalAttrs, isDir)
	default:
		return errMetadata
	}
}

func checkUnixAttrs(attrs uint32, isDir bool) error {
	unixMode := attrs >> 16
	if unixMode == 0 {
		return checkMSDOSAttrs(attrs, isDir)
	}
	typeBits := unixMode & zipUnixModeType
	switch {
	case isDir && typeBits != zipUnixModeDir, !isDir && typeBits != zipUnixModeRegular:
		return errEntryType
	case unixMode&(zipUnixModeSetuid|zipUnixModeSetgid|zipUnixModeSticky) != 0, unixMode&^(typeBits|0o777) != 0:
		return errSpecialBits
	}
	msdos := attrs & 0xffff
	switch {
	case msdos&^(zipMSDOSDir|zipMSDOSReadOnly) != 0:
		return errMetadata
	case !isDir && msdos&zipMSDOSDir != 0:
		return errEntryType
	}
	return nil
}

func checkMSDOSAttrs(attrs uint32, isDir bool) error {
	switch {
	case attrs&^(zipMSDOSDir|zipMSDOSReadOnly) != 0:
		return errMetadata
	case isDir && attrs&zipMSDOSDir == 0:
		return errMetadata
	case !isDir && attrs&zipMSDOSDir != 0:
		return errEntryType
	}
	return nil
}

var errPath = &ValidationError{Message: "archive entry paths must be relative paths inside the root folder, without empty, ., .. or \\ segments or a drive letter"}

// checkPath returns an entry name without a directory's trailing slash. A
// regular file must lie inside a folder.
func checkPath(name string, isDir bool) (string, error) {
	if isDir {
		name = strings.TrimSuffix(name, "/")
	}
	if name == "" || !utf8.ValidString(name) || len(name) > 4096 || strings.ContainsAny(name, "\x00\\") ||
		strings.HasPrefix(name, "/") || hasDriveLetter(name) {
		return "", errPath
	}
	for _, segment := range strings.Split(name, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return "", errPath
		}
	}
	if !isDir && !strings.Contains(name, "/") {
		return "", errPath
	}
	return name, nil
}

func hasDriveLetter(name string) bool {
	return len(name) >= 2 && name[1] == ':' && ((name[0] >= 'A' && name[0] <= 'Z') || (name[0] >= 'a' && name[0] <= 'z'))
}

// packageTree holds the paths seen so far, to refuse duplicates, a file where
// a folder is needed and a second root folder.
type packageTree struct {
	root         string
	files        map[string]bool
	dirs         map[string]bool
	explicitDirs map[string]bool
}

func (t *packageTree) add(clean string, isDir bool) error {
	root, _, _ := strings.Cut(clean, "/")
	if t.root == "" {
		t.root = root
	} else if root != t.root {
		return &ValidationError{Message: "the archive must hold exactly one root folder"}
	}
	switch {
	case t.files[clean] && isDir, !isDir && t.dirs[clean]:
		return errConflict
	case t.files[clean], isDir && t.explicitDirs[clean]:
		return &ValidationError{Message: "the archive holds a path twice"}
	}
	for dir := path.Dir(clean); dir != "."; dir = path.Dir(dir) {
		if t.files[dir] {
			return errConflict
		}
		t.dirs[dir] = true
	}
	if isDir {
		t.dirs[clean] = true
		t.explicitDirs[clean] = true
	} else {
		t.files[clean] = true
	}
	return nil
}

var errConflict = &ValidationError{Message: "the archive holds a file where another entry needs a folder"}
