package skill

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	MaxUploadFileBytes      int64 = 30_000_000
	MaxUploadFileParts            = 1000
	MaxPackageEntryBytes    int64 = 10 * 1024 * 1024
	MaxPackageExpandedBytes int64 = 200 * 1024 * 1024
	MaxNormalizedZipBytes   int64 = 31 * 1024 * 1024
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

	zipMSDOSDir      = 0x10
	zipMSDOSReadOnly = 0x01
)

var normalizedZipTime = time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)

// UploadBudget tracks cumulative files[] upload limits for one
// request. The budget is intentionally caller-owned so HTTP can stage
// parts incrementally without package-level mutable state.
type UploadBudget struct {
	Bytes int64
	Parts int
}

// StagedPackage is a validated, normalized custom Skill package ready
// for blob storage. It owns a server-named temp file containing the
// deterministic normalized zip bytes.
type StagedPackage struct {
	Name        string
	Description string
	Directory   string
	SizeBytes   int64
	SHA256      string

	tempPath string
}

// StageUploadPart streams one multipart files[] part to a server-owned
// temp file while updating request-wide byte and part budgets.
func StageUploadPart(ctx context.Context, reader io.Reader, stageDir, filename string, budget *UploadBudget) (StagedUploadPart, error) {
	if err := ctx.Err(); err != nil {
		return StagedUploadPart{}, err
	}
	if stageDir == "" {
		return StagedUploadPart{}, errors.New("skill: stageDir is required")
	}
	if budget == nil {
		return StagedUploadPart{}, errors.New("skill: upload budget is required")
	}
	if budget.Parts >= MaxUploadFileParts {
		return StagedUploadPart{}, &RequestTooLargeError{Message: "upload contains too many files"}
	}
	temp, err := os.CreateTemp(stageDir, "skill-file-*.part")
	if err != nil {
		return StagedUploadPart{}, fmt.Errorf("skill: stage temp file: %w", err)
	}
	tempPath := temp.Name()
	hasher := sha256.New()
	limited := &countingLimitedReader{R: reader, N: MaxUploadFileBytes - budget.Bytes}
	buffer := make([]byte, 32*1024)
	var written int64
	for {
		if err := ctx.Err(); err != nil {
			cleanupTemp(temp, tempPath)
			return StagedUploadPart{}, err
		}
		n, readErr := limited.Read(buffer)
		if n > 0 {
			chunk := buffer[:n]
			if _, err := temp.Write(chunk); err != nil {
				cleanupTemp(temp, tempPath)
				return StagedUploadPart{}, &ValidationError{Message: "upload read failed"}
			}
			_, _ = hasher.Write(chunk)
			written += int64(n)
		}
		if errors.Is(readErr, errCompressedCapExceeded) {
			cleanupTemp(temp, tempPath)
			return StagedUploadPart{}, &RequestTooLargeError{Message: "uploaded files exceed the size cap"}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			var tooLarge *RequestTooLargeError
			if errors.As(readErr, &tooLarge) {
				cleanupTemp(temp, tempPath)
				return StagedUploadPart{}, tooLarge
			}
			cleanupTemp(temp, tempPath)
			return StagedUploadPart{}, &ValidationError{Message: "upload read failed"}
		}
	}
	if budget.Bytes+written >= MaxUploadFileBytes {
		cleanupTemp(temp, tempPath)
		return StagedUploadPart{}, &RequestTooLargeError{Message: "uploaded files exceed the size cap"}
	}
	if err := temp.Close(); err != nil {
		_ = os.Remove(tempPath)
		return StagedUploadPart{}, fmt.Errorf("skill: close staged upload: %w", err)
	}
	budget.Bytes += written
	budget.Parts++
	return StagedUploadPart{
		Filename:  filename,
		SizeBytes: written,
		SHA256:    hex.EncodeToString(hasher.Sum(nil)),
		TempPath:  tempPath,
	}, nil
}

// CleanupStagedUploadParts removes staged files[] temp files.
func CleanupStagedUploadParts(parts []StagedUploadPart) error {
	var first error
	for _, part := range parts {
		if part.TempPath == "" {
			continue
		}
		if err := os.Remove(part.TempPath); err != nil && !errors.Is(err, os.ErrNotExist) && first == nil {
			first = err
		}
	}
	return first
}

// BuildNormalizedPackage validates files[] parts, canonicalizes the
// package tree, writes a deterministic normalized zip, and returns the
// staged normalized package.
func BuildNormalizedPackage(ctx context.Context, parts []StagedUploadPart, stageDir string) (*StagedPackage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(parts) == 0 {
		return nil, &ValidationError{Message: "request is missing files[]"}
	}
	if len(parts) > MaxUploadFileParts {
		return nil, &RequestTooLargeError{Message: "upload contains too many files"}
	}
	entries, err := canonicalEntriesFromParts(ctx, parts, stageDir)
	if err != nil {
		return nil, err
	}
	staged, err := writeNormalizedPackage(ctx, entries, stageDir)
	entries.cleanupOwnedEntryTemps()
	if err != nil {
		return nil, err
	}
	return staged, nil
}

func (p *StagedPackage) Open() (io.ReadCloser, error) {
	if p == nil {
		return nil, errors.New("skill: StagedPackage is nil")
	}
	if p.tempPath == "" {
		return nil, errors.New("skill: StagedPackage has no staged bytes")
	}
	return os.Open(p.tempPath) //nolint:gosec // path is server-generated, never client input
}

func (p *StagedPackage) Cleanup() error {
	if p == nil || p.tempPath == "" {
		return nil
	}
	err := os.Remove(p.tempPath)
	p.tempPath = ""
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

type packageEntry struct {
	path        string
	isDir       bool
	explicitDir bool
	size        int64
	tempPath    string
	owned       bool
}

type canonicalPackage struct {
	root        string
	name        string
	description string
	entries     map[string]*packageEntry
}

func canonicalEntriesFromParts(ctx context.Context, parts []StagedUploadPart, stageDir string) (*canonicalPackage, error) {
	if len(parts) == 1 && !isRootedPackagePath(parts[0].Filename) {
		return canonicalEntriesFromZip(ctx, parts[0], stageDir)
	}
	for _, part := range parts {
		if !isRootedPackagePath(part.Filename) {
			return nil, &ValidationError{Message: "files[] mixes zip package and individual file modes"}
		}
	}
	pkg := &canonicalPackage{entries: map[string]*packageEntry{}}
	var expanded int64
	for _, part := range parts {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if part.SizeBytes > MaxPackageEntryBytes {
			return nil, &RequestTooLargeError{Message: "package file exceeds the size cap"}
		}
		expanded += part.SizeBytes
		if expanded > MaxPackageExpandedBytes {
			return nil, &RequestTooLargeError{Message: "package exceeds the expanded size cap"}
		}
		clean, err := validatePackagePath(part.Filename, false)
		if err != nil {
			return nil, err
		}
		if err := pkg.addRegularFile(clean, part.SizeBytes, part.TempPath, false); err != nil {
			return nil, err
		}
	}
	if err := pkg.finalize(ctx); err != nil {
		return nil, err
	}
	return pkg, nil
}

func canonicalEntriesFromZip(ctx context.Context, part StagedUploadPart, stageDir string) (*canonicalPackage, error) {
	file, err := os.Open(part.TempPath) //nolint:gosec // path is server-generated, never client input
	if err != nil {
		return nil, &ValidationError{Message: "package upload is not readable"}
	}
	defer func() { _ = file.Close() }()
	zr, err := zip.NewReader(file, part.SizeBytes)
	if err != nil {
		return nil, &ValidationError{Message: "package zip is malformed"}
	}
	if len(zr.File) > MaxFileCount {
		return nil, &RequestTooLargeError{Message: "package contains too many entries"}
	}
	pkg := &canonicalPackage{entries: map[string]*packageEntry{}}
	var expanded int64
	var stagedTemps []string
	cleanupTemps := func() {
		for _, tempPath := range stagedTemps {
			_ = os.Remove(tempPath)
		}
	}
	for _, zipFile := range zr.File {
		if err := ctx.Err(); err != nil {
			cleanupTemps()
			return nil, err
		}
		clean, isDir, err := validateZipEntry(zipFile)
		if err != nil {
			cleanupTemps()
			return nil, err
		}
		if isDir {
			if err := pkg.addDirectory(clean); err != nil {
				cleanupTemps()
				return nil, err
			}
			continue
		}
		tempPath, size, err := stageZipEntry(ctx, zipFile, stageDir)
		if err != nil {
			cleanupTemps()
			return nil, err
		}
		stagedTemps = append(stagedTemps, tempPath)
		expanded += size
		if expanded > MaxPackageExpandedBytes {
			cleanupTemps()
			return nil, &RequestTooLargeError{Message: "package exceeds the expanded size cap"}
		}
		if err := pkg.addRegularFile(clean, size, tempPath, true); err != nil {
			cleanupTemps()
			return nil, err
		}
	}
	if err := pkg.finalize(ctx); err != nil {
		cleanupTemps()
		return nil, err
	}
	return pkg, nil
}

func validateZipEntry(zipFile *zip.File) (string, bool, error) {
	if zipFile.NonUTF8 || zipFile.Comment != "" || len(zipFile.Extra) != 0 {
		return "", false, &ValidationError{Message: "package zip entry metadata is not supported"}
	}
	mode := zipFile.Mode()
	name := zipFile.Name
	isDir := strings.HasSuffix(name, "/") || mode.IsDir()
	if err := validateZipExternalAttrs(zipFile, isDir); err != nil {
		return "", false, err
	}
	clean, err := validatePackagePath(name, isDir)
	if err != nil {
		return "", false, err
	}
	if err := validateZipMode(mode, isDir); err != nil {
		return "", false, err
	}
	return clean, isDir, nil
}

func validateZipExternalAttrs(zipFile *zip.File, isDir bool) error {
	if zipFile.ExternalAttrs == 0 {
		return nil
	}
	creator := zipFile.CreatorVersion >> 8
	switch creator {
	case zipCreatorUnix, zipCreatorMacOSX:
		return validateUnixZipExternalAttrs(zipFile.ExternalAttrs, isDir)
	case zipCreatorFAT, zipCreatorNTFS, zipCreatorVFAT:
		return validateMSDOSZipExternalAttrs(zipFile.ExternalAttrs, isDir)
	default:
		return &ValidationError{Message: "package zip entry metadata is not supported"}
	}
}

func validateUnixZipExternalAttrs(attrs uint32, isDir bool) error {
	unixMode := attrs >> 16
	if unixMode == 0 {
		return validateMSDOSZipExternalAttrs(attrs, isDir)
	}
	typeBits := unixMode & zipUnixModeType
	if isDir {
		if typeBits != zipUnixModeDir {
			return &ValidationError{Message: "package contains an unsupported entry type"}
		}
	} else if typeBits != zipUnixModeRegular {
		return &ValidationError{Message: "package contains an unsupported entry type"}
	}
	if unixMode&(zipUnixModeSetuid|zipUnixModeSetgid|zipUnixModeSticky) != 0 {
		return &ValidationError{Message: "package contains unsupported entry metadata"}
	}
	if unixMode&^(typeBits|0o777) != 0 {
		return &ValidationError{Message: "package contains unsupported entry metadata"}
	}
	msdosAttrs := attrs & 0xffff
	if msdosAttrs&^(zipMSDOSDir|zipMSDOSReadOnly) != 0 {
		return &ValidationError{Message: "package zip entry metadata is not supported"}
	}
	if !isDir && msdosAttrs&zipMSDOSDir != 0 {
		return &ValidationError{Message: "package contains an unsupported entry type"}
	}
	return nil
}

func validateMSDOSZipExternalAttrs(attrs uint32, isDir bool) error {
	if attrs&^(zipMSDOSDir|zipMSDOSReadOnly) != 0 {
		return &ValidationError{Message: "package zip entry metadata is not supported"}
	}
	if isDir {
		if attrs&zipMSDOSDir == 0 {
			return &ValidationError{Message: "package contains unsupported entry metadata"}
		}
		return nil
	}
	if attrs&zipMSDOSDir != 0 {
		return &ValidationError{Message: "package contains an unsupported entry type"}
	}
	return nil
}

func validateZipMode(mode os.FileMode, isDir bool) error {
	if isDir {
		if mode&^os.FileMode(os.ModeDir|0o777) != 0 {
			return &ValidationError{Message: "package contains unsupported entry metadata"}
		}
		if mode != 0 && !mode.IsDir() {
			return &ValidationError{Message: "package contains an unsupported entry type"}
		}
		return nil
	}
	if mode&^os.FileMode(0o777) != 0 {
		return &ValidationError{Message: "package contains unsupported entry metadata"}
	}
	if !mode.IsRegular() {
		return &ValidationError{Message: "package contains an unsupported entry type"}
	}
	return nil
}

func stageZipEntry(ctx context.Context, zipFile *zip.File, stageDir string) (string, int64, error) {
	rc, err := zipFile.Open()
	if err != nil {
		return "", 0, &ValidationError{Message: "package zip entry is not readable"}
	}
	defer func() { _ = rc.Close() }()
	temp, err := os.CreateTemp(stageDir, "skill-zip-entry-*.part")
	if err != nil {
		return "", 0, fmt.Errorf("skill: stage zip entry: %w", err)
	}
	tempPath := temp.Name()
	limited := &countingLimitedReader{R: rc, N: MaxPackageEntryBytes + 1}
	buffer := make([]byte, 32*1024)
	var written int64
	for {
		if err := ctx.Err(); err != nil {
			cleanupTemp(temp, tempPath)
			return "", 0, err
		}
		n, readErr := limited.Read(buffer)
		if n > 0 {
			if _, err := temp.Write(buffer[:n]); err != nil {
				cleanupTemp(temp, tempPath)
				return "", 0, &ValidationError{Message: "package zip entry is not readable"}
			}
			written += int64(n)
		}
		if errors.Is(readErr, errCompressedCapExceeded) {
			cleanupTemp(temp, tempPath)
			return "", 0, &RequestTooLargeError{Message: "package file exceeds the size cap"}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			cleanupTemp(temp, tempPath)
			return "", 0, &ValidationError{Message: "package zip entry is not readable"}
		}
	}
	if err := temp.Close(); err != nil {
		_ = os.Remove(tempPath)
		return "", 0, fmt.Errorf("skill: close zip entry stage: %w", err)
	}
	if written > MaxPackageEntryBytes {
		_ = os.Remove(tempPath)
		return "", 0, &RequestTooLargeError{Message: "package file exceeds the size cap"}
	}
	return tempPath, written, nil
}

func (p *canonicalPackage) addRegularFile(clean string, size int64, tempPath string, owned bool) error {
	if err := p.recordRoot(clean); err != nil {
		return err
	}
	if len(p.entries) >= MaxFileCount {
		return &RequestTooLargeError{Message: "package contains too many entries"}
	}
	if _, exists := p.entries[clean]; exists {
		return &ValidationError{Message: "package contains duplicate paths"}
	}
	if err := p.rejectPrefixConflicts(clean, false); err != nil {
		return err
	}
	p.entries[clean] = &packageEntry{path: clean, size: size, tempPath: tempPath, owned: owned}
	return p.addInferredDirectories(clean)
}

func (p *canonicalPackage) addDirectory(clean string) error {
	if err := p.recordRoot(clean); err != nil {
		return err
	}
	if len(p.entries) >= MaxFileCount {
		return &RequestTooLargeError{Message: "package contains too many entries"}
	}
	if existing := p.entries[clean]; existing != nil {
		if existing.isDir {
			if existing.explicitDir {
				return &ValidationError{Message: "package contains duplicate paths"}
			}
			existing.explicitDir = true
			return nil
		}
		return &ValidationError{Message: "package contains file and directory conflicts"}
	}
	if err := p.rejectPrefixConflicts(clean, true); err != nil {
		return err
	}
	p.entries[clean] = &packageEntry{path: clean, isDir: true, explicitDir: true}
	return p.addInferredDirectories(clean)
}

func (p *canonicalPackage) addInferredDirectories(clean string) error {
	dir := path.Dir(clean)
	for dir != "." && dir != "/" {
		if existing := p.entries[dir]; existing != nil {
			if !existing.isDir {
				return &ValidationError{Message: "package contains file and directory conflicts"}
			}
		} else {
			if len(p.entries) >= MaxFileCount {
				return &RequestTooLargeError{Message: "package contains too many entries"}
			}
			p.entries[dir] = &packageEntry{path: dir, isDir: true}
		}
		if dir == p.root {
			break
		}
		dir = path.Dir(dir)
	}
	return nil
}

func (p *canonicalPackage) rejectPrefixConflicts(clean string, isDir bool) error {
	dir := path.Dir(clean)
	for dir != "." && dir != "/" {
		if existing := p.entries[dir]; existing != nil && !existing.isDir {
			return &ValidationError{Message: "package contains file and directory conflicts"}
		}
		dir = path.Dir(dir)
	}
	if isDir {
		prefix := clean + "/"
		for existingPath, existing := range p.entries {
			if strings.HasPrefix(existingPath, prefix) && !existing.isDir {
				continue
			}
			if strings.HasPrefix(existingPath, prefix) && existingPath != clean {
				continue
			}
		}
	}
	return nil
}

func (p *canonicalPackage) recordRoot(clean string) error {
	root := strings.Split(clean, "/")[0]
	if p.root == "" {
		p.root = root
		return nil
	}
	if p.root != root {
		return &ValidationError{Message: "package must contain exactly one root directory"}
	}
	return nil
}

func (p *canonicalPackage) finalize(ctx context.Context) error {
	if len(p.entries) > MaxFileCount {
		return &RequestTooLargeError{Message: "package contains too many entries"}
	}
	skillMDPath := p.root + "/" + SkillMDFilename
	entry := p.entries[skillMDPath]
	if entry == nil || entry.isDir {
		return &ValidationError{Message: "package is missing root SKILL.md"}
	}
	raw, err := readEntryFrontmatter(ctx, entry.tempPath)
	if err != nil {
		return err
	}
	frontmatter, err := ParseFrontmatter(raw)
	if err != nil {
		return err
	}
	p.name = frontmatter.Name
	p.description = frontmatter.Description
	return nil
}

func (p *canonicalPackage) cleanupOwnedEntryTemps() {
	for _, entry := range p.entries {
		if entry.owned && entry.tempPath != "" {
			_ = os.Remove(entry.tempPath)
			entry.tempPath = ""
		}
	}
}

func readEntryFrontmatter(ctx context.Context, tempPath string) ([]byte, error) {
	file, err := os.Open(tempPath) //nolint:gosec // path is server-generated, never client input
	if err != nil {
		return nil, &ValidationError{Message: "package file is not readable"}
	}
	defer func() { _ = file.Close() }()

	reader := bufio.NewReaderSize(file, 16*1024)
	line, tooLong, err := readBoundedFrontmatterLine(ctx, reader, MaxFrontmatterBytes+1)
	if tooLong {
		return nil, &ValidationError{Message: "SKILL.md must begin with a `---` frontmatter delimiter"}
	}
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, &ValidationError{Message: "SKILL.md frontmatter is missing"}
		}
		return nil, err
	}

	var raw bytes.Buffer
	raw.WriteString(line)
	if !isFrontmatterDelimiterLine(line) {
		return raw.Bytes(), nil
	}

	var frontmatterBytes int
	for {
		line, tooLong, err := readBoundedFrontmatterLine(ctx, reader, maximumInt(MaxFrontmatterBytes-frontmatterBytes+1, len("---\r\n")))
		if tooLong {
			return nil, &RequestTooLargeError{Message: "SKILL.md frontmatter exceeds the bounded size cap"}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, &ValidationError{Message: "SKILL.md frontmatter is missing the closing `---` delimiter"}
			}
			return nil, err
		}
		if isFrontmatterDelimiterLine(line) {
			raw.WriteString(line)
			return raw.Bytes(), nil
		}
		frontmatterBytes += len(line)
		if frontmatterBytes > MaxFrontmatterBytes {
			return nil, &RequestTooLargeError{Message: "SKILL.md frontmatter exceeds the bounded size cap"}
		}
		raw.WriteString(line)
	}
}

func readBoundedFrontmatterLine(ctx context.Context, reader *bufio.Reader, maxBytes int) (string, bool, error) {
	var line bytes.Buffer
	for {
		if err := ctx.Err(); err != nil {
			return "", false, err
		}
		fragment, err := reader.ReadSlice('\n')
		if len(fragment) > 0 {
			if line.Len()+len(fragment) > maxBytes {
				return "", true, nil
			}
			_, _ = line.Write(fragment)
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if errors.Is(err, io.EOF) {
			if line.Len() == 0 {
				return "", false, io.EOF
			}
			return line.String(), false, nil
		}
		if err != nil {
			return "", false, &ValidationError{Message: "package file is not readable"}
		}
		if err := ctx.Err(); err != nil {
			return "", false, err
		}
		return line.String(), false, nil
	}
}

func isFrontmatterDelimiterLine(line string) bool {
	return isFrontmatterDelimiter(strings.TrimSuffix(line, "\n"))
}

func maximumInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func writeNormalizedPackage(ctx context.Context, pkg *canonicalPackage, stageDir string) (*StagedPackage, error) {
	temp, err := os.CreateTemp(stageDir, "skill-normalized-*.zip")
	if err != nil {
		return nil, fmt.Errorf("skill: stage normalized package: %w", err)
	}
	tempPath := temp.Name()
	hasher := sha256.New()
	capped := &limitedWriter{W: io.MultiWriter(temp, hasher), N: MaxNormalizedZipBytes - 1}
	zw := zip.NewWriter(capped)
	names := make([]string, 0, len(pkg.entries))
	for name := range pkg.entries {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			_ = zw.Close()
			cleanupTemp(temp, tempPath)
			return nil, err
		}
		entry := pkg.entries[name]
		headerName := entry.path
		if entry.isDir {
			headerName += "/"
		}
		header := &zip.FileHeader{Name: headerName}
		setPortableZipTime(header, normalizedZipTime)
		if entry.isDir {
			header.Method = zip.Store
			header.SetMode(0o755 | os.ModeDir)
		} else {
			header.Method = zip.Deflate
			header.SetMode(0o644)
		}
		writer, err := zw.CreateHeader(header)
		if err != nil {
			_ = zw.Close()
			cleanupTemp(temp, tempPath)
			if errors.Is(err, errNormalizedZipCapExceeded) {
				return nil, &RequestTooLargeError{Message: "normalized package exceeds the size cap"}
			}
			return nil, err
		}
		if !entry.isDir {
			if err := copyFileToZip(ctx, writer, entry.tempPath); err != nil {
				_ = zw.Close()
				cleanupTemp(temp, tempPath)
				return nil, err
			}
		}
	}
	if err := zw.Close(); err != nil {
		cleanupTemp(temp, tempPath)
		if errors.Is(err, errNormalizedZipCapExceeded) {
			return nil, &RequestTooLargeError{Message: "normalized package exceeds the size cap"}
		}
		return nil, err
	}
	if err := temp.Close(); err != nil {
		_ = os.Remove(tempPath)
		return nil, fmt.Errorf("skill: close normalized package: %w", err)
	}
	return &StagedPackage{
		Name:        pkg.name,
		Description: pkg.description,
		Directory:   pkg.root,
		SizeBytes:   capped.written,
		SHA256:      hex.EncodeToString(hasher.Sum(nil)),
		tempPath:    tempPath,
	}, nil
}

func setPortableZipTime(header *zip.FileHeader, timestamp time.Time) {
	year, month, day := timestamp.Date()
	hour, minute, second := timestamp.Clock()
	// Use the portable DOS fields directly so archive/zip does not add
	// an extended timestamp Extra field to normalized output.
	header.ModifiedTime = uint16(hour<<11 | minute<<5 | second/2)      //nolint:staticcheck
	header.ModifiedDate = uint16((year-1980)<<9 | int(month)<<5 | day) //nolint:staticcheck
	header.Modified = time.Time{}
}

func copyFileToZip(ctx context.Context, writer io.Writer, tempPath string) error {
	file, err := os.Open(tempPath) //nolint:gosec // path is server-generated, never client input
	if err != nil {
		return &ValidationError{Message: "package file is not readable"}
	}
	defer func() { _ = file.Close() }()
	buffer := make([]byte, 32*1024)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, readErr := file.Read(buffer)
		if n > 0 {
			if _, err := writer.Write(buffer[:n]); err != nil {
				if errors.Is(err, errNormalizedZipCapExceeded) {
					return &RequestTooLargeError{Message: "normalized package exceeds the size cap"}
				}
				return err
			}
		}
		if readErr == io.EOF {
			return nil
		}
		if readErr != nil {
			return &ValidationError{Message: "package file is not readable"}
		}
	}
}

func validatePackagePath(name string, isDir bool) (string, error) {
	if isDir {
		name = strings.TrimSuffix(name, "/")
	}
	if name == "" || !utf8.ValidString(name) || len(name) > 4096 || strings.Contains(name, "\x00") || strings.Contains(name, "\\") {
		return "", &ValidationError{Message: "package path is invalid"}
	}
	if strings.HasPrefix(name, "/") || hasWindowsDrivePrefix(name) {
		return "", &ValidationError{Message: "package path is invalid"}
	}
	for _, segment := range strings.Split(name, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return "", &ValidationError{Message: "package path is invalid"}
		}
	}
	clean := path.Clean(name)
	if clean != name || clean == "." || (!isDir && !strings.Contains(clean, "/")) {
		return "", &ValidationError{Message: "package path is invalid"}
	}
	return clean, nil
}

func isRootedPackagePath(name string) bool {
	if strings.HasSuffix(name, "/") {
		return false
	}
	clean, err := validatePackagePath(name, false)
	return err == nil && strings.Contains(clean, "/")
}

func hasWindowsDrivePrefix(name string) bool {
	return len(name) >= 2 && name[1] == ':' && ((name[0] >= 'A' && name[0] <= 'Z') || (name[0] >= 'a' && name[0] <= 'z'))
}

func cleanupTemp(file *os.File, tempPath string) {
	_ = file.Close()
	_ = os.Remove(tempPath)
}

var errNormalizedZipCapExceeded = errors.New("normalized zip cap exceeded")

type limitedWriter struct {
	W       io.Writer
	N       int64
	written int64
}

func (w *limitedWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > w.N-w.written {
		allowed := w.N - w.written
		if allowed > 0 {
			n, _ := w.W.Write(p[:allowed])
			w.written += int64(n)
		}
		return 0, errNormalizedZipCapExceeded
	}
	n, err := w.W.Write(p)
	w.written += int64(n)
	return n, err
}
