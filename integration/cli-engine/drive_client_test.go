package joint

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// do sends method to path with body of contentType and the header pairs
// headers, and returns the answer's status, headers and body, with one
// record line.
func (d *developerClient) do(ctx context.Context, method, path string, body []byte, contentType string, headers ...string) (int, http.Header, []byte) {
	d.t.Helper()
	request, err := http.NewRequestWithContext(ctx, method, d.baseURL+path, bytes.NewReader(body))
	if err != nil {
		d.t.Fatal(err)
	}
	request.Header.Set("X-Api-Key", d.apiKey)
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	for i := 0; i+1 < len(headers); i += 2 {
		request.Header.Set(headers[i], headers[i+1])
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		d.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer func() { _ = response.Body.Close() }()
	answer, err := io.ReadAll(response.Body)
	if err != nil {
		d.t.Fatal(err)
	}
	d.record.command("developer `%s %s` answered %d", method, path, response.StatusCode)
	return response.StatusCode, response.Header, answer
}

// doJSON sends body as JSON, as do does.
func (d *developerClient) doJSON(ctx context.Context, method, path string, body any) (int, []byte) {
	d.t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		d.t.Fatal(err)
	}
	status, _, answer := d.do(ctx, method, path, data, "application/json")
	return status, answer
}

// createDrive creates a drive and returns its ID.
func (d *developerClient) createDrive(ctx context.Context) string {
	d.t.Helper()
	var created struct {
		ID string `json:"id"`
	}
	d.post(ctx, "/v1/drives", map[string]any{}, http.StatusCreated, &created)
	return created.ID
}

// uploadForm is the multipart body of drives.files.upload of content to
// path, with extra parts such as expected_content_sha256.
func uploadForm(path string, content []byte, extra map[string]string) ([]byte, string, error) {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	if err := writer.WriteField("path", path); err != nil {
		return nil, "", err
	}
	for name, value := range extra {
		if err := writer.WriteField(name, value); err != nil {
			return nil, "", err
		}
	}
	part, err := writer.CreateFormFile("file", "file")
	if err != nil {
		return nil, "", err
	}
	if _, err := part.Write(content); err != nil {
		return nil, "", err
	}
	if err := writer.Close(); err != nil {
		return nil, "", err
	}
	return body.Bytes(), writer.FormDataContentType(), nil
}

// uploadFile sends drives.files.upload of content to path and returns the
// answer's status, headers and body.
func (d *developerClient) uploadFile(ctx context.Context, driveID, path string, content []byte, extra map[string]string) (int, http.Header, []byte) {
	d.t.Helper()
	body, contentType, err := uploadForm(path, content, extra)
	if err != nil {
		d.t.Fatal(err)
	}
	return d.do(ctx, http.MethodPost, "/v1/drives/"+driveID+"/files", body, contentType)
}

// upload writes content to path through drives.files.upload, which must
// answer 201 or 200, and returns the version's ID.
func (d *developerClient) upload(ctx context.Context, driveID, path, content string) string {
	d.t.Helper()
	status, _, answer := d.uploadFile(ctx, driveID, path, []byte(content), nil)
	var version struct {
		VersionID string `json:"version_id"`
	}
	if status != http.StatusCreated && status != http.StatusOK || json.Unmarshal(answer, &version) != nil {
		d.t.Fatalf("upload of %s to %s answered %d %s", path, driveID, status, answer)
	}
	return version.VersionID
}

// readFile is drives.files.download of path: its status and content.
func (d *developerClient) readFile(ctx context.Context, driveID, path string) (int, string) {
	d.t.Helper()
	status, _, answer := d.do(ctx, http.MethodGet, "/v1/drives/"+driveID+"/files/content?path="+url.QueryEscape(path), nil, "")
	return status, string(answer)
}

// deleteFile deletes path through the API, expecting its content to be
// content, and returns the answer's status.
func (d *developerClient) deleteFile(ctx context.Context, driveID, path, content string) int {
	d.t.Helper()
	status, _, _ := d.do(ctx, http.MethodDelete, "/v1/drives/"+driveID+"/files?path="+url.QueryEscape(path)+"&expected_content_sha256="+sha256Hex(content), nil, "")
	return status
}

// driveFile is a file of drives.files.list.
type driveFile struct {
	Path          string `json:"path"`
	SizeBytes     int64  `json:"size_bytes"`
	ContentSHA256 string `json:"content_sha256"`
	VersionID     string `json:"version_id"`
	Executable    bool   `json:"executable"`
}

// listFiles is every file of the drive, through every page.
func (d *developerClient) listFiles(ctx context.Context, driveID string) []driveFile {
	d.t.Helper()
	var files []driveFile
	page := ""
	for {
		var answer struct {
			Data     []driveFile `json:"data"`
			NextPage *string     `json:"next_page"`
		}
		path := "/v1/drives/" + driveID + "/files?limit=100"
		if page != "" {
			path += "&page=" + url.QueryEscape(page)
		}
		d.getQuiet(ctx, path, &answer)
		files = append(files, answer.Data...)
		if answer.NextPage == nil {
			d.record.command("developer `GET /v1/drives/%s/files` listed %d files", driveID, len(files))
			return files
		}
		page = *answer.NextPage
	}
}

// driveWriter is the writer of a version or a change record.
type driveWriter struct {
	Type       string `json:"type"`
	APIKeyID   string `json:"api_key_id"`
	SessionID  string `json:"session_id"`
	ComputerID string `json:"computer_id"`
}

// driveChange is a record of drives.changes.list.
type driveChange struct {
	Seq       int64       `json:"seq"`
	Index     int         `json:"index"`
	Path      string      `json:"path"`
	Operation string      `json:"operation"`
	VersionID string      `json:"version_id"`
	Writer    driveWriter `json:"writer"`
}

// listChanges is every change record of the drive after seq after, of
// sessionID when it is not empty.
func (d *developerClient) listChanges(ctx context.Context, driveID string, after int64, sessionID string) []driveChange {
	d.t.Helper()
	var changes []driveChange
	query := url.Values{"after": {strconv.FormatInt(after, 10)}, "limit": {"1000"}}
	if sessionID != "" {
		query.Set("session_id", sessionID)
	}
	for {
		var answer struct {
			Data    []driveChange `json:"data"`
			HasMore bool          `json:"has_more"`
		}
		d.getQuiet(ctx, "/v1/drives/"+driveID+"/changes?"+query.Encode(), &answer)
		changes = append(changes, answer.Data...)
		if !answer.HasMore || len(answer.Data) == 0 {
			d.record.command("developer `GET /v1/drives/%s/changes?%s` listed %d records", driveID, query.Encode(), len(changes))
			return changes
		}
		last := answer.Data[len(answer.Data)-1]
		query.Set("after", strconv.FormatInt(last.Seq, 10))
		query.Set("after_index", strconv.Itoa(last.Index))
	}
}

// driveVersion is a version of drives.files.versions.
type driveVersion struct {
	VersionID     string      `json:"version_id"`
	Path          string      `json:"path"`
	Operation     string      `json:"operation"`
	ContentSHA256 *string     `json:"content_sha256"`
	Executable    bool        `json:"executable"`
	Writer        driveWriter `json:"writer"`
}

// listVersions is the first 100 versions of path, oldest first.
func (d *developerClient) listVersions(ctx context.Context, driveID, path string) []driveVersion {
	d.t.Helper()
	var answer struct {
		Data []driveVersion `json:"data"`
	}
	d.get(ctx, "/v1/drives/"+driveID+"/files/versions?limit=100&path="+url.QueryEscape(path), &answer)
	return answer.Data
}

// sessionDrive is a Session's binding of a drive.
type sessionDrive struct {
	DriveID string `json:"drive_id"`
	Access  string `json:"access"`
}

// readWrite and readOnly are the binding items of a Session's drives.
func readWrite(driveID string) sessionDrive {
	return sessionDrive{DriveID: driveID, Access: "read_write"}
}
func readOnly(driveID string) sessionDrive {
	return sessionDrive{DriveID: driveID, Access: "read_only"}
}

// addDrive binds driveID to the Session with access and returns the
// answer's status and body.
func (d *developerClient) addDrive(ctx context.Context, sessionID string, binding sessionDrive) (int, string) {
	d.t.Helper()
	status, answer := d.doJSON(ctx, http.MethodPost, "/v1/sessions/"+sessionID+"/drives?beta=true", binding)
	return status, string(answer)
}

// setDriveAccess changes the access of the Session's binding of driveID and
// returns the answer's status.
func (d *developerClient) setDriveAccess(ctx context.Context, sessionID, driveID, access string) int {
	d.t.Helper()
	status, _ := d.doJSON(ctx, http.MethodPost, "/v1/sessions/"+sessionID+"/drives/"+driveID+"?beta=true", map[string]string{"access": access})
	return status
}

// removeDrive removes the Session's binding of driveID and returns the
// answer's status.
func (d *developerClient) removeDrive(ctx context.Context, sessionID, driveID string) int {
	d.t.Helper()
	status, _, _ := d.do(ctx, http.MethodDelete, "/v1/sessions/"+sessionID+"/drives/"+driveID+"?beta=true", nil, "")
	return status
}

func sha256Hex(content string) string {
	sum := sha256.Sum256([]byte(content))
	return hex.EncodeToString(sum[:])
}

// openChangeStream subscribes to drives.changes.stream of driveID and returns
// the seq of each drive.changed event as it arrives.
func (d *developerClient) openChangeStream(ctx context.Context, driveID string) <-chan int64 {
	d.t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, d.baseURL+"/v1/drives/"+driveID+"/changes/stream", nil)
	if err != nil {
		d.t.Fatal(err)
	}
	request.Header.Set("X-Api-Key", d.apiKey)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		d.t.Fatalf("open the change stream of %s: %v", driveID, err)
	}
	if response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != "text/event-stream" {
		_ = response.Body.Close()
		d.t.Fatalf("the change stream of %s answered %d %s", driveID, response.StatusCode, response.Header.Get("Content-Type"))
	}
	d.t.Cleanup(func() { _ = response.Body.Close() })
	d.record.command("developer `GET /v1/drives/%s/changes/stream` opened", driveID)
	seqs := make(chan int64, 64)
	go func() {
		defer close(seqs)
		scanner := bufio.NewScanner(response.Body)
		var name string
		for scanner.Scan() {
			line := scanner.Text()
			switch {
			case strings.HasPrefix(line, "event: "):
				name = strings.TrimPrefix(line, "event: ")
			case strings.HasPrefix(line, "data: ") && name == "drive.changed":
				var event struct {
					DriveID string `json:"drive_id"`
					Seq     int64  `json:"seq"`
				}
				if json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &event) == nil && event.DriveID == driveID {
					seqs <- event.Seq
				}
			}
		}
	}()
	return seqs
}
