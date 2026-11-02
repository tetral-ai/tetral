package httpapi_test

import (
	"context"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/drive"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestDriveSDKPaginationRetriesAndNullableResponses(t *testing.T) {
	sdkRoot := os.Getenv("TETRAL_ENGINE_SDK_ROOT")
	if sdkRoot == "" {
		t.Fatal("TETRAL_ENGINE_SDK_ROOT is required")
	}
	api := newDriveAPI(t, drive.DefaultUploadBudgetBytesPerMinute)
	filesID, skillsID, changesID, invalidID := api.newDrive(`{}`), api.newDrive(`{}`), api.newDrive(`{}`), api.newDrive(`{}`)
	decodeDrive(t, api.upload(filesID, "same.txt", "original"), http.StatusCreated, nil)
	decodeDrive(t, api.upload(skillsID, "skills", "occupied"), http.StatusCreated, nil)
	decodeDrive(t, api.upload(invalidID, "skills/broken/SKILL.md", "---\nname: broken\n---\nNo description."), http.StatusCreated, nil)
	content, err := api.drives.PutContent(t.Context(), workspace.DefaultID, changesID, strings.NewReader("x"), 1, "")
	if err != nil {
		t.Fatal(err)
	}
	changes := make([]drive.Change, 1001)
	for i := range changes {
		changes[i] = drive.Change{Path: fmt.Sprintf("file-%04d", i), ContentSHA256: content.SHA256, Base: drive.Base{Kind: drive.BaseAny}}
	}
	if _, err := api.drives.Commit(t.Context(), workspace.DefaultID, changesID, drive.Writer{Kind: drive.WriterAPIKey, ID: "fixture"}, drive.ModeAllOrNothing, changes); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	posts := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			mu.Lock()
			posts[r.URL.Path]++
			mu.Unlock()
		}
		api.handler.ServeHTTP(w, r)
	}))
	defer server.Close()
	pressured := newDriveAPI(t, drive.MinUploadCount)
	now := time.Now()
	pressured.budget.WithClock(func() time.Time { return now })
	pressuredID := pressured.newDrive(`{}`)
	decodeDrive(t, pressured.upload(pressuredID, "same.txt", "original"), http.StatusCreated, nil)
	now = now.Add(time.Minute)
	var pressurePosts atomic.Int32
	pressureServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			pressurePosts.Add(1)
		}
		pressured.handler.ServeHTTP(w, r)
	}))
	defer pressureServer.Close()
	var storagePuts atomic.Int32
	storage := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if r.Method != http.MethodPut {
			t.Errorf("unexpected storage request %s", r.Method)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if storagePuts.Add(1)%2 == 1 {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusForbidden)
			_, _ = io.WriteString(w, `<Error><Code>AccessDenied</Code><Message>private-provider-diagnostic</Message></Error>`)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer storage.Close()
	ca := filepath.Join(t.TempDir(), "storage-ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: storage.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AWS_CA_BUNDLE", ca)
	store, err := blob.NewS3BlobStore(t.Context(), &blob.Config{Endpoint: storage.URL, Region: "us-east-1", Bucket: "sdk-upload", AccessKey: "fixture", SecretKey: "fixture", LocalTestMode: true})
	if err != nil {
		t.Fatal(err)
	}
	storageAPI := newDriveAPIWithStore(t, drive.DefaultUploadBudgetBytesPerMinute, store, t.TempDir())
	storageID := storageAPI.newDrive(`{}`)
	var storageFilePosts, storageSkillPosts atomic.Int32
	storageServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			if strings.HasSuffix(r.URL.Path, "/skills") {
				storageSkillPosts.Add(1)
			} else {
				storageFilePosts.Add(1)
			}
		}
		storageAPI.handler.ServeHTTP(w, r)
	}))
	defer storageServer.Close()
	sessions := newSessionIntegrationEnv(t)
	sessionServer := httptest.NewServer(sessions.router)
	defer sessionServer.Close()
	archive := skillZip(t, file("report/SKILL.md", reportSkillMD("Writes reports.")))
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, "bun", "--eval", driveSDKProof)
	command.Env = append(os.Environ(),
		"SDK_ENTRY="+filepath.Join(sdkRoot, "src/index.ts"), "DRIVE_API="+server.URL, "DRIVE_KEY="+api.env.envKey,
		"SESSION_API="+sessionServer.URL, "SESSION_KEY="+testAPIKey,
		"FILES_ID="+filesID, "SKILLS_ID="+skillsID, "CHANGES_ID="+changesID, "INVALID_ID="+invalidID,
		"PRESSURE_API="+pressureServer.URL, "PRESSURE_ID="+pressuredID, "STORAGE_API="+storageServer.URL, "STORAGE_ID="+storageID,
		"SKILL_ARCHIVE="+base64.StdEncoding.EncodeToString(archive))
	if out, err := command.CombinedOutput(); err != nil {
		t.Fatalf("SDK proof: %v (storage PUTs=%d, file POSTs=%d, Skill POSTs=%d)\n%s", err, storagePuts.Load(), storageFilePosts.Load(), storageSkillPosts.Load(), out)
	}
	if pressurePosts.Load() != 1 {
		t.Fatalf("pressured conflict POSTs=%d", pressurePosts.Load())
	}
	if storagePuts.Load() != 4 || storageFilePosts.Load() != 2 || storageSkillPosts.Load() != 2 {
		t.Fatalf("storage calls: puts=%d file posts=%d skill posts=%d", storagePuts.Load(), storageFilePosts.Load(), storageSkillPosts.Load())
	}
	expectError(t, pressured.upload(pressuredID, "same.txt", "conflicting", "must_not_exist", "true"), http.StatusTooManyRequests, "rate_limit_error", "")
	mu.Lock()
	defer mu.Unlock()
	for path, want := range map[string]int{
		"/v1/drives/" + filesID + "/files": 1, "/v1/drives/" + skillsID + "/skills": 1,
	} {
		if posts[path] != want {
			t.Fatalf("POST %s attempts=%d want=%d", path, posts[path], want)
		}
	}
}

const driveSDKProof = `
import assert from 'node:assert/strict';
const { default: Anthropic, toFile, ConflictError } = await import(process.env.SDK_ENTRY);
const client = new Anthropic({ apiKey: process.env.DRIVE_KEY, baseURL: process.env.DRIVE_API });
await assert.rejects(client.beta.drives.files.upload(process.env.FILES_ID, {
  path: 'same.txt', file: await toFile(Buffer.from('conflicting'), 'same.txt'), must_not_exist: true,
}), ConflictError);
await assert.rejects(client.beta.drives.skills.upload(process.env.SKILLS_ID, {
  archive: await toFile(Buffer.from(process.env.SKILL_ARCHIVE, 'base64'), 'report.zip'),
}), ConflictError);
const pressured = new Anthropic({apiKey: process.env.DRIVE_KEY, baseURL: process.env.PRESSURE_API});
await assert.rejects(pressured.beta.drives.files.upload(process.env.PRESSURE_ID, {
  path: 'same.txt', file: await toFile(Buffer.from('conflicting'), 'same.txt'), must_not_exist: true,
}), ConflictError);
const storage = new Anthropic({apiKey: process.env.DRIVE_KEY, baseURL: process.env.STORAGE_API});
await storage.beta.drives.files.upload(process.env.STORAGE_ID, {path: 'retried.txt', file: await toFile(Buffer.from('ok'), 'retried.txt')});
await storage.beta.drives.skills.upload(process.env.STORAGE_ID, {archive: await toFile(Buffer.from(process.env.SKILL_ARCHIVE, 'base64'), 'report.zip')});
let cursor = {}, seen = [];
while (true) {
  const page = await client.beta.drives.changes.list(process.env.CHANGES_ID, {...cursor, limit: 100});
  for (const change of page.data) {
    assert.equal(change.seq, 1);
    assert.equal(change.index, seen.length);
    seen.push(change.path);
    cursor = {after: change.seq, after_index: change.index};
  }
  if (!page.has_more) break;
}
assert.equal(seen.length, 1001);
assert.equal(new Set(seen).size, 1001);
assert.equal((await client.beta.drives.changes.list(process.env.CHANGES_ID, {after: 1})).data.length, 0);
const invalid = await client.beta.drives.skills.retrieve(process.env.INVALID_ID, 'broken');
assert.equal(invalid.description, null);
assert.equal(typeof invalid.error, 'string');
assert.equal((invalid.description ?? invalid.error ?? invalid.name).length > 0, true);
const sessions = new Anthropic({apiKey: process.env.SESSION_KEY, baseURL: process.env.SESSION_API});
const session = await sessions.beta.sessions.create({agent: 'agent_http_session', vault_ids: [], computers: [{environment_id: 'env_http_session', repositories: [{url: 'https://github.com/acme/app', authorization_token: 'fixture_token'}]}]}, {idempotencyKey: 'sdk-default-identity'});
const repository = session.computers[0].repositories[0];
assert.equal(repository.git_identity, null);
assert.equal(repository.git_identity?.name ?? 'Default identity', 'Default identity');
`
