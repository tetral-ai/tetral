package tetralsandbox

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/daytonaio/daytona/libs/sdk-go/pkg/daytona"
	"github.com/daytonaio/daytona/libs/sdk-go/pkg/types"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/sandbox"
	"github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/internal/workspace"
	"github.com/tetral-ai/tetral/services/sandbox/internal/resourceprojection"
)

const (
	envResourceProjectionLive            = "TETRAL_RESOURCE_PROJECTION_LIVE"
	envResourceProjectionLiveArtifactRef = "TETRAL_RESOURCE_PROJECTION_LIVE_ARTIFACT_REF"
)

func TestLiveResourceProjectionFUSEBindSmoke(t *testing.T) {
	live := loadLiveResourceProjectionEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	blobStore := newLiveBlobStore(ctx, t, live)
	workspaceID, sessionID, sandboxID := liveResourceProjectionIDs(t)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cleanupCancel()
		_ = blobStore.DeletePrefix(cleanupCtx, resourceprojection.SessionPrefix(workspaceID, sessionID))
		_ = blobStore.DeletePrefix(cleanupCtx, "files/"+workspaceID+"/")
	})

	setup := sandbox.SandboxSetup{
		WorkspaceID:         workspace.ID(workspaceID),
		SessionID:           sessionID,
		SandboxID:           sandboxID,
		EnvironmentID:       "env-resource-projection-live",
		ProviderArtifactRef: live.artifactRef,
		Network:             sandbox.NetworkSetup{Type: "unrestricted"},
		Resources: sandbox.ResourceSetup{Files: []sandbox.FileMount{
			liveFile("sesrsc_default", "file_default", "obj_default", ""),
			liveFile("sesrsc_workspace", "file_workspace", "obj_workspace", "/workspace/live/data.csv"),
			liveFile("sesrsc_receipt", "file_receipt", "obj_receipt", "/uploads/receipt.pdf"),
			liveFile("sesrsc_mmap", "file_mmap", "obj_mmap", "/workspace/live/mmap.bin"),
		}},
	}
	putLiveCanonical(ctx, t, blobStore, workspaceID, "obj_default", []byte("default bytes\n"))
	putLiveCanonical(ctx, t, blobStore, workspaceID, "obj_workspace", []byte("workspace data\n"))
	putLiveCanonical(ctx, t, blobStore, workspaceID, "obj_receipt", []byte("receipt bytes\n"))
	putLiveCanonical(ctx, t, blobStore, workspaceID, "obj_mmap", bytes.Repeat([]byte("mmap-read-block\n"), 1024))

	provider, runner, handle := createLiveDaytonaSandbox(ctx, t, live, setup)
	minter := newLiveCountingCredentialMinter(t, live)
	preparer := newLiveFUSEBindPreparer(t, blobStore, minter, runner, live)

	start := time.Now()
	prepared, err := preparer.MaterializeFileResources(ctx, setup, handle)
	if err != nil {
		t.Fatalf("MaterializeFileResources initial: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 45*time.Second {
		t.Fatalf("initial prepare took %s; want daemonized rclone path to return without childreap stall", elapsed)
	}
	if minter.Count() != 1 {
		t.Fatalf("mint count after initial prepare = %d; want 1", minter.Count())
	}
	if prepared.ResourceCredExpiresAt == nil {
		t.Fatal("ResourceCredExpiresAt is nil after FUSE bind prepare")
	}
	setup.Resources.ResourceCredExpiresAt = prepared.ResourceCredExpiresAt
	setup.Resources.ResourceRootsJSON = prepared.ResourceRootsJSON
	assertLiveProjectionReady(ctx, t, runner, handle, setup.Resources.Files, workspaceID, sessionID, live.bucket)
	snapshotLiveBindMountIDs(ctx, t, runner, handle, setup.Resources.Files)

	_, err = preparer.MaterializeFileResources(ctx, setup, handle)
	if err != nil {
		t.Fatalf("MaterializeFileResources idempotent replay: %v", err)
	}
	if minter.Count() != 1 {
		t.Fatalf("mint count after idempotent replay = %d; want still 1", minter.Count())
	}
	assertLiveBindMountIDsUnchanged(ctx, t, runner, handle, setup.Resources.Files)
	assertLiveProjectionReady(ctx, t, runner, handle, setup.Resources.Files, workspaceID, sessionID, live.bucket)

	putLiveCanonical(ctx, t, blobStore, workspaceID, "obj_idle_add", []byte("idle add bytes\n"))
	setup.Resources.Files = append(setup.Resources.Files, liveFile("sesrsc_idle_add", "file_idle_add", "obj_idle_add", "/workspace/live/idle-add.txt"))
	prepared, err = preparer.MaterializeFileResources(ctx, setup, handle)
	if err != nil {
		t.Fatalf("MaterializeFileResources add-at-idle: %v", err)
	}
	if minter.Count() != 1 {
		t.Fatalf("mint count after add-at-idle = %d; want no re-mint while mount is alive", minter.Count())
	}
	setup.Resources.ResourceCredExpiresAt = prepared.ResourceCredExpiresAt
	setup.Resources.ResourceRootsJSON = prepared.ResourceRootsJSON
	assertLiveProjectionReady(ctx, t, runner, handle, setup.Resources.Files, workspaceID, sessionID, live.bucket)

	assertLiveCredentialBoundary(ctx, t, runner, blobStore, handle, workspaceID, sessionID, live.bucket)
	_ = provider
}

func TestLiveResourceProjectionSmallCacheReadsOversizedResourceAndKeepsOutputsWritable(t *testing.T) {
	live := loadLiveResourceProjectionEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	blobStore := newLiveBlobStore(ctx, t, live)
	workspaceID, sessionID, sandboxID := liveResourceProjectionIDs(t)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cleanupCancel()
		_ = blobStore.DeletePrefix(cleanupCtx, resourceprojection.SessionPrefix(workspaceID, sessionID))
		_ = blobStore.DeletePrefix(cleanupCtx, "files/"+workspaceID+"/")
	})

	setup := sandbox.SandboxSetup{
		WorkspaceID:         workspace.ID(workspaceID),
		SessionID:           sessionID,
		SandboxID:           sandboxID,
		EnvironmentID:       "env-resource-projection-live-cache",
		ProviderArtifactRef: live.artifactRef,
		Network:             sandbox.NetworkSetup{Type: "unrestricted"},
		Resources: sandbox.ResourceSetup{Files: []sandbox.FileMount{
			liveFile("sesrsc_oversized", "file_oversized", "obj_oversized", "/workspace/live/oversized.bin"),
		}},
	}
	putLiveCanonical(ctx, t, blobStore, workspaceID, "obj_oversized", bytes.Repeat([]byte("0123456789abcdef"), 1024*1024))

	_, runner, handle := createLiveDaytonaSandbox(ctx, t, live, setup)
	minter := newLiveCountingCredentialMinter(t, live)
	preparer, err := NewDaytonaFileResourceMaterializer(DaytonaFileResourceMaterializerConfig{
		Blob:                    blobStore,
		CredentialMinter:        minter,
		CommandRunner:           runner,
		Bucket:                  live.bucket,
		AccountID:               live.r2AccountID,
		CredentialTTL:           time.Hour,
		CredentialRefreshMargin: 10 * time.Minute,
		CommandTimeout:          2 * time.Minute,
		RcloneVFSCacheMaxSize:   "8M",
		RcloneVFSMinFree:        "1M",
	})
	if err != nil {
		t.Fatalf("NewDaytonaFileResourceMaterializer: %v", err)
	}
	if _, err := preparer.MaterializeFileResources(ctx, setup, handle); err != nil {
		t.Fatalf("MaterializeFileResources small-cache oversized resource: %v", err)
	}
	command := strings.Join([]string{
		"set -eu",
		"PATH=/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin",
		"sudo -u " + shellQuote(driver.RuntimeUser) + " sh -c 'dd if=\"$1\" of=/dev/null bs=1M status=none' _ '/workspace/live/oversized.bin'",
		"sudo -u " + shellQuote(driver.RuntimeUser) + " sh -c 'printf output-after-oversized-read > /mnt/session/outputs/resource-projection-cache.txt'",
		"sudo -u " + shellQuote(driver.RuntimeUser) + " grep -q output-after-oversized-read /mnt/session/outputs/resource-projection-cache.txt",
	}, "\n") + "\n"
	if err := runner.RunDaytonaCommand(ctx, driver.DaytonaCommandTarget{ProviderSandboxID: handle.SandboxID}, command, nil, 2*time.Minute); err != nil {
		t.Fatalf("small-cache oversized read/output verification command: %v", err)
	}
}

func TestLiveResourceProjectionTempCredentialPrefixIsolation(t *testing.T) {
	live := loadLiveResourceProjectionR2Env(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	blobStore := newLiveBlobStore(ctx, t, live)
	suffix := liveRandomSuffix(t)
	workspaceID := "ws-live-" + suffix
	sessionA := "sesn-live-a-" + suffix
	sessionB := "sesn-live-b-" + suffix
	canonicalKey := resourceprojection.CanonicalObjectKey(workspaceID, "obj_a")
	sessionAKey := resourceprojection.SessionResourceKey(workspaceID, sessionA, "sesrsc_a")
	sessionBKey := resourceprojection.SessionResourceKey(workspaceID, sessionB, "sesrsc_b")
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cleanupCancel()
		_ = blobStore.DeletePrefix(cleanupCtx, "workspaces/"+workspaceID+"/")
		_ = blobStore.DeletePrefix(cleanupCtx, "files/"+workspaceID+"/")
	})

	putLiveBlob(ctx, t, blobStore, canonicalKey, []byte("canonical bytes\n"))
	putLiveBlob(ctx, t, blobStore, sessionAKey, []byte("session A bytes\n"))
	putLiveBlob(ctx, t, blobStore, sessionBKey, []byte("session B bytes\n"))

	minter := newLiveCountingCredentialMinter(t, live)
	minted, err := minter.Mint(ctx, resourceprojection.CredentialMintRequest{
		WorkspaceID: workspaceID,
		SessionID:   sessionA,
		TTL:         time.Hour,
		Now:         time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("mint session-scoped resource credential: %v", err)
	}
	if got, want := minted.Prefix, resourceprojection.SessionResourcesPrefix(workspaceID, sessionA); got != want {
		t.Fatalf("minted prefix = %q; want %q", got, want)
	}
	tempStore := newLiveBlobStoreWithCredential(ctx, t, live, minted.Credential)

	assertLiveBlobBytes(ctx, t, tempStore, sessionAKey, "session A bytes\n")
	keys, err := tempStore.ListPrefix(ctx, resourceprojection.SessionResourcesPrefix(workspaceID, sessionA))
	if err != nil {
		t.Fatalf("session A credential ListPrefix(own prefix): %v", err)
	}
	if !containsString(keys, sessionAKey) {
		t.Fatalf("own-prefix list keys = %v; want %q", keys, sessionAKey)
	}
	assertLiveTempCredentialCannotGet(ctx, t, tempStore, sessionBKey)
	assertLiveTempCredentialCannotGet(ctx, t, tempStore, canonicalKey)
	assertLiveTempCredentialCannotList(ctx, t, tempStore, resourceprojection.SessionResourcesPrefix(workspaceID, sessionB))
	assertLiveTempCredentialCannotList(ctx, t, tempStore, "files/"+workspaceID+"/")
	assertLiveTempCredentialCannotList(ctx, t, tempStore, "workspaces/"+workspaceID+"/sessions/")
}

type liveResourceProjectionEnv struct {
	daytona          driver.Config
	blobConfig       *blob.Config
	bucket           string
	r2AccountID      string
	r2ParentAPIToken string
	r2ParentAccessID string
	artifactRef      string
}

func loadLiveResourceProjectionR2Env(t *testing.T) liveResourceProjectionEnv {
	t.Helper()
	if os.Getenv(envResourceProjectionLive) != "1" {
		t.Skipf("%s=1 is required for live R2 resource projection tests", envResourceProjectionLive)
	}
	missing := missingEnvVars(
		EnvR2AccountID,
		EnvR2ParentAPIToken,
		EnvR2ParentAccessKeyID,
	)
	if len(missing) > 0 {
		t.Skipf("live R2 resource projection test requires env vars: %s", strings.Join(missing, ", "))
	}
	blobConfig, err := blob.LoadConfig()
	if err != nil {
		t.Skipf("live R2 resource projection test requires valid blob config: %v", err)
	}
	return liveResourceProjectionEnv{
		blobConfig:       blobConfig,
		bucket:           blobConfig.Bucket,
		r2AccountID:      os.Getenv(EnvR2AccountID),
		r2ParentAPIToken: os.Getenv(EnvR2ParentAPIToken),
		r2ParentAccessID: os.Getenv(EnvR2ParentAccessKeyID),
	}
}

func loadLiveResourceProjectionEnv(t *testing.T) liveResourceProjectionEnv {
	t.Helper()
	if os.Getenv(envResourceProjectionLive) != "1" {
		t.Skipf("%s=1 is required for live Daytona/R2 resource projection smoke", envResourceProjectionLive)
	}
	missing := missingEnvVars(
		envResourceProjectionLiveArtifactRef,
		EnvDaytonaAPIURL,
		EnvDaytonaAPIKey,
		EnvR2AccountID,
		EnvR2ParentAPIToken,
		EnvR2ParentAccessKeyID,
	)
	if len(missing) > 0 {
		t.Skipf("live resource projection smoke requires env vars: %s", strings.Join(missing, ", "))
	}
	blobConfig, err := blob.LoadConfig()
	if err != nil {
		t.Skipf("live resource projection smoke requires valid blob config: %v", err)
	}
	return liveResourceProjectionEnv{
		daytona: driver.Config{
			DaytonaAPIURL:  os.Getenv(EnvDaytonaAPIURL),
			DaytonaTarget:  os.Getenv(EnvDaytonaTarget),
			DaytonaAPIKey:  os.Getenv(EnvDaytonaAPIKey),
			CommandTimeout: 2 * time.Minute,
		},
		blobConfig:       blobConfig,
		bucket:           blobConfig.Bucket,
		r2AccountID:      os.Getenv(EnvR2AccountID),
		r2ParentAPIToken: os.Getenv(EnvR2ParentAPIToken),
		r2ParentAccessID: os.Getenv(EnvR2ParentAccessKeyID),
		artifactRef:      os.Getenv(envResourceProjectionLiveArtifactRef),
	}
}

func missingEnvVars(names ...string) []string {
	var missing []string
	for _, name := range names {
		if strings.TrimSpace(os.Getenv(name)) == "" {
			missing = append(missing, name)
		}
	}
	return missing
}

func newLiveBlobStore(ctx context.Context, t *testing.T, live liveResourceProjectionEnv) *blob.S3BlobStore {
	t.Helper()
	store, err := blob.NewS3BlobStore(ctx, live.blobConfig)
	if err != nil {
		t.Fatalf("NewS3BlobStore: %v", err)
	}
	return store
}

func newLiveCountingCredentialMinter(t *testing.T, live liveResourceProjectionEnv) *liveCountingCredentialMinter {
	t.Helper()
	inner, err := resourceprojection.NewCredentialMinter(resourceprojection.CredentialMintConfig{
		AccountID:         live.r2AccountID,
		Bucket:            live.bucket,
		ParentAccessKeyID: live.r2ParentAccessID,
		ParentAPIToken:    live.r2ParentAPIToken,
	})
	if err != nil {
		t.Fatalf("NewCredentialMinter: %v", err)
	}
	return &liveCountingCredentialMinter{inner: inner}
}

type liveCountingCredentialMinter struct {
	mu       sync.Mutex
	inner    resourceCredentialMinter
	requests []resourceprojection.CredentialMintRequest
}

func (m *liveCountingCredentialMinter) Mint(ctx context.Context, request resourceprojection.CredentialMintRequest) (resourceprojection.CredentialMintResult, error) {
	m.mu.Lock()
	m.requests = append(m.requests, request)
	m.mu.Unlock()
	return m.inner.Mint(ctx, request)
}

func (m *liveCountingCredentialMinter) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.requests)
}

func newLiveFUSEBindPreparer(t *testing.T, blobStore blob.BlobStore, minter resourceCredentialMinter, runner driver.DaytonaCommandRunner, live liveResourceProjectionEnv) *DaytonaFileResourceMaterializer {
	t.Helper()
	preparer, err := NewDaytonaFileResourceMaterializer(DaytonaFileResourceMaterializerConfig{
		Blob:                    blobStore,
		CredentialMinter:        minter,
		CommandRunner:           runner,
		Bucket:                  live.bucket,
		AccountID:               live.r2AccountID,
		CredentialTTL:           time.Hour,
		CredentialRefreshMargin: 10 * time.Minute,
		CommandTimeout:          2 * time.Minute,
		RcloneVFSCacheMaxSize:   "64M",
		RcloneVFSMinFree:        "16M",
	})
	if err != nil {
		t.Fatalf("NewDaytonaFileResourceMaterializer: %v", err)
	}
	return preparer
}

func createLiveDaytonaSandbox(ctx context.Context, t *testing.T, live liveResourceProjectionEnv, setup sandbox.SandboxSetup) (*driver.DaytonaLifecycleProvider, *driver.DaytonaHelperExecutor, sandbox.ProviderHandle) {
	t.Helper()
	client, err := daytona.NewClientWithConfig(&types.DaytonaConfig{
		APIKey: live.daytona.DaytonaAPIKey, APIUrl: live.daytona.DaytonaAPIURL, Target: live.daytona.DaytonaTarget,
	})
	if err != nil {
		t.Fatalf("create Daytona SDK client: %v", err)
	}
	provider, err := driver.NewDaytonaLifecycleProviderForSDKClient(client, live.daytona)
	if err != nil {
		t.Fatalf("NewDaytonaLifecycleProviderForSDKClient: %v", err)
	}
	runner, err := driver.NewDaytonaHelperExecutorForSDKClient(client, live.daytona.CommandTimeout)
	if err != nil {
		t.Fatalf("NewDaytonaHelperExecutorForSDKClient: %v", err)
	}
	handle, err := provider.CreateSandbox(ctx, sandbox.CreateSandboxRequest{Setup: setup})
	if err != nil {
		t.Fatalf("CreateSandbox: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cleanupCancel()
		_ = provider.ReleaseSandbox(cleanupCtx, handle)
	})
	if err := provider.CheckBaseTemplateHealth(ctx, handle); err != nil {
		t.Fatalf("CheckBaseTemplateHealth: %v", err)
	}
	if err := provider.PrepareBaseDirectories(ctx, handle); err != nil {
		t.Fatalf("PrepareBaseDirectories: %v", err)
	}
	return provider, runner, handle
}

func liveResourceProjectionIDs(t *testing.T) (string, string, string) {
	t.Helper()
	suffix := liveRandomSuffix(t)
	return "ws-live-" + suffix, "sesn-live-" + suffix, "tetral-rp-live-" + suffix
}

func liveRandomSuffix(t *testing.T) string {
	t.Helper()
	var raw [6]byte
	if _, err := rand.Read(raw[:]); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	return hex.EncodeToString(raw[:])
}

func liveFile(resourceID, sessionFileID, objectID, mountPath string) sandbox.FileMount {
	return sandbox.FileMount{
		ResourceID:    resourceID,
		SourceFileID:  "src_" + sessionFileID,
		SessionFileID: sessionFileID,
		ObjectID:      objectID,
		MountPath:     mountPath,
		ReadOnly:      true,
	}
}

func putLiveCanonical(ctx context.Context, t *testing.T, store blob.BlobStore, workspaceID string, objectID string, body []byte) {
	t.Helper()
	key := resourceprojection.CanonicalObjectKey(workspaceID, objectID)
	putLiveBlob(ctx, t, store, key, body)
}

func putLiveBlob(ctx context.Context, t *testing.T, store blob.BlobStore, key string, body []byte) {
	t.Helper()
	if err := store.Put(ctx, key, bytes.NewReader(body), int64(len(body))); err != nil {
		t.Fatalf("put live blob %s: %v", key, err)
	}
}

func newLiveBlobStoreWithCredential(ctx context.Context, t *testing.T, live liveResourceProjectionEnv, credential resourceprojection.Credential) *blob.S3BlobStore {
	t.Helper()
	config := *live.blobConfig
	config.AccessKey = credential.AccessKeyID
	config.SecretKey = credential.SecretAccessKey
	config.SessionToken = credential.SessionToken
	store, err := blob.NewS3BlobStore(ctx, &config)
	if err != nil {
		t.Fatalf("NewS3BlobStore temporary credential: %v", err)
	}
	return store
}

func assertLiveBlobBytes(ctx context.Context, t *testing.T, store blob.BlobStore, key string, want string) {
	t.Helper()
	reader, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get(%s): %v", key, err)
	}
	body, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil {
		t.Fatalf("read %s: %v", key, readErr)
	}
	if closeErr != nil {
		t.Fatalf("close %s: %v", key, closeErr)
	}
	if string(body) != want {
		t.Fatalf("Get(%s) = %q; want %q", key, string(body), want)
	}
}

func assertLiveTempCredentialCannotGet(ctx context.Context, t *testing.T, store blob.BlobStore, key string) {
	t.Helper()
	reader, err := store.Get(ctx, key)
	if err == nil {
		body, readErr := io.ReadAll(reader)
		closeErr := reader.Close()
		if readErr != nil {
			t.Fatalf("unexpected readable %s and read failed: %v", key, readErr)
		}
		if closeErr != nil {
			t.Fatalf("unexpected readable %s and close failed: %v", key, closeErr)
		}
		t.Fatalf("temporary resource credential read %s = %q; want prefix-scope denial", key, string(body))
	}
}

func assertLiveTempCredentialCannotList(ctx context.Context, t *testing.T, store *blob.S3BlobStore, prefix string) {
	t.Helper()
	keys, err := store.ListPrefix(ctx, prefix)
	if err == nil {
		t.Fatalf("temporary resource credential listed prefix %s = %v; want prefix-scope denial", prefix, keys)
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func assertLiveProjectionReady(ctx context.Context, t *testing.T, runner driver.DaytonaCommandRunner, handle sandbox.ProviderHandle, files []sandbox.FileMount, workspaceID string, sessionID string, bucket string) {
	t.Helper()
	var b strings.Builder
	b.WriteString("set -eu\n")
	b.WriteString("PATH=/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin\n")
	hasReceipt := false
	hasMmap := false
	for _, file := range files {
		mountPath := file.MountPath
		if mountPath == "" {
			mountPath = "/mnt/session/uploads/" + file.SessionFileID
		}
		hasReceipt = hasReceipt || mountPath == "/uploads/receipt.pdf"
		hasMmap = hasMmap || mountPath == "/workspace/live/mmap.bin"
		parent := parentDirForLiveShell(mountPath)
		b.WriteString("sudo -u " + shellQuote(driver.RuntimeUser) + " test ! -L " + shellQuote(mountPath) + "\n")
		b.WriteString("sudo -u " + shellQuote(driver.RuntimeUser) + " test -f " + shellQuote(mountPath) + "\n")
		b.WriteString("sudo -u " + shellQuote(driver.RuntimeUser) + " head -c 1 -- " + shellQuote(mountPath) + " >/dev/null\n")
		b.WriteString("if sudo -u " + shellQuote(driver.RuntimeUser) + " sh -c 'printf live-write > \"$1\"' _ " + shellQuote(mountPath) + " 2>/dev/null; then echo 'resource file was writable: " + mountPath + "' >&2; false; fi\n")
		b.WriteString("sudo -u " + shellQuote(driver.RuntimeUser) + " sh -c 'tmp=$(mktemp \"$1/.tetral-live-parent.XXXXXX\") && rm -f \"$tmp\"' _ " + shellQuote(parent) + "\n")
		b.WriteString("if [ " + shellQuote(parent) + " != " + shellQuote(mountPath) + " ]; then sudo -u " + shellQuote(driver.RuntimeUser) + " test -w " + shellQuote(parent) + "; fi\n")
		b.WriteString("if findmnt -rn --mountpoint " + shellQuote(mountPath) + " >/dev/null 2>&1; then if [ \"$(findmnt -rn --mountpoint " + shellQuote(mountPath) + " | wc -l)\" -ne 1 ]; then echo 'stacked bind at " + mountPath + "' >&2; false; fi; fi\n")
	}
	if hasReceipt {
		b.WriteString("sudo -u " + shellQuote(driver.RuntimeUser) + " grep -q 'receipt bytes' -- '/uploads/receipt.pdf'\n")
	}
	b.WriteString("if mountpoint -q '/mnt/tetral/r2'; then pgrep -f " + shellQuote("rclone.*"+bucket+".*"+workspaceID+".*"+sessionID+".*resources") + " >/dev/null; fi\n")
	b.WriteString("sudo -u " + shellQuote(driver.RuntimeUser) + " sh -c 'printf output-ok > /mnt/session/outputs/resource-projection-live.txt'\n")
	if hasMmap {
		b.WriteString(liveMmapReadCommand("/workspace/live/mmap.bin", "mmap-read-block"))
	}
	if err := runner.RunDaytonaCommand(ctx, driver.DaytonaCommandTarget{ProviderSandboxID: handle.SandboxID}, b.String(), nil, 2*time.Minute); err != nil {
		t.Fatalf("live projection verification command: %v", err)
	}
}

func snapshotLiveBindMountIDs(ctx context.Context, t *testing.T, runner driver.DaytonaCommandRunner, handle sandbox.ProviderHandle, files []sandbox.FileMount) {
	t.Helper()
	const snapshotPath = "/tmp/tetral-runtime/resource-projection-replay.mount-ids"
	var b strings.Builder
	b.WriteString("set -eu\n")
	b.WriteString("SNAPSHOT=" + shellQuote(snapshotPath) + "\n")
	b.WriteString(": > \"$SNAPSHOT\"\n")
	for _, file := range files {
		mountPath := file.MountPath
		if mountPath == "" {
			mountPath = "/mnt/session/uploads/" + file.SessionFileID
		}
		sourcePath := resourceprojection.RcloneStagingRoot + "/" + file.ResourceID + "/file"
		b.WriteString("[ " + shellQuote(sourcePath) + " -ef " + shellQuote(mountPath) + " ]\n")
		b.WriteString("findmnt -rn --mountpoint " + shellQuote(mountPath) + " --output ID >> \"$SNAPSHOT\"\n")
	}
	if err := runner.RunDaytonaCommand(ctx, driver.DaytonaCommandTarget{ProviderSandboxID: handle.SandboxID}, b.String(), nil, 2*time.Minute); err != nil {
		t.Fatalf("snapshot live bind mount IDs: %v", err)
	}
}

func assertLiveBindMountIDsUnchanged(ctx context.Context, t *testing.T, runner driver.DaytonaCommandRunner, handle sandbox.ProviderHandle, files []sandbox.FileMount) {
	t.Helper()
	const snapshotPath = "/tmp/tetral-runtime/resource-projection-replay.mount-ids"
	const currentPath = "/tmp/tetral-runtime/resource-projection-replay.current-mount-ids"
	var b strings.Builder
	b.WriteString("set -eu\n")
	b.WriteString("SNAPSHOT=" + shellQuote(snapshotPath) + "\n")
	b.WriteString("CURRENT=" + shellQuote(currentPath) + "\n")
	b.WriteString(": > \"$CURRENT\"\n")
	for _, file := range files {
		mountPath := file.MountPath
		if mountPath == "" {
			mountPath = "/mnt/session/uploads/" + file.SessionFileID
		}
		b.WriteString("findmnt -rn --mountpoint " + shellQuote(mountPath) + " --output ID >> \"$CURRENT\"\n")
	}
	b.WriteString("cmp -s \"$SNAPSHOT\" \"$CURRENT\" || { echo 'resource projection replay recreated a correct bind mount' >&2; false; }\n")
	b.WriteString("rm -f -- \"$SNAPSHOT\" \"$CURRENT\"\n")
	if err := runner.RunDaytonaCommand(ctx, driver.DaytonaCommandTarget{ProviderSandboxID: handle.SandboxID}, b.String(), nil, 2*time.Minute); err != nil {
		t.Fatalf("bind mount IDs changed across idempotent replay: %v", err)
	}
}

func parentDirForLiveShell(filePath string) string {
	idx := strings.LastIndex(filePath, "/")
	if idx <= 0 {
		return "/"
	}
	return filePath[:idx]
}

func liveMmapReadCommand(path string, want string) string {
	return "sudo -u " + shellQuote(driver.RuntimeUser) + " python3 - <<'PY'\n" +
		"import mmap, os\n" +
		"path = " + strconv.Quote(path) + "\n" +
		"want = " + strconv.Quote(want) + ".encode()\n" +
		"fd = os.open(path, os.O_RDONLY)\n" +
		"try:\n" +
		"    mm = mmap.mmap(fd, 0, access=mmap.ACCESS_READ)\n" +
		"    try:\n" +
		"        data = mm[:]\n" +
		"        assert want in data\n" +
		"    finally:\n" +
		"        mm.close()\n" +
		"finally:\n" +
		"    os.close(fd)\n" +
		"PY\n"
}

func assertLiveCredentialBoundary(ctx context.Context, t *testing.T, runner driver.DaytonaCommandRunner, store blob.BlobStore, handle sandbox.ProviderHandle, workspaceID string, sessionID string, bucket string) {
	t.Helper()
	sessionKey := resourceprojection.SessionResourceKey(workspaceID, sessionID, "sesrsc_default")
	command := strings.Join([]string{
		"set -eu",
		"PATH=/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin",
		"STAGING=/mnt/tetral/r2",
		"if findmnt -rn --mountpoint '/mnt/session/uploads/file_default' >/dev/null 2>&1; then sudo umount -l -- '/mnt/session/uploads/file_default'; fi",
		"if mountpoint -q \"$STAGING\"; then sudo umount -l -- \"$STAGING\"; fi",
		"setsid sudo rclone --config /tmp/tetral-runtime/rclone.conf mount " + shellQuote("r2:"+bucket+"/"+strings.TrimSuffix(resourceprojection.SessionResourcesPrefix(workspaceID, sessionID), "/")) + " \"$STAGING\" --allow-other --vfs-cache-mode full --vfs-cache-max-size 64M --vfs-cache-min-free-space 16M --vfs-cache-max-age 1m --dir-cache-time 1m --poll-interval 0 --log-level INFO --daemon --daemon-wait 30s </dev/null >/dev/null 2>&1",
		"sudo -u " + shellQuote(driver.RuntimeUser) + " sh -c 'printf mutated-through-non-readonly-remount > \"$1\"' _ \"$STAGING/sesrsc_default/file\"",
		"sync || true",
		"sleep 3",
	}, "\n") + "\n"
	if err := runner.RunDaytonaCommand(ctx, driver.DaytonaCommandTarget{ProviderSandboxID: handle.SandboxID}, command, nil, 2*time.Minute); err != nil {
		t.Fatalf("credential-boundary remount/write command: %v", err)
	}
	reader, err := store.Get(ctx, sessionKey)
	if err != nil {
		t.Fatalf("get session object after credential-boundary write: %v", err)
	}
	body, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil {
		t.Fatalf("read session object after credential-boundary write: %v", readErr)
	}
	if closeErr != nil {
		t.Fatalf("close session object after credential-boundary write: %v", closeErr)
	}
	if string(body) != "default bytes\n" {
		t.Fatalf("session object mutated through prefix credential: %q", string(body))
	}
}
