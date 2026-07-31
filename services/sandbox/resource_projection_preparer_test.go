package tetralsandbox

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/sandbox"
	"github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/internal/workspace"
	"github.com/tetral-ai/tetral/services/sandbox/internal/resourceprojection"
)

func TestResourceProjectionPreparerCopiesMintsRunsCommandAndReturnsMetadata(t *testing.T) {
	ctx := context.Background()
	blobStore := blob.NewFakeBlobStore()
	if err := blobStore.Put(ctx, "files/ws_test/obj_file", bytes.NewReader([]byte("canonical")), int64(len("canonical"))); err != nil {
		t.Fatalf("put canonical: %v", err)
	}
	expiresAt := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	minter := &recordingResourceCredentialMinter{result: resourceprojection.CredentialMintResult{
		Credential: resourceprojection.Credential{
			AccessKeyID:     "access-key",
			SecretAccessKey: "secret-key",
			SessionToken:    "session-token",
		},
		ExpiresAt: expiresAt,
		Prefix:    "workspaces/ws_test/sessions/sesn_test/resources/",
	}}
	runner := &recordingPreparationCommandRunner{}
	preparer := newTestResourceProjectionPreparer(t, blobStore, minter, runner)

	prepared, err := preparer.PrepareSessionResources(ctx, testResourceProjectionSetup(), sandbox.ProviderHandle{SandboxID: "provider_sandbox"})
	if err != nil {
		t.Fatalf("PrepareSessionResources: %v", err)
	}
	if len(prepared.Files) != 0 {
		t.Fatalf("prepared files = %+v; want raw file mounts consumed by bind projection", prepared.Files)
	}
	if prepared.ResourceCredExpiresAt == nil || !prepared.ResourceCredExpiresAt.Equal(expiresAt) {
		t.Fatalf("ResourceCredExpiresAt = %v; want %s", prepared.ResourceCredExpiresAt, expiresAt)
	}
	if got, want := prepared.ResourceRootsJSON, `[{"path":"/mnt/session/uploads/file_session","mode":"read"}]`; got != want {
		t.Fatalf("ResourceRootsJSON = %s; want %s", got, want)
	}
	assertBlobBytes(t, blobStore, "workspaces/ws_test/sessions/sesn_test/resources/sesrsc_file/file", "canonical")
	if len(minter.requests) != 1 || minter.requests[0].TTL != 24*time.Hour || minter.requests[0].WorkspaceID != "ws_test" || minter.requests[0].SessionID != "sesn_test" {
		t.Fatalf("mint requests = %+v; want session ttl mint", minter.requests)
	}
	if len(runner.calls) != 1 {
		t.Fatalf("runner calls = %d; want one preparation exec", len(runner.calls))
	}
	call := runner.calls[0]
	if call.target.ProviderSandboxID != "provider_sandbox" || call.timeout != 45*time.Second {
		t.Fatalf("runner call target=%+v timeout=%s; want provider sandbox and 45s timeout", call.target, call.timeout)
	}
	for _, secret := range []string{"access-key", "secret-key", "session-token"} {
		if strings.Contains(call.command, secret) {
			t.Fatalf("command leaked secret %q: %s", secret, call.command)
		}
	}
	for _, fragment := range []string{
		"RCLONE_CONFIG=/tmp/tetral-runtime/rclone.conf",
		"cat > \"$RCLONE_CONFIG\" <<EOF",
		"access_key_id = ${RCLONE_CONFIG_R2_ACCESS_KEY_ID}",
		"session_token = ${RCLONE_CONFIG_R2_SESSION_TOKEN}",
		"chmod 0600 \"$RCLONE_CONFIG\"",
		"if ! timeout 5s ls \"$STAGING\" >/dev/null 2>&1; then sudo umount -l -- \"$STAGING\"; fi",
		"setsid sudo rclone --config \"$RCLONE_CONFIG\" mount 'r2:tetral-files/workspaces/ws_test/sessions/sesn_test/resources'",
		"--read-only --allow-other --vfs-cache-mode full",
		"findmnt -rn --mountpoint '/mnt/session/uploads/file_session'",
		"if ! [ '/mnt/tetral/r2/sesrsc_file/file' -ef '/mnt/session/uploads/file_session' ]; then sudo umount -l -- '/mnt/session/uploads/file_session'; fi",
		"sudo mount --bind '/mnt/tetral/r2/sesrsc_file/file' '/mnt/session/uploads/file_session'",
		"sudo install -d -m 0755 -o '" + driver.RuntimeUser + "' -g '" + driver.RuntimeUser + "' -- '/mnt/session/uploads'",
		"if [ -L '/mnt/session/uploads/file_session' ]; then sudo -u '" + driver.RuntimeUser + "' rm -f -- '/mnt/session/uploads/file_session'; fi",
		"if [ -e '/mnt/session/uploads/file_session' ] && [ ! -f '/mnt/session/uploads/file_session' ]; then echo 'resource projection target is not a regular file' >&2; false; fi",
		"sudo -u '" + driver.RuntimeUser + "' touch -- '/mnt/session/uploads/file_session'",
		"sudo -u '" + driver.RuntimeUser + "' test ! -L '/mnt/session/uploads/file_session'",
		"sudo -u '" + driver.RuntimeUser + "' test -f '/mnt/session/uploads/file_session'",
		"sudo -u '" + driver.RuntimeUser + "' head -c 1 -- '/mnt/session/uploads/file_session'",
	} {
		if !strings.Contains(call.command, fragment) {
			t.Fatalf("command missing fragment %q in:\n%s", fragment, call.command)
		}
	}
	if strings.Contains(call.command, "findmnt -rn --target") {
		t.Fatalf("command used non-exact findmnt --target guard:\n%s", call.command)
	}
	if strings.Contains(call.command, "--output SOURCE") {
		t.Fatalf("command compared rendered findmnt SOURCE instead of same-file identity:\n%s", call.command)
	}
	assertResourceProjectionCommandRemountsBeforeFreshMount(t, call.command)
	assertResourceProjectionCommandRejectsSymlinkTargetBeforeTouch(t, call.command)
	if call.env["RCLONE_CONFIG_R2_ACCESS_KEY_ID"] != "access-key" ||
		call.env["RCLONE_CONFIG_R2_SECRET_ACCESS_KEY"] != "secret-key" ||
		call.env["RCLONE_CONFIG_R2_SESSION_TOKEN"] != "session-token" ||
		call.env["RCLONE_CONFIG_R2_ENDPOINT"] != "https://acct_123.r2.cloudflarestorage.com" ||
		call.env["RCLONE_CONFIG_R2_NO_CHECK_BUCKET"] != "true" {
		t.Fatalf("runner env = %+v; want rclone env with minted triple and endpoint", call.env)
	}
}

func TestResourceProjectionPreparerBatchesTwentyFilesUnderOneMountAndCredential(t *testing.T) {
	ctx := context.Background()
	blobStore := blob.NewFakeBlobStore()
	files := make([]sandbox.FileMount, 0, 20)
	for i := 0; i < 20; i++ {
		objectID := fmt.Sprintf("obj_%02d", i)
		resourceID := fmt.Sprintf("sesrsc_file_%02d", i)
		sessionFileID := fmt.Sprintf("file_%02d", i)
		canonical := fmt.Sprintf("canonical-%02d", i)
		if err := blobStore.Put(ctx, "files/ws_test/"+objectID, bytes.NewReader([]byte(canonical)), int64(len(canonical))); err != nil {
			t.Fatalf("put canonical %d: %v", i, err)
		}
		files = append(files, sandbox.FileMount{
			ResourceID:    resourceID,
			SourceFileID:  "source_" + sessionFileID,
			SessionFileID: sessionFileID,
			ObjectID:      objectID,
			MountPath:     "/mnt/session/uploads/" + sessionFileID,
			ReadOnly:      true,
		})
	}
	expiresAt := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	minter := &recordingResourceCredentialMinter{result: resourceprojection.CredentialMintResult{
		Credential: resourceprojection.Credential{
			AccessKeyID:     "access-key",
			SecretAccessKey: "secret-key",
			SessionToken:    "session-token",
		},
		ExpiresAt: expiresAt,
		Prefix:    "workspaces/ws_test/sessions/sesn_test/resources/",
	}}
	runner := &recordingPreparationCommandRunner{}
	preparer := newTestResourceProjectionPreparer(t, blobStore, minter, runner)
	setup := sandbox.SandboxSetup{
		WorkspaceID: workspace.ID("ws_test"),
		SessionID:   "sesn_test",
		SandboxID:   "sandbox_test",
		Resources:   sandbox.ResourceSetup{Files: files},
	}

	prepared, err := preparer.PrepareSessionResources(ctx, setup, sandbox.ProviderHandle{SandboxID: "provider_sandbox"})
	if err != nil {
		t.Fatalf("PrepareSessionResources: %v", err)
	}
	if len(prepared.Files) != 0 {
		t.Fatalf("prepared files = %+v; want raw file mounts consumed", prepared.Files)
	}
	if prepared.ResourceCredExpiresAt == nil || !prepared.ResourceCredExpiresAt.Equal(expiresAt) {
		t.Fatalf("ResourceCredExpiresAt = %v; want shared credential expiry %s", prepared.ResourceCredExpiresAt, expiresAt)
	}
	if len(minter.requests) != 1 {
		t.Fatalf("credential mint requests = %+v; want one session-scoped credential", minter.requests)
	}
	if len(runner.calls) != 1 {
		t.Fatalf("runner calls = %d; want one batched mount/bind command", len(runner.calls))
	}
	command := runner.calls[0].command
	if got := strings.Count(command, "setsid sudo rclone --config \"$RCLONE_CONFIG\" mount"); got != 1 {
		t.Fatalf("rclone mount count = %d; want exactly one command:\n%s", got, command)
	}
	if got := strings.Count(command, "sudo mount --bind "); got != 20 {
		t.Fatalf("bind mount count = %d; want one per file", got)
	}
	for i := 0; i < 20; i++ {
		resourceID := fmt.Sprintf("sesrsc_file_%02d", i)
		sessionFileID := fmt.Sprintf("file_%02d", i)
		assertBlobBytes(t, blobStore, "workspaces/ws_test/sessions/sesn_test/resources/"+resourceID+"/file", fmt.Sprintf("canonical-%02d", i))
		for _, fragment := range []string{
			"sudo mount --bind '/mnt/tetral/r2/" + resourceID + "/file' '/mnt/session/uploads/" + sessionFileID + "'",
			`"path":"/mnt/session/uploads/` + sessionFileID + `"`,
		} {
			if !strings.Contains(command+prepared.ResourceRootsJSON, fragment) {
				t.Fatalf("batched projection missing fragment %q\ncommand:\n%s\nroots:%s", fragment, command, prepared.ResourceRootsJSON)
			}
		}
	}
}

func TestResourceProjectionPreparerCredentialMintRejectionStopsBeforeMountAndDeletesSessionCopies(t *testing.T) {
	ctx := context.Background()
	blobStore := blob.NewFakeBlobStore()
	if err := blobStore.Put(ctx, "files/ws_test/obj_file", bytes.NewReader([]byte("canonical")), int64(len("canonical"))); err != nil {
		t.Fatalf("put canonical: %v", err)
	}
	mintErr := errors.New("r2 rejected requested ttl")
	minter := &recordingResourceCredentialMinter{err: mintErr}
	runner := &recordingPreparationCommandRunner{}
	preparer := newTestResourceProjectionPreparer(t, blobStore, minter, runner)

	_, err := preparer.PrepareSessionResources(ctx, testResourceProjectionSetup(), sandbox.ProviderHandle{SandboxID: "provider_sandbox"})
	if !errors.Is(err, mintErr) {
		t.Fatalf("PrepareSessionResources err = %v; want mint rejection", err)
	}
	if len(minter.requests) != 1 || minter.requests[0].TTL != 24*time.Hour {
		t.Fatalf("mint requests = %+v; want one unclamped default TTL mint", minter.requests)
	}
	if len(runner.calls) != 0 {
		t.Fatalf("runner calls = %d; want no mount/bind command after mint rejection", len(runner.calls))
	}
	assertBlobAbsent(t, blobStore, "workspaces/ws_test/sessions/sesn_test/resources/sesrsc_file/file")
}

func TestResourceProjectionPreparerCreatesArbitraryAbsoluteParentAsRuntimeUser(t *testing.T) {
	ctx := context.Background()
	blobStore := blob.NewFakeBlobStore()
	if err := blobStore.Put(ctx, "files/ws_test/obj_file", bytes.NewReader([]byte("canonical")), int64(len("canonical"))); err != nil {
		t.Fatalf("put canonical: %v", err)
	}
	runner := &recordingPreparationCommandRunner{}
	minter := &recordingResourceCredentialMinter{result: resourceprojection.CredentialMintResult{
		Credential: resourceprojection.Credential{AccessKeyID: "access-key", SecretAccessKey: "secret-key", SessionToken: "session-token"},
		ExpiresAt:  time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC),
	}}
	preparer := newTestResourceProjectionPreparer(t, blobStore, minter, runner)
	setup := testResourceProjectionSetup()
	setup.Resources.Files[0].MountPath = "/uploads/receipt.pdf"

	prepared, err := preparer.PrepareSessionResources(ctx, setup, sandbox.ProviderHandle{SandboxID: "provider_sandbox"})
	if err != nil {
		t.Fatalf("PrepareSessionResources: %v", err)
	}
	if got, want := prepared.ResourceRootsJSON, `[{"path":"/uploads/receipt.pdf","mode":"read"}]`; got != want {
		t.Fatalf("ResourceRootsJSON = %s; want %s", got, want)
	}
	if len(runner.calls) != 1 {
		t.Fatalf("runner calls = %d; want one preparation exec", len(runner.calls))
	}
	command := runner.calls[0].command
	for _, fragment := range []string{
		"if [ -e '/uploads' ]; then",
		"sudo -u '" + driver.RuntimeUser + "' test -w '/uploads'",
		"sudo -u '" + driver.RuntimeUser + "' test -x '/uploads'",
		"sudo install -d -m 0755 -o '" + driver.RuntimeUser + "' -g '" + driver.RuntimeUser + "' -- '/uploads'",
		"sudo -u '" + driver.RuntimeUser + "' touch -- '/uploads/receipt.pdf'",
		"sudo mount --bind '/mnt/tetral/r2/sesrsc_file/file' '/uploads/receipt.pdf'",
		"sudo -u '" + driver.RuntimeUser + "' sh -c 'tmp=$(mktemp \"$1/.tetral-resource-verify.XXXXXX\") && rm -f \"$tmp\"' _ '/uploads'",
	} {
		if !strings.Contains(command, fragment) {
			t.Fatalf("command missing fragment %q in:\n%s", fragment, command)
		}
	}
	if strings.Contains(command, "sudo -u '"+driver.RuntimeUser+"' mkdir -p -- '/uploads'") {
		t.Fatalf("command creates root-level parent as runtime user:\n%s", command)
	}
	for _, forbidden := range []string{"sudo chown ", "sudo chmod 0755 -- '/uploads'"} {
		if strings.Contains(command, forbidden) {
			t.Fatalf("command mutates existing parent metadata with %q:\n%s", forbidden, command)
		}
	}
}

func TestResourceProjectionPreparerMaterializesSkillPackages(t *testing.T) {
	ctx := context.Background()
	blobStore := blob.NewFakeBlobStore()
	skillZip := buildSkillPackageZip(t, "finance")
	skillSHA := sha256Hex(skillZip)
	const skillBlobKey = "skills/ws_test/skill_finance/versions/1/package.zip"
	if err := blobStore.Put(ctx, skillBlobKey, bytes.NewReader(skillZip), int64(len(skillZip))); err != nil {
		t.Fatalf("put skill package: %v", err)
	}
	expiresAt := time.Date(2026, 7, 2, 11, 0, 0, 0, time.UTC)
	minter := &recordingResourceCredentialMinter{result: resourceprojection.CredentialMintResult{ExpiresAt: expiresAt}}
	runner := &recordingPreparationCommandRunner{}
	preparer := newTestResourceProjectionPreparer(t, blobStore, minter, runner)
	setup := sandbox.SandboxSetup{
		WorkspaceID: workspace.ID("ws_test"),
		SessionID:   "sesn_skill",
		Resources: sandbox.ResourceSetup{Skills: []sandbox.SkillMount{{
			SkillID:        "skill_finance",
			SkillVersionID: "skill_version_finance",
			Version:        "1",
			Directory:      "finance",
			BlobKey:        skillBlobKey,
			SizeBytes:      int64(len(skillZip)),
			SHA256:         skillSHA,
		}}},
	}

	prepared, err := preparer.PrepareSessionResources(ctx, setup, sandbox.ProviderHandle{SandboxID: "provider_sandbox"})
	if err != nil {
		t.Fatalf("PrepareSessionResources: %v", err)
	}
	if prepared.ResourceRootsJSON != "[]" || prepared.ResourceCredExpiresAt == nil || !prepared.ResourceCredExpiresAt.Equal(expiresAt) {
		t.Fatalf("prepared roots=%q expires=%v; want bounded materialization receipt", prepared.ResourceRootsJSON, prepared.ResourceCredExpiresAt)
	}
	if len(minter.requests) != 1 {
		t.Fatalf("credential mint requests = %d; want one for skill-only materialization", len(minter.requests))
	}
	if len(runner.uploads) != 1 {
		t.Fatalf("uploads = %+v; want one staged skill archive", runner.uploads)
	}
	if !strings.HasPrefix(runner.uploads[0].remotePath, skillProjectionStagingRoot+"/") || !strings.HasSuffix(runner.uploads[0].remotePath, "/package.tar.gz") {
		t.Fatalf("skill upload path = %q; want skill projection staging archive", runner.uploads[0].remotePath)
	}
	if names := skillArchiveNames(t, []byte(runner.uploads[0].body)); strings.Join(names, ",") != "finance/,finance/SKILL.md" {
		t.Fatalf("staged skill archive names = %v; want finance directory and SKILL.md", names)
	}
	if len(runner.calls) != 1 {
		t.Fatalf("commands = %+v; want one skill materialization command", runner.calls)
	}
	command := runner.calls[0].command
	for _, want := range []string{"tar -xzf", "/skills/finance", "chmod 0444", "test -r '/skills/finance/SKILL.md'"} {
		if !strings.Contains(command, want) {
			t.Fatalf("skill materialization command missing %q:\n%s", want, command)
		}
	}
}

func TestResourceProjectionPreparerMintsBoundedReceiptWithoutFileResources(t *testing.T) {
	expiresAt := time.Date(2026, 7, 2, 11, 0, 0, 0, time.UTC)
	minter := &recordingResourceCredentialMinter{result: resourceprojection.CredentialMintResult{
		ExpiresAt: expiresAt,
	}}
	preparer := newTestResourceProjectionPreparer(
		t,
		blob.NewFakeBlobStore(),
		minter,
		&recordingPreparationCommandRunner{},
	)

	prepared, err := preparer.PrepareSessionResources(context.Background(), sandbox.SandboxSetup{
		WorkspaceID: workspace.ID("ws_test"),
		SessionID:   "sesn_no_resources",
	}, sandbox.ProviderHandle{SandboxID: "provider_sandbox"})
	if err != nil {
		t.Fatalf("PrepareSessionResources: %v", err)
	}
	if prepared.ResourceCredExpiresAt == nil || !prepared.ResourceCredExpiresAt.Equal(expiresAt) {
		t.Fatalf("resource credential expiry = %v; want %s", prepared.ResourceCredExpiresAt, expiresAt)
	}
	if len(minter.requests) != 1 {
		t.Fatalf("credential mint requests = %d; want one bounded materialization credential", len(minter.requests))
	}
}

func TestResourceProjectionPreparerRejectsSkillPackageHashMismatchBeforeCommand(t *testing.T) {
	ctx := context.Background()
	blobStore := blob.NewFakeBlobStore()
	skillZip := buildSkillPackageZip(t, "finance")
	const skillBlobKey = "skills/ws_test/skill_finance/versions/1/package.zip"
	if err := blobStore.Put(ctx, skillBlobKey, bytes.NewReader(skillZip), int64(len(skillZip))); err != nil {
		t.Fatalf("put skill package: %v", err)
	}
	runner := &recordingPreparationCommandRunner{}
	preparer := newTestResourceProjectionPreparer(t, blobStore, &recordingResourceCredentialMinter{}, runner)
	setup := sandbox.SandboxSetup{
		WorkspaceID: workspace.ID("ws_test"),
		SessionID:   "sesn_skill_hash",
		Resources: sandbox.ResourceSetup{Skills: []sandbox.SkillMount{{
			SkillID:        "skill_finance",
			SkillVersionID: "skill_version_finance",
			Version:        "1",
			Directory:      "finance",
			BlobKey:        skillBlobKey,
			SizeBytes:      int64(len(skillZip)),
			SHA256:         strings.Repeat("d", 64),
		}}},
	}

	_, err := preparer.PrepareSessionResources(ctx, setup, sandbox.ProviderHandle{SandboxID: "provider_sandbox"})
	if err == nil || !strings.Contains(err.Error(), "skill_package_sha256_mismatch") {
		t.Fatalf("PrepareSessionResources err = %v; want skill package sha mismatch", err)
	}
	if len(runner.uploads) != 0 || len(runner.calls) != 0 {
		t.Fatalf("runner uploads=%d commands=%d; want fail-before-command", len(runner.uploads), len(runner.calls))
	}
}

func TestResourceProjectionPreparerRejectsDuplicateSkillDirectoriesBeforeWrites(t *testing.T) {
	ctx := context.Background()
	blobStore := blob.NewFakeBlobStore()
	runner := &recordingPreparationCommandRunner{}
	preparer := newTestResourceProjectionPreparer(t, blobStore, &recordingResourceCredentialMinter{}, runner)
	setup := sandbox.SandboxSetup{
		WorkspaceID: workspace.ID("ws_test"),
		SessionID:   "sesn_skill_duplicate",
		Resources: sandbox.ResourceSetup{Skills: []sandbox.SkillMount{
			{SkillID: "skill_a", SkillVersionID: "skill_version_a", Version: "1", Directory: "finance", BlobKey: "skills/ws/skill_a.zip", SizeBytes: 1, SHA256: strings.Repeat("a", 64)},
			{SkillID: "skill_b", SkillVersionID: "skill_version_b", Version: "1", Directory: "finance", BlobKey: "skills/ws/skill_b.zip", SizeBytes: 1, SHA256: strings.Repeat("b", 64)},
		}},
	}

	_, err := preparer.PrepareSessionResources(ctx, setup, sandbox.ProviderHandle{SandboxID: "provider_sandbox"})
	if err == nil || !strings.Contains(err.Error(), "duplicate_skill_directory") {
		t.Fatalf("PrepareSessionResources err = %v; want duplicate skill directory", err)
	}
	if len(runner.uploads) != 0 || len(runner.calls) != 0 {
		t.Fatalf("runner uploads=%d commands=%d; want fail-before-write", len(runner.uploads), len(runner.calls))
	}
}

func TestResourceProjectionPreparerCommandFailureCleansMaterializedSkills(t *testing.T) {
	ctx := context.Background()
	blobStore := blob.NewFakeBlobStore()
	if err := blobStore.Put(ctx, "files/ws_test/obj_file", bytes.NewReader([]byte("canonical")), int64(len("canonical"))); err != nil {
		t.Fatalf("put canonical: %v", err)
	}
	skillZip := buildSkillPackageZip(t, "finance")
	const skillBlobKey = "skills/ws_test/skill_finance/versions/1/package.zip"
	if err := blobStore.Put(ctx, skillBlobKey, bytes.NewReader(skillZip), int64(len(skillZip))); err != nil {
		t.Fatalf("put skill package: %v", err)
	}
	runner := &recordingPreparationCommandRunner{errs: []error{nil, errors.New("mount failed"), nil}}
	minter := &recordingResourceCredentialMinter{result: resourceprojection.CredentialMintResult{
		Credential: resourceprojection.Credential{AccessKeyID: "access-key", SecretAccessKey: "secret-key", SessionToken: "session-token"},
		ExpiresAt:  time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC),
	}}
	preparer := newTestResourceProjectionPreparer(t, blobStore, minter, runner)
	setup := testResourceProjectionSetup()
	setup.Resources.Skills = []sandbox.SkillMount{{
		SkillID:        "skill_finance",
		SkillVersionID: "skill_version_finance",
		Version:        "1",
		Directory:      "finance",
		BlobKey:        skillBlobKey,
		SizeBytes:      int64(len(skillZip)),
		SHA256:         sha256Hex(skillZip),
	}}

	_, err := preparer.PrepareSessionResources(ctx, setup, sandbox.ProviderHandle{SandboxID: "provider_sandbox"})
	if err == nil || !strings.Contains(err.Error(), "mount failed") {
		t.Fatalf("PrepareSessionResources err = %v; want mount failure", err)
	}
	if len(runner.calls) != 3 {
		t.Fatalf("runner calls = %d; want skill materialization, file projection, skill cleanup", len(runner.calls))
	}
	cleanup := runner.calls[2].command
	for _, want := range []string{"rm -rf -- '/skills/finance'", skillProjectionStagingRoot + "/"} {
		if !strings.Contains(cleanup, want) {
			t.Fatalf("skill cleanup command missing %q:\n%s", want, cleanup)
		}
	}
}

func TestResourceProjectionPreparerLiveRotationWithoutCarriedExpiryTearsDownBeforeRemount(t *testing.T) {
	ctx := context.Background()
	blobStore := blob.NewFakeBlobStore()
	if err := blobStore.Put(ctx, "files/ws_test/obj_file", bytes.NewReader([]byte("canonical")), int64(len("canonical"))); err != nil {
		t.Fatalf("put canonical: %v", err)
	}
	newExpiresAt := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	runner := &recordingPreparationCommandRunner{}
	minter := &recordingResourceCredentialMinter{result: resourceprojection.CredentialMintResult{
		Credential: resourceprojection.Credential{AccessKeyID: "access-key", SecretAccessKey: "secret-key", SessionToken: "session-token"},
		ExpiresAt:  newExpiresAt,
	}}
	preparer := newTestResourceProjectionPreparer(t, blobStore, minter, runner)
	setup := testResourceProjectionSetup()
	setup.Resources.ResourceCredExpiresAt = nil

	prepared, err := preparer.PrepareSessionResources(ctx, setup, sandbox.ProviderHandle{SandboxID: "provider_sandbox"})
	if err != nil {
		t.Fatalf("PrepareSessionResources: %v", err)
	}
	if prepared.ResourceCredExpiresAt == nil || !prepared.ResourceCredExpiresAt.Equal(newExpiresAt) {
		t.Fatalf("ResourceCredExpiresAt = %v; want live-rotation minted expiry %s", prepared.ResourceCredExpiresAt, newExpiresAt)
	}
	call := runner.calls[0]
	targetUnmount := "if findmnt -rn --mountpoint '/mnt/session/uploads/file_session' >/dev/null 2>&1; then sudo umount -l -- '/mnt/session/uploads/file_session'; fi"
	stagingUnmount := "if mountpoint -q \"$STAGING\"; then sudo umount -l -- \"$STAGING\"; fi"
	mountCommand := "setsid sudo rclone --config \"$RCLONE_CONFIG\" mount"
	if !strings.Contains(call.command, targetUnmount) || !strings.Contains(call.command, stagingUnmount) {
		t.Fatalf("live rotation command missing teardown in:\n%s", call.command)
	}
	if strings.Index(call.command, targetUnmount) > strings.Index(call.command, mountCommand) ||
		strings.Index(call.command, stagingUnmount) > strings.Index(call.command, mountCommand) {
		t.Fatalf("live-rotation teardown must precede fresh rclone mount:\n%s", call.command)
	}
}

func TestResourceProjectionPreparerMintsByRecreatingMountWithoutCarriedExpiry(t *testing.T) {
	ctx := context.Background()
	blobStore := blob.NewFakeBlobStore()
	if err := blobStore.Put(ctx, "files/ws_test/obj_file", bytes.NewReader([]byte("canonical")), int64(len("canonical"))); err != nil {
		t.Fatalf("put canonical: %v", err)
	}
	expiresAt := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	runner := &recordingPreparationCommandRunner{}
	minter := &recordingResourceCredentialMinter{result: resourceprojection.CredentialMintResult{
		Credential: resourceprojection.Credential{AccessKeyID: "access-key", SecretAccessKey: "secret-key", SessionToken: "session-token"},
		ExpiresAt:  expiresAt,
	}}
	preparer := newTestResourceProjectionPreparer(t, blobStore, minter, runner)
	setup := testResourceProjectionSetup()
	setup.Resources.ResourceCredExpiresAt = nil

	prepared, err := preparer.PrepareSessionResources(ctx, setup, sandbox.ProviderHandle{SandboxID: "provider_sandbox"})
	if err != nil {
		t.Fatalf("PrepareSessionResources: %v", err)
	}
	if prepared.ResourceCredExpiresAt == nil || !prepared.ResourceCredExpiresAt.Equal(expiresAt) {
		t.Fatalf("ResourceCredExpiresAt = %v; want minted expiry %s", prepared.ResourceCredExpiresAt, expiresAt)
	}
	if len(minter.requests) != 1 {
		t.Fatalf("credential mint requests = %+v; want exactly one mint", minter.requests)
	}
	if len(runner.calls) != 1 {
		t.Fatalf("runner calls = %d; want one mount/bind command", len(runner.calls))
	}
	assertResourceProjectionCommandRemountsBeforeFreshMount(t, runner.calls[0].command)
}

func TestResourceProjectionPreparerCarriesExpiringExpiryWhenMountAlive(t *testing.T) {
	ctx := context.Background()
	blobStore := blob.NewFakeBlobStore()
	if err := blobStore.Put(ctx, "files/ws_test/obj_file", bytes.NewReader([]byte("canonical")), int64(len("canonical"))); err != nil {
		t.Fatalf("put canonical: %v", err)
	}
	oldExpiresAt := time.Date(2026, 7, 1, 11, 40, 0, 0, time.UTC)
	runner := &recordingPreparationCommandRunner{}
	minter := &recordingResourceCredentialMinter{}
	preparer, err := NewResourceProjectionPreparer(ResourceProjectionPreparerConfig{
		Blob:                    blobStore,
		CredentialMinter:        minter,
		CommandRunner:           runner,
		Bucket:                  "tetral-files",
		AccountID:               "acct_123",
		CredentialTTL:           24 * time.Hour,
		CredentialRefreshMargin: 45 * time.Minute,
		CommandTimeout:          45 * time.Second,
		RcloneVFSCacheMaxSize:   "2G",
		RcloneVFSMinFree:        "1G",
		Clock:                   func() time.Time { return time.Date(2026, 7, 1, 11, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatalf("NewResourceProjectionPreparer: %v", err)
	}
	setup := testResourceProjectionSetup()
	setup.Resources.ResourceCredExpiresAt = &oldExpiresAt

	prepared, err := preparer.PrepareSessionResources(ctx, setup, sandbox.ProviderHandle{SandboxID: "provider_sandbox"})
	if err != nil {
		t.Fatalf("PrepareSessionResources: %v", err)
	}
	if prepared.ResourceCredExpiresAt == nil || !prepared.ResourceCredExpiresAt.Equal(oldExpiresAt) {
		t.Fatalf("ResourceCredExpiresAt = %v; want carried expiring expiry %s", prepared.ResourceCredExpiresAt, oldExpiresAt)
	}
	if len(minter.requests) != 0 {
		t.Fatalf("mint requests = %d; want no mint while mount-alive guard succeeds", len(minter.requests))
	}
	if len(runner.calls) != 2 {
		t.Fatalf("runner calls = %d; want mount guard plus incremental command", len(runner.calls))
	}
	if !strings.Contains(runner.calls[0].command, "mountpoint -q \"$STAGING\"") {
		t.Fatalf("guard command =\n%s\nwant mount guard", runner.calls[0].command)
	}
	if strings.Contains(runner.calls[1].command, "setsid sudo rclone --config \"$RCLONE_CONFIG\" mount") {
		t.Fatalf("incremental command remounted despite live mount:\n%s", runner.calls[1].command)
	}
}

func TestResourceProjectionPreparerDoesNotMintForIncrementalAddWhenMountAlive(t *testing.T) {
	ctx := context.Background()
	blobStore := blob.NewFakeBlobStore()
	if err := blobStore.Put(ctx, "files/ws_test/obj_file", bytes.NewReader([]byte("canonical")), int64(len("canonical"))); err != nil {
		t.Fatalf("put canonical: %v", err)
	}
	oldExpiresAt := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	runner := &recordingPreparationCommandRunner{}
	minter := &recordingResourceCredentialMinter{}
	preparer := newTestResourceProjectionPreparer(t, blobStore, minter, runner)
	setup := testResourceProjectionSetup()
	setup.Resources.ResourceCredExpiresAt = &oldExpiresAt

	prepared, err := preparer.PrepareSessionResources(ctx, setup, sandbox.ProviderHandle{SandboxID: "provider_sandbox"})
	if err != nil {
		t.Fatalf("PrepareSessionResources: %v", err)
	}
	if prepared.ResourceCredExpiresAt == nil || !prepared.ResourceCredExpiresAt.Equal(oldExpiresAt) {
		t.Fatalf("ResourceCredExpiresAt = %v; want carried expiry %s", prepared.ResourceCredExpiresAt, oldExpiresAt)
	}
	if len(minter.requests) != 0 {
		t.Fatalf("credential mint requests = %+v; want no mint while live mount is fresh", minter.requests)
	}
	if len(runner.calls) != 2 {
		t.Fatalf("runner calls = %d; want guard plus incremental bind", len(runner.calls))
	}
	if !strings.Contains(runner.calls[0].command, "mountpoint -q \"$STAGING\"") || !strings.Contains(runner.calls[0].command, "timeout 5s ls \"$STAGING\"") {
		t.Fatalf("guard command =\n%s\nwant mount alive guard", runner.calls[0].command)
	}
	call := runner.calls[1]
	for _, fragment := range []string{
		"RCLONE_CONFIG=/tmp/tetral-runtime/rclone.conf",
		"mountpoint -q \"$STAGING\"",
		"if [ -L '/mnt/session/uploads/file_session' ]; then sudo -u '" + driver.RuntimeUser + "' rm -f -- '/mnt/session/uploads/file_session'; fi",
		"sudo -u '" + driver.RuntimeUser + "' touch -- '/mnt/session/uploads/file_session'",
		"sudo -u '" + driver.RuntimeUser + "' test ! -L '/mnt/session/uploads/file_session'",
		"sudo mount --bind '/mnt/tetral/r2/sesrsc_file/file' '/mnt/session/uploads/file_session'",
	} {
		if !strings.Contains(call.command, fragment) {
			t.Fatalf("incremental command missing fragment %q in:\n%s", fragment, call.command)
		}
	}
	for _, forbidden := range []string{"cat > \"$RCLONE_CONFIG\" <<EOF", "RCLONE_CONFIG_R2_ACCESS_KEY_ID", "setsid sudo rclone --config \"$RCLONE_CONFIG\" mount"} {
		if strings.Contains(call.command, forbidden) {
			t.Fatalf("incremental command contains forbidden mint/remount fragment %q in:\n%s", forbidden, call.command)
		}
	}
	if len(call.env) != 0 {
		t.Fatalf("incremental env = %+v; want no credential env", call.env)
	}
	assertResourceProjectionCommandRejectsSymlinkTargetBeforeTouch(t, call.command)
}

func TestResourceProjectionPreparerIncrementalAddCompensatesOnlyThisAttempt(t *testing.T) {
	ctx := context.Background()
	blobStore := blob.NewFakeBlobStore()
	if err := blobStore.Put(ctx, "files/ws_test/obj_new", bytes.NewReader([]byte("new")), int64(len("new"))); err != nil {
		t.Fatalf("put new canonical: %v", err)
	}
	if err := blobStore.Put(ctx, "workspaces/ws_test/sessions/sesn_test/resources/sesrsc_existing/file", bytes.NewReader([]byte("existing")), int64(len("existing"))); err != nil {
		t.Fatalf("put existing session copy: %v", err)
	}
	oldExpiresAt := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	runner := &recordingPreparationCommandRunner{errs: []error{nil, errors.New("bind failed")}}
	minter := &recordingResourceCredentialMinter{}
	preparer := newTestResourceProjectionPreparer(t, blobStore, minter, runner)
	setup := sandbox.SandboxSetup{
		WorkspaceID: workspace.ID("ws_test"),
		SessionID:   "sesn_test",
		SandboxID:   "sandbox_test",
		Resources: sandbox.ResourceSetup{
			Files: []sandbox.FileMount{
				{
					ResourceID:    "sesrsc_existing",
					SourceFileID:  "file_existing_source",
					SessionFileID: "file_existing",
					ObjectID:      "obj_existing",
					MountPath:     "/mnt/session/uploads/file_existing",
					ReadOnly:      true,
				},
				{
					ResourceID:    "sesrsc_new",
					SourceFileID:  "file_new_source",
					SessionFileID: "file_new",
					ObjectID:      "obj_new",
					MountPath:     "/mnt/session/uploads/file_new",
					ReadOnly:      true,
				},
			},
			ResourceCredExpiresAt: &oldExpiresAt,
			ResourceRootsJSON:     `[{"path":"/mnt/session/uploads/file_existing","mode":"read"}]`,
		},
	}

	err := func() error {
		_, err := preparer.PrepareSessionResources(ctx, setup, sandbox.ProviderHandle{SandboxID: "provider_sandbox"})
		return err
	}()
	if err == nil {
		t.Fatal("PrepareSessionResources succeeded; want bind failure")
	}
	if len(minter.requests) != 0 {
		t.Fatalf("credential mint requests = %+v; want no mint while live mount is reused", minter.requests)
	}
	if len(runner.calls) != 2 {
		t.Fatalf("runner calls = %d; want guard plus incremental command", len(runner.calls))
	}
	command := runner.calls[1].command
	for _, forbidden := range []string{
		"setsid sudo rclone --config \"$RCLONE_CONFIG\" mount",
		"cat > \"$RCLONE_CONFIG\" <<EOF",
	} {
		if strings.Contains(command, forbidden) {
			t.Fatalf("incremental command touched already-materialized fragment %q in:\n%s", forbidden, command)
		}
	}
	for _, fragment := range []string{
		`if [ "${TETRAL_BIND_CREATED_0:-}" = 1 ]; then`,
		`if [ "${TETRAL_BIND_CREATED_1:-}" = 1 ]; then`,
		"TETRAL_BIND_CREATED_0=1",
		"TETRAL_BIND_CREATED_1=1",
		"if findmnt -rn --mountpoint '/mnt/session/uploads/file_existing' >/dev/null 2>&1;",
		"if ! [ '/mnt/tetral/r2/sesrsc_existing/file' -ef '/mnt/session/uploads/file_existing' ]; then sudo umount -l -- '/mnt/session/uploads/file_existing'; fi",
		"sudo mount --bind '/mnt/tetral/r2/sesrsc_existing/file' '/mnt/session/uploads/file_existing'",
		"sudo mount --bind '/mnt/tetral/r2/sesrsc_new/file' '/mnt/session/uploads/file_new'",
		"sudo -u '" + driver.RuntimeUser + "' test -f '/mnt/session/uploads/file_existing'",
		"sudo -u '" + driver.RuntimeUser + "' head -c 1 -- '/mnt/session/uploads/file_existing' >/dev/null",
		"sudo umount -l -- '/mnt/session/uploads/file_existing'",
		"sudo umount -l -- '/mnt/session/uploads/file_new'",
	} {
		if !strings.Contains(command, fragment) {
			t.Fatalf("incremental command missing new-resource fragment %q in:\n%s", fragment, command)
		}
	}
	if !blobStore.Has("workspaces/ws_test/sessions/sesn_test/resources/sesrsc_existing/file") {
		t.Fatal("existing session copy was deleted by incremental compensation")
	}
	if blobStore.Has("workspaces/ws_test/sessions/sesn_test/resources/sesrsc_new/file") {
		t.Fatal("new session copy survived failed incremental attempt; want attempt-scoped deletion")
	}
}

func TestResourceProjectionPreparerDeleteThenAddAtSamePathCopiesAndRebindsReplacement(t *testing.T) {
	ctx := context.Background()
	blobStore := blob.NewFakeBlobStore()
	if err := blobStore.Put(ctx, "files/ws_test/obj_replacement", bytes.NewReader([]byte("replacement")), int64(len("replacement"))); err != nil {
		t.Fatalf("put replacement canonical: %v", err)
	}
	if err := blobStore.Put(ctx, "workspaces/ws_test/sessions/sesn_test/resources/sesrsc_deleted/file", bytes.NewReader([]byte("old")), int64(len("old"))); err != nil {
		t.Fatalf("put deleted session copy: %v", err)
	}
	oldExpiresAt := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	runner := &recordingPreparationCommandRunner{}
	preparer := newTestResourceProjectionPreparer(t, blobStore, &recordingResourceCredentialMinter{}, runner)
	const sharedPath = "/mnt/session/uploads/shared.txt"
	setup := sandbox.SandboxSetup{
		WorkspaceID: workspace.ID("ws_test"),
		SessionID:   "sesn_test",
		Resources: sandbox.ResourceSetup{
			Files: []sandbox.FileMount{{
				ResourceID: "sesrsc_replacement", SourceFileID: "file_replacement_source",
				SessionFileID: "file_replacement", ObjectID: "obj_replacement", MountPath: sharedPath, ReadOnly: true,
			}},
			DeletedFiles: []sandbox.FileMount{{
				ResourceID: "sesrsc_deleted", SessionFileID: "file_deleted", MountPath: sharedPath, ReadOnly: true,
			}},
			ResourceCredExpiresAt: &oldExpiresAt,
			ResourceRootsJSON:     `[{"path":"/mnt/session/uploads/shared.txt","mode":"read"}]`,
		},
	}

	if _, err := preparer.PrepareSessionResources(ctx, setup, sandbox.ProviderHandle{SandboxID: "provider_sandbox"}); err != nil {
		t.Fatalf("PrepareSessionResources replacement: %v", err)
	}
	assertBlobBytes(t, blobStore, "workspaces/ws_test/sessions/sesn_test/resources/sesrsc_replacement/file", "replacement")
	if blobStore.Has("workspaces/ws_test/sessions/sesn_test/resources/sesrsc_deleted/file") {
		t.Fatal("deleted same-path session copy survived replacement")
	}
	if len(runner.calls) != 3 {
		t.Fatalf("runner calls = %d; want delete cleanup, mount probe, and replacement bind", len(runner.calls))
	}
	command := runner.calls[2].command
	for _, required := range []string{
		"if ! [ '/mnt/tetral/r2/sesrsc_replacement/file' -ef '" + sharedPath + "' ]; then sudo umount -l -- '" + sharedPath + "'; fi",
		"sudo mount --bind '/mnt/tetral/r2/sesrsc_replacement/file' '" + sharedPath + "'",
	} {
		if !strings.Contains(command, required) {
			t.Fatalf("replacement command missing %q:\n%s", required, command)
		}
	}
}

func TestResourceProjectionPreparerIncrementalAddBindsExistingSessionCopy(t *testing.T) {
	ctx := context.Background()
	blobStore := blob.NewFakeBlobStore()
	if err := blobStore.Put(ctx, "files/ws_test/obj_new", bytes.NewReader([]byte("new")), int64(len("new"))); err != nil {
		t.Fatalf("put new canonical: %v", err)
	}
	if err := blobStore.Put(ctx, "workspaces/ws_test/sessions/sesn_test/resources/sesrsc_new/file", bytes.NewReader([]byte("new")), int64(len("new"))); err != nil {
		t.Fatalf("put preexisting new session copy: %v", err)
	}
	if err := blobStore.Put(ctx, "workspaces/ws_test/sessions/sesn_test/resources/sesrsc_existing/file", bytes.NewReader([]byte("existing")), int64(len("existing"))); err != nil {
		t.Fatalf("put existing session copy: %v", err)
	}
	oldExpiresAt := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	runner := &recordingPreparationCommandRunner{}
	minter := &recordingResourceCredentialMinter{}
	preparer := newTestResourceProjectionPreparer(t, blobStore, minter, runner)
	setup := sandbox.SandboxSetup{
		WorkspaceID: workspace.ID("ws_test"),
		SessionID:   "sesn_test",
		SandboxID:   "sandbox_test",
		Resources: sandbox.ResourceSetup{
			Files: []sandbox.FileMount{
				{
					ResourceID:    "sesrsc_existing",
					SourceFileID:  "file_existing_source",
					SessionFileID: "file_existing",
					ObjectID:      "obj_existing",
					MountPath:     "/mnt/session/uploads/file_existing",
					ReadOnly:      true,
				},
				{
					ResourceID:    "sesrsc_new",
					SourceFileID:  "file_new_source",
					SessionFileID: "file_new",
					ObjectID:      "obj_new",
					MountPath:     "/mnt/session/uploads/file_new",
					ReadOnly:      true,
				},
			},
			ResourceCredExpiresAt: &oldExpiresAt,
			ResourceRootsJSON:     `[{"path":"/mnt/session/uploads/file_existing","mode":"read"}]`,
		},
	}

	prepared, err := preparer.PrepareSessionResources(ctx, setup, sandbox.ProviderHandle{SandboxID: "provider_sandbox"})
	if err != nil {
		t.Fatalf("PrepareSessionResources: %v", err)
	}
	if prepared.ResourceCredExpiresAt == nil || !prepared.ResourceCredExpiresAt.Equal(oldExpiresAt) {
		t.Fatalf("ResourceCredExpiresAt = %v; want carried expiry %s", prepared.ResourceCredExpiresAt, oldExpiresAt)
	}
	if len(minter.requests) != 0 {
		t.Fatalf("credential mint requests = %+v; want no mint while live mount is reused", minter.requests)
	}
	if len(runner.calls) != 2 {
		t.Fatalf("runner calls = %d; want guard plus incremental command", len(runner.calls))
	}
	command := runner.calls[1].command
	for _, fragment := range []string{
		"if findmnt -rn --mountpoint '/mnt/session/uploads/file_existing' >/dev/null 2>&1;",
		"if ! [ '/mnt/tetral/r2/sesrsc_existing/file' -ef '/mnt/session/uploads/file_existing' ]; then sudo umount -l -- '/mnt/session/uploads/file_existing'; fi",
		"sudo mount --bind '/mnt/tetral/r2/sesrsc_existing/file' '/mnt/session/uploads/file_existing'",
		"sudo mount --bind '/mnt/tetral/r2/sesrsc_new/file' '/mnt/session/uploads/file_new'",
		"sudo -u '" + driver.RuntimeUser + "' touch -- '/mnt/session/uploads/file_new'",
		"sudo -u '" + driver.RuntimeUser + "' test -f '/mnt/session/uploads/file_existing'",
	} {
		if !strings.Contains(command, fragment) {
			t.Fatalf("incremental skipped-copy command missing fragment %q in:\n%s", fragment, command)
		}
	}
	for _, forbidden := range []string{
		"setsid sudo rclone --config \"$RCLONE_CONFIG\" mount",
		"cat > \"$RCLONE_CONFIG\" <<EOF",
	} {
		if strings.Contains(command, forbidden) {
			t.Fatalf("incremental skipped-copy command touched forbidden fragment %q in:\n%s", forbidden, command)
		}
	}
	assertBlobBytes(t, blobStore, "workspaces/ws_test/sessions/sesn_test/resources/sesrsc_new/file", "new")
}

func TestResourceProjectionPreparerForcesRemountWhenMountProbeFails(t *testing.T) {
	ctx := context.Background()
	blobStore := blob.NewFakeBlobStore()
	if err := blobStore.Put(ctx, "files/ws_test/obj_file", bytes.NewReader([]byte("canonical")), int64(len("canonical"))); err != nil {
		t.Fatalf("put canonical: %v", err)
	}
	oldExpiresAt := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	newExpiresAt := time.Date(2026, 7, 1, 13, 0, 0, 0, time.UTC)
	runner := &recordingPreparationCommandRunner{errs: []error{errors.New("stale mount probe"), nil}}
	minter := &recordingResourceCredentialMinter{result: resourceprojection.CredentialMintResult{
		Credential: resourceprojection.Credential{AccessKeyID: "access-key", SecretAccessKey: "secret-key", SessionToken: "session-token"},
		ExpiresAt:  newExpiresAt,
	}}
	preparer := newTestResourceProjectionPreparer(t, blobStore, minter, runner)
	setup := testResourceProjectionSetup()
	setup.Resources.ResourceCredExpiresAt = &oldExpiresAt

	prepared, err := preparer.PrepareSessionResources(ctx, setup, sandbox.ProviderHandle{SandboxID: "provider_sandbox"})
	if err != nil {
		t.Fatalf("PrepareSessionResources: %v", err)
	}
	if prepared.ResourceCredExpiresAt == nil || !prepared.ResourceCredExpiresAt.Equal(newExpiresAt) {
		t.Fatalf("ResourceCredExpiresAt = %v; want freshly minted expiry %s", prepared.ResourceCredExpiresAt, newExpiresAt)
	}
	if len(minter.requests) != 1 {
		t.Fatalf("credential mint requests = %+v; want one mint after failed mount probe", minter.requests)
	}
	if len(runner.calls) != 2 {
		t.Fatalf("runner calls = %d; want probe plus forced remount", len(runner.calls))
	}
	call := runner.calls[1]
	for _, fragment := range []string{
		"if mountpoint -q \"$STAGING\"; then sudo umount -l -- \"$STAGING\"; fi",
		"setsid sudo rclone --config \"$RCLONE_CONFIG\" mount",
	} {
		if !strings.Contains(call.command, fragment) {
			t.Fatalf("forced remount command missing fragment %q in:\n%s", fragment, call.command)
		}
	}
}

func TestResourceProjectionPreparerLocalCopyFallbackStagesBytesWithoutMintOrMount(t *testing.T) {
	ctx := context.Background()
	blobStore := blob.NewFakeBlobStore()
	if err := blobStore.Put(ctx, "files/ws_test/obj_file", bytes.NewReader([]byte("canonical")), int64(len("canonical"))); err != nil {
		t.Fatalf("put canonical: %v", err)
	}
	expiresAt := time.Date(2026, 7, 2, 11, 0, 0, 0, time.UTC)
	minter := &recordingResourceCredentialMinter{result: resourceprojection.CredentialMintResult{ExpiresAt: expiresAt}}
	runner := &recordingPreparationCommandRunner{}
	preparer := newTestResourceProjectionPreparerWithLevel(t, blobStore, minter, runner, ResourceProjectionLevelLocalCopy)

	prepared, err := preparer.PrepareSessionResources(ctx, testResourceProjectionSetup(), sandbox.ProviderHandle{SandboxID: "provider_sandbox"})
	if err != nil {
		t.Fatalf("PrepareSessionResources: %v", err)
	}
	if len(prepared.Files) != 0 || len(prepared.DeletedFiles) != 0 {
		t.Fatalf("prepared files=%+v deleted=%+v; want raw file mounts consumed", prepared.Files, prepared.DeletedFiles)
	}
	if prepared.ResourceCredExpiresAt == nil || !prepared.ResourceCredExpiresAt.Equal(expiresAt) {
		t.Fatalf("ResourceCredExpiresAt = %v; want bounded materialization receipt", prepared.ResourceCredExpiresAt)
	}
	if len(minter.requests) != 1 {
		t.Fatalf("credential mint requests = %d; want one receipt credential not injected into local_copy", len(minter.requests))
	}
	if got, want := prepared.ResourceRootsJSON, `[{"path":"/mnt/session/uploads/file_session","mode":"read"}]`; got != want {
		t.Fatalf("ResourceRootsJSON = %s; want %s", got, want)
	}
	if len(runner.uploads) != 1 {
		t.Fatalf("staged uploads = %+v; want one local copy upload", runner.uploads)
	}
	if runner.uploads[0].target.ProviderSandboxID != "provider_sandbox" ||
		!strings.HasPrefix(runner.uploads[0].remotePath, "/tmp/tetral-runtime/resource-projection/") ||
		runner.uploads[0].body != "canonical" {
		t.Fatalf("staged upload = %+v; want provider target, resource-projection path, canonical bytes", runner.uploads[0])
	}
	if len(runner.calls) != 1 {
		t.Fatalf("runner calls = %d; want one install/verify command", len(runner.calls))
	}
	call := runner.calls[0]
	if len(call.env) != 0 {
		t.Fatalf("local_copy env = %+v; want no secret-bearing env", call.env)
	}
	for _, forbidden := range []string{
		"RCLONE_CONFIG",
		"RCLONE_CONFIG_R2_ACCESS_KEY_ID",
		"setsid sudo rclone",
		"sudo mount --bind",
		"mount -o remount,bind,ro",
		"canonical",
	} {
		if strings.Contains(call.command, forbidden) {
			t.Fatalf("local_copy command contains forbidden fragment %q in:\n%s", forbidden, call.command)
		}
	}
	for _, fragment := range []string{
		"if [ -e '/mnt/session/uploads' ]; then",
		"sudo -u '" + driver.RuntimeUser + "' test -w '/mnt/session/uploads'",
		"sudo -u '" + driver.RuntimeUser + "' test -x '/mnt/session/uploads'",
		"sudo install -d -m 0755 -o '" + driver.RuntimeUser + "' -g '" + driver.RuntimeUser + "' -- '/mnt/session/uploads'",
		"if [ -L '/mnt/session/uploads/file_session' ]; then sudo -u '" + driver.RuntimeUser + "' rm -f -- '/mnt/session/uploads/file_session'; fi",
		"if [ -e '/mnt/session/uploads/file_session' ] && [ ! -f '/mnt/session/uploads/file_session' ]; then echo 'resource projection target is not a regular file' >&2; false; fi",
		"sudo install -m 0444 -o '" + driver.RuntimeUser + "' -g '" + driver.RuntimeUser + "' -- '" + runner.uploads[0].remotePath + "' '/mnt/session/uploads/file_session'",
		"sudo -u '" + driver.RuntimeUser + "' test ! -L '/mnt/session/uploads/file_session'",
		"sudo -u '" + driver.RuntimeUser + "' test -f '/mnt/session/uploads/file_session'",
		"sudo -u '" + driver.RuntimeUser + "' head -c 1 -- '/mnt/session/uploads/file_session'",
		"if sudo -u '" + driver.RuntimeUser + "' sh -c 'exec 9>\"$1\"' _ '/mnt/session/uploads/file_session' 2>/dev/null; then false; fi",
		"sudo -u '" + driver.RuntimeUser + "' sh -c 'tmp=$(mktemp \"$1/.tetral-resource-verify.XXXXXX\") && rm -f \"$tmp\"' _ '/mnt/session/uploads'",
		"sudo rm -rf -- '/tmp/tetral-runtime/resource-projection/",
	} {
		if !strings.Contains(call.command, fragment) {
			t.Fatalf("local_copy command missing fragment %q in:\n%s", fragment, call.command)
		}
	}
}

func TestResourceProjectionPreparerCleansDeletedFileBeforeActiveProjection(t *testing.T) {
	ctx := context.Background()
	blobStore := blob.NewFakeBlobStore()
	if err := blobStore.Put(ctx, "files/ws_test/obj_file", bytes.NewReader([]byte("canonical")), int64(len("canonical"))); err != nil {
		t.Fatalf("put canonical: %v", err)
	}
	if err := blobStore.Put(ctx, "workspaces/ws_test/sessions/sesn_test/resources/sesrsc_deleted/file", bytes.NewReader([]byte("old")), int64(len("old"))); err != nil {
		t.Fatalf("put deleted session copy: %v", err)
	}
	expiresAt := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	minter := &recordingResourceCredentialMinter{result: resourceprojection.CredentialMintResult{
		Credential: resourceprojection.Credential{
			AccessKeyID:     "access-key",
			SecretAccessKey: "secret-key",
			SessionToken:    "session-token",
		},
		ExpiresAt: expiresAt,
		Prefix:    "workspaces/ws_test/sessions/sesn_test/resources/",
	}}
	events := []string{}
	runner := &recordingPreparationCommandRunner{eventsRef: &events}
	preparer := newTestResourceProjectionPreparer(t, blobStore, minter, runner)
	setup := testResourceProjectionSetup()
	setup.Resources.DeletedFiles = []sandbox.FileMount{{
		ResourceID:    "sesrsc_deleted",
		SessionFileID: "file_deleted",
		MountPath:     "/mnt/session/uploads/file_session",
		ReadOnly:      true,
	}}
	setup.ResourceCleanup = &recordingResourceCleanupCoordinator{eventsRef: &events, pending: true}

	prepared, err := preparer.PrepareSessionResources(ctx, setup, sandbox.ProviderHandle{SandboxID: "provider_sandbox"})
	if err != nil {
		t.Fatalf("PrepareSessionResources: %v", err)
	}
	if len(prepared.DeletedFiles) != 0 || len(prepared.Files) != 0 {
		t.Fatalf("prepared files=%+v deleted=%+v; want cleanup and active raw inputs consumed", prepared.Files, prepared.DeletedFiles)
	}
	if blobStore.Has("workspaces/ws_test/sessions/sesn_test/resources/sesrsc_deleted/file") {
		t.Fatal("deleted resource session copy survived cleanup")
	}
	if got := blobStore.Deletes(); len(got) != 1 || got[0] != "workspaces/ws_test/sessions/sesn_test/resources/sesrsc_deleted/file" {
		t.Fatalf("deletes = %v; want deleted resource session copy", got)
	}
	if len(runner.calls) != 2 {
		t.Fatalf("runner calls = %d; want deleted cleanup then active mount", len(runner.calls))
	}
	cleanup := runner.calls[0]
	if cleanup.target.ProviderSandboxID != "provider_sandbox" || len(cleanup.env) != 0 {
		t.Fatalf("cleanup call target=%+v env=%+v; want provider sandbox without credentials", cleanup.target, cleanup.env)
	}
	for _, fragment := range []string{
		"findmnt -rn --mountpoint '/mnt/session/uploads/file_session'",
		"sudo umount -l -- '/mnt/session/uploads/file_session'",
		"sudo rm -rf -- '/mnt/session/uploads/file_session'",
	} {
		if !strings.Contains(cleanup.command, fragment) {
			t.Fatalf("cleanup command missing fragment %q in:\n%s", fragment, cleanup.command)
		}
	}
	if strings.Contains(cleanup.command, "umount -l -- '/mnt/tetral/r2'") {
		t.Fatalf("cleanup command unmounted staging despite active resources following:\n%s", cleanup.command)
	}
	activeMount := runner.calls[1]
	if !strings.Contains(activeMount.command, "setsid sudo rclone --config \"$RCLONE_CONFIG\" mount") {
		t.Fatalf("active mount command missing rclone mount:\n%s", activeMount.command)
	}
	if want := []string{"file-remove", "resource-detach", "file-materialize"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %v; want removal ACK, durable detach, then ordinary successor bind %v", events, want)
	}
}

func TestResourceProjectionPreparerCommandFailurePreservesDeletedFileAndSessionCopy(t *testing.T) {
	ctx := context.Background()
	blobStore := blob.NewFakeBlobStore()
	const sessionKey = "workspaces/ws_test/sessions/sesn_test/resources/sesrsc_deleted/file"
	if err := blobStore.Put(ctx, sessionKey, bytes.NewReader([]byte("old")), int64(len("old"))); err != nil {
		t.Fatalf("put deleted session copy: %v", err)
	}
	wantErr := errors.New("injected filesystem removal failure")
	runner := &recordingPreparationCommandRunner{err: wantErr}
	events := []string{}
	preparer := newTestResourceProjectionPreparer(t, blobStore, &recordingResourceCredentialMinter{}, runner)
	setup := deletedFileOnlySetup()
	setup.ResourceCleanup = &recordingResourceCleanupCoordinator{eventsRef: &events, pending: true}

	_, err := preparer.PrepareSessionResources(ctx, setup, sandbox.ProviderHandle{SandboxID: "provider_sandbox"})
	if !errors.Is(err, wantErr) {
		t.Fatalf("PrepareSessionResources error = %v; want command failure", err)
	}
	if !blobStore.Has(sessionKey) {
		t.Fatal("command failure deleted the session copy")
	}
	if got := blobStore.Deletes(); len(got) != 0 {
		t.Fatalf("Blob Delete calls = %v; want zero after command failure", got)
	}
	if len(events) != 0 {
		t.Fatalf("cleanup events = %v; want no durable detach", events)
	}
}

func TestResourceProjectionPreparerBlobFailureRetriesBetweenRemovalAndDetach(t *testing.T) {
	ctx := context.Background()
	blobStore := blob.NewFakeBlobStore()
	const sessionKey = "workspaces/ws_test/sessions/sesn_test/resources/sesrsc_deleted/file"
	if err := blobStore.Put(ctx, sessionKey, bytes.NewReader([]byte("old")), int64(len("old"))); err != nil {
		t.Fatalf("put deleted session copy: %v", err)
	}
	wantErr := errors.New("injected Blob delete failure")
	blobStore.SetDeleteHook(func(context.Context, string) error { return wantErr })
	runner := &recordingPreparationCommandRunner{}
	events := []string{}
	cleanup := &recordingResourceCleanupCoordinator{eventsRef: &events, pending: true}
	preparer := newTestResourceProjectionPreparer(t, blobStore, &recordingResourceCredentialMinter{}, runner)
	setup := deletedFileOnlySetup()
	setup.ResourceCleanup = cleanup

	if _, err := preparer.PrepareSessionResources(ctx, setup, sandbox.ProviderHandle{SandboxID: "provider_sandbox"}); !errors.Is(err, wantErr) {
		t.Fatalf("first PrepareSessionResources error = %v; want Blob failure", err)
	}
	if !blobStore.Has(sessionKey) || len(events) != 0 {
		t.Fatalf("Blob failure state: copy_present=%v events=%v; want pending without detach", blobStore.Has(sessionKey), events)
	}

	blobStore.SetDeleteHook(nil)
	if _, err := preparer.PrepareSessionResources(ctx, setup, sandbox.ProviderHandle{SandboxID: "provider_sandbox"}); err != nil {
		t.Fatalf("retry PrepareSessionResources: %v", err)
	}
	if blobStore.Has(sessionKey) {
		t.Fatal("successful retry preserved deleted session copy")
	}
	if got := blobStore.Deletes(); !reflect.DeepEqual(got, []string{sessionKey, sessionKey}) {
		t.Fatalf("Blob Delete calls = %v; want failed call then retry", got)
	}
	if want := []string{"resource-detach"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("cleanup events = %v; want %v", events, want)
	}
	if got := countPreparationCommandsContaining(runner.calls, "/workspace/deleted.txt"); got != 2 {
		t.Fatalf("filesystem removal calls = %d; want idempotent replay", got)
	}
	if len(runner.calls) != 3 || !strings.Contains(runner.calls[2].command, "mountpoint -q '"+resourceprojection.RcloneStagingRoot+"'") {
		t.Fatalf("preparation commands = %d; want two target removals then staging unmount", len(runner.calls))
	}
}

func TestResourceProjectionPreparerDetachFailureRetriesAfterBlobDeletion(t *testing.T) {
	ctx := context.Background()
	blobStore := blob.NewFakeBlobStore()
	const sessionKey = "workspaces/ws_test/sessions/sesn_test/resources/sesrsc_deleted/file"
	if err := blobStore.Put(ctx, sessionKey, bytes.NewReader([]byte("old")), int64(len("old"))); err != nil {
		t.Fatalf("put deleted session copy: %v", err)
	}
	wantErr := errors.New("injected detach commit failure")
	runner := &recordingPreparationCommandRunner{}
	cleanup := &detachAfterRemovalFailureCoordinator{err: wantErr}
	preparer := newTestResourceProjectionPreparer(t, blobStore, &recordingResourceCredentialMinter{}, runner)
	setup := deletedFileOnlySetup()
	setup.ResourceCleanup = cleanup

	if _, err := preparer.PrepareSessionResources(ctx, setup, sandbox.ProviderHandle{SandboxID: "provider_sandbox"}); !errors.Is(err, wantErr) {
		t.Fatalf("first PrepareSessionResources error = %v; want detach failure", err)
	}
	if blobStore.Has(sessionKey) || cleanup.detached {
		t.Fatalf("detach-boundary state: copy_present=%v detached=%v; want copy gone and detach pending", blobStore.Has(sessionKey), cleanup.detached)
	}

	if _, err := preparer.PrepareSessionResources(ctx, setup, sandbox.ProviderHandle{SandboxID: "provider_sandbox"}); err != nil {
		t.Fatalf("retry PrepareSessionResources: %v", err)
	}
	if !cleanup.detached {
		t.Fatal("retry did not commit durable detach")
	}
	if got := countPreparationCommandsContaining(runner.calls, "/workspace/deleted.txt"); got != 2 {
		t.Fatalf("filesystem removal calls = %d; want idempotent replay before detach", got)
	}
	if len(runner.calls) != 3 || !strings.Contains(runner.calls[2].command, "mountpoint -q '"+resourceprojection.RcloneStagingRoot+"'") {
		t.Fatalf("preparation commands = %d; want two target removals then staging unmount", len(runner.calls))
	}
	if got := blobStore.Deletes(); !reflect.DeepEqual(got, []string{sessionKey, sessionKey}) {
		t.Fatalf("Blob Delete calls = %v; want delete then not-found replay", got)
	}
}

func TestResourceProjectionPreparerDeletingNonLastFilePreservesStagingAndCredentialExpiry(t *testing.T) {
	ctx := context.Background()
	blobStore := blob.NewFakeBlobStore()
	if err := blobStore.Put(ctx, "files/ws_test/obj_active", bytes.NewReader([]byte("active")), int64(len("active"))); err != nil {
		t.Fatalf("put active canonical: %v", err)
	}
	if err := blobStore.Put(ctx, "workspaces/ws_test/sessions/sesn_test/resources/sesrsc_active/file", bytes.NewReader([]byte("active")), int64(len("active"))); err != nil {
		t.Fatalf("put active session copy: %v", err)
	}
	if err := blobStore.Put(ctx, "workspaces/ws_test/sessions/sesn_test/resources/sesrsc_deleted/file", bytes.NewReader([]byte("old")), int64(len("old"))); err != nil {
		t.Fatalf("put deleted session copy: %v", err)
	}
	expiresAt := time.Date(2026, 7, 1, 18, 0, 0, 0, time.UTC)
	runner := &recordingPreparationCommandRunner{}
	minter := &recordingResourceCredentialMinter{}
	preparer := newTestResourceProjectionPreparer(t, blobStore, minter, runner)
	setup := sandbox.SandboxSetup{
		WorkspaceID: workspace.ID("ws_test"),
		SessionID:   "sesn_test",
		Resources: sandbox.ResourceSetup{
			Files: []sandbox.FileMount{{
				ResourceID: "sesrsc_active", SourceFileID: "file_source", SessionFileID: "file_active", ObjectID: "obj_active", MountPath: "/workspace/active.txt", ReadOnly: true,
			}},
			DeletedFiles: []sandbox.FileMount{{
				ResourceID: "sesrsc_deleted", SessionFileID: "file_deleted", MountPath: "/workspace/deleted.txt", ReadOnly: true,
			}},
			ResourceCredExpiresAt: &expiresAt,
		},
	}

	prepared, err := preparer.PrepareSessionResources(ctx, setup, sandbox.ProviderHandle{SandboxID: "provider_sandbox"})
	if err != nil {
		t.Fatalf("PrepareSessionResources: %v", err)
	}
	if prepared.ResourceCredExpiresAt == nil || !prepared.ResourceCredExpiresAt.Equal(expiresAt) {
		t.Fatalf("resource credential expiry = %v; want preserved %s", prepared.ResourceCredExpiresAt, expiresAt)
	}
	if len(minter.requests) != 0 {
		t.Fatalf("credential mint calls = %d; want existing shared mount credential", len(minter.requests))
	}
	if len(runner.calls) != 3 {
		t.Fatalf("runner calls = %d; want removal, mount probe, and active verification", len(runner.calls))
	}
	if strings.Contains(runner.calls[0].command, "umount -l -- '/mnt/tetral/r2'") {
		t.Fatalf("non-last-file cleanup unmounted shared staging mount:\n%s", runner.calls[0].command)
	}
}

func deletedFileOnlySetup() sandbox.SandboxSetup {
	expiresAt := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	return sandbox.SandboxSetup{
		WorkspaceID: workspace.ID("ws_test"),
		SessionID:   "sesn_test",
		Resources: sandbox.ResourceSetup{
			DeletedFiles: []sandbox.FileMount{{
				ResourceID: "sesrsc_deleted", SessionFileID: "file_deleted", MountPath: "/workspace/deleted.txt", ReadOnly: true,
			}},
			ResourceCredExpiresAt: &expiresAt,
		},
	}
}

type detachAfterRemovalFailureCoordinator struct {
	err      error
	detached bool
}

func (c *detachAfterRemovalFailureCoordinator) CleanupSessionResource(ctx context.Context, _ string, remove func(context.Context) error) error {
	if c.detached {
		return nil
	}
	if err := remove(ctx); err != nil {
		return err
	}
	if c.err != nil {
		err := c.err
		c.err = nil
		return err
	}
	c.detached = true
	return nil
}

func TestResourceProjectionPreparerCleansDeletedLastFileAndUnmountsStaging(t *testing.T) {
	ctx := context.Background()
	blobStore := blob.NewFakeBlobStore()
	const sessionKey = "workspaces/ws_test/sessions/sesn_test/resources/sesrsc_deleted/file"
	if err := blobStore.Put(ctx, sessionKey, bytes.NewReader([]byte("old")), int64(len("old"))); err != nil {
		t.Fatalf("put deleted session copy: %v", err)
	}
	events := []string{}
	blobStore.SetDeleteHook(func(context.Context, string) error {
		events = append(events, "blob-delete")
		return nil
	})
	expiresAt := time.Date(2026, 7, 2, 11, 0, 0, 0, time.UTC)
	minter := &recordingResourceCredentialMinter{result: resourceprojection.CredentialMintResult{ExpiresAt: expiresAt}}
	runner := &recordingPreparationCommandRunner{eventsRef: &events}
	preparer := newTestResourceProjectionPreparer(t, blobStore, minter, runner)
	oldExpiresAt := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	setup := sandbox.SandboxSetup{
		WorkspaceID: workspace.ID("ws_test"),
		SessionID:   "sesn_test",
		Resources: sandbox.ResourceSetup{DeletedFiles: []sandbox.FileMount{{
			ResourceID:    "sesrsc_deleted",
			SessionFileID: "file_deleted",
			MountPath:     "/workspace/deleted.csv",
			ReadOnly:      true,
		}}, ResourceCredExpiresAt: &oldExpiresAt},
		ResourceCleanup: &recordingResourceCleanupCoordinator{eventsRef: &events, pending: true},
	}

	prepared, err := preparer.PrepareSessionResources(ctx, setup, sandbox.ProviderHandle{SandboxID: "provider_sandbox"})
	if err != nil {
		t.Fatalf("PrepareSessionResources: %v", err)
	}
	if prepared.ResourceRootsJSON != "[]" || prepared.ResourceCredExpiresAt == nil || !prepared.ResourceCredExpiresAt.Equal(expiresAt) {
		t.Fatalf("prepared metadata roots=%q expires=%v; want empty roots with bounded materialization receipt", prepared.ResourceRootsJSON, prepared.ResourceCredExpiresAt)
	}
	if len(minter.requests) != 1 {
		t.Fatalf("mint requests = %d; want one for cleanup-only materialization", len(minter.requests))
	}
	if blobStore.Has(sessionKey) {
		t.Fatal("deleted resource session copy survived cleanup")
	}
	if len(runner.calls) != 2 {
		t.Fatalf("runner calls = %d; want target cleanup followed by staging unmount", len(runner.calls))
	}
	targetCleanup := runner.calls[0]
	if strings.Contains(targetCleanup.command, "umount -l -- '/mnt/tetral/r2'") {
		t.Fatalf("target cleanup unmounted staging before Blob deletion and detach:\n%s", targetCleanup.command)
	}
	stagingCleanup := runner.calls[1]
	if !strings.Contains(stagingCleanup.command, "if mountpoint -q '/mnt/tetral/r2'; then sudo umount -l -- '/mnt/tetral/r2'; fi") {
		t.Fatalf("post-detach command missing staging unmount:\n%s", stagingCleanup.command)
	}
	if strings.Contains(stagingCleanup.command, "/workspace/deleted.csv") {
		t.Fatalf("post-detach staging command repeated deleted target removal:\n%s", stagingCleanup.command)
	}
	if want := []string{"file-remove", "blob-delete", "resource-detach", "staging-unmount"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("last-file cleanup events = %v; want %v", events, want)
	}
}

func TestResourceProjectionPreparerRetriesStagingUnmountAfterDetachWithoutDeletedRow(t *testing.T) {
	ctx := context.Background()
	blobStore := blob.NewFakeBlobStore()
	const sessionKey = "workspaces/ws_test/sessions/sesn_test/resources/sesrsc_deleted/file"
	if err := blobStore.Put(ctx, sessionKey, bytes.NewReader([]byte("old")), int64(len("old"))); err != nil {
		t.Fatalf("put deleted session copy: %v", err)
	}
	events := []string{}
	blobStore.SetDeleteHook(func(context.Context, string) error {
		events = append(events, "blob-delete")
		return nil
	})
	wantErr := errors.New("injected staging unmount failure after detach")
	runner := &recordingPreparationCommandRunner{eventsRef: &events, errs: []error{nil, wantErr}}
	preparer := newTestResourceProjectionPreparer(t, blobStore, &recordingResourceCredentialMinter{}, runner)
	setup := deletedFileOnlySetup()
	setup.ResourceCleanup = &recordingResourceCleanupCoordinator{eventsRef: &events, pending: true}

	if _, err := preparer.PrepareSessionResources(ctx, setup, sandbox.ProviderHandle{SandboxID: "provider_sandbox"}); !errors.Is(err, wantErr) {
		t.Fatalf("first PrepareSessionResources error = %v; want post-detach staging failure", err)
	}
	if blobStore.Has(sessionKey) {
		t.Fatal("post-detach staging failure preserved deleted session copy")
	}
	if want := []string{"file-remove", "blob-delete", "resource-detach", "staging-unmount"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("first-attempt events = %v; want %v", events, want)
	}

	retrySetup := setup
	retrySetup.Resources.DeletedFiles = nil
	retrySetup.ResourceCleanup = nil
	prepared, err := preparer.PrepareSessionResources(ctx, retrySetup, sandbox.ProviderHandle{SandboxID: "provider_sandbox"})
	if err != nil {
		t.Fatalf("retry without deleted row: %v", err)
	}
	if prepared.ResourceCredExpiresAt == nil {
		t.Fatal("retry materialization did not produce a bounded credential receipt")
	}
	if want := []string{"file-remove", "blob-delete", "resource-detach", "staging-unmount", "staging-unmount"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("retry events = %v; want %v", events, want)
	}
	if got := blobStore.Deletes(); !reflect.DeepEqual(got, []string{sessionKey}) {
		t.Fatalf("Blob Delete calls = %v; want no replay after durable detach", got)
	}
	if len(runner.calls) != 3 {
		t.Fatalf("runner calls = %d; want target cleanup, failed staging, successful staging retry", len(runner.calls))
	}
}

func TestResourceProjectionPreparerGCDeletesSessionPrefix(t *testing.T) {
	ctx := context.Background()
	blobStore := blob.NewFakeBlobStore()
	for key, value := range map[string]string{
		"files/ws_test/obj_file": "canonical",
		"workspaces/ws_test/sessions/sesn_test/resources/sesrsc_a/file":      "copy-a",
		"workspaces/ws_test/sessions/sesn_test/resources/sesrsc_b/file":      "copy-b",
		"workspaces/ws_test/sessions/sesn_test/other/session-artifact":       "sibling",
		"workspaces/ws_test/sessions/sesn_other/resources/sesrsc_other/file": "other-session",
	} {
		if err := blobStore.Put(ctx, key, bytes.NewReader([]byte(value)), int64(len(value))); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}
	preparer := newTestResourceProjectionPreparer(t, blobStore, &recordingResourceCredentialMinter{}, &recordingPreparationCommandRunner{})

	if err := preparer.DeleteSessionResourceCopiesForGC(ctx, sandbox.SandboxSetup{WorkspaceID: workspace.ID("ws_test"), SessionID: "sesn_test", SandboxID: "sandbox_test"}); err != nil {
		t.Fatalf("DeleteSessionResourceCopiesForGC: %v", err)
	}
	for _, key := range []string{
		"workspaces/ws_test/sessions/sesn_test/resources/sesrsc_a/file",
		"workspaces/ws_test/sessions/sesn_test/resources/sesrsc_b/file",
		"workspaces/ws_test/sessions/sesn_test/other/session-artifact",
	} {
		if _, ok := blobStore.Bytes(key); ok {
			t.Fatalf("%s still exists; want session resource copy deleted", key)
		}
	}
	for key, want := range map[string]string{
		"files/ws_test/obj_file": "canonical",
		"workspaces/ws_test/sessions/sesn_other/resources/sesrsc_other/file": "other-session",
	} {
		assertBlobBytes(t, blobStore, key, want)
	}
}

func TestResourceProjectionPreparerCompensatesFileProjectionMountsWithoutDeletingSessionCopy(t *testing.T) {
	ctx := context.Background()
	blobStore := blob.NewFakeBlobStore()
	for key, value := range map[string]string{
		"files/ws_test/obj_file": "canonical",
		"workspaces/ws_test/sessions/sesn_test/resources/sesrsc_file/file": "session-copy",
	} {
		if err := blobStore.Put(ctx, key, bytes.NewReader([]byte(value)), int64(len(value))); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}
	runner := &recordingPreparationCommandRunner{}
	preparer := newTestResourceProjectionPreparer(t, blobStore, &recordingResourceCredentialMinter{}, runner)

	if err := preparer.CompensateSessionResourcePreparation(ctx, testResourceProjectionSetup(), sandbox.ProviderHandle{SandboxID: "provider_sandbox"}); err != nil {
		t.Fatalf("CompensateSessionResourcePreparation: %v", err)
	}
	if len(runner.calls) != 1 {
		t.Fatalf("runner calls = %d; want one compensation command", len(runner.calls))
	}
	command := runner.calls[0].command
	for _, fragment := range []string{
		"if findmnt -rn --mountpoint '/mnt/session/uploads/file_session' >/dev/null 2>&1; then sudo umount -l -- '/mnt/session/uploads/file_session'; fi",
		"sudo -u '" + driver.RuntimeUser + "' rm -f -- '/mnt/session/uploads/file_session' >/dev/null 2>&1 || true",
		"if mountpoint -q \"$STAGING\"; then sudo umount -l -- \"$STAGING\"; fi",
		"sudo rm -rf -- '/tmp/tetral-runtime/resource-projection/",
	} {
		if !strings.Contains(command, fragment) {
			t.Fatalf("compensation command missing fragment %q in:\n%s", fragment, command)
		}
	}
	assertBlobBytes(t, blobStore, "workspaces/ws_test/sessions/sesn_test/resources/sesrsc_file/file", "session-copy")
	assertBlobBytes(t, blobStore, "files/ws_test/obj_file", "canonical")
}

func TestResourceProjectionPreparerCompensationPreservesSessionCopyWhenCanonicalMissing(t *testing.T) {
	ctx := context.Background()
	blobStore := blob.NewFakeBlobStore()
	if err := blobStore.Put(ctx, "workspaces/ws_test/sessions/sesn_test/resources/sesrsc_file/file", bytes.NewReader([]byte("session-copy")), int64(len("session-copy"))); err != nil {
		t.Fatalf("put session copy: %v", err)
	}
	runner := &recordingPreparationCommandRunner{}
	preparer := newTestResourceProjectionPreparer(t, blobStore, &recordingResourceCredentialMinter{}, runner)

	if err := preparer.CompensateSessionResourcePreparation(ctx, testResourceProjectionSetup(), sandbox.ProviderHandle{SandboxID: "provider_sandbox"}); err != nil {
		t.Fatalf("CompensateSessionResourcePreparation: %v", err)
	}

	assertBlobBytes(t, blobStore, "workspaces/ws_test/sessions/sesn_test/resources/sesrsc_file/file", "session-copy")
	if _, ok := blobStore.Bytes("files/ws_test/obj_file"); ok {
		t.Fatal("canonical object unexpectedly exists")
	}
}

func TestResourceProjectionPreparerRejectsGitHubOnlyPreflightBeforeDeletedCleanup(t *testing.T) {
	ctx := context.Background()
	blobStore := blob.NewFakeBlobStore()
	if err := blobStore.Put(ctx, "workspaces/ws_test/sessions/sesn_test/resources/sesrsc_deleted/file", bytes.NewReader([]byte("old")), int64(len("old"))); err != nil {
		t.Fatalf("put deleted session copy: %v", err)
	}
	runner := &recordingPreparationCommandRunner{}
	preparer := newTestResourceProjectionPreparer(t, blobStore, &recordingResourceCredentialMinter{}, runner)
	setup := sandbox.SandboxSetup{
		WorkspaceID: workspace.ID("ws_test"),
		SessionID:   "sesn_test",
		Resources: sandbox.ResourceSetup{
			DeletedFiles: []sandbox.FileMount{{
				ResourceID:    "sesrsc_deleted",
				SessionFileID: "file_deleted",
				MountPath:     "/workspace/deleted.csv",
				ReadOnly:      true,
			}},
			GitHubRepositories: []sandbox.GitHubRepositoryMount{
				{ResourceID: "sesrsc_repo_a", URL: "https://github.com/tetral-ai/tetral"},
				{ResourceID: "sesrsc_repo_b", URL: "https://github.com/other/tetral.git"},
			},
		},
	}

	_, err := preparer.PrepareSessionResources(ctx, setup, sandbox.ProviderHandle{SandboxID: "provider_sandbox"})
	if err == nil {
		t.Fatal("PrepareSessionResources succeeded; want GitHub preflight failure")
	}
	var kinded interface{ PreparationFailureKind() string }
	if !errors.As(err, &kinded) || kinded.PreparationFailureKind() != "duplicate_github_mount_path" {
		t.Fatalf("error = %T %v; want duplicate_github_mount_path", err, err)
	}
	if !blobStore.Has("workspaces/ws_test/sessions/sesn_test/resources/sesrsc_deleted/file") {
		t.Fatal("deleted resource copy was removed before GitHub preflight failed")
	}
	if len(runner.calls) != 0 {
		t.Fatalf("runner calls = %d; want no cleanup command before GitHub preflight passes", len(runner.calls))
	}
}

func TestResourceProjectionPreparerRejectsGitHubDefaultPathBeforeCopy(t *testing.T) {
	ctx := context.Background()
	blobStore := blob.NewFakeBlobStore()
	if err := blobStore.Put(ctx, "files/ws_test/obj_file", bytes.NewReader([]byte("canonical")), int64(len("canonical"))); err != nil {
		t.Fatalf("put canonical: %v", err)
	}
	minter := &recordingResourceCredentialMinter{}
	runner := &recordingPreparationCommandRunner{}
	preparer := newTestResourceProjectionPreparer(t, blobStore, minter, runner)
	setup := testResourceProjectionSetup()
	setup.Resources.Files[0].MountPath = "/workspace/tetral/data.txt"
	setup.Resources.GitHubRepositories = []sandbox.GitHubRepositoryMount{{
		ResourceID: "sesrsc_repo",
		URL:        "https://github.com/tetral-ai/tetral.git",
	}}

	_, err := preparer.PrepareSessionResources(ctx, setup, sandbox.ProviderHandle{SandboxID: "provider_sandbox"})
	if err == nil {
		t.Fatal("PrepareSessionResources succeeded; want github mount path conflict")
	}
	var kinded interface{ PreparationFailureKind() string }
	if !errors.As(err, &kinded) || kinded.PreparationFailureKind() != "github_mount_path_conflict" {
		t.Fatalf("error = %T %v; want github_mount_path_conflict", err, err)
	}
	if blobStore.Has("workspaces/ws_test/sessions/sesn_test/resources/sesrsc_file/file") {
		t.Fatal("session copy was written before GitHub default path validation failed")
	}
	if len(minter.requests) != 0 || len(runner.calls) != 0 {
		t.Fatalf("side effects minter=%d runner=%d; want fail before mint/command", len(minter.requests), len(runner.calls))
	}
}

func TestResourceProjectionPreparerCanonicalMissingReturnsFailureKind(t *testing.T) {
	blobStore := blob.NewFakeBlobStore()
	preparer := newTestResourceProjectionPreparer(t, blobStore, &recordingResourceCredentialMinter{}, &recordingPreparationCommandRunner{})

	_, err := preparer.PrepareSessionResources(context.Background(), testResourceProjectionSetup(), sandbox.ProviderHandle{SandboxID: "provider_sandbox"})
	if err == nil {
		t.Fatal("PrepareSessionResources succeeded; want canonical_missing")
	}
	var kinded interface{ PreparationFailureKind() string }
	if !errors.As(err, &kinded) {
		t.Fatalf("error = %T %v; want preparation failure kind", err, err)
	}
	if kinded.PreparationFailureKind() != "canonical_missing" {
		t.Fatalf("failure kind = %q; want canonical_missing", kinded.PreparationFailureKind())
	}
}

func TestResourceProjectionPreparerCommandFailureDeletesSessionCopies(t *testing.T) {
	ctx := context.Background()
	blobStore := blob.NewFakeBlobStore()
	if err := blobStore.Put(ctx, "files/ws_test/obj_file", bytes.NewReader([]byte("canonical")), int64(len("canonical"))); err != nil {
		t.Fatalf("put canonical: %v", err)
	}
	runner := &recordingPreparationCommandRunner{err: errors.New("mount failed")}
	preparer := newTestResourceProjectionPreparer(t, blobStore, &recordingResourceCredentialMinter{result: resourceprojection.CredentialMintResult{
		Credential: resourceprojection.Credential{AccessKeyID: "access-key", SecretAccessKey: "secret-key", SessionToken: "session-token"},
		ExpiresAt:  time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC),
	}}, runner)

	_, err := preparer.PrepareSessionResources(ctx, testResourceProjectionSetup(), sandbox.ProviderHandle{SandboxID: "provider_sandbox"})
	if err == nil {
		t.Fatal("PrepareSessionResources succeeded; want command failure")
	}
	if blobStore.Has("workspaces/ws_test/sessions/sesn_test/resources/sesrsc_file/file") {
		t.Fatal("session copy survived command failure; want cleanup")
	}
	deletes := blobStore.Deletes()
	if len(deletes) != 1 || deletes[0] != "workspaces/ws_test/sessions/sesn_test/resources/sesrsc_file/file" {
		t.Fatalf("deletes = %v; want session copy cleanup", deletes)
	}
}

func TestResourceProjectionPreparerCommandFailurePreservesExistingSessionCopies(t *testing.T) {
	ctx := context.Background()
	blobStore := blob.NewFakeBlobStore()
	if err := blobStore.Put(ctx, "files/ws_test/obj_file", bytes.NewReader([]byte("canonical")), int64(len("canonical"))); err != nil {
		t.Fatalf("put canonical: %v", err)
	}
	if err := blobStore.Put(ctx, "workspaces/ws_test/sessions/sesn_test/resources/sesrsc_file/file", bytes.NewReader([]byte("canonical")), int64(len("canonical"))); err != nil {
		t.Fatalf("put existing session copy: %v", err)
	}
	runner := &recordingPreparationCommandRunner{err: errors.New("mount failed")}
	preparer := newTestResourceProjectionPreparer(t, blobStore, &recordingResourceCredentialMinter{result: resourceprojection.CredentialMintResult{
		Credential: resourceprojection.Credential{AccessKeyID: "access-key", SecretAccessKey: "secret-key", SessionToken: "session-token"},
		ExpiresAt:  time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC),
	}}, runner)

	_, err := preparer.PrepareSessionResources(ctx, testResourceProjectionSetup(), sandbox.ProviderHandle{SandboxID: "provider_sandbox"})
	if err == nil {
		t.Fatal("PrepareSessionResources succeeded; want command failure")
	}
	if !blobStore.Has("workspaces/ws_test/sessions/sesn_test/resources/sesrsc_file/file") {
		t.Fatal("existing session copy was deleted by command failure; want session copy to survive")
	}
	deletes := blobStore.Deletes()
	if len(deletes) != 0 {
		t.Fatalf("deletes = %v; want no cleanup for pre-existing session copy", deletes)
	}
}

func TestResourceProjectionPreparerValidatesMemoryMountsWithoutR2Work(t *testing.T) {
	minter := &recordingResourceCredentialMinter{}
	runner := &recordingPreparationCommandRunner{}
	preparer := newTestResourceProjectionPreparer(t, blob.NewFakeBlobStore(), minter, runner)

	_, err := preparer.PrepareSessionResources(context.Background(), sandbox.SandboxSetup{
		WorkspaceID: workspace.ID("ws_test"),
		SessionID:   "sesn_test",
		Resources: sandbox.ResourceSetup{MemoryStores: []sandbox.MemoryStoreMount{{
			ResourceID:    "sesrsc_memory",
			MemoryStoreID: "memstore_test",
			MountPath:     "/mnt/memory",
		}}},
	}, sandbox.ProviderHandle{})
	if err == nil {
		t.Fatal("PrepareSessionResources accepted memory root mount path")
	}
	var kinded interface{ PreparationFailureKind() string }
	if !errors.As(err, &kinded) {
		t.Fatalf("error = %T %v; want preparation failure kind", err, err)
	}
	if kinded.PreparationFailureKind() != "invalid_memory_mount_path" {
		t.Fatalf("failure kind = %q; want invalid_memory_mount_path", kinded.PreparationFailureKind())
	}
	if len(minter.requests) != 0 || len(runner.calls) != 0 {
		t.Fatalf("side effects after validation failure: mint=%d runner=%d; want none", len(minter.requests), len(runner.calls))
	}
}

func buildSkillPackageZip(t *testing.T, directory string) []byte {
	t.Helper()
	var buf bytes.Buffer
	writer := zip.NewWriter(&buf)
	dirHeader := &zip.FileHeader{Name: directory + "/", Method: zip.Store}
	dirHeader.SetMode(0o755)
	if _, err := writer.CreateHeader(dirHeader); err != nil {
		t.Fatalf("create skill package directory: %v", err)
	}
	fileHeader := &zip.FileHeader{Name: directory + "/SKILL.md", Method: zip.Deflate}
	fileHeader.SetMode(0o644)
	file, err := writer.CreateHeader(fileHeader)
	if err != nil {
		t.Fatalf("create skill package file: %v", err)
	}
	if _, err := file.Write([]byte("# " + directory + "\n")); err != nil {
		t.Fatalf("write skill package file: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close skill package zip: %v", err)
	}
	return buf.Bytes()
}

func skillArchiveNames(t *testing.T, body []byte) []string {
	t.Helper()
	gzipReader, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("open skill tar.gz: %v", err)
	}
	defer func() { _ = gzipReader.Close() }()
	tarReader := tar.NewReader(gzipReader)
	var names []string
	for {
		header, err := tarReader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("read skill tar.gz: %v", err)
		}
		names = append(names, header.Name)
	}
	return names
}

func sha256Hex(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func newTestResourceProjectionPreparer(t *testing.T, store blob.BlobStore, minter resourceCredentialMinter, runner driver.PreparationCommandRunner) *ResourceProjectionPreparer {
	t.Helper()
	return newTestResourceProjectionPreparerWithLevel(t, store, minter, runner, "")
}

func newTestResourceProjectionPreparerWithLevel(t *testing.T, store blob.BlobStore, minter resourceCredentialMinter, runner driver.PreparationCommandRunner, level ResourceProjectionLevel) *ResourceProjectionPreparer {
	t.Helper()
	preparer, err := NewResourceProjectionPreparer(ResourceProjectionPreparerConfig{
		Blob:                  store,
		CredentialMinter:      minter,
		CommandRunner:         runner,
		Bucket:                "tetral-files",
		AccountID:             "acct_123",
		CredentialTTL:         24 * time.Hour,
		CommandTimeout:        45 * time.Second,
		RcloneVFSCacheMaxSize: "2G",
		RcloneVFSMinFree:      "1G",
		ProjectionLevel:       level,
		Clock:                 func() time.Time { return time.Date(2026, 7, 1, 11, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatalf("NewResourceProjectionPreparer: %v", err)
	}
	return preparer
}

func testResourceProjectionSetup() sandbox.SandboxSetup {
	return sandbox.SandboxSetup{
		WorkspaceID: workspace.ID("ws_test"),
		SessionID:   "sesn_test",
		SandboxID:   "sandbox_test",
		Resources: sandbox.ResourceSetup{Files: []sandbox.FileMount{{
			ResourceID:    "sesrsc_file",
			SourceFileID:  "file_source",
			SessionFileID: "file_session",
			ObjectID:      "obj_file",
			MountPath:     "/mnt/session/uploads/file_session",
			ReadOnly:      true,
		}}},
	}
}

func assertResourceProjectionCommandRemountsBeforeFreshMount(t *testing.T, command string) {
	t.Helper()
	targetUnmount := "if findmnt -rn --mountpoint '/mnt/session/uploads/file_session' >/dev/null 2>&1; then sudo umount -l -- '/mnt/session/uploads/file_session'; fi"
	stagingUnmount := "if mountpoint -q \"$STAGING\"; then sudo umount -l -- \"$STAGING\"; fi"
	mountCommand := "setsid sudo rclone --config \"$RCLONE_CONFIG\" mount"
	for _, fragment := range []string{targetUnmount, stagingUnmount, mountCommand} {
		if !strings.Contains(command, fragment) {
			t.Fatalf("mount command missing fragment %q in:\n%s", fragment, command)
		}
	}
	if strings.Index(command, targetUnmount) > strings.Index(command, mountCommand) ||
		strings.Index(command, stagingUnmount) > strings.Index(command, mountCommand) {
		t.Fatalf("teardown must precede fresh rclone mount:\n%s", command)
	}
}

func assertResourceProjectionCommandRejectsSymlinkTargetBeforeTouch(t *testing.T, command string) {
	t.Helper()
	symlinkGuard := "if [ -L '/mnt/session/uploads/file_session' ]; then sudo -u '" + driver.RuntimeUser + "' rm -f -- '/mnt/session/uploads/file_session'; fi"
	regularGuard := "if [ -e '/mnt/session/uploads/file_session' ] && [ ! -f '/mnt/session/uploads/file_session' ]; then echo 'resource projection target is not a regular file' >&2; false; fi"
	touchCommand := "sudo -u '" + driver.RuntimeUser + "' touch -- '/mnt/session/uploads/file_session'"
	verifyNotSymlink := "sudo -u '" + driver.RuntimeUser + "' test ! -L '/mnt/session/uploads/file_session'"
	for _, fragment := range []string{symlinkGuard, regularGuard, touchCommand, verifyNotSymlink} {
		if !strings.Contains(command, fragment) {
			t.Fatalf("resource projection command missing symlink-safety fragment %q in:\n%s", fragment, command)
		}
	}
	if strings.Index(command, symlinkGuard) > strings.Index(command, touchCommand) ||
		strings.Index(command, regularGuard) > strings.Index(command, touchCommand) {
		t.Fatalf("symlink and regular-file guards must run before placeholder touch:\n%s", command)
	}
	if strings.Index(command, verifyNotSymlink) < strings.Index(command, touchCommand) {
		t.Fatalf("non-symlink verification must run after placeholder/bind setup:\n%s", command)
	}
}

func assertBlobBytes(t *testing.T, store *blob.FakeBlobStore, key string, want string) {
	t.Helper()
	got, ok := store.Bytes(key)
	if !ok {
		t.Fatalf("missing blob key %q", key)
	}
	if string(got) != want {
		t.Fatalf("blob key %q = %q; want %q", key, got, want)
	}
}

func assertBlobAbsent(t *testing.T, store *blob.FakeBlobStore, key string) {
	t.Helper()
	if _, ok := store.Bytes(key); ok {
		t.Fatalf("blob key %q exists; want absent", key)
	}
}

type recordingResourceCredentialMinter struct {
	result   resourceprojection.CredentialMintResult
	err      error
	requests []resourceprojection.CredentialMintRequest
}

func (m *recordingResourceCredentialMinter) Mint(_ context.Context, request resourceprojection.CredentialMintRequest) (resourceprojection.CredentialMintResult, error) {
	m.requests = append(m.requests, request)
	if m.err == nil && m.result.ExpiresAt.IsZero() {
		m.result.ExpiresAt = request.Now.Add(request.TTL)
	}
	return m.result, m.err
}

type preparationCommandCall struct {
	target  driver.PreparationCommandTarget
	command string
	env     map[string]string
	timeout time.Duration
}

func countPreparationCommandsContaining(calls []preparationCommandCall, fragment string) int {
	count := 0
	for _, call := range calls {
		if strings.Contains(call.command, fragment) {
			count++
		}
	}
	return count
}

type preparationFileUpload struct {
	target     driver.PreparationCommandTarget
	remotePath string
	body       string
}

type recordingPreparationCommandRunner struct {
	err       error
	errs      []error
	stageErr  error
	calls     []preparationCommandCall
	uploads   []preparationFileUpload
	eventsRef *[]string
}

func (r *recordingPreparationCommandRunner) RunPreparationCommand(_ context.Context, target driver.PreparationCommandTarget, command string, env map[string]string, timeout time.Duration) error {
	if r.eventsRef != nil {
		event := "file-materialize"
		if strings.Contains(command, "mountpoint -q '"+resourceprojection.RcloneStagingRoot+"'") && !strings.Contains(command, "findmnt -rn --mountpoint") {
			event = "staging-unmount"
		} else if !strings.Contains(command, "setsid sudo rclone") {
			event = "file-remove"
		}
		*r.eventsRef = append(*r.eventsRef, event)
	}
	envCopy := map[string]string{}
	for key, value := range env {
		envCopy[key] = value
	}
	r.calls = append(r.calls, preparationCommandCall{target: target, command: command, env: envCopy, timeout: timeout})
	if len(r.errs) > 0 {
		err := r.errs[0]
		r.errs = r.errs[1:]
		return err
	}
	return r.err
}

type recordingResourceCleanupCoordinator struct {
	eventsRef *[]string
	pending   bool
	err       error
}

func (c *recordingResourceCleanupCoordinator) CleanupSessionResource(ctx context.Context, _ string, remove func(context.Context) error) error {
	if c.err != nil {
		return c.err
	}
	if !c.pending {
		return nil
	}
	if err := remove(ctx); err != nil {
		return err
	}
	if c.eventsRef != nil {
		*c.eventsRef = append(*c.eventsRef, "resource-detach")
	}
	return nil
}

func (r *recordingPreparationCommandRunner) StagePreparationFile(_ context.Context, target driver.PreparationCommandTarget, remotePath string, content io.Reader) error {
	body, err := io.ReadAll(content)
	if err != nil {
		return err
	}
	r.uploads = append(r.uploads, preparationFileUpload{target: target, remotePath: remotePath, body: string(body)})
	return r.stageErr
}

// Daytona executes preparation commands as the runtime user; every operation
// on the root-owned /skills tree must be sudo'd or the whole materialization
// dies at its first chown.
func TestSkillMaterializationCommandRunsPrivilegedStepsUnderSudo(t *testing.T) {
	command := skillMaterializationCommand([]sandbox.SkillMount{{
		SkillID:        "skill_test",
		SkillVersionID: "skill_version_test",
		Version:        "1",
		Directory:      "finance",
		BlobKey:        "skills/ws/skill_test/versions/1/package.zip",
	}})
	for _, required := range []string{
		"sudo mkdir -p /skills\n",
		"sudo chown root:root /skills\n",
		"sudo chmod 0755 /skills\n",
		"sudo rm -rf -- '/skills/finance'\n",
		"sudo mv -- ",
		"sudo chown -R root:root '/skills/finance'\n",
		"sudo find '/skills/finance' -type d -exec chmod 0555 {} +\n",
		"sudo find '/skills/finance' -type f -exec chmod 0444 {} +\n",
	} {
		if !strings.Contains(command, required) {
			t.Fatalf("skill materialization command missing %q in:\n%s", required, command)
		}
	}
	for _, line := range strings.Split(command, "\n") {
		if strings.Contains(line, "/skills") && !strings.HasPrefix(line, "sudo ") && !strings.HasPrefix(line, "test ") {
			t.Fatalf("skill materialization touches /skills unprivileged: %q", line)
		}
	}
	cleanup := skillProjectionCleanupCommand([]sandbox.SkillMount{{Directory: "finance"}})
	if !strings.Contains(cleanup, "sudo rm -rf -- '/skills/finance'\n") {
		t.Fatalf("skill cleanup removes /skills content unprivileged:\n%s", cleanup)
	}
}

// The preparation runner never surfaces command output, so the caller-side
// label is what names a failed command in the dead-letter row.
func TestLabelPreparationCommandErrorNamesCommandInSafeMessage(t *testing.T) {
	labeled := labelPreparationCommandError(&sandbox.ProviderError{
		Provider:    "daytona",
		Stage:       sandbox.StageMountResources,
		Kind:        sandbox.ProviderErrorUnknown,
		Retryable:   true,
		SafeMessage: "daytona preparation command failed",
	}, "skill_materialization")
	var providerErr *sandbox.ProviderError
	if !errors.As(labeled, &providerErr) {
		t.Fatalf("labeled error lost its provider classification: %v", labeled)
	}
	if providerErr.SafeMessage != "skill_materialization: daytona preparation command failed" {
		t.Fatalf("safe message = %q; want label prefix", providerErr.SafeMessage)
	}
	if !providerErr.Retryable || providerErr.Stage != sandbox.StageMountResources {
		t.Fatalf("labeling changed classification: %+v", providerErr)
	}
	plain := labelPreparationCommandError(errors.New("opaque"), "mount_bind_verify")
	if plain.Error() != "mount_bind_verify: opaque" {
		t.Fatalf("plain error label = %q", plain.Error())
	}
}
