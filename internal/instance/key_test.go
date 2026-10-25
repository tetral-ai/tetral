package instance

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestGeneratedKeysAreDistinctAndOnlyTheirDigestsAreStored(t *testing.T) {
	ctx := context.Background()
	runtimeDB, adminDB := storagetest.NewPostgreSQLDBWithAdmin(t)
	const ws = workspace.ID("ws_instance_keys")
	if _, err := adminDB.Exec(`INSERT INTO workspaces (id,name,created_at) VALUES ($1,$1,clock_timestamp())`, string(ws)); err != nil {
		t.Fatal(err)
	}
	client := dbconnect.NewClientForTesting(runtimeDB)
	keys := make([]ConnectionKey, 1000)
	seen := make(map[string]bool, len(keys))
	for index := range keys {
		key := GenerateKey()
		plaintext := key.Reveal()
		secret, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(plaintext, KeyPrefix))
		if seen[plaintext] || len(plaintext) != 53 || KeyLength != 53 || !strings.HasPrefix(plaintext, KeyPrefix) || err != nil || len(secret) != 32 {
			t.Fatalf("key %d: duplicate=%v length=%d decoded=%d err=%v", index, seen[plaintext], len(plaintext), len(secret), err)
		}
		if digest := sha256.Sum256([]byte(plaintext)); !bytes.Equal(key.Digest(), digest[:]) {
			t.Fatalf("key %d: digest is not the SHA-256 of the plaintext", index)
		}
		seen[plaintext] = true
		keys[index] = key
	}
	var (
		credential Credential
		issued     ConnectionKey
	)
	if err := client.WithWorkspaceTx(ctx, string(ws), "instance.test_keys", func(tx *dbconnect.Tx) error {
		registered, err := CreateUserInstance(ctx, tx, ws, "keys")
		if err != nil {
			return err
		}
		now, err := Now(ctx, tx)
		if err != nil {
			return err
		}
		if credential, issued, err = IssueCredential(ctx, tx, ws, registered.ID, CredentialActive, now, now.Add(time.Hour)); err != nil {
			return err
		}
		read, err := ReadCredential(ctx, tx, ws, credential.ID)
		if err != nil || read != credential {
			return fmt.Errorf("read credential = %+v, %v; want %+v", read, err, credential)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var (
		digest []byte
		stored string
	)
	if err := adminDB.QueryRow(`SELECT key_digest,row_to_json(c)::text FROM instance_credentials c WHERE id=$1`, credential.ID).Scan(&digest, &stored); err != nil {
		t.Fatal(err)
	}
	plaintext := issued.Reveal()
	if !bytes.Equal(digest, issued.Digest()) || strings.Contains(stored, plaintext) || strings.Contains(stored, strings.TrimPrefix(plaintext, KeyPrefix)) {
		t.Fatal("the issued credential's row holds more than its key's digest")
	}
	var logged bytes.Buffer
	slog.New(slog.NewJSONHandler(&logged, nil)).Info("issued", "key", keys[0])
	formatted := fmt.Sprintf("%v %+v %#v %s", keys[0], keys[0], keys[0], keys[0])
	if strings.Contains(logged.String(), keys[0].Reveal()) || strings.Contains(formatted, keys[0].Reveal()) {
		t.Fatal("a formatted or logged key holds its plaintext")
	}
}
