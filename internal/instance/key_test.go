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
	name := "keys"
	var credentials []Credential
	if err := client.WithWorkspaceTx(ctx, string(ws), "instance.test_keys", func(tx *dbconnect.Tx) error {
		registered, err := InsertInstance(ctx, tx, ws, NewInstance{Kind: KindUser, Name: &name})
		if err != nil {
			return err
		}
		for _, key := range keys {
			credential, err := InsertCredential(ctx, tx, ws, NewCredential{InstanceID: registered.ID, State: CredentialActive, Key: key, ExpiresAt: time.Now().Add(time.Hour)})
			if err != nil {
				return err
			}
			credentials = append(credentials, credential)
		}
		read, err := ReadCredential(ctx, tx, ws, credentials[0].ID)
		if err != nil || read != credentials[0] {
			return fmt.Errorf("read credential = %+v, %v; want %+v", read, err, credentials[0])
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	rows, err := adminDB.Query(`SELECT id,key_digest,row_to_json(c)::text FROM instance_credentials c ORDER BY storage_sequence`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	index := 0
	for rows.Next() {
		var credentialID, stored string
		var digest []byte
		if err := rows.Scan(&credentialID, &digest, &stored); err != nil {
			t.Fatal(err)
		}
		plaintext := keys[index].Reveal()
		secret := strings.TrimPrefix(plaintext, KeyPrefix)
		if credentialID != credentials[index].ID || !bytes.Equal(digest, keys[index].Digest()) || strings.Contains(stored, plaintext) || strings.Contains(stored, secret) {
			t.Fatalf("row %d holds more than its key's digest", index)
		}
		index++
	}
	if err := rows.Err(); err != nil || index != len(keys) {
		t.Fatalf("stored rows = %d, %v; want %d", index, err, len(keys))
	}
	var logged bytes.Buffer
	slog.New(slog.NewJSONHandler(&logged, nil)).Info("issued", "key", keys[0])
	formatted := fmt.Sprintf("%v %+v %#v %s", keys[0], keys[0], keys[0], keys[0])
	if strings.Contains(logged.String(), keys[0].Reveal()) || strings.Contains(formatted, keys[0].Reveal()) {
		t.Fatal("a formatted or logged key holds its plaintext")
	}
}
