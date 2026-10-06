package integration

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/integration/transporttest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/vault"
)

// The real CLI must finish its serial write and native close even when unused
// connection handshakes cannot finish. The relay forwards the first peer to real
// TLS PostgreSQL and holds any spare SSLRequest without interpreting TLS or SQL.
const platformKeyCloseChild = "TETRAL_PLATFORM_KEY_CLOSE_CHILD"

func TestPostgreSQLPlatformKeyCLINativeClose(t *testing.T) {
	if os.Getenv(platformKeyCloseChild) == t.Name() {
		dsn, err := url.Parse(os.Getenv("TETRAL_PLATFORM_KEY_CLOSE_POSTGRES_URL"))
		if err != nil || dsn.Host == "" {
			t.Fatal("platform key PostgreSQL URL invalid")
		}
		query := dsn.Query()
		query.Set("sslmode", "require")
		dsn.RawQuery = query.Encode()
		t.Setenv(storagetest.EnvTestDatabaseURL, dsn.String())
		t.Setenv(storagetest.EnvTestRunID, "")
		runPlatformKeyNativeClose(t)
		return
	}
	// The storage helper owns one registry/template per Go process. Select the
	// isolated TLS database before any helper runs in a fresh immutable binary.
	postgres := transporttest.NewPostgreSQL(t)
	executable, err := os.Executable()
	if err != nil {
		t.Fatal("platform key current test executable unavailable")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 95*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, executable, "-test.run=^TestPostgreSQLPlatformKeyCLINativeClose$", "-test.v", "-test.timeout=90s")
	command.Env = append(os.Environ(), platformKeyCloseChild+"="+t.Name(), "TETRAL_PLATFORM_KEY_CLOSE_POSTGRES_URL="+postgres.URL)
	output, err := command.CombinedOutput()
	t.Logf("isolated native platform key output:\n%s", output)
	if err != nil || !strings.Contains(string(output), "platform_key_native_close_assertion=serial-native-write-close-and-peer-cleanup") {
		t.Fatal("actual isolated platform key CLI proof failed")
	}
}

func runPlatformKeyNativeClose(t *testing.T) {
	_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	observer, err := admin.Conn(ctx)
	if err != nil {
		t.Fatal("platform key database observer unavailable")
	}
	defer observer.Close()
	baselinePeers := platformKeyDatabasePeers(t, ctx, observer)
	childURL, err := url.Parse(storagetest.AdminDatabaseURL(t, admin))
	if err != nil {
		t.Fatal("platform key child database URL invalid")
	}
	relay, err := newPlatformKeyHandshakeRelay(childURL.Host)
	if err != nil {
		t.Fatal("platform key handshake relay unavailable")
	}
	defer relay.close()
	childURL.Host = relay.listener.Addr().String()
	keyContext, stopKey := context.WithTimeout(ctx, 35*time.Second)
	defer stopKey()
	command := exec.CommandContext(keyContext, "bun", "scripts/platform-key.ts", "insert", "--provider", "anthropic", "--key-id", "pfk_content_e2e", "--cache-scope", "content-e2e")
	command.Dir = "../services/gateway"
	command.Env = append(os.Environ(), "TETRAL_DATABASE_URL="+childURL.String(), "ENGINE_VAULT_KEY="+sdkIntegrationVaultKey)
	command.Stdin = strings.NewReader("content-fixture-provider-key")
	// Native error output can contain credentials. Stdio stays discarded; only
	// the existing closed phase/time rail is inherited where ExtraFiles works.
	var phaseFile *os.File
	if runtime.GOOS != "windows" {
		phaseFile, err = os.CreateTemp(t.TempDir(), "platform-key-close-")
		if err != nil {
			t.Fatal("platform key phase file unavailable")
		}
		defer phaseFile.Close()
		command.ExtraFiles = []*os.File{phaseFile}
		command.Env = append(command.Env, "TETRAL_PLATFORM_KEY_DIAGNOSTIC_FD=3")
	}
	var mutationStart time.Time
	if err := observer.QueryRowContext(ctx, "SELECT clock_timestamp()").Scan(&mutationStart); err != nil {
		t.Fatal("platform key mutation clock unavailable")
	}
	started := time.Now()
	runErr := command.Run() // Run joins the real child, including deadline cancellation.
	var phases []platformKeyPhase
	phaseValid := false
	if phaseFile != nil {
		if _, err := phaseFile.Seek(0, io.SeekStart); err != nil {
			t.Fatal("platform key phase seek failed")
		}
		data, err := io.ReadAll(io.LimitReader(phaseFile, 4097))
		phases, phaseValid = decodePlatformKeyPhases(data)
		phaseValid = phaseValid && err == nil
		if phaseValid {
			// Re-encode closed records; preserve the close prefix on failures too.
			encoded, _ := json.Marshal(phases)
			t.Logf("platform_key_native_close phases=%s", encoded)
		}
	}
	if runErr != nil {
		exitCode := -1
		if command.ProcessState != nil {
			exitCode = command.ProcessState.ExitCode()
		}
		t.Logf("platform_key_native_close elapsed_ms=%d deadline=%t exit_code=%d", time.Since(started).Milliseconds(), errors.Is(keyContext.Err(), context.DeadlineExceeded), exitCode)
		t.Fatal("actual platform key CLI did not finish native close")
	}
	if phaseFile != nil && (!phaseValid || len(phases) != 8 || phases[5].Phase != "close_begin" || phases[6].Phase != "close_complete" || phases[7].Phase != "exit_begin") {
		t.Fatal("actual platform key native close phases incomplete")
	}
	var encrypted []byte
	if err := observer.QueryRowContext(ctx, `SELECT encrypted_key FROM platform_provider_keys WHERE key_id='pfk_content_e2e' AND provider_id='anthropic' AND status='active' AND cache_scope='content-e2e' AND updated_at >= $1`, mutationStart).Scan(&encrypted); err != nil {
		t.Fatal("actual platform key durable row absent")
	}
	cipher, err := vault.NewEncryptor(sdkIntegrationVaultKey)
	if err != nil {
		t.Fatal("platform key credential oracle unavailable")
	}
	plaintext, err := cipher.Decrypt(encrypted)
	if err != nil || !bytes.Equal(plaintext, []byte("content-fixture-provider-key")) {
		t.Fatal("actual platform key committed credential differs")
	}
	// Check real PostgreSQL connections before relay cleanup can hide a leak.
	probe, stopProbe := context.WithTimeout(ctx, 3*time.Second)
	defer stopProbe()
	for {
		peers := platformKeyDatabasePeers(t, probe, observer)
		extra := 0
		for pid := range peers {
			if !baselinePeers[pid] {
				extra++
			}
		}
		if extra == 0 {
			break
		}
		select {
		case <-probe.Done():
			t.Fatal("actual platform key connection survived process exit")
		case <-time.After(10 * time.Millisecond):
		}
	}
	relay.close()
	relay.mu.Lock()
	defer relay.mu.Unlock()
	if relay.accepted != 1 || relay.active != 0 || relay.failed {
		t.Fatalf("platform key peer ownership differs: accepted=%d active=%d failed=%t", relay.accepted, relay.active, relay.failed)
	}
	t.Log("platform_key_native_close_assertion=serial-native-write-close-and-peer-cleanup")
}

func platformKeyDatabasePeers(t *testing.T, ctx context.Context, observer *sql.Conn) map[int]bool {
	t.Helper()
	rows, err := observer.QueryContext(ctx, "SELECT pid FROM pg_stat_activity WHERE datname=current_database()")
	if err != nil {
		t.Fatal("platform key peer oracle unavailable")
	}
	defer rows.Close()
	peers := map[int]bool{}
	for rows.Next() {
		var pid int
		if rows.Scan(&pid) != nil {
			t.Fatal("platform key peer scan failed")
		}
		peers[pid] = true
	}
	if rows.Err() != nil {
		t.Fatal("platform key peer iteration failed")
	}
	return peers
}

type platformKeyHandshakeRelay struct {
	listener         net.Listener
	backend          string
	mu               sync.Mutex
	accepted, active int
	closed, failed   bool
	sockets          map[net.Conn]bool
	stopped          chan struct{}
	closeOnce        sync.Once
	workers          sync.WaitGroup
}

func newPlatformKeyHandshakeRelay(backend string) (*platformKeyHandshakeRelay, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	relay := &platformKeyHandshakeRelay{listener: listener, backend: backend, sockets: map[net.Conn]bool{}, stopped: make(chan struct{})}
	relay.workers.Add(1)
	go func() {
		defer relay.workers.Done()
		for {
			peer, err := listener.Accept()
			if err != nil {
				return
			}
			relay.mu.Lock()
			if relay.closed {
				relay.mu.Unlock()
				peer.Close()
				return
			}
			relay.accepted++
			ordinal := relay.accepted
			relay.active++
			relay.sockets[peer] = true
			relay.workers.Add(1)
			relay.mu.Unlock()
			go relay.forward(peer, ordinal)
		}
	}()
	return relay, nil
}
func (r *platformKeyHandshakeRelay) forward(peer net.Conn, ordinal int) {
	defer r.workers.Done()
	defer func() { peer.Close(); r.mu.Lock(); delete(r.sockets, peer); r.active--; r.mu.Unlock() }()
	if ordinal > 1 {
		// The fixed PostgreSQL SSLRequest precedes the encrypted handshake.
		// Holding it keeps spare connections pending through the real CLI close.
		var request [8]byte
		peer.SetReadDeadline(time.Now().Add(3 * time.Second))
		_, err := io.ReadFull(peer, request[:])
		peer.SetReadDeadline(time.Time{})
		if err != nil || !bytes.Equal(request[:], []byte{0, 0, 0, 8, 4, 210, 22, 47}) {
			r.mu.Lock()
			r.failed = true
			r.mu.Unlock()
			return
		}
		<-r.stopped
		return
	}
	server, err := net.DialTimeout("tcp", r.backend, 3*time.Second)
	if err != nil {
		r.mu.Lock()
		r.failed = true
		r.mu.Unlock()
		return
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		server.Close()
		return
	}
	r.sockets[server] = true
	r.mu.Unlock()
	defer func() { server.Close(); r.mu.Lock(); delete(r.sockets, server); r.mu.Unlock() }()
	joined := make(chan struct{})
	go func() { defer close(joined); _, _ = io.Copy(server, peer); server.Close(); peer.Close() }()
	_, _ = io.Copy(peer, server)
	server.Close()
	peer.Close()
	<-joined
}
func (r *platformKeyHandshakeRelay) close() {
	r.closeOnce.Do(func() {
		r.mu.Lock()
		r.closed = true
		close(r.stopped)
		for peer := range r.sockets {
			peer.Close()
		}
		r.mu.Unlock()
		r.listener.Close()
	})
	r.workers.Wait()
}
