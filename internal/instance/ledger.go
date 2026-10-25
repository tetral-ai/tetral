package instance

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// The upstream routes a replay record names.
const (
	RouteReady         = "ready"
	RouteHeartbeat     = "heartbeat"
	RouteProtocolError = "protocol-error"
	// RouteCredential's records keep no answer: the answer held a key.
	RouteCredential = "credential"
)

// LedgerEntry is the replay record of one message_id on a connection: the
// route and the canonical body fingerprint, and, for a ledger row, the exact
// answer. The ready and the latest heartbeat are recorded on the connection
// row, whose fields give their answers.
type LedgerEntry struct {
	Route       string
	Fingerprint [sha256.Size]byte
	Status      int
	Body        []byte
}

// FindUpstreamMessage returns the replay record of messageID on connection:
// its ready's, its latest heartbeat's or a ledger row.
func FindUpstreamMessage(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, connection Connection, messageID string) (LedgerEntry, bool, error) {
	if connection.ReadyMessageID != nil && *connection.ReadyMessageID == messageID {
		return LedgerEntry{Route: RouteReady, Fingerprint: *connection.ReadyFingerprint}, true, nil
	}
	if connection.Heartbeat != nil && connection.Heartbeat.MessageID == messageID {
		return LedgerEntry{Route: RouteHeartbeat, Fingerprint: connection.Heartbeat.Fingerprint}, true, nil
	}
	digest := sha256.Sum256([]byte(messageID))
	var entry LedgerEntry
	var fingerprint []byte
	err := tx.QueryRow(ctx, `SELECT route,fingerprint,status,body FROM connection_upstream_messages
		WHERE workspace_id=$1 AND connection_id=$2 AND message_digest=$3`, string(ws), connection.ID, digest[:]).Scan(&entry.Route, &fingerprint, &entry.Status, &entry.Body)
	if errors.Is(err, sql.ErrNoRows) {
		return LedgerEntry{}, false, nil
	}
	if err != nil {
		return LedgerEntry{}, false, err
	}
	copy(entry.Fingerprint[:], fingerprint)
	return entry, true, nil
}

// SaveUpstreamMessage records messageID on connectionID with its route,
// fingerprint and answer, in the transaction that accepts it.
func SaveUpstreamMessage(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, connectionID, messageID string, entry LedgerEntry) error {
	digest := sha256.Sum256([]byte(messageID))
	_, err := tx.Exec(ctx, `INSERT INTO connection_upstream_messages (workspace_id,connection_id,message_digest,message_id,route,fingerprint,status,body)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`, string(ws), connectionID, digest[:], messageID, entry.Route, entry.Fingerprint[:], entry.Status, entry.Body)
	return err
}
