package instance

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// IdempotencyRetention is how long after its instance's deregistration a
// record is honoured; while the instance is registered it always is.
const IdempotencyRetention = 30 * 24 * time.Hour

// IdempotencyConflictError answers a request whose Idempotency-Key names a
// record with a different fingerprint.
type IdempotencyConflictError struct{}

func (*IdempotencyConflictError) Error() string {
	return "this Idempotency-Key was used with a different request"
}

// IdempotencyScope identifies one record: the principal's credential, the
// route as method and concrete path, and the trimmed Idempotency-Key, all
// within the transaction's Workspace.
type IdempotencyScope struct {
	PrincipalCredentialID string
	Route                 string
	Key                   string
}

// IdempotencyRecord is the result identity of the request that wrote it.
type IdempotencyRecord struct {
	Fingerprint  [sha256.Size]byte
	InstanceID   string
	CredentialID string
	Status       int
}

// FindIdempotencyRecord returns the honoured record of scope. A record whose
// instance was deregistered IdempotencyRetention or longer ago is deleted, so
// a request with its key is a new operation.
func FindIdempotencyRecord(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, scope IdempotencyScope) (IdempotencyRecord, bool, error) {
	var record IdempotencyRecord
	var fingerprint []byte
	var stale bool
	err := tx.QueryRow(ctx, `SELECT r.fingerprint,r.instance_id,r.credential_id,r.status,
		COALESCE(i.deregistered_at<=clock_timestamp()-make_interval(secs=>$5),false)
		FROM instance_idempotency_records r JOIN instances i ON i.workspace_id=r.workspace_id AND i.id=r.instance_id
		WHERE r.workspace_id=$1 AND r.principal_credential_id=$2 AND r.route=$3 AND r.idempotency_key=$4`,
		string(ws), scope.PrincipalCredentialID, scope.Route, []byte(scope.Key), IdempotencyRetention.Seconds()).
		Scan(&fingerprint, &record.InstanceID, &record.CredentialID, &record.Status, &stale)
	if errors.Is(err, sql.ErrNoRows) {
		return IdempotencyRecord{}, false, nil
	}
	if err != nil {
		return IdempotencyRecord{}, false, err
	}
	if stale {
		_, err := tx.Exec(ctx, `DELETE FROM instance_idempotency_records
			WHERE workspace_id=$1 AND principal_credential_id=$2 AND route=$3 AND idempotency_key=$4`,
			string(ws), scope.PrincipalCredentialID, scope.Route, []byte(scope.Key))
		return IdempotencyRecord{}, false, err
	}
	copy(record.Fingerprint[:], fingerprint)
	return record, true, nil
}

// SaveIdempotencyRecord writes the record of scope in the transaction of its
// effect. When a concurrent request with the same scope committed first, the
// insert fails with an error IsIdempotencyRace reports; the caller retries
// its transaction once and then finds that record.
func SaveIdempotencyRecord(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, scope IdempotencyScope, record IdempotencyRecord) error {
	_, err := tx.Exec(ctx, `INSERT INTO instance_idempotency_records
		(workspace_id,principal_credential_id,route,idempotency_key,fingerprint,instance_id,credential_id,status)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		string(ws), scope.PrincipalCredentialID, scope.Route, []byte(scope.Key), record.Fingerprint[:], record.InstanceID, record.CredentialID, record.Status)
	return err
}

// IsIdempotencyRace reports the unique violation of a record that a
// concurrent identical request committed first.
func IsIdempotencyRace(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == "instance_idempotency_records_pkey"
}

// Fingerprint returns the SHA-256 of body re-encoded canonically: object
// keys sorted, no whitespace, strings and numbers as given. Whitespace and
// key order therefore change nothing, while an omitted field and an explicit
// value differ. ok is false when body is not one JSON value or an object
// repeats a key.
func Fingerprint(body []byte) (fingerprint [sha256.Size]byte, ok bool) {
	if !json.Valid(body) {
		return fingerprint, false
	}
	c := canonicalizer{src: body}
	var out bytes.Buffer
	if !c.value(&out) {
		return fingerprint, false
	}
	return sha256.Sum256(out.Bytes()), true
}

// canonicalizer re-encodes JSON that json.Valid accepted, so it only tracks
// structure and copies every string and scalar token unchanged.
type canonicalizer struct {
	src []byte
	pos int
}

func (c *canonicalizer) skipSpace() {
	for c.pos < len(c.src) && strings.IndexByte(" \t\r\n", c.src[c.pos]) >= 0 {
		c.pos++
	}
}

func (c *canonicalizer) value(out *bytes.Buffer) bool {
	c.skipSpace()
	switch c.src[c.pos] {
	case '{':
		return c.object(out)
	case '[':
		return c.array(out)
	case '"':
		out.Write(c.str())
	default:
		start := c.pos
		for c.pos < len(c.src) && strings.IndexByte(" \t\r\n,:]}", c.src[c.pos]) < 0 {
			c.pos++
		}
		out.Write(c.src[start:c.pos])
	}
	return true
}

func (c *canonicalizer) str() []byte {
	start := c.pos
	for c.pos++; c.src[c.pos] != '"'; c.pos++ {
		if c.src[c.pos] == '\\' {
			c.pos++
		}
	}
	c.pos++
	return c.src[start:c.pos]
}

func (c *canonicalizer) object(out *bytes.Buffer) bool {
	type member struct {
		key   string
		raw   []byte
		value []byte
	}
	var members []member
	c.pos++
	c.skipSpace()
	for c.src[c.pos] != '}' {
		c.skipSpace()
		raw := c.str()
		var key string
		if json.Unmarshal(raw, &key) != nil || slices.ContainsFunc(members, func(m member) bool { return m.key == key }) {
			return false
		}
		c.skipSpace()
		c.pos++ // ':'
		var value bytes.Buffer
		if !c.value(&value) {
			return false
		}
		members = append(members, member{key: key, raw: raw, value: value.Bytes()})
		c.skipSpace()
		if c.src[c.pos] == ',' {
			c.pos++
		}
	}
	c.pos++
	slices.SortFunc(members, func(a, b member) int { return strings.Compare(a.key, b.key) })
	out.WriteByte('{')
	for i, m := range members {
		if i > 0 {
			out.WriteByte(',')
		}
		out.Write(m.raw)
		out.WriteByte(':')
		out.Write(m.value)
	}
	out.WriteByte('}')
	return true
}

func (c *canonicalizer) array(out *bytes.Buffer) bool {
	c.pos++
	c.skipSpace()
	out.WriteByte('[')
	for first := true; c.src[c.pos] != ']'; first = false {
		if !first {
			out.WriteByte(',')
		}
		if !c.value(out) {
			return false
		}
		c.skipSpace()
		if c.src[c.pos] == ',' {
			c.pos++
		}
		c.skipSpace()
	}
	c.pos++
	out.WriteByte(']')
	return true
}
