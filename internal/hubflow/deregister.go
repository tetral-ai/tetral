package hubflow

import (
	"context"
	"sort"

	"github.com/tetral-ai/tetral/internal/computer"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// Deregistration is what a deregistration did, for its caller to record
// after commit: the credentials it revoked and the wake requests it failed.
type Deregistration struct {
	Revoked []string
	Failed  []computer.Ended
}

// Deregister runs the whole of DELETE /v1/instances/{instance_id} in its
// caller's transaction. It marks the instance deregistered and revokes its
// credentials (instance.Deregister), locks its connection that has not ended,
// takes the runtime mutation lock of every Session with an attached computer
// on the instance or an open wake request for it, in Session ID order, and
// then:
//   - ends that connection with cause deregistered (EndConnection);
//   - detaches every attached computer of a user instance with its removed
//     machine message (computer.Detach), whatever the Session's run state;
//     the connection has ended, so no close is sent and each computer that
//     had a binding stays unknown;
//   - retires a cloud instance as the current instance of every attached
//     computer it serves (computer.RetireCurrentInstance);
//   - fails every open wake request for the instance (FailRequest), which
//     ends the calls waiting for it not executed;
//   - refreshes the instance's unused.
//
// The computers and requests are read again under the Session locks, so a
// removal or call that committed meanwhile is seen. An instance already
// deregistered is left as it is.
func Deregister(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, instanceID string) (Deregistration, error) {
	before, revoked, err := instance.Deregister(ctx, tx, ws, instanceID)
	if err != nil || before.RegistrationState == instance.RegistrationDeregistered {
		return Deregistration{}, err
	}
	connection, connected, err := instance.LockUnendedConnection(ctx, tx, ws, instanceID)
	if err != nil {
		return Deregistration{}, err
	}
	computers, requests, err := usersOf(ctx, tx, ws, instanceID)
	if err != nil {
		return Deregistration{}, err
	}
	if err := lockSessions(ctx, tx, ws, computers, requests); err != nil {
		return Deregistration{}, err
	}
	if computers, requests, err = usersOf(ctx, tx, ws, instanceID); err != nil {
		return Deregistration{}, err
	}
	if connected {
		if err := EndConnection(ctx, tx, ws, connection, instance.EndDeregistered); err != nil {
			return Deregistration{}, err
		}
	}
	now, err := instance.Now(ctx, tx)
	if err != nil {
		return Deregistration{}, err
	}
	for _, c := range computers {
		if before.Kind == instance.KindUser {
			_, err = computer.Detach(ctx, tx, ws, c.SessionID, c.ComputerID, true, now)
		} else {
			err = computer.RetireCurrentInstance(ctx, tx, ws, c.SessionID, c.ComputerID, now)
		}
		if err != nil {
			return Deregistration{}, err
		}
	}
	result := Deregistration{Revoked: revoked}
	for _, request := range requests {
		ended, err := FailRequest(ctx, tx, ws, request.ID, true)
		if err != nil {
			return Deregistration{}, err
		}
		result.Failed = append(result.Failed, ended)
	}
	return result, computer.RefreshUnused(ctx, tx, ws, instanceID)
}

// usersOf reads the attached computers whose current instance is instanceID
// and the open wake requests for it.
func usersOf(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, instanceID string) ([]computer.Ref, []computer.Request, error) {
	computers, err := computer.AttachedOn(ctx, tx, ws, instanceID)
	if err != nil {
		return nil, nil, err
	}
	requests, err := computer.OpenWakeRequests(ctx, tx, ws, instanceID)
	return computers, requests, err
}

// lockSessions takes the runtime mutation lock of each Session the computers
// and requests belong to, in Session ID order.
func lockSessions(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, computers []computer.Ref, requests []computer.Request) error {
	seen := map[string]bool{}
	var sessions []string
	for _, c := range computers {
		if !seen[c.SessionID] {
			seen[c.SessionID] = true
			sessions = append(sessions, c.SessionID)
		}
	}
	for _, request := range requests {
		if !seen[request.SessionID] {
			seen[request.SessionID] = true
			sessions = append(sessions, request.SessionID)
		}
	}
	sort.Strings(sessions)
	for _, sessionID := range sessions {
		if err := storage.AcquireSessionRuntimeMutationLock(ctx, tx, string(ws), sessionID); err != nil {
			return err
		}
	}
	return nil
}
