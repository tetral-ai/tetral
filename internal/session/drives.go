package session

import (
	"context"

	"github.com/tetral-ai/tetral/internal/workspace"
)

// Drive accesses of a Session drive binding.
const (
	DriveAccessReadOnly  = "read_only"
	DriveAccessReadWrite = "read_write"
)

// validateDriveBindings checks the drives of a creation: each one valid
// (validateDriveBinding) and no drive_id listed twice.
func validateDriveBindings(bindings []DriveBinding) error {
	seen := make(map[string]bool, len(bindings))
	for _, binding := range bindings {
		if err := validateDriveBinding(binding); err != nil {
			return err
		}
		if seen[binding.DriveID] {
			return &ValidationError{Message: "drive " + binding.DriveID + " is listed twice"}
		}
		seen[binding.DriveID] = true
	}
	return nil
}

// validateDriveBinding checks one binding: a drive_id and an access of
// read_only or read_write.
func validateDriveBinding(binding DriveBinding) error {
	if binding.DriveID == "" {
		return &ValidationError{Message: "drive_id is required"}
	}
	return validateDriveAccess(binding.Access)
}

func validateDriveAccess(access string) error {
	if access != DriveAccessReadOnly && access != DriveAccessReadWrite {
		return &ValidationError{Message: "access must be read_only or read_write"}
	}
	return nil
}

// AddDrive runs POST /v1/sessions/{session_id}/drives: it binds a live drive
// that the idle Session does not bind yet and sends the new bindings to the
// Session's prepared machines (setDriveTx).
func (s *Service) AddDrive(ctx context.Context, ws workspace.ID, sessionID string, binding DriveBinding) (*DriveBinding, error) {
	if err := validateDriveBinding(binding); err != nil {
		return nil, err
	}
	err := s.store.WithRuntimeMutationTx(ctx, ws, sessionID, func(tx Transaction) error {
		current, err := s.lockDriveBindings(ctx, tx, sessionID)
		if err != nil {
			return err
		}
		if driveIndex(current, binding.DriveID) >= 0 {
			return &ConflictError{Message: "the drive is already bound to the session", InvalidRequest: true}
		}
		return setDriveTx(ctx, tx, sessionID, binding, withDrive(current, binding))
	})
	if err != nil {
		return nil, err
	}
	return &binding, nil
}

// UpdateDrive runs POST /v1/sessions/{session_id}/drives/{drive_id}: it
// changes the access of a binding of the idle Session. The same access
// changes and sends nothing.
func (s *Service) UpdateDrive(ctx context.Context, ws workspace.ID, sessionID, driveID, access string) (*DriveBinding, error) {
	if err := validateDriveAccess(access); err != nil {
		return nil, err
	}
	binding := DriveBinding{DriveID: driveID, Access: access}
	err := s.store.WithRuntimeMutationTx(ctx, ws, sessionID, func(tx Transaction) error {
		current, err := s.lockDriveBindings(ctx, tx, sessionID)
		if err != nil {
			return err
		}
		index := driveIndex(current, driveID)
		if index < 0 {
			return &NotFoundError{Message: "drive binding not found"}
		}
		if current[index].Access == access {
			return nil
		}
		return setDriveTx(ctx, tx, sessionID, binding, withDrive(current, binding))
	})
	if err != nil {
		return nil, err
	}
	return &binding, nil
}

// RemoveDrive runs DELETE /v1/sessions/{session_id}/drives/{drive_id}: it
// removes a binding of the idle Session, the last one included.
func (s *Service) RemoveDrive(ctx context.Context, ws workspace.ID, sessionID, driveID string) error {
	return s.store.WithRuntimeMutationTx(ctx, ws, sessionID, func(tx Transaction) error {
		current, err := s.lockDriveBindings(ctx, tx, sessionID)
		if err != nil {
			return err
		}
		index := driveIndex(current, driveID)
		if index < 0 {
			return &NotFoundError{Message: "drive binding not found"}
		}
		if err := tx.DeleteDriveBinding(ctx, sessionID, driveID); err != nil {
			return err
		}
		return tx.SendDriveBindings(ctx, sessionID, append(append([]DriveBinding{}, current[:index]...), current[index+1:]...))
	})
}

// lockDriveBindings reads the bindings of a Session that may change them:
// visible, usable for mutation and idle, under the runtime mutation lock its
// caller holds.
func (s *Service) lockDriveBindings(ctx context.Context, tx Transaction, sessionID string) ([]DriveBinding, error) {
	if _, err := tx.GetSession(ctx, sessionID); err != nil {
		return nil, err
	}
	if _, err := s.lockMutableSession(ctx, tx, sessionID); err != nil {
		return nil, err
	}
	return tx.DriveBindings(ctx, sessionID)
}

// setDriveTx stores binding, an addition or an access change after which the
// Session's bindings are next, refuses next when an attached computer's
// session.prepare would exceed its limit with it, and sends next to the
// Session's prepared machines, all in the caller's transaction.
func setDriveTx(ctx context.Context, tx Transaction, sessionID string, binding DriveBinding, next []DriveBinding) error {
	if err := tx.SetDriveBinding(ctx, sessionID, binding); err != nil {
		return err
	}
	if err := tx.CheckDriveBindingsFit(ctx, sessionID, next); err != nil {
		return err
	}
	return tx.SendDriveBindings(ctx, sessionID, next)
}

// driveIndex is the index of driveID in bindings, or -1.
func driveIndex(bindings []DriveBinding, driveID string) int {
	for i, binding := range bindings {
		if binding.DriveID == driveID {
			return i
		}
	}
	return -1
}

// withDrive is bindings with binding set, kept in UTF-8 byte order of
// drive_id.
func withDrive(bindings []DriveBinding, binding DriveBinding) []DriveBinding {
	next := make([]DriveBinding, 0, len(bindings)+1)
	added := false
	for _, current := range bindings {
		switch {
		case current.DriveID == binding.DriveID:
			continue
		case !added && binding.DriveID < current.DriveID:
			next, added = append(next, binding), true
		}
		next = append(next, current)
	}
	if !added {
		next = append(next, binding)
	}
	return next
}

// driveBindings is a Session's drives as the Session object lists them.
func driveBindings(bindings []DriveBinding) []DriveBinding {
	if bindings == nil {
		return []DriveBinding{}
	}
	return bindings
}
