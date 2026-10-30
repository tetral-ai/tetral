package computer

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// Kinds of computer.
const (
	KindCloud = "cloud"
	KindUser  = "user"
)

// Association states.
const (
	Attached = "attached"
	Detached = "detached"
)

// Execution states.
const (
	Waiting   = "waiting"
	Preparing = "preparing"
	Ready     = "ready"
	Failed    = "failed"
	Closing   = "closing"
	Closed    = "closed"
	Unknown   = "unknown"
)

// ErrNotFound answers a read of a computer the Session does not have, or one
// the read route no longer serves.
var ErrNotFound = errors.New("computer: not found")

// InvalidError answers an entry that breaks a rule of the field table.
type InvalidError struct{ Message string }

func (e *InvalidError) Error() string { return e.Message }

// NotFoundError answers an entry naming an Environment or instance the
// Workspace does not have.
type NotFoundError struct{ Message string }

func (e *NotFoundError) Error() string { return e.Message }

// NotAttachableError answers an entry naming a cloud or deregistered instance.
type NotAttachableError struct{}

func (*NotAttachableError) Error() string {
	return "a cloud or deregistered instance cannot be attached to a Session"
}

// AlreadyAttachedError answers an entry naming an instance the Session
// already has a computer on.
type AlreadyAttachedError struct{}

func (*AlreadyAttachedError) Error() string {
	return "the Session already has a computer on this instance"
}

// DriveExclude leaves paths of one drive out of a computer's machine.
type DriveExclude struct {
	DriveID string   `json:"drive_id"`
	Paths   []string `json:"paths"`
}

// Object is the public computer object.
type Object struct {
	ID                string          `json:"id"`
	Kind              string          `json:"kind"`
	EnvironmentID     *string         `json:"environment_id"`
	InstanceID        *string         `json:"instance_id"`
	CurrentInstanceID *string         `json:"current_instance_id"`
	WorkingDirectory  string          `json:"working_directory"`
	CWD               *string         `json:"cwd"`
	Networking        json.RawMessage `json:"networking"`
	Repositories      []Repository    `json:"repositories"`
	DriveExclude      []DriveExclude  `json:"drive_exclude"`
	AssociationState  string          `json:"association_state"`
	ExecutionState    string          `json:"execution_state"`
	ConnectionID      *string         `json:"connection_id"`
	BindingID         *string         `json:"binding_id"`
	Error             *Error          `json:"error"`

	sessionID string
}

// Repository is one repository of a cloud computer as the object lists it:
// ID is its Session resource ID and Path the resolved clone path.
type Repository struct {
	ID          string       `json:"id"`
	URL         string       `json:"url"`
	Checkout    *Checkout    `json:"checkout"`
	MountPath   *string      `json:"mount_path"`
	GitIdentity *GitIdentity `json:"git_identity"`
	Path        string       `json:"path"`
}

type Checkout struct {
	Type string `json:"type"`
	Name string `json:"name,omitempty"`
	SHA  string `json:"sha,omitempty"`
}

type GitIdentity struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

// Error is the latest failed preparation.
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

const objectColumns = `session_id,id,kind,environment_id,instance_id,current_instance_id,working_directory,cwd,networking,drive_exclude,
 association_state,execution_state,connection_id,binding_id,error_code,error_message`

// ListAttached returns the attached computers of each Session in sessionIDs,
// in the order they were added, with one query for the computers and one for
// their repositories.
func ListAttached(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionIDs []string) (map[string][]Object, error) {
	listed := map[string][]Object{}
	if len(sessionIDs) == 0 {
		return listed, nil
	}
	objects, err := scanObjects(tx.Query(ctx, `SELECT `+objectColumns+` FROM computers
		WHERE workspace_id=$1 AND session_id=ANY($2) AND association_state='attached' ORDER BY storage_sequence`, string(ws), sessionIDs))
	if err != nil {
		return nil, err
	}
	if err := attachRepositories(ctx, tx, ws, objects); err != nil {
		return nil, err
	}
	for _, object := range objects {
		listed[object.sessionID] = append(listed[object.sessionID], object)
	}
	return listed, nil
}

// Get returns the computer the read route serves: an attached one, or a
// detached one whose closure is closing or unknown.
func Get(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID, computerID string) (Object, error) {
	objects, err := scanObjects(tx.Query(ctx, `SELECT `+objectColumns+` FROM computers
		WHERE workspace_id=$1 AND session_id=$2 AND id=$3 AND (association_state='attached' OR execution_state IN ('closing','unknown'))`,
		string(ws), sessionID, computerID))
	if err != nil {
		return Object{}, err
	}
	if len(objects) == 0 {
		return Object{}, ErrNotFound
	}
	if err := attachRepositories(ctx, tx, ws, objects); err != nil {
		return Object{}, err
	}
	return objects[0], nil
}

func scanObjects(rows *dbconnect.Rows, err error) ([]Object, error) {
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var objects []Object
	for rows.Next() {
		var object Object
		var environmentID, instanceID, currentInstanceID, cwd, connectionID, bindingID, errorCode, errorMessage sql.NullString
		var networking, driveExclude []byte
		if err := rows.Scan(&object.sessionID, &object.ID, &object.Kind, &environmentID, &instanceID, &currentInstanceID, &object.WorkingDirectory, &cwd,
			&networking, &driveExclude, &object.AssociationState, &object.ExecutionState, &connectionID, &bindingID, &errorCode, &errorMessage); err != nil {
			return nil, err
		}
		object.EnvironmentID, object.InstanceID, object.CurrentInstanceID = nullable(environmentID), nullable(instanceID), nullable(currentInstanceID)
		object.CWD, object.ConnectionID, object.BindingID = nullable(cwd), nullable(connectionID), nullable(bindingID)
		object.Networking = json.RawMessage(networking)
		if err := json.Unmarshal(driveExclude, &object.DriveExclude); err != nil {
			return nil, err
		}
		if errorCode.Valid {
			object.Error = &Error{Code: errorCode.String, Message: errorMessage.String}
		}
		object.Repositories = []Repository{}
		objects = append(objects, object)
	}
	return objects, rows.Err()
}

// attachRepositories fills the repositories of objects, in the order their
// resources were created.
func attachRepositories(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, objects []Object) error {
	byID := map[string]*Object{}
	ids := make([]string, 0, len(objects))
	for i := range objects {
		byID[objects[i].ID] = &objects[i]
		ids = append(ids, objects[i].ID)
	}
	if len(ids) == 0 {
		return nil
	}
	rows, err := tx.Query(ctx, `SELECT gr.computer_id,gr.resource_id,gr.url,gr.checkout_type,gr.checkout_ref,gr.mount_path,gr.git_identity_name,gr.git_identity_email,gr.path
		FROM session_github_repository_resources gr
		JOIN session_resources sr ON sr.workspace_id=gr.workspace_id AND sr.session_id=gr.session_id AND sr.resource_id=gr.resource_id
		WHERE gr.workspace_id=$1 AND gr.computer_id=ANY($2) ORDER BY sr.storage_sequence`, string(ws), ids)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var computerID string
		var repository Repository
		var checkoutType, checkoutRef, mountPath, identityName, identityEmail sql.NullString
		if err := rows.Scan(&computerID, &repository.ID, &repository.URL, &checkoutType, &checkoutRef, &mountPath, &identityName, &identityEmail, &repository.Path); err != nil {
			return err
		}
		switch checkoutType.String {
		case "branch":
			repository.Checkout = &Checkout{Type: "branch", Name: checkoutRef.String}
		case "commit":
			repository.Checkout = &Checkout{Type: "commit", SHA: checkoutRef.String}
		}
		repository.MountPath = nullable(mountPath)
		if identityName.Valid {
			repository.GitIdentity = &GitIdentity{Name: identityName.String, Email: identityEmail.String}
		}
		object := byID[computerID]
		object.Repositories = append(object.Repositories, repository)
	}
	return rows.Err()
}

func nullable(value sql.NullString) *string {
	if !value.Valid {
		return nil
	}
	return &value.String
}
