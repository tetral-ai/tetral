package computer

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/drive"
	"github.com/tetral-ai/tetral/internal/environment"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// DefaultCloudWorkingDirectory is a cloud computer's working directory when
// its entry names none.
const DefaultCloudWorkingDirectory = "/workspace"

// Entry is one requested computer. A nil field was absent from the request;
// Networking is the networking type a user computer names. Repositories
// counts the entry's repositories, which internal/session checks itself.
type Entry struct {
	EnvironmentID    *string
	InstanceID       *string
	WorkingDirectory *string
	Networking       *string
	Repositories     int
	DriveExclude     []DriveExclude
}

// Checked is an entry that passed CheckEntries, with its defaults filled.
type Checked struct {
	Kind             string
	EnvironmentID    string
	InstanceID       string
	WorkingDirectory string
	Networking       json.RawMessage
	DriveExclude     []DriveExclude
}

// CheckEntries checks entries, the computers one request adds to the
// Session, one entry at a time, in this order: exactly one of environment_id
// and instance_id, the Environment or instance it names, working_directory,
// networking, repositories and drive_exclude; the first broken rule answers.
// It fills their defaults. A cloud computer copies its Environment's networking
// now, so a later Environment change does not reach it.
func CheckEntries(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID string, entries []Entry) ([]Checked, error) {
	checked := make([]Checked, 0, len(entries))
	named := map[string]bool{}
	for _, entry := range entries {
		var result Checked
		if (entry.EnvironmentID == nil) == (entry.InstanceID == nil) {
			return nil, &InvalidError{Message: "a computer names exactly one of environment_id and instance_id"}
		}
		if entry.EnvironmentID != nil {
			networking, err := environmentNetworking(ctx, tx, ws, *entry.EnvironmentID)
			if err != nil {
				return nil, err
			}
			result.Kind, result.EnvironmentID, result.Networking = KindCloud, *entry.EnvironmentID, networking
		} else {
			if err := checkInstance(ctx, tx, ws, sessionID, *entry.InstanceID, named); err != nil {
				return nil, err
			}
			named[*entry.InstanceID] = true
			result.Kind, result.InstanceID = KindUser, *entry.InstanceID
		}
		switch {
		case entry.WorkingDirectory != nil:
			if !strings.HasPrefix(*entry.WorkingDirectory, "/") || strings.ContainsRune(*entry.WorkingDirectory, 0) {
				return nil, &InvalidError{Message: "working_directory must be an absolute path without U+0000"}
			}
			result.WorkingDirectory = *entry.WorkingDirectory
		case result.Kind == KindCloud:
			result.WorkingDirectory = DefaultCloudWorkingDirectory
		default:
			return nil, &InvalidError{Message: "working_directory is required on a user computer"}
		}
		if result.Kind == KindCloud {
			if entry.Networking != nil {
				return nil, &InvalidError{Message: "networking is not allowed on a cloud computer; it takes its Environment's networking"}
			}
		} else {
			networking := "blocked"
			if entry.Networking != nil {
				networking = *entry.Networking
			}
			if networking != "blocked" && networking != "unrestricted" {
				return nil, &InvalidError{Message: `networking type must be "blocked" or "unrestricted"`}
			}
			result.Networking = json.RawMessage(`{"type":"` + networking + `"}`)
		}
		if result.Kind == KindUser && entry.Repositories > 0 {
			return nil, &InvalidError{Message: "repositories are not allowed on a user computer"}
		}
		exclude, err := checkDriveExclude(entry.DriveExclude)
		if err != nil {
			return nil, err
		}
		result.DriveExclude = exclude
		checked = append(checked, result)
	}
	return checked, nil
}

// environmentNetworking returns the networking object of a live Environment
// of the Workspace.
func environmentNetworking(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, environmentID string) (json.RawMessage, error) {
	var archived bool
	var configJSON string
	err := tx.QueryRow(ctx, `SELECT archived_at IS NOT NULL,config_json FROM environments WHERE workspace_id=$1 AND id=$2`, string(ws), environmentID).Scan(&archived, &configJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, &NotFoundError{Message: "environment not found"}
	}
	if err != nil {
		return nil, err
	}
	if archived {
		return nil, &InvalidError{Message: "environment is archived"}
	}
	var config environment.EnvironmentConfig
	if err := json.Unmarshal([]byte(configJSON), &config); err != nil {
		return nil, err
	}
	normalized, err := environment.NormalizeEnvironmentConfig(config)
	if err != nil {
		return nil, err
	}
	return json.Marshal(normalized.Networking)
}

// checkInstance refuses an instance that is missing, cloud, deregistered,
// already named by the request, or behind a computer of the Session that is
// attached or whose closure is outstanding.
func checkInstance(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID, instanceID string, named map[string]bool) error {
	value, err := instance.ReadInstance(ctx, tx, ws, instanceID)
	if errors.Is(err, instance.ErrNotFound) {
		return &NotFoundError{Message: "instance not found"}
	}
	if err != nil {
		return err
	}
	if value.Kind == instance.KindCloud || value.RegistrationState == instance.RegistrationDeregistered {
		return &NotAttachableError{}
	}
	if named[instanceID] {
		return &AlreadyAttachedError{}
	}
	var attached bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM computers WHERE workspace_id=$1 AND session_id=$2 AND instance_id=$3
		AND (association_state='attached' OR execution_state IN ('closing','unknown')))`, string(ws), sessionID, instanceID).Scan(&attached); err != nil {
		return err
	}
	if attached {
		return &AlreadyAttachedError{}
	}
	return nil
}

func checkDriveExclude(entries []DriveExclude) ([]DriveExclude, error) {
	checked := make([]DriveExclude, 0, len(entries))
	for _, entry := range entries {
		if entry.DriveID == "" {
			return nil, &InvalidError{Message: "drive_exclude drive_id must not be empty"}
		}
		paths := make([]string, 0, len(entry.Paths))
		for _, path := range entry.Paths {
			if rule := drive.CheckPath(strings.TrimSuffix(path, "/")); rule != "" {
				return nil, &InvalidError{Message: fmt.Sprintf("drive_exclude path %q is invalid: %s", path, rule)}
			}
			paths = append(paths, path)
		}
		checked = append(checked, DriveExclude{DriveID: entry.DriveID, Paths: paths})
	}
	return checked, nil
}

// AddComputers is the one writer of new computers: it stores checked as the
// Session's next computers, attached and waiting, in their order, and returns
// their IDs.
func AddComputers(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID string, checked []Checked) ([]string, error) {
	ids := make([]string, 0, len(checked))
	for _, computer := range checked {
		computerID := id.New("comp_")
		exclude, err := json.Marshal(computer.DriveExclude)
		if err != nil {
			return nil, err
		}
		var environmentID, instanceID any
		if computer.Kind == KindCloud {
			environmentID = computer.EnvironmentID
		} else {
			instanceID = computer.InstanceID
		}
		if _, err := tx.Exec(ctx, `INSERT INTO computers (workspace_id,session_id,id,kind,environment_id,instance_id,current_instance_id,
			working_directory,networking,drive_exclude,association_state,execution_state)
			VALUES ($1,$2,$3,$4,$5,$6,$6,$7,$8,$9,'attached','waiting')`,
			string(ws), sessionID, computerID, computer.Kind, environmentID, instanceID, computer.WorkingDirectory,
			string(computer.Networking), string(exclude)); err != nil {
			return nil, err
		}
		ids = append(ids, computerID)
	}
	// Only a user computer can have an online instance now, and it has no
	// repositories, so its session.prepare lacks nothing stored later.
	for _, computerID := range ids {
		if _, err := IssuePreparation(ctx, tx, ws, sessionID, computerID); err != nil {
			return nil, err
		}
	}
	return ids, nil
}
