package computer

import (
	"context"
	"encoding/json"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// Changes a machine message reports.
const (
	ChangeAdded    = "added"
	ChangeRemoved  = "removed"
	ChangeReplaced = "replaced"
)

// RuntimeRow is one computer as the Runtime's machine table holds it.
type RuntimeRow struct {
	ComputerID       string                `json:"computerId"`
	Kind             string                `json:"kind"`
	WorkingDirectory string                `json:"workingDirectory"`
	Networking       json.RawMessage       `json:"networking"`
	Repositories     []RuntimeRepository   `json:"repositories"`
	DriveExclude     []RuntimeDriveExclude `json:"driveExclude"`
}

// RuntimeRepository is one repository of a cloud computer at its resolved
// clone path.
type RuntimeRepository struct {
	URL  string `json:"url"`
	Path string `json:"path"`
}

type RuntimeDriveExclude struct {
	DriveID string   `json:"driveId"`
	Paths   []string `json:"paths"`
}

// ReadRuntimeRow projects one computer of the Session as its Runtime row,
// whatever its association state.
func ReadRuntimeRow(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID, computerID string) (RuntimeRow, error) {
	objects, err := scanObjects(tx.Query(ctx, `SELECT `+objectColumns+` FROM computers WHERE workspace_id=$1 AND session_id=$2 AND id=$3`,
		string(ws), sessionID, computerID))
	if err != nil {
		return RuntimeRow{}, err
	}
	if len(objects) == 0 {
		return RuntimeRow{}, ErrNotFound
	}
	if err := attachRepositories(ctx, tx, ws, objects); err != nil {
		return RuntimeRow{}, err
	}
	object := objects[0]
	row := RuntimeRow{
		ComputerID: object.ID, Kind: object.Kind, WorkingDirectory: object.WorkingDirectory, Networking: object.Networking,
		Repositories: []RuntimeRepository{}, DriveExclude: []RuntimeDriveExclude{},
	}
	for _, repository := range object.Repositories {
		row.Repositories = append(row.Repositories, RuntimeRepository{URL: repository.URL, Path: repository.Path})
	}
	for _, exclude := range object.DriveExclude {
		row.DriveExclude = append(row.DriveExclude, RuntimeDriveExclude(exclude))
	}
	return row, nil
}

// WriteMachineMessageTx writes, in the transaction that makes the change, one
// machine message about a computer: a session_machine_messages row holding the
// change and the computer's Runtime row at that moment, and the
// machine_message input of the Session's main thread with the same
// runtime_input_id.
func WriteMachineMessageTx(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID, computerID, change string, now time.Time) error {
	row, err := ReadRuntimeRow(ctx, tx, ws, sessionID, computerID)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(struct {
		Change string     `json:"change"`
		Row    RuntimeRow `json:"row"`
	}{change, row})
	if err != nil {
		return err
	}
	var mainThreadID string
	if err := tx.QueryRow(ctx, `SELECT main_thread_id FROM sessions WHERE workspace_id=$1 AND id=$2`, string(ws), sessionID).Scan(&mainThreadID); err != nil {
		return err
	}
	runtimeInputID := queue.RuntimeInputKindMachineMessage + ":" + id.New("mmsg_")
	if _, err := tx.Exec(ctx, `INSERT INTO session_machine_messages (workspace_id,session_id,runtime_input_id,computer_id,change,payload_json,created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7)`, string(ws), sessionID, runtimeInputID, computerID, change, string(payload), now); err != nil {
		return err
	}
	return runtimecontrol.AppendNonStartingInputTx(ctx, tx, string(ws), sessionID, mainThreadID, runtimeInputID, queue.RuntimeInputKindMachineMessage, now)
}
