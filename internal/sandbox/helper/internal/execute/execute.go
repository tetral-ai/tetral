package execute

import (
	"github.com/tetral-ai/tetral/internal/sandbox/helper/internal/task"
	"github.com/tetral-ai/tetral/internal/sandbox/helper/protocol"
)

type ExecResult = task.ExecResult
type Response = task.Response

func Run(payload protocol.Payload) Response {
	return task.RunExec(payload)
}
