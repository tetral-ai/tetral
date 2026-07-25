package agent_test

import (
	"reflect"
	"testing"

	"github.com/tetral-ai/tetral/internal/agent"
)

func TestPostgreSQLAgentStoreDoesNotExportServiceMethods(t *testing.T) {
	storeType := reflect.TypeOf(agent.NewPostgreSQLAgentStore(nil))
	for _, methodName := range []string{"Create", "Get", "List", "Update", "UpdatePatch", "GetVersion"} {
		if _, ok := storeType.MethodByName(methodName); ok {
			t.Errorf("PostgreSQLAgentStore exports %s; business behavior belongs on agent.Service", methodName)
		}
	}
}
