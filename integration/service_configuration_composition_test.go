package integration

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
)

func TestPostgreSQLSeparatedOwnersRuntimeConfiguration(t *testing.T) {
	raw, err := os.ReadFile("testdata/service-configuration.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors map[string]struct{ Hot, Cold map[string]any }
	if err := json.Unmarshal(raw, &vectors); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"configured", "empty"} {
		t.Run(name, func(t *testing.T) {
			f := newSeparatedOwners(t, "config_"+name, false)
			agent := `{"name":"original","model":"anthropic/claude-opus-4-8","system":null,"tools":[{"type":"tetral_agent_toolset","family":"gpt"},{"type":"mcp_toolset","mcp_server_name":"obsolete"}],"mcp_servers":[{"type":"url","name":"obsolete","url":"https://obsolete.example/mcp"}],"skills":[]}`
			installed := `{"tools":[{"type":"tetral_agent_toolset","family":"claude"}],"mcp_servers":[]}`
			if name == "configured" {
				agent = `{"name":"original","model":"anthropic/claude-opus-4-8","system":"Operate as the session specialist.","tools":[{"type":"tetral_agent_toolset","family":"gpt"}],"mcp_servers":[],"skills":[]}`
				installed = `{"tools":[{"type":"tetral_agent_toolset","family":"claude"},{"type":"mcp_toolset","mcp_server_name":"github","default_config":{"enabled":false,"permission_policy":{"type":"always_ask"}},"configs":[{"name":"github_search","enabled":true,"permission_policy":{"type":"always_allow"}}]}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}]}`
			}
			sessionfixture.SeedBridgeAPIAgentConfig(t, f.admin, "default", f.sessionID, agent)
			f.sql(t, `UPDATE sessions SET approval_mode='approve_for_me',config_generation=7,installed_tools_json=$1 WHERE workspace_id='default' AND id=$2`, installed, f.sessionID)
			if name == "configured" {
				for _, memory := range []struct{ id, state string }{{"memstore_runtime_config", "attached"}, {"memstore_detached", "detached_at"}, {"memstore_deleting", "delete_requested_at"}} {
					resource := "res_" + memory.id
					f.sql(t, `INSERT INTO memory_stores(workspace_id,memory_store_id,name,created_at,updated_at) VALUES('default',$1,$1,clock_timestamp(),clock_timestamp())`, memory.id)
					f.sql(t, `INSERT INTO session_resources(workspace_id,session_id,resource_id,type,created_at,updated_at) VALUES('default',$1,$2,'memory_store',clock_timestamp(),clock_timestamp())`, f.sessionID, resource)
					f.sql(t, `INSERT INTO session_memory_store_resources(workspace_id,session_id,resource_id,memory_store_id,access,name,mount_path,instructions) VALUES('default',$1,$2,$3,'read_write','Project notes',$4,'Preserve this guidance.')`, f.sessionID, resource, memory.id, "/mnt/memory/"+memory.id)
					if memory.state != "attached" {
						f.sql(t, `UPDATE session_resources SET `+memory.state+`=clock_timestamp() WHERE workspace_id='default' AND session_id=$1 AND resource_id=$2`, f.sessionID, resource)
					}
				}
			}
			// Queue generation 1 is deliberately obsolete. The actual command rebuild
			// must select generation 7 and the installed snapshot, not the agent tools.
			sender := separatedSender()
			result, err := (jobrunner.RuntimePodDirectDeliverer{Store: f.runner, Sender: sender}).DeliverRuntimeJob(f.ctx, f.configJob("1"))
			if err != nil || result.Status != jobrunner.RuntimeDeliveryAccepted || len(sender.requests) != 1 {
				t.Fatalf("hot delivery result/send count=%#v/%v/%d", result, err, len(sender.requests))
			}
			request, ok := sender.requests[0].(*agentruntimev1.ApplyRuntimeConfigRequest)
			if !ok || request.GetSessionConfig().GetGeneration() != 7 {
				t.Fatalf("hot current generation: %v", sender.requests[0])
			}
			hot := separatedJSON(t, request.GetSessionConfig().GetContentJson()).(map[string]any)
			if hot["workspace_id"] != "default" || hot["session_id"] != f.sessionID {
				t.Fatalf("hot routing identity=%v", hot)
			}
			delete(hot, "workspace_id")
			delete(hot, "session_id")
			separatedEqual(t, hot, vectors[name].Hot)
			scope := f.declare(t)
			cold := f.cold(t, scope)["runtimeConfig"].(map[string]any)
			projection := map[string]any{}
			for key := range vectors[name].Cold {
				projection[key] = cold[key]
			}
			separatedEqual(t, projection, vectors[name].Cold)
			t.Logf("literal independent hot/cold oracles pass; stale queued generation=1 rebuilt=7; installed family=claude; sends=1; detached/deleting memories omitted")
		})
	}
}
