package agentruntimebridge

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// Every RPC is classified. A mutation or current RPC reaches a process
// fence; a receipt replay by a retired process is answered
// before the fence, and the retirement tests hold what the fences refuse.
var runtimeProcessCallerInventory = map[string]string{
	"RegisterRuntimeProcess": "registry", "ReportRuntimeProcess": "registry", "ReleaseRuntimeBinding": "handoff", "McpManifestChanged": "service",
	"LoadContext": "current", "RefreshRuntimeBindingToken": "current", "CommitInputs": "mutation", "CommitTaskNotificationResult": "mutation", "WriteEvent": "mutation", "SettleToolResult": "mutation", "WriteRequestEnd": "mutation", "FinishIdle": "mutation", "CreateSubagentThread": "mutation", "EnsureApprovalReviewerTrunk": "mutation", "EnsureApprovalReviewerSidecar": "mutation", "AdmitApprovalReviewInput": "mutation", "ResolveChildThread": "read", "ListChildThreads": "read", "DeliverInterAgentMail": "mutation", "ReadAgentMail": "read", "AdmitChildInterrupt": "mutation", "AwaitChildInterrupt": "read", "CloseChildControl": "mutation", "CloseApprovalReviewer": "mutation", "MarkChildThreadActive": "mutation", "AcceptSandboxExecution": "mutation", "AwaitSandboxExecution": "receipt-read", "AdmitToolCall": "mutation", "AdmitToolControl": "mutation", "CancelToolCall": "mutation", "AwaitToolCall": "receipt-read", "ReadCommandResult": "mutation", "SendCommandInput": "mutation", "CancelCommand": "mutation", "AuthorizeWebToolExecution": "current", "RunMemory": "mutation", "ResolveTransientAttachment": "read", "ResolveAttachment": "read", "ResolveFileAttachmentMetadata": "read", "ReadFileAttachmentChunk": "read", "ClaimMcpToolResult": "mutation", "CommitMcpToolResult": "mutation", "RelinquishMcpToolResult": "mutation", "CommitInternalToolRepair": "mutation", "CommitRuntimeTermination": "mutation",
}

func runtimeFenceInventoryErrors(names []string, graph map[string][]string, fences []string) []string {
	var failures []string
	seen := map[string]bool{}
	var reaches func(string, map[string]bool) bool
	reaches = func(name string, visited map[string]bool) bool {
		if slices.Contains(fences, name) {
			return true
		}
		if visited[name] {
			return false
		}
		visited[name] = true
		return slices.ContainsFunc(graph[name], func(callee string) bool { return reaches(callee, visited) })
	}
	for _, name := range names {
		seen[name] = true
		class, ok := runtimeProcessCallerInventory[name]
		if !ok {
			failures = append(failures, "unclassified RPC "+name)
			continue
		}
		if (class == "mutation" || class == "current") && !reaches(name, map[string]bool{}) {
			failures = append(failures, name+" reaches no process fence")
		}
	}
	for name := range runtimeProcessCallerInventory {
		if !seen[name] {
			failures = append(failures, "the inventory names "+name+", which is no RPC")
		}
	}
	slices.Sort(failures)
	return failures
}

// bridgeStoreCallGraph names, for each function of the store and each plain
// function of the package, the functions and methods of the package its body
// calls. Its process fences are the functions that judge a locked process
// record, reading when it retired, and refuse it as stale.
func bridgeStoreCallGraph(t *testing.T) (graph map[string][]string, fences []string) {
	t.Helper()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	graph = map[string][]string{}
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		imported := map[string]bool{}
		for _, spec := range file.Imports {
			name := path.Base(strings.Trim(spec.Path.Value, `"`))
			if spec.Name != nil {
				name = spec.Name.Name
			}
			imported[name] = true
		}
		for _, declaration := range file.Decls {
			fn, ok := declaration.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			if fn.Recv != nil {
				receiver := fn.Recv.List[0].Type
				if pointer, ok := receiver.(*ast.StarExpr); ok {
					receiver = pointer.X
				}
				name, ok := receiver.(*ast.Ident)
				if !ok || (name.Name != "PostgreSQLBridgeAPIStore" && name.Name != "lockedRuntimeScope") {
					continue
				}
			}
			var refusesStale, readsRetirement bool
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				switch value := node.(type) {
				case *ast.BasicLit:
					refusesStale = refusesStale || value.Value == `"RUNTIME_PROCESS_STALE"`
				case *ast.SelectorExpr:
					readsRetirement = readsRetirement || value.Sel.Name == "RetiredAt"
				}
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch callee := call.Fun.(type) {
				case *ast.Ident:
					graph[fn.Name.Name] = append(graph[fn.Name.Name], callee.Name)
				case *ast.SelectorExpr:
					// Another package's function is no function of the store.
					if owner, ok := callee.X.(*ast.Ident); ok && imported[owner.Name] {
						return true
					}
					graph[fn.Name.Name] = append(graph[fn.Name.Name], callee.Sel.Name)
				}
				return true
			})
			if refusesStale && readsRetirement {
				fences = append(fences, fn.Name.Name)
			}
		}
	}
	return graph, fences
}

func TestEachMutationRPCReachesAProcessFence(t *testing.T) {
	graph, processFences := bridgeStoreCallGraph(t)
	if len(processFences) == 0 {
		t.Fatal("the package has no process fence")
	}
	var names []string
	for _, method := range bridgev1.AgentRuntimeBridgeService_ServiceDesc.Methods {
		names = append(names, method.MethodName)
	}
	if failures := runtimeFenceInventoryErrors(names, graph, processFences); len(failures) > 0 {
		t.Fatal(strings.Join(failures, "; "))
	}
	// Each fence carries RPCs of its own.
	for _, fence := range processFences {
		others := slices.DeleteFunc(slices.Clone(processFences), func(name string) bool { return name == fence })
		if failures := runtimeFenceInventoryErrors(names, graph, others); len(failures) == 0 {
			t.Errorf("no RPC depends on %s", fence)
		}
	}
}

func TestProcessFenceInventoryNamesEachGap(t *testing.T) {
	graph := map[string][]string{
		"CommitInputs":  {"withScope"},
		"withScope":     {"withScope", "fence"},
		"WriteEvent":    {"appendEvent"},
		"LoadContext":   {"readContext"},
		"ReadAgentMail": {"readMail"},
	}
	got := runtimeFenceInventoryErrors([]string{"CommitInputs", "WriteEvent", "LoadContext", "ReadAgentMail", "NewMutation"}, graph, []string{"fence"})
	want := []string{"LoadContext reaches no process fence", "WriteEvent reaches no process fence", "unclassified RPC NewMutation"}
	got = slices.DeleteFunc(got, func(failure string) bool { return strings.HasPrefix(failure, "the inventory names ") })
	if !slices.Equal(got, want) {
		t.Fatalf("failures = %v; want %v", got, want)
	}
}

// The generated base type answers Unimplemented for an RPC the server does
// not define. Without a store every defined RPC refuses before it reads its
// request, so any other answer shows the server's own method ran.
func TestBridgeAPIServerDefinesEveryRPC(t *testing.T) {
	for _, method := range bridgev1.AgentRuntimeBridgeService_ServiceDesc.Methods {
		_, err := method.Handler(NewBridgeAPIServer(nil), t.Context(), func(any) error { return nil }, nil)
		if code := status.Code(err); code != codes.FailedPrecondition {
			t.Errorf("%s without a store answers %s, not the server's refusal", method.MethodName, code)
		}
	}
}
