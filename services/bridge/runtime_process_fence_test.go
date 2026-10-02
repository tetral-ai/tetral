package agentruntimebridge

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"

	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// Every public method is classified. Ordinary receipt exceptions retain a
// current-process path for new effects; a new RPC or an omitted guard fails CI.
var runtimeProcessCallerInventory = map[string]string{
	"RegisterRuntimeProcess": "registry", "ReportRuntimeProcess": "registry", "ReleaseRuntimeBinding": "handoff", "McpManifestChanged": "service",
	"LoadContext": "current", "RefreshRuntimeBindingToken": "current", "CommitInputs": "mutation", "CommitTaskNotificationResult": "mutation", "WriteEvent": "mutation", "SettleToolResult": "mutation", "WriteRequestEnd": "mutation", "FinishIdle": "mutation", "CreateSubagentThread": "mutation", "EnsureApprovalReviewerTrunk": "mutation", "EnsureApprovalReviewerSidecar": "mutation", "AdmitApprovalReviewInput": "mutation", "ResolveChildThread": "read", "ListChildThreads": "read", "DeliverInterAgentMail": "mutation", "ReadAgentMail": "read", "AdmitChildInterrupt": "mutation", "AwaitChildInterrupt": "read", "CloseChildControl": "mutation", "CloseApprovalReviewer": "mutation", "MarkChildThreadActive": "mutation", "AcceptSandboxExecution": "mutation", "AwaitSandboxExecution": "receipt-read", "ReadCommandResult": "mutation", "SendCommandInput": "mutation", "CancelCommand": "mutation", "AuthorizeWebToolExecution": "current", "RunMemory": "mutation", "ResolveTransientAttachment": "read", "ResolveFileAttachmentMetadata": "read", "ReadFileAttachmentChunk": "read", "ClaimMcpToolResult": "mutation", "CommitMcpToolResult": "mutation", "RelinquishMcpToolResult": "mutation", "CommitInternalToolRepair": "mutation", "CommitRuntimeTermination": "mutation",
}

func runtimeFenceInventoryErrors(names []string, graph map[string][]string) []string {
	var failures []string
	seen := map[string]bool{}
	var reaches func(string, map[string]bool) bool
	reaches = func(name string, visited map[string]bool) bool {
		if name == "current_process_authority" {
			return true
		}
		if visited[name] {
			return false
		}
		visited[name] = true
		for _, child := range graph[name] {
			if reaches(child, visited) {
				return true
			}
		}
		return false
	}
	for _, name := range names {
		seen[name] = true
		class, ok := runtimeProcessCallerInventory[name]
		if !ok {
			failures = append(failures, "unclassified public RPC "+name)
			continue
		}
		if (class == "mutation" || class == "current") && !reaches(name, map[string]bool{}) {
			failures = append(failures, "missing current-process mutation fence "+name)
		}
	}
	for name := range runtimeProcessCallerInventory {
		if !seen[name] {
			failures = append(failures, "obsolete caller inventory "+name)
		}
	}
	return failures
}

func TestRuntimeMutationProcessFenceCallerInventory(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	graph := map[string][]string{}
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, declaration := range file.Decls {
			fn, ok := declaration.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			if fn.Recv != nil {
				receiver := fn.Recv.List[0].Type
				if ptr, ok := receiver.(*ast.StarExpr); ok {
					receiver = ptr.X
				}
				name, ok := receiver.(*ast.Ident)
				if !ok || name.Name != "PostgreSQLBridgeAPIStore" {
					continue
				}
			}
			fields := map[string]bool{}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				if selector, ok := node.(*ast.SelectorExpr); ok {
					fields[selector.Sel.Name] = true
				}
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch callee := call.Fun.(type) {
				case *ast.Ident:
					graph[fn.Name.Name] = append(graph[fn.Name.Name], callee.Name)
				case *ast.SelectorExpr:
					owner, ok := callee.X.(*ast.Ident)
					if ok && owner.Name == "runtimecontrol" {
						graph[fn.Name.Name] = append(graph[fn.Name.Name], "runtimecontrol."+callee.Sel.Name)
					} else {
						graph[fn.Name.Name] = append(graph[fn.Name.Name], callee.Sel.Name)
					}
				}
				return true
			})
			if fn.Name.Name == "requireRuntimeProcessCurrentTx" && fields["Current"] && fields["RetiredAt"] && fields["Phase"] && fields["ScopeSupersededError"] {
				for _, callee := range graph[fn.Name.Name] {
					if callee == "lockRuntimeBindingProcessTx" {
						graph[fn.Name.Name] = append(graph[fn.Name.Name], "current_process_authority")
						break
					}
				}
			}
		}
	}
	var names []string
	for _, method := range bridgev1.AgentRuntimeBridgeService_ServiceDesc.Methods {
		names = append(names, method.MethodName)
	}
	if failures := runtimeFenceInventoryErrors(names, graph); len(failures) > 0 {
		t.Fatal(strings.Join(failures, "; "))
	}
	// A newly exposed mutator is never silently accepted as a read exception.
	if failures := runtimeFenceInventoryErrors(append(append([]string{}, names...), "UnclassifiedRuntimeMutation"), graph); len(failures) != 1 || !strings.Contains(failures[0], "unclassified") {
		t.Fatalf("new caller escaped guard:%v", failures)
	}
	// Remove the actual common current-process edge from its parsed body. The
	// inventory must detect every mutation family that depended on that edge.
	changed := make(map[string][]string, len(graph))
	for name, calls := range graph {
		changed[name] = append([]string{}, calls...)
	}
	changed["requireRuntimeProcessCurrentTx"] = nil
	if failures := runtimeFenceInventoryErrors(names, changed); len(failures) < 3 {
		t.Fatalf("removed production process fence escaped inventory:%v", failures)
	}
}
