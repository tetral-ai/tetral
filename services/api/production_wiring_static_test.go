package tetralapi

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

// These static guards read the real service-local api production source and
// pin the construction symbols the public API composition must keep wired.
// Package-local placement is correct: the risk is that a refactor of this
// single owning package silently drops a required construction call, and the
// guard reads the actual source file rather than comparing constants.
func readTetralAPIProductionSource(t *testing.T) string {
	t.Helper()
	body, err := os.ReadFile("tetralapi.go")
	if err != nil {
		t.Fatalf("read tetralapi.go: %v", err)
	}
	return string(body)
}

func TestTetralAPIProductionConstructionUsesAES256GCMEncryptorNotVaultEncryptor(t *testing.T) {
	source := readTetralAPIProductionSource(t)
	if !strings.Contains(source, "encryption.NewAES256GCMEncryptor(") {
		t.Fatal("tetralapi.go must construct the vault encryptor via encryption.NewAES256GCMEncryptor")
	}
	if strings.Contains(source, "vault.NewEncryptor") {
		t.Fatal("tetralapi.go must not reference vault.NewEncryptor")
	}
}

func TestTetralAPIProductionConstructionUsesStablePublicPageTokenSecret(t *testing.T) {
	source := readTetralAPIProductionSource(t)
	for _, required := range []string{
		"session.WithPageTokenSecret(eventPageTokenSecret)",
		"agent.WithPageTokenSecret(eventPageTokenSecret)",
		"skill.WithPageTokenSecret(pageTokenSecret)",
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("tetralapi.go missing stable public page-token wiring %q", required)
		}
	}
}

func TestTetralAPIProductionConstructionInstallsEnvironmentService(t *testing.T) {
	source := readTetralAPIProductionSource(t)
	if !strings.Contains(source, "environment.NewService(") {
		t.Fatal("tetralapi.go must construct the environment service via environment.NewService")
	}
	if !strings.Contains(source, "httpapi.WithEnvironmentHandler(") {
		t.Fatal("tetralapi.go must install the environment handler via httpapi.WithEnvironmentHandler")
	}
	if !strings.Contains(source, "httpapi.NewEnvironmentHandler(") {
		t.Fatal("tetralapi.go must build the environment handler via httpapi.NewEnvironmentHandler")
	}
}

func TestTetralAPIProductionConstructionUsesDurableSessionEventAdmissionWithoutDispatcher(t *testing.T) {
	source := readTetralAPIProductionSource(t)
	for _, required := range []string{
		"sessionevent.NewPostgreSQLStore(cfg.RuntimeClient, sessionEventStoreOptions...)",
		"sessionevent.WithFileAttachmentValidator(fileStore)",
		"sessionevent.NewService(",
		"httpapi.NewSessionEventHandler(",
	} {
		if !strings.Contains(source, required) {
			t.Fatalf("tetralapi.go missing durable session-event admission fragment %q", required)
		}
	}
}

func TestTetralAPIProductionConstructionKeepsSandboxLifecycleOutOfPublicSessionService(t *testing.T) {
	source := readTetralAPIProductionSource(t)
	for _, forbidden := range []string{
		"session.WithSandboxLifecycle(",
		"sandbox.NewService(",
		"sandbox.NewUnavailableLifecycleProvider(",
		"sandbox.NewPostgreSQLStore(",
	} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("tetralapi.go must not wire public Session API directly to sandbox lifecycle fragment %q", forbidden)
		}
	}
}

// TestTetralAPIProductionConstructionBindsEncryptorIntoCredentialStore proves
// the encryptor VARIABLE assigned from encryption.NewAES256GCMEncryptor(...) is the
// SAME identifier passed as a positional argument into vault.NewPostgreSQLCredentialStore(...).
// Presence-only checks (the call exists somewhere) do not catch a refactor that
// constructs the encryptor and then passes a DIFFERENT value into the credential store,
// which would silently disable Vault credential encryption. The binding is asserted via go/ast:
// (1) a variable is bound from the constructor, and (2) that exact identifier appears
// inside the consumer call's paren span.
func TestTetralAPIProductionConstructionBindsEncryptorIntoCredentialStore(t *testing.T) {
	source := readTetralAPIProductionSource(t)
	bound, err := constructorResultBoundIntoCall(source, "encryption", "NewAES256GCMEncryptor", "vault", "NewPostgreSQLCredentialStore")
	if err != nil {
		t.Fatalf("encryptor binding into vault.NewPostgreSQLCredentialStore: %v", err)
	}
	if !bound {
		t.Fatal("the encryption.NewAES256GCMEncryptor(...) result variable is not passed as an argument into vault.NewPostgreSQLCredentialStore(...)")
	}
}

func TestTetralAPIProductionConstructionBindsEncryptorIntoSessionService(t *testing.T) {
	source := readTetralAPIProductionSource(t)
	bound, err := constructorResultBoundIntoCall(source, "encryption", "NewAES256GCMEncryptor", "session", "NewService")
	if err != nil {
		t.Fatalf("encryptor binding into session.NewService: %v", err)
	}
	if !bound {
		t.Fatal("the encryption.NewAES256GCMEncryptor(...) result variable is not passed into session.NewService(...)")
	}
}

// TestTetralAPIProductionConstructionBindsEnvironmentServiceIntoHandler (P5a) proves
// the service VARIABLE assigned from environment.NewService(...) is the SAME identifier
// passed as a positional argument into httpapi.NewEnvironmentHandler(...).
func TestTetralAPIProductionConstructionBindsEnvironmentServiceIntoHandler(t *testing.T) {
	source := readTetralAPIProductionSource(t)
	bound, err := constructorResultBoundIntoCall(source, "environment", "NewService", "httpapi", "NewEnvironmentHandler")
	if err != nil {
		t.Fatalf("environment service binding into httpapi.NewEnvironmentHandler: %v", err)
	}
	if !bound {
		t.Fatal("the environment.NewService(...) result variable is not passed as an argument into httpapi.NewEnvironmentHandler(...)")
	}
}

// TestConstructorResultBindingHelperRejectsSwappedArgument (P5a negative control)
// runs the SAME binding helper the positive tests use against an in-test source
// STRING where the argument is swapped to a different identifier, and asserts it is
// REJECTED — so the binding proof would actually fail if the wiring were broken.
// An in-test string is used (not a committed .go file) because a real .go file with
// the swapped wiring would not compile against the real signatures and would pollute
// the build.
func TestConstructorResultBindingHelperRejectsSwappedArgument(t *testing.T) {
	const correct = `package p
func build() {
	encryptor, err := encryption.NewAES256GCMEncryptor(cfg.VaultKey)
	_ = err
	credentialStore := vault.NewPostgreSQLCredentialStore(
		runtimeClient,
		encryptor,
	)
	_ = credentialStore
}
`
	const swapped = `package p
func build() {
	encryptor, err := encryption.NewAES256GCMEncryptor(cfg.VaultKey)
	_ = err
	noopEncryptor := vault.NewNoopEncryptor()
	credentialStore := vault.NewPostgreSQLCredentialStore(
		runtimeClient,
		noopEncryptor,
	)
	_ = credentialStore
	_ = encryptor
}
`

	boundCorrect, err := constructorResultBoundIntoCall(correct, "encryption", "NewAES256GCMEncryptor", "vault", "NewPostgreSQLCredentialStore")
	if err != nil {
		t.Fatalf("helper on correct snippet: %v", err)
	}
	if !boundCorrect {
		t.Fatal("binding helper rejected a correctly-wired snippet; it would never catch a real wiring")
	}

	boundSwapped, err := constructorResultBoundIntoCall(swapped, "encryption", "NewAES256GCMEncryptor", "vault", "NewPostgreSQLCredentialStore")
	if err != nil {
		t.Fatalf("helper on swapped snippet: %v", err)
	}
	if boundSwapped {
		t.Fatal("binding helper accepted a snippet whose session.NewService(...) is passed a DIFFERENT identifier than the encryptor it bound; the negative control failed")
	}
}

// constructorResultBoundIntoCall parses source via go/ast and reports whether the
// variable bound from constructorPkg.constructorName(...) appears as a positional
// argument inside a consumerPkg.consumerName(...) call. The binding is two-part: it
// records the LHS identifier(s) of the assignment whose RHS is the constructor call,
// then walks every consumer call and checks one of those identifiers is a top-level
// positional argument. The constructor's error result name (e.g. err) is excluded so
// only the constructed value counts as the bound identifier.
func constructorResultBoundIntoCall(source, constructorPkg, constructorName, consumerPkg, consumerName string) (bool, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "source.go", source, 0)
	if err != nil {
		return false, err
	}

	bound := map[string]bool{}
	ast.Inspect(file, func(node ast.Node) bool {
		assign, ok := node.(*ast.AssignStmt)
		if !ok || len(assign.Rhs) != 1 {
			return true
		}
		call, ok := assign.Rhs[0].(*ast.CallExpr)
		if !ok || !astSelectorCall(call, constructorPkg, constructorName) {
			return true
		}
		for index, lhs := range assign.Lhs {
			identifier, ok := lhs.(*ast.Ident)
			if !ok || identifier.Name == "_" {
				continue
			}
			// The constructor returns (value, error); the value is the first result.
			// Only the first LHS identifier is the constructed value to bind.
			if index == 0 {
				bound[identifier.Name] = true
			}
		}
		return true
	})
	if len(bound) == 0 {
		return false, nil
	}

	found := false
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || !astSelectorCall(call, consumerPkg, consumerName) {
			return true
		}
		for _, arg := range call.Args {
			identifier, ok := arg.(*ast.Ident)
			if ok && bound[identifier.Name] {
				found = true
				return false
			}
		}
		return true
	})
	return found, nil
}

// astSelectorCall reports whether call is pkg.name(...).
func astSelectorCall(call *ast.CallExpr, pkg, name string) bool {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != name {
		return false
	}
	identifier, ok := selector.X.(*ast.Ident)
	return ok && identifier.Name == pkg
}
