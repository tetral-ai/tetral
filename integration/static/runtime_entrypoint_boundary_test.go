package static

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

type runtimeEntrypointBoundaryReport struct {
	GuardCount int                                  `json:"guardCount"`
	Violations []runtimeEntrypointBoundaryViolation `json:"violations"`
}

type runtimeEntrypointBoundaryViolation struct {
	File            string `json:"file"`
	GuardLine       int    `json:"guardLine"`
	DeclarationLine int    `json:"declarationLine"`
	DeclarationKind string `json:"declarationKind"`
}

func TestRuntimeEntrypointDeclarationBoundary(t *testing.T) {
	engineRoot := finalArchitectureEngineRoot(t)
	workspaceRoot := filepath.Join(engineRoot, "services", "agent-runtime")
	typescriptPath, err := filepath.Abs(filepath.Join(workspaceRoot, "node_modules", "typescript", "lib", "typescript.js"))
	if err != nil {
		t.Fatalf("resolve TypeScript parser: %v", err)
	}
	if _, err := os.Stat(typescriptPath); err != nil {
		t.Fatalf("runtime entrypoint boundary requires the workspace TypeScript parser at %s: %v", typescriptPath, err)
	}
	bunPath, err := exec.LookPath("bun")
	if err != nil {
		t.Fatalf("runtime entrypoint boundary requires bun: %v", err)
	}

	sourceFiles := runtimeEntrypointTypeScriptFiles(t, engineRoot)
	tempDir := t.TempDir()
	filesPath := filepath.Join(tempDir, "files.json")
	filesJSON, err := json.Marshal(sourceFiles)
	if err != nil {
		t.Fatalf("marshal TypeScript source file list: %v", err)
	}
	if err := os.WriteFile(filesPath, filesJSON, 0o600); err != nil {
		t.Fatalf("write TypeScript source file list: %v", err)
	}
	scriptPath := filepath.Join(tempDir, "runtime-entrypoint-boundary.mjs")
	if err := os.WriteFile(scriptPath, []byte(runtimeEntrypointBoundaryScript), 0o600); err != nil {
		t.Fatalf("write TypeScript boundary parser: %v", err)
	}

	command := exec.Command(bunPath, scriptPath, typescriptPath, filesPath) //nolint:gosec // repository parser and test-owned paths.
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run TypeScript runtime-entrypoint boundary parser: %v\n%s", err, output)
	}
	var report runtimeEntrypointBoundaryReport
	if err := json.Unmarshal(output, &report); err != nil {
		t.Fatalf("decode TypeScript runtime-entrypoint boundary report: %v\n%s", err, output)
	}
	t.Logf("runtime entrypoint declaration boundary scanned %d import.meta.main guards", report.GuardCount)
	for _, violation := range report.Violations {
		t.Errorf(
			"%s import.meta.main guard at line %d has top-level %s declaration at line %d",
			violation.File,
			violation.GuardLine,
			violation.DeclarationKind,
			violation.DeclarationLine,
		)
	}
}

func runtimeEntrypointTypeScriptFiles(t *testing.T, engineRoot string) []string {
	t.Helper()
	var files []string
	for _, root := range []string{
		filepath.Join(engineRoot, "services", "agent-runtime"),
		filepath.Join(engineRoot, "services", "gateway"),
	} {
		err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				switch entry.Name() {
				case "node_modules", "dist", "coverage":
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(entry.Name(), ".ts") {
				return nil
			}
			absolutePath, err := filepath.Abs(path)
			if err != nil {
				return fmt.Errorf("resolve TypeScript source %s: %w", path, err)
			}
			files = append(files, absolutePath)
			return nil
		})
		if err != nil {
			t.Fatalf("enumerate TypeScript sources under %s: %v", root, err)
		}
	}
	sort.Strings(files)
	return files
}

const runtimeEntrypointBoundaryScript = `
import { pathToFileURL } from "node:url";

const [typescriptPath, filesPath] = Bun.argv.slice(2);
if (typescriptPath === undefined || filesPath === undefined) {
  throw new Error("expected TypeScript parser and source-list paths");
}
const imported = await import(pathToFileURL(typescriptPath).href);
const ts = imported.default ?? imported;
const files = JSON.parse(await Bun.file(filesPath).text());
const report = { guardCount: 0, violations: [] };

for (const file of files) {
  const source = await Bun.file(file).text();
  const sourceFile = ts.createSourceFile(file, source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
  for (let index = 0; index < sourceFile.statements.length; index++) {
    const statement = sourceFile.statements[index];
    if (!isImportMetaMainGuard(statement, sourceFile)) {
      continue;
    }
    report.guardCount++;
    const guardLine = lineOf(sourceFile, statement);
    for (const candidate of sourceFile.statements.slice(index + 1)) {
      const declarationKind = runtimeDeclarationKind(candidate);
      if (declarationKind === undefined) {
        continue;
      }
      report.violations.push({
        file,
        guardLine,
        declarationLine: lineOf(sourceFile, candidate),
        declarationKind,
      });
    }
  }
}

process.stdout.write(JSON.stringify(report));

function isImportMetaMainGuard(statement, sourceFile) {
  if (!ts.isIfStatement(statement)) {
    return false;
  }
  return statement.expression.getText(sourceFile) === "import.meta.main";
}

function runtimeDeclarationKind(statement) {
  if (ts.isClassDeclaration(statement)) {
    return "class";
  }
  if (ts.isEnumDeclaration(statement)) {
    return "enum";
  }
  if (!ts.isVariableStatement(statement)) {
    return undefined;
  }
  if (statement.modifiers?.some((modifier) => modifier.kind === ts.SyntaxKind.DeclareKeyword)) {
    return undefined;
  }
  if ((statement.declarationList.flags & ts.NodeFlags.Const) !== 0) {
    return "const";
  }
  if ((statement.declarationList.flags & ts.NodeFlags.Let) !== 0) {
    return "let";
  }
  return "var";
}

function lineOf(sourceFile, node) {
  return sourceFile.getLineAndCharacterOfPosition(node.getStart(sourceFile)).line + 1;
}
`
