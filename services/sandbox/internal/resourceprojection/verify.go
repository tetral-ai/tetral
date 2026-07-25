package resourceprojection

import (
	"context"
	"path"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/sandbox/driver"
)

const ResourceLocalCopyStagingRoot = "/tmp/tetral-runtime/resource-projection"

func RunLocalCopyVerify(ctx context.Context, runner driver.PreparationCommandRunner, target driver.PreparationCommandTarget, plan Plan, timeout time.Duration) error {
	return runner.RunPreparationCommand(ctx, target, LocalCopyVerifyCommand(plan), nil, timeout)
}

func LocalCopyVerifyCommand(plan Plan) string {
	var b strings.Builder
	b.WriteString("set -eu\n")
	b.WriteString("PATH=/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin\n")
	stageRoot := LocalCopyStageRoot(plan)
	b.WriteString("cleanup_on_error() {\n")
	b.WriteString("  set +e\n")
	for _, action := range ActionsOfType(plan.Actions, ActionBind) {
		b.WriteString("  sudo -u " + shellQuote(driver.RuntimeUser) + " rm -f -- " + shellQuote(action.MountPath) + " >/dev/null 2>&1 || true\n")
	}
	b.WriteString("  sudo rm -rf -- " + shellQuote(stageRoot) + " >/dev/null 2>&1 || true\n")
	b.WriteString("}\n")
	b.WriteString("trap cleanup_on_error ERR\n")
	for _, action := range ActionsOfType(plan.Actions, ActionBind) {
		stagePath := LocalCopyStagePath(plan, action.ResourceID)
		target := action.MountPath
		parent := path.Dir(target)
		b.WriteString(EnsureParentCommand(parent))
		b.WriteString("if [ -L " + shellQuote(target) + " ]; then sudo -u " + shellQuote(driver.RuntimeUser) + " rm -f -- " + shellQuote(target) + "; fi\n")
		b.WriteString("if [ -e " + shellQuote(target) + " ] && [ ! -f " + shellQuote(target) + " ]; then echo 'resource projection target is not a regular file' >&2; false; fi\n")
		b.WriteString("sudo install -m 0444 -o " + shellQuote(driver.RuntimeUser) + " -g " + shellQuote(driver.RuntimeUser) + " -- " + shellQuote(stagePath) + " " + shellQuote(target) + "\n")
	}
	writeVerifyChecks(&b, ActionsOfType(plan.Actions, ActionVerify))
	b.WriteString("sudo rm -rf -- " + shellQuote(stageRoot) + "\n")
	b.WriteString("trap - ERR\n")
	return b.String()
}

// writeVerifyChecks emits, per projected file, the checks that confirm the
// read-only projection as the runtime user (sudo -u daytona) rather than root,
// because the runtime user is the surface the model experiences:
//   - target is not a symlink;
//   - target is a regular file;
//   - its first byte is readable (this exercises the --allow-other cross-user
//     FUSE boundary that root would bypass);
//   - opening it for write fails (the projection is read-only);
//   - the parent directory is writable by the runtime user (this exercises
//     runtime-user ownership of the parent, which root would also bypass).
func writeVerifyChecks(b *strings.Builder, actions []Action) {
	for _, action := range actions {
		target := action.MountPath
		parent := path.Dir(target)
		b.WriteString("sudo -u " + shellQuote(driver.RuntimeUser) + " test ! -L " + shellQuote(target) + "\n")
		b.WriteString("sudo -u " + shellQuote(driver.RuntimeUser) + " test -f " + shellQuote(target) + "\n")
		b.WriteString("sudo -u " + shellQuote(driver.RuntimeUser) + " head -c 1 -- " + shellQuote(target) + " >/dev/null\n")
		b.WriteString("if sudo -u " + shellQuote(driver.RuntimeUser) + " sh -c 'exec 9>\"$1\"' _ " + shellQuote(target) + " 2>/dev/null; then false; fi\n")
		b.WriteString("sudo -u " + shellQuote(driver.RuntimeUser) + " sh -c 'tmp=$(mktemp \"$1/.tetral-resource-verify.XXXXXX\") && rm -f \"$tmp\"' _ " + shellQuote(parent) + "\n")
	}
}
