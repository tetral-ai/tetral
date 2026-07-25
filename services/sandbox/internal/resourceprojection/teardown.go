package resourceprojection

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"path"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/sandbox/driver"
)

type DeletedFileCleanupTarget struct {
	ResourceID     string
	MountPath      string
	DestinationKey string
}

func ValidateCleanupMountPath(resourceID string, mountPath string) error {
	if mountPath == "" || !strings.HasPrefix(mountPath, "/") || strings.Contains(mountPath, "\x00") {
		return &PlanError{Code: "invalid_mount_path", ResourceID: resourceID, Path: mountPath, Message: "deleted mount_path must be absolute and valid"}
	}
	if clean := path.Clean(mountPath); clean != mountPath || clean == "/" {
		return &PlanError{Code: "invalid_mount_path", ResourceID: resourceID, Path: mountPath, Message: "deleted mount_path must be lexically clean"}
	}
	return nil
}

func RunDeletedFileCleanup(ctx context.Context, runner driver.PreparationCommandRunner, target driver.PreparationCommandTarget, targets []DeletedFileCleanupTarget, unmountStaging bool, timeout time.Duration) error {
	if len(targets) == 0 && !unmountStaging {
		return nil
	}
	return runner.RunPreparationCommand(ctx, target, DeletedFileCleanupCommand(targets, unmountStaging), nil, timeout)
}

func DeletedFileCleanupCommand(targets []DeletedFileCleanupTarget, unmountStaging bool) string {
	var b strings.Builder
	b.WriteString("set -eu\n")
	b.WriteString("PATH=/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin\n")
	for _, target := range targets {
		b.WriteString("if findmnt -rn --mountpoint " + shellQuote(target.MountPath) + " >/dev/null 2>&1; then sudo umount -l -- " + shellQuote(target.MountPath) + "; fi\n")
		b.WriteString("sudo rm -rf -- " + shellQuote(target.MountPath) + "\n")
	}
	if unmountStaging {
		b.WriteString("if mountpoint -q " + shellQuote(RcloneStagingRoot) + "; then sudo umount -l -- " + shellQuote(RcloneStagingRoot) + "; fi\n")
	}
	return b.String()
}

func RunFileProjectionCompensation(ctx context.Context, runner driver.PreparationCommandRunner, target driver.PreparationCommandTarget, plan Plan, timeout time.Duration) error {
	return runner.RunPreparationCommand(ctx, target, FileProjectionCompensationCommand(plan), nil, timeout)
}

func FileProjectionCompensationCommand(plan Plan) string {
	var b strings.Builder
	b.WriteString("set -eu\n")
	b.WriteString("PATH=/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin\n")
	b.WriteString("STAGING=" + shellQuote(RcloneStagingRoot) + "\n")
	for _, action := range ActionsOfType(plan.Actions, ActionBind) {
		b.WriteString("if findmnt -rn --mountpoint " + shellQuote(action.MountPath) + " >/dev/null 2>&1; then sudo umount -l -- " + shellQuote(action.MountPath) + "; fi\n")
		b.WriteString("sudo -u " + shellQuote(driver.RuntimeUser) + " rm -f -- " + shellQuote(action.MountPath) + " >/dev/null 2>&1 || true\n")
	}
	b.WriteString("if mountpoint -q \"$STAGING\"; then sudo umount -l -- \"$STAGING\"; fi\n")
	b.WriteString("sudo rm -rf -- " + shellQuote(LocalCopyStageRoot(plan)) + " >/dev/null 2>&1 || true\n")
	return b.String()
}

func LocalCopyStageRoot(plan Plan) string {
	sum := sha256.Sum256([]byte(plan.SessionPrefix))
	return ResourceLocalCopyStagingRoot + "/" + hex.EncodeToString(sum[:])
}

func LocalCopyStagePath(plan Plan, resourceID string) string {
	sum := sha256.Sum256([]byte(resourceID))
	return LocalCopyStageRoot(plan) + "/" + hex.EncodeToString(sum[:]) + "/file"
}

func EnsureParentCommand(parent string) string {
	if parent == "/" {
		return ""
	}
	quotedParent := shellQuote(parent)
	quotedUser := shellQuote(driver.RuntimeUser)
	return "if [ -e " + quotedParent + " ]; then\n" +
		"  [ -d " + quotedParent + " ]\n" +
		"  sudo -u " + quotedUser + " test -w " + quotedParent + "\n" +
		"  sudo -u " + quotedUser + " test -x " + quotedParent + "\n" +
		"else\n" +
		"  sudo install -d -m 0755 -o " + quotedUser + " -g " + quotedUser + " -- " + quotedParent + "\n" +
		"fi\n"
}

func ActionsOfType(actions []Action, actionType ActionType) []Action {
	out := make([]Action, 0, len(actions))
	for _, action := range actions {
		if action.Type == actionType {
			out = append(out, action)
		}
	}
	return out
}

func indentShell(script string, prefix string) string {
	if script == "" {
		return ""
	}
	lines := strings.SplitAfter(script, "\n")
	var b strings.Builder
	for _, line := range lines {
		if line == "" {
			continue
		}
		b.WriteString(prefix)
		b.WriteString(line)
	}
	return b.String()
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}
