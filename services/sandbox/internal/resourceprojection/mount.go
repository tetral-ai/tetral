package resourceprojection

import (
	"context"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/sandbox/driver"
)

const (
	rcloneRemoteName  = "r2"
	RcloneStagingRoot = "/mnt/tetral/r2"
)

type MountBindVerifyCommandConfig struct {
	Bucket                  string
	RcloneVFSCacheMaxSize   string
	RcloneVFSMinFree        string
	ForceRemount            bool
	ReuseExistingCredential bool
}

func RcloneEnv(accountID string, credential Credential) map[string]string {
	// RCLONE_CONFIG_R2_NO_CHECK_BUCKET skips the HeadBucket probe (and is mirrored
	// by no_check_bucket in the generated rclone.conf): a prefix-scoped
	// object-read-only token cannot HeadBucket, so without skipping that probe
	// the mount fails to come up.
	return map[string]string{
		"RCLONE_CONFIG_R2_TYPE":              "s3",
		"RCLONE_CONFIG_R2_PROVIDER":          "Cloudflare",
		"RCLONE_CONFIG_R2_ENDPOINT":          "https://" + accountID + ".r2.cloudflarestorage.com",
		"RCLONE_CONFIG_R2_ACCESS_KEY_ID":     credential.AccessKeyID,
		"RCLONE_CONFIG_R2_SECRET_ACCESS_KEY": credential.SecretAccessKey,
		"RCLONE_CONFIG_R2_SESSION_TOKEN":     credential.SessionToken,
		"RCLONE_CONFIG_R2_NO_CHECK_BUCKET":   "true",
		"RCLONE_CONFIG_R2_ACL":               "private",
	}
}

func MountAliveCommand() string {
	var b strings.Builder
	b.WriteString("set -eu\n")
	b.WriteString("PATH=/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin\n")
	b.WriteString("STAGING=" + shellQuote(RcloneStagingRoot) + "\n")
	b.WriteString("mountpoint -q \"$STAGING\"\n")
	b.WriteString("timeout 5s ls \"$STAGING\" >/dev/null\n")
	return b.String()
}

func RunMountBindVerify(ctx context.Context, runner driver.PreparationCommandRunner, target driver.PreparationCommandTarget, plan Plan, config MountBindVerifyCommandConfig, env map[string]string, timeout time.Duration) error {
	return runner.RunPreparationCommand(ctx, target, MountBindVerifyCommand(plan, config), env, timeout)
}

func MountBindVerifyCommand(plan Plan, config MountBindVerifyCommandConfig) string {
	var b strings.Builder
	bindActions := ActionsOfType(plan.Actions, ActionBind)
	b.WriteString("set -eu\n")
	b.WriteString("PATH=/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin\n")
	b.WriteString("STAGING=" + shellQuote(RcloneStagingRoot) + "\n")
	b.WriteString("cleanup_on_error() {\n")
	b.WriteString("  set +e\n")
	for index, action := range bindActions {
		createdVariable := "TETRAL_BIND_CREATED_" + strconv.Itoa(index)
		b.WriteString("  if [ \"${" + createdVariable + ":-}\" = 1 ]; then\n")
		b.WriteString("    sudo umount -l -- " + shellQuote(action.MountPath) + " >/dev/null 2>&1 || true\n")
		b.WriteString("    sudo -u " + shellQuote(driver.RuntimeUser) + " rm -f -- " + shellQuote(action.MountPath) + " >/dev/null 2>&1 || true\n")
		b.WriteString("  fi\n")
	}
	if !config.ReuseExistingCredential {
		b.WriteString("  sudo umount -l -- \"$STAGING\" >/dev/null 2>&1 || true\n")
	}
	b.WriteString("}\n")
	b.WriteString("trap cleanup_on_error ERR\n")
	b.WriteString("install -d -m 0700 /tmp/tetral-runtime /tmp/tetral-runtime/rclone-cache\n")
	b.WriteString("RCLONE_CONFIG=/tmp/tetral-runtime/rclone.conf\n")
	if !config.ReuseExistingCredential {
		b.WriteString("umask 077\n")
		b.WriteString("cat > \"$RCLONE_CONFIG\" <<EOF\n")
		b.WriteString("[r2]\n")
		b.WriteString("type = s3\n")
		b.WriteString("provider = Cloudflare\n")
		b.WriteString("endpoint = ${RCLONE_CONFIG_R2_ENDPOINT}\n")
		b.WriteString("access_key_id = ${RCLONE_CONFIG_R2_ACCESS_KEY_ID}\n")
		b.WriteString("secret_access_key = ${RCLONE_CONFIG_R2_SECRET_ACCESS_KEY}\n")
		b.WriteString("session_token = ${RCLONE_CONFIG_R2_SESSION_TOKEN}\n")
		b.WriteString("no_check_bucket = true\n")
		b.WriteString("acl = private\n")
		b.WriteString("EOF\n")
		b.WriteString("chmod 0600 \"$RCLONE_CONFIG\"\n")
	}
	b.WriteString("install -d -m 0755 \"$STAGING\"\n")
	if config.ForceRemount {
		writeLiveRotationTeardown(&b, bindActions)
	}
	if config.ReuseExistingCredential {
		b.WriteString("mountpoint -q \"$STAGING\"\n")
		b.WriteString("timeout 5s ls \"$STAGING\" >/dev/null\n")
	} else {
		b.WriteString("if mountpoint -q \"$STAGING\"; then\n")
		b.WriteString("  if ! timeout 5s ls \"$STAGING\" >/dev/null 2>&1; then sudo umount -l -- \"$STAGING\"; fi\n")
		b.WriteString("fi\n")
		// --vfs-cache-mode full gives O_RDONLY opens a real local backing file
		// so mmap and seekable reads work; the cache is bounded by
		// --vfs-cache-max-size / --vfs-cache-min-free-space so one large read
		// cannot fill the disk and starve output capture. setsid + --daemon
		// plus redirecting the exec pipe to /dev/null make this synchronous
		// exec return in seconds and leave the daemon in its own process group,
		// out of reach of the exec-timeout group-kill. The log level stays INFO
		// (never --dump/DEBUG) so the session token never reaches logs.
		b.WriteString("if ! mountpoint -q \"$STAGING\"; then\n")
		b.WriteString("  setsid sudo rclone --config \"$RCLONE_CONFIG\" mount " + shellQuote(rcloneRemoteName+":"+config.Bucket+"/"+strings.TrimSuffix(plan.ResourcePrefix, "/")) + " \"$STAGING\" \\\n")
		b.WriteString("    --read-only --allow-other --vfs-cache-mode full \\\n")
		b.WriteString("    --vfs-cache-max-size " + shellQuote(config.RcloneVFSCacheMaxSize) + " \\\n")
		b.WriteString("    --vfs-cache-min-free-space " + shellQuote(config.RcloneVFSMinFree) + " \\\n")
		b.WriteString("    --vfs-cache-max-age 1h --cache-dir /tmp/tetral-runtime/rclone-cache \\\n")
		b.WriteString("    --dir-cache-time 1h --poll-interval 0 --log-level INFO \\\n")
		b.WriteString("    --daemon --daemon-wait 30s </dev/null >/dev/null 2>&1\n")
		b.WriteString("fi\n")
	}
	// The per-target findmnt guard exists because mount --bind onto an
	// already-bound path stacks a second bind that a single umount cannot fully
	// pop, so an existing bind of the same source is left in place and a stale
	// one is popped first. The parent directory and the placeholder file are
	// created as the runtime user (sudo -u daytona) so the verify parent-write
	// check passes and the leaf bind does not change parent ownership; the bind
	// itself and the remount,ro run as root because they need CAP_SYS_ADMIN.
	for index, action := range bindActions {
		source := action.StagingPath
		target := action.MountPath
		parent := path.Dir(target)
		b.WriteString("if findmnt -rn --mountpoint " + shellQuote(target) + " >/dev/null 2>&1; then\n")
		b.WriteString("  if ! [ " + shellQuote(source) + " -ef " + shellQuote(target) + " ]; then sudo umount -l -- " + shellQuote(target) + "; fi\n")
		b.WriteString("fi\n")
		b.WriteString("if ! findmnt -rn --mountpoint " + shellQuote(target) + " >/dev/null 2>&1; then\n")
		b.WriteString(indentShell(EnsureParentCommand(parent), "  "))
		b.WriteString("  if [ -L " + shellQuote(target) + " ]; then sudo -u " + shellQuote(driver.RuntimeUser) + " rm -f -- " + shellQuote(target) + "; fi\n")
		b.WriteString("  if [ -e " + shellQuote(target) + " ] && [ ! -f " + shellQuote(target) + " ]; then echo 'resource projection target is not a regular file' >&2; false; fi\n")
		b.WriteString("  sudo -u " + shellQuote(driver.RuntimeUser) + " touch -- " + shellQuote(target) + "\n")
		b.WriteString("  sudo mount --bind " + shellQuote(source) + " " + shellQuote(target) + "\n")
		b.WriteString("  TETRAL_BIND_CREATED_" + strconv.Itoa(index) + "=1\n")
		b.WriteString("  sudo mount -o remount,bind,ro " + shellQuote(target) + "\n")
		b.WriteString("fi\n")
	}
	writeVerifyChecks(&b, ActionsOfType(plan.Actions, ActionVerify))
	b.WriteString("trap - ERR\n")
	return b.String()
}
