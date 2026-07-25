package resourceprojection

import "strings"

// writeLiveRotationTeardown unmounts every per-file bind before lazy-unmounting
// the staging mount, and that order is mandatory: a bind is a point-in-time
// clone that does not follow an unmount/remount of its source, so remounting
// the staging mount alone would leave the binds pointing at the old detached
// mount (EIO). This ordering is what distinguishes live-rotation (mount and
// binds still present, torn down here) from cold-return (mount and binds
// already gone).
//
// UPDATE-WITH: mount.go (MountBindVerifyCommand, which invokes this on
// ForceRemount before re-minting and re-mounting).
func writeLiveRotationTeardown(b *strings.Builder, bindActions []Action) {
	for _, action := range bindActions {
		b.WriteString("if findmnt -rn --mountpoint " + shellQuote(action.MountPath) + " >/dev/null 2>&1; then sudo umount -l -- " + shellQuote(action.MountPath) + "; fi\n")
	}
	b.WriteString("if mountpoint -q \"$STAGING\"; then sudo umount -l -- \"$STAGING\"; fi\n")
}
