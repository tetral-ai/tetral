package resourceprojection

import (
	"path"
	"strings"

	"github.com/tetral-ai/tetral/internal/sandbox/driver"
)

// writeVerifyChecks emits the runtime-user checks that prove each projected
// file is readable, regular, non-symlinked, and mounted read-only while its
// parent remains writable for ordinary sandbox work.
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
