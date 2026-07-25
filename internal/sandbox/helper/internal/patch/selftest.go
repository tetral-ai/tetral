package patch

import (
	"fmt"

	"github.com/tetral-ai/tetral/internal/sandbox/helper/internal/pathsafe"
)

const cannedPatch = `*** Begin Patch
*** Add File: added.txt
+created
*** Update File: target.txt
@@
-old
+new
*** Delete File: delete.txt
*** Update File: move.txt
*** Move to: moved.txt
@@
-before
+after
*** End Patch
`

func SelfTest() error {
	ops, err := parsePatch(cannedPatch)
	if err != nil {
		return err
	}
	files := map[string]string{
		"target.txt": "old\n",
		"delete.txt": "bye\n",
		"move.txt":   "before\n",
	}
	for _, op := range ops {
		switch op.kind {
		case opAdd:
			if _, ok := files[op.path]; ok {
				return fmt.Errorf("self-test add target already exists: %s", op.path)
			}
			files[op.path] = string(addFileBytes(op.lines))
		case opDelete:
			if _, ok := files[op.path]; !ok {
				return fmt.Errorf("self-test delete target is missing: %s", op.path)
			}
			delete(files, op.path)
		case opUpdate:
			content, ok := files[op.path]
			if !ok {
				return fmt.Errorf("self-test update target is missing: %s", op.path)
			}
			next, toolErr := applyUpdate(pathsafe.Resolved{DisplayPath: op.path}, content, op.chunks)
			if toolErr != nil {
				return fmt.Errorf("%s: %s", toolErr.Kind, toolErr.Message)
			}
			if op.moveTo != nil {
				if _, ok := files[*op.moveTo]; ok {
					return fmt.Errorf("self-test move destination already exists: %s", *op.moveTo)
				}
				delete(files, op.path)
				files[*op.moveTo] = next
			} else {
				files[op.path] = next
			}
		}
	}
	for name, want := range map[string]string{
		"added.txt":  "created\n",
		"target.txt": "new\n",
		"moved.txt":  "after\n",
	} {
		if files[name] != want {
			return fmt.Errorf("%s = %q; want %q", name, files[name], want)
		}
	}
	if _, ok := files["delete.txt"]; ok {
		return fmt.Errorf("delete.txt survived self-test dry run")
	}
	if _, ok := files["move.txt"]; ok {
		return fmt.Errorf("move.txt survived self-test dry run")
	}
	return nil
}
