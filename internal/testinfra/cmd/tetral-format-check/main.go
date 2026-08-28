package main

import (
	"bytes"
	"fmt"
	"go/format"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func main() {
	var unformatted []string
	err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "node_modules", "dist", ".test-results":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(entry.Name(), ".go") {
			return nil
		}
		// WalkDir supplies repository-local paths from the closed traversal above.
		//nolint:gosec
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		formatted, err := format.Source(body)
		if err != nil {
			return fmt.Errorf("format %s: %w", path, err)
		}
		if !bytes.Equal(body, formatted) {
			unformatted = append(unformatted, filepath.ToSlash(path))
		}
		return nil
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if len(unformatted) == 0 {
		return
	}
	sort.Strings(unformatted)
	for _, path := range unformatted {
		fmt.Println(path)
	}
	os.Exit(1)
}
