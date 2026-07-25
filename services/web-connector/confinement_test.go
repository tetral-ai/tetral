package webconnector

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestConnectorProductionSourceRecursivelyUsesOnlyBlobForStorage(t *testing.T) {
	t.Parallel()
	const blobImport = "github.com/tetral-ai/tetral/internal/blob"
	const awsS3Import = "github.com/aws/" + "aws-sdk-go-v2/service/s3"
	inspected := map[string]bool{}
	sawBlob := false
	err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			return nil
		}
		relative := filepath.ToSlash(filepath.Clean(path))
		relative = strings.TrimPrefix(relative, "./")
		inspected[relative] = true
		parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, item := range parsed.Imports {
			importPath, err := strconv.Unquote(item.Path.Value)
			if err != nil {
				return err
			}
			if importPath == blobImport {
				sawBlob = true
				continue
			}
			for _, forbidden := range []string{
				"database/sql",
				"github.com/jackc/pgx",
				"github.com/lib/pq",
				"github.com/go-sql-driver/",
				"github.com/redis/",
				"github.com/valkey-io/",
				awsS3Import,
				"github.com/minio/minio-go",
				"cloud.google.com/go/storage",
				"github.com/Azure/azure-sdk-for-go/sdk/storage",
				"go.mongodb.org/",
				"gorm.io/",
				"modernc.org/sqlite",
				"github.com/tetral-ai/tetral/internal/dbconnect",
				"github.com/tetral-ai/tetral/internal/storage",
				"github.com/tetral-ai/tetral/services/event-stream",
				"github.com/tetral-ai/tetral/services/bridge",
			} {
				if strings.HasPrefix(importPath, forbidden) {
					t.Errorf("%s imports disallowed storage or service dependency %s", relative, importPath)
					break
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !inspected["cmd/web-connector/main.go"] {
		t.Fatal("nested connector command was not inspected")
	}
	if !sawBlob {
		t.Fatal("connector production source does not import the blob boundary")
	}
}
