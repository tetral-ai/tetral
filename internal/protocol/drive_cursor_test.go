package protocol

import (
	"errors"
	"testing"
)

func TestChangesWithoutIndexMustStartAfterSequence(t *testing.T) {
	ctx := ListChangesResponseContext{Query: Some(ListChangesQuery{ConnectionID: "conn", BindingID: "bind", AfterSeq: 41})}
	for _, seq := range []int64{41, 42} {
		page := ListChangesResponse{Records: []ChangeRecord{{Seq: seq, Index: 0, Path: "file", Operation: "deleted", VersionID: "version", ContentSHA256: Null[string]()}}}
		_, err := DecodeListChangesResponse(Encode(page), ctx)
		if seq == 42 {
			if err != nil {
				t.Fatalf("greater-than control: %v", err)
			}
			continue
		}
		var failure *DecodeError
		if !errors.As(err, &failure) || failure.Pointer != "/records/0" || failure.Reason != "page not after its cursor" {
			t.Fatalf("equal-sequence page = %v; want cursor rejection", err)
		}
	}
}
