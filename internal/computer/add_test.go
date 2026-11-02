package computer

import (
	"fmt"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/protocol"
)

func TestDriveExcludeLengthMatchesPreparationDecoder(t *testing.T) {
	for _, length := range []int{1024, 1025} {
		for _, directory := range []bool{false, true} {
			t.Run(fmt.Sprintf("bytes=%d/directory=%t", length, directory), func(t *testing.T) {
				// Every segment is valid independently of the complete-text limit.
				path := strings.Repeat(strings.Repeat("a", 254)+"/", 4)
				last := length - len(path)
				if directory {
					last--
				}
				path += strings.Repeat("b", last)
				if directory {
					path += "/"
				}
				exclude := []DriveExclude{{DriveID: "drive_test", Paths: []string{path}}}
				_, admissionErr := checkDriveExclude(exclude)
				payload := preparePayload("sesn_test", "bind_test", KindCloud, "/workspace", exclude, nil, nil)
				_, decoderErr := protocol.DecodeDownstream(protocol.Encode(protocol.Downstream{ConnectionID: "conn_test", MessageID: "msg_test", Payload: payload}))
				valid := length == 1024
				if (admissionErr == nil) != valid || (decoderErr == nil) != valid {
					t.Fatalf("admission=%v decoder=%v; want valid=%t", admissionErr, decoderErr, valid)
				}
			})
		}
	}
}
