package agentruntimebridge

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCurrentRequestMessageSelectionSharedLiteralCases(t *testing.T) {
	data, err := os.ReadFile("../agent-runtime/packages/protocol/testdata/current-request-selection.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Name     string                       `json:"name"`
		Events   []bridgeLoadContextTurnEvent `json:"events"`
		Messages []struct {
			MessageSequence int64   `json:"messageSequence"`
			ContextKind     string  `json:"contextKind"`
			ModelRequestID  *string `json:"modelRequestId"`
		} `json:"messages"`
		Expected *bridgeRuntimeCurrentRequestMessage `json:"expected"`
		Error    bool                                `json:"error"`
	}
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	if len(cases) != 13 {
		t.Fatalf("literal case count = %d; want 13", len(cases))
	}
	for _, test := range cases {
		t.Run(test.Name, func(t *testing.T) {
			messages := make([]bridgeLoadContextMessageDescriptor, 0, len(test.Messages))
			for _, m := range test.Messages {
				messages = append(messages, bridgeLoadContextMessageDescriptor{MessageSequence: m.MessageSequence, Kind: m.ContextKind, ModelRequestID: m.ModelRequestID})
			}
			actual, err := selectCurrentRequestMessage(bridgeLoadContextTurnFacts{Events: test.Events}, messages)
			if test.Error {
				if status.Code(err) != codes.FailedPrecondition {
					t.Fatalf("ambiguous association = %v/%v", actual, err)
				}
				return
			}
			if err != nil || !reflect.DeepEqual(actual, test.Expected) {
				t.Fatalf("association = %#v/%v; want %#v", actual, err, test.Expected)
			}
		})
	}
}
