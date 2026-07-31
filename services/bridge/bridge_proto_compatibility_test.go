package agentruntimebridge

import (
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"

	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func TestResolveInterAgentDeliveryResponsePreservesRetiredWireFields(t *testing.T) {
	descriptor := (&bridgev1.ResolveInterAgentDeliveryResponse{}).ProtoReflect().Descriptor()

	for _, number := range []protoreflect.FieldNumber{2, 3, 4, 5} {
		if !descriptor.ReservedRanges().Has(number) {
			t.Errorf("field number %d is not reserved", number)
		}
	}
	for _, name := range []protoreflect.Name{
		"sent_exists",
		"received_exists",
		"child_receivable",
		"child_thread_json",
	} {
		if !descriptor.ReservedNames().Has(name) {
			t.Errorf("field name %q is not reserved", name)
		}
	}

	wantNumbers := map[protoreflect.Name]protoreflect.FieldNumber{
		"ack":                      1,
		"delivery_id":              6,
		"source_thread_id":         7,
		"target_thread_id":         8,
		"source_tool_use_event_id": 9,
		"received_event_id":        10,
		"received_sequence":        11,
		"message_json":             12,
	}
	for name, want := range wantNumbers {
		field := descriptor.Fields().ByName(name)
		if field == nil {
			t.Errorf("field %q is missing", name)
			continue
		}
		if got := field.Number(); got != want {
			t.Errorf("field %q number = %d; want %d", name, got, want)
		}
	}
}
