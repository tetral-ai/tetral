package sessionrpc

import (
	"testing"
)

func TestTransportFusesPinScopedMessageCapacity(t *testing.T) {
	if MaxInboundGRPCMessageBytes != 64*1024 || MaxOutboundGRPCMessageBytes != 64*1024 {
		t.Fatalf(
			"shared gRPC fuses = (%d, %d); want 64 KiB in both directions",
			MaxInboundGRPCMessageBytes,
			MaxOutboundGRPCMessageBytes,
		)
	}
	if MaxRuntimeCommandGRPCMessageBytes != 4*1024*1024 {
		t.Fatalf("runtime command gRPC fuse = %d; want 4 MiB", MaxRuntimeCommandGRPCMessageBytes)
	}
	if MaxQueueLeaseGRPCMessageBytes != 4*1024*1024 {
		t.Fatalf("queue Lease gRPC fuse = %d; want 4 MiB", MaxQueueLeaseGRPCMessageBytes)
	}
	if MaxAttachmentGRPCMessageBytes != 32*1024*1024 {
		t.Fatalf("Bridge gRPC fuse = %d; want 32 MiB", MaxAttachmentGRPCMessageBytes)
	}
	if MaxRuntimeCommandGRPCMessageBytes <= 2*1024*1024 {
		t.Fatalf("runtime command gRPC fuse = %d; must leave protobuf headroom above the 2 MiB payload fuse", MaxRuntimeCommandGRPCMessageBytes)
	}
}
