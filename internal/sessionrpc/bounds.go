package sessionrpc

// MaxRuntimeCommandGRPCMessageBytes bounds one gRPC message on the Runtime Pod
// command channel (the Bridge dials the Pod to deliver a runtime input
// command). It is a fuse sized above the largest admission-legal payload plus
// protobuf/envelope overhead, never a gate below legal traffic: the command
// payload carrier is separately fused at 2 MiB (a 1 MiB admission-legal text
// block plus JSON envelope headroom), and this 4 MiB value clears that 2 MiB
// payload fuse with protobuf headroom.
// UPDATE-WITH: internal/internalgrpc/session_rpc_options.go;
// services/bridge/runtime_delivery.go;
// services/agent-runtime/packages/runtime-pod/src/bounds.ts;
// integration/static/runtime_transport_bounds_test.go;
// internal/sessionrpc/bounds_test.go
const (
	MaxInboundGRPCMessageBytes        = 64 * 1024
	MaxOutboundGRPCMessageBytes       = 64 * 1024
	MaxRuntimeCommandGRPCMessageBytes = 4 * 1024 * 1024
	MaxQueueLeaseGRPCMessageBytes     = 4 * 1024 * 1024
	// The MCP connector server admits a 10 MiB media result plus 1 MiB of
	// protobuf/envelope overhead; ListMcpTools shares that service channel.
	MaxMCPConnectorGRPCMessageBytes = 11 * 1024 * 1024
	// A helper task result can carry two independently bounded 1 MiB process
	// streams plus its JSON envelope.
	MaxTaskResultGRPCMessageBytes = 4 * 1024 * 1024
	MaxAttachmentGRPCMessageBytes = 32 * 1024 * 1024
)
