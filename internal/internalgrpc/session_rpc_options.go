package internalgrpc

import (
	"time"

	"github.com/tetral-ai/tetral/internal/sessionrpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

const (
	internalServerMaxConnectionAge      = 5 * time.Minute
	internalServerMaxConnectionAgeGrace = 30 * time.Minute
	internalClientKeepaliveTime         = 30 * time.Second
	internalClientKeepaliveTimeout      = 10 * time.Second
)

func internalServerKeepaliveParameters() keepalive.ServerParameters {
	return keepalive.ServerParameters{
		MaxConnectionAge:      internalServerMaxConnectionAge,
		MaxConnectionAgeGrace: internalServerMaxConnectionAgeGrace,
	}
}

func internalClientKeepaliveParameters() keepalive.ClientParameters {
	return keepalive.ClientParameters{
		Time:                internalClientKeepaliveTime,
		Timeout:             internalClientKeepaliveTimeout,
		PermitWithoutStream: false,
	}
}

func internalServerKeepaliveEnforcementPolicy() keepalive.EnforcementPolicy {
	return keepalive.EnforcementPolicy{
		MinTime:             internalClientKeepaliveTime,
		PermitWithoutStream: false,
	}
}

func boundedInternalDialOptions(maxMessageBytes int) []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxMessageBytes),
			grpc.MaxCallSendMsgSize(maxMessageBytes),
		),
		grpc.WithKeepaliveParams(internalClientKeepaliveParameters()),
	}
}

func SessionRPCServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.MaxRecvMsgSize(sessionrpc.MaxInboundGRPCMessageBytes),
		grpc.MaxSendMsgSize(sessionrpc.MaxOutboundGRPCMessageBytes),
		grpc.KeepaliveParams(internalServerKeepaliveParameters()),
		grpc.KeepaliveEnforcementPolicy(internalServerKeepaliveEnforcementPolicy()),
	}
}

func SessionRPCDialOptions() []grpc.DialOption {
	return boundedInternalDialOptions(sessionrpc.MaxInboundGRPCMessageBytes)
}

func RuntimeCommandRPCDialOptions() []grpc.DialOption {
	return boundedInternalDialOptions(sessionrpc.MaxRuntimeCommandGRPCMessageBytes)
}

func MCPConnectorRPCDialOptions() []grpc.DialOption {
	return boundedInternalDialOptions(sessionrpc.MaxMCPConnectorGRPCMessageBytes)
}

func TaskResultRPCDialOptions() []grpc.DialOption {
	return boundedInternalDialOptions(sessionrpc.MaxTaskResultGRPCMessageBytes)
}

func QueueRPCServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.MaxRecvMsgSize(sessionrpc.MaxQueueLeaseGRPCMessageBytes),
		grpc.MaxSendMsgSize(sessionrpc.MaxQueueLeaseGRPCMessageBytes),
		grpc.KeepaliveParams(internalServerKeepaliveParameters()),
		grpc.KeepaliveEnforcementPolicy(internalServerKeepaliveEnforcementPolicy()),
	}
}

func QueueRPCDialOptions() []grpc.DialOption {
	return boundedInternalDialOptions(sessionrpc.MaxQueueLeaseGRPCMessageBytes)
}
