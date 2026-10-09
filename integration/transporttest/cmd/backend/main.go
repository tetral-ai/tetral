// This test-only backend exposes controlled local fixture probes, not product APIs.
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/interop/grpc_testing"
)

type sequenceService struct {
	grpc_testing.UnimplementedTestServiceServer
	streams sync.Map
}

func (s *sequenceService) StreamingOutputCall(request *grpc_testing.StreamingOutputCallRequest, stream grpc_testing.TestService_StreamingOutputCallServer) error {
	id := string(request.GetPayload().GetBody())
	if id == "" {
		return context.Canceled
	}
	gate := make(chan struct{}, 1)
	if _, exists := s.streams.LoadOrStore(id, gate); exists {
		return context.Canceled
	}
	defer s.streams.Delete(id)
	for sequence := int64(1); ; sequence++ {
		if err := stream.Send(&grpc_testing.StreamingOutputCallResponse{Payload: &grpc_testing.Payload{Body: strconv.AppendInt(nil, sequence, 10)}}); err != nil {
			return err
		}
		select {
		case <-gate:
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

func main() {
	var operations, active, cancelled atomic.Int64
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		operations.Add(1)
		active.Add(1)
		defer active.Add(-1)
		if message, ok := request.(*grpc_health_v1.HealthCheckRequest); ok && strings.HasPrefix(message.Service, "delay:") {
			duration, err := time.ParseDuration(strings.TrimPrefix(message.Service, "delay:"))
			if err != nil {
				return nil, err
			}
			timer := time.NewTimer(duration)
			defer timer.Stop()
			select {
			case <-timer.C:
				message.Service = ""
			case <-ctx.Done():
				cancelled.Add(1)
				return nil, ctx.Err()
			}
		}
		return handler(ctx, request)
	}), grpc.StreamInterceptor(func(service any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		operations.Add(1)
		active.Add(1)
		defer active.Add(-1)
		err := handler(service, stream)
		if stream.Context().Err() != nil {
			cancelled.Add(1)
		}
		return err
	}))
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(server, healthServer)
	sequences := &sequenceService{}
	grpc_testing.RegisterTestServiceServer(server, sequences)
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	// #nosec G102 -- isolated Docker fixture requires proxy access across its private network; host ports are loopback-only.
	listener, err := net.Listen("tcp", ":9090")
	if err != nil {
		slog.Error("fixture listener startup failed")
		os.Exit(1)
	}
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/release-response", func(w http.ResponseWriter, r *http.Request) {
			gate, ok := sequences.streams.Load(r.URL.Query().Get("id"))
			if !ok {
				http.Error(w, "unknown operation", 404)
				return
			}
			select {
			case gate.(chan struct{}) <- struct{}{}:
				w.WriteHeader(200)
			default:
				http.Error(w, "operation already released", http.StatusConflict)
			}
		})
		mux.HandleFunc("/state", func(w http.ResponseWriter, _ *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]int64{"operations": operations.Load(), "active": active.Load(), "cancelled": cancelled.Load()})
		})
		mux.HandleFunc("/operations", func(w http.ResponseWriter, _ *http.Request) { _ = json.NewEncoder(w).Encode(operations.Load()) })
		httpServer := &http.Server{Addr: ":8888", Handler: mux, ReadHeaderTimeout: time.Second}
		if err := httpServer.ListenAndServe(); err != nil {
			slog.Error("fixture control listener failed")
		}
	}()
	if err := server.Serve(listener); err != nil {
		slog.Error("fixture gRPC listener failed")
		os.Exit(1)
	}
}
