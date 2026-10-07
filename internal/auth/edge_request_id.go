package auth

import (
	"context"
	"sync/atomic"
)

// The edge request ID is the request_id claim of a verified internal
// principal: the identifier the public edge generated for this request, which
// Auth Check records as request.id when it signs the principal. A backend keeps
// its own request ID for responses, error envelopes and request.id, and records
// this signed value separately so Check and backend records of one request
// join. It is diagnostic correlation only. Nothing authorizes on it, and an
// incoming X-Request-Id header never supplies it.

type edgeRequestIDContextKey struct{}

type edgeRequestIDHolder struct {
	value atomic.Pointer[string]
}

// WithEdgeRequestIDRecorder lets a request boundary that wraps principal
// verification read the edge request ID after the inner handler returns.
func WithEdgeRequestIDRecorder(ctx context.Context) context.Context {
	if _, ok := ctx.Value(edgeRequestIDContextKey{}).(*edgeRequestIDHolder); ok {
		return ctx
	}
	return context.WithValue(ctx, edgeRequestIDContextKey{}, &edgeRequestIDHolder{})
}

// WithVerifiedEdgeRequestID records the request_id claim of a principal whose
// signature, audience, lifetime and method/path binding were just verified.
func WithVerifiedEdgeRequestID(ctx context.Context, claims InternalPrincipalClaims) context.Context {
	if claims.RequestID == "" {
		return ctx
	}
	holder, ok := ctx.Value(edgeRequestIDContextKey{}).(*edgeRequestIDHolder)
	if !ok {
		holder = &edgeRequestIDHolder{}
		ctx = context.WithValue(ctx, edgeRequestIDContextKey{}, holder)
	}
	requestID := claims.RequestID
	holder.value.Store(&requestID)
	return ctx
}

// EdgeRequestIDFromContext returns the verified edge request ID, or "" when no
// principal carrying one was verified for this request.
func EdgeRequestIDFromContext(ctx context.Context) string {
	holder, ok := ctx.Value(edgeRequestIDContextKey{}).(*edgeRequestIDHolder)
	if !ok {
		return ""
	}
	if value := holder.value.Load(); value != nil {
		return *value
	}
	return ""
}
