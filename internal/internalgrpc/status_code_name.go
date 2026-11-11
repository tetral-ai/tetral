package internalgrpc

import "google.golang.org/grpc/codes"

var statusCodeNames = map[codes.Code]string{
	codes.OK:                 "ok",
	codes.Canceled:           "canceled",
	codes.Unknown:            "unknown",
	codes.InvalidArgument:    "invalid_argument",
	codes.DeadlineExceeded:   "deadline_exceeded",
	codes.NotFound:           "not_found",
	codes.AlreadyExists:      "already_exists",
	codes.PermissionDenied:   "permission_denied",
	codes.ResourceExhausted:  "resource_exhausted",
	codes.FailedPrecondition: "failed_precondition",
	codes.Aborted:            "aborted",
	codes.OutOfRange:         "out_of_range",
	codes.Unimplemented:      "unimplemented",
	codes.Internal:           "internal",
	codes.Unavailable:        "unavailable",
	codes.DataLoss:           "data_loss",
	codes.Unauthenticated:    "unauthenticated",
}

// StatusCodeName is the error.code of a gRPC status: its name in snake_case.
// A number outside the status codes gRPC defines is "unknown", as gRPC
// treats it.
func StatusCodeName(code codes.Code) string {
	if name, ok := statusCodeNames[code]; ok {
		return name
	}
	return "unknown"
}
