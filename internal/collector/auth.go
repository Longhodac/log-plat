package collector

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/Longhodac/log-plat/internal/apikey"
)

type serviceKey struct{}

// ServiceFromContext returns the service the stream authenticated as.
func ServiceFromContext(ctx context.Context) (string, bool) {
	s, ok := ctx.Value(serviceKey{}).(string)
	return s, ok
}

// StreamAuth rejects streams without a known x-api-key and attaches the
// key's service to the stream context.
func StreamAuth(keys *apikey.Store, header string) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		md, _ := metadata.FromIncomingContext(ss.Context())
		vals := md.Get(header)
		if len(vals) != 1 {
			authFailures.Inc()
			return status.Error(codes.Unauthenticated, "missing api key")
		}
		svc, ok := keys.Service(vals[0])
		if !ok {
			authFailures.Inc()
			return status.Error(codes.Unauthenticated, "unknown api key")
		}
		return handler(srv, &authedStream{ServerStream: ss, ctx: context.WithValue(ss.Context(), serviceKey{}, svc)})
	}
}

type authedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *authedStream) Context() context.Context { return s.ctx }
