// Package grpcx adapts a standard google.golang.org/grpc.Server into a
// serverx.Component.
package grpcx

import (
	"context"
	"net"

	"github.com/leetun2k2/go-bedrock/logx"
	"github.com/leetun2k2/go-bedrock/serverx"
	"google.golang.org/grpc"
)

// Server wraps a *grpc.Server and adapts it to serverx.Component.
type Server struct {
	cfg        Config
	logger     *logx.Logger
	grpcServer *grpc.Server
}

// New constructs the underlying grpc.Server. It does not start listening
// yet — callers obtain the *grpc.Server via GRPCServer and register their
// services before calling Start.
func New(cfg Config, logger *logx.Logger, opts ...grpc.ServerOption) *Server {
	if cfg.CanonicalLogging {
		logging := &canonicalHandler{logger: logger}
		loggingOpts := []grpc.ServerOption{
			grpc.StatsHandler(logging),
			grpc.ChainUnaryInterceptor(logging.intercept),
		}
		opts = append(loggingOpts, opts...)
	}
	return &Server{
		cfg:        cfg,
		logger:     logger,
		grpcServer: grpc.NewServer(opts...),
	}
}

// GRPCServer exposes the underlying *grpc.Server so callers can register
// their generated RegisterXxxServer implementations before calling Start.
func (s *Server) GRPCServer() *grpc.Server {
	return s.grpcServer
}

// Component adapts the Server to serverx.Component.
func (s *Server) Component() serverx.Component {
	return serverx.Component{
		Name:  "grpcx",
		Start: s.Start,
		Stop:  s.Stop,
	}
}

// Start binds a TCP listener on cfg.Addr (default :6000) and blocks serving
// until ctx is canceled or the server fails.
func (s *Server) Start(ctx context.Context) error {
	addr := s.cfg.addr()

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		s.logger.Error(ctx, "grpcx: failed to listen", "addr", addr, "error", err)
		return err
	}

	s.logger.Info(ctx, "grpcx: listening", "addr", addr)

	if err := s.grpcServer.Serve(lis); err != nil {
		s.logger.Error(ctx, "grpcx: serve failed", "addr", addr, "error", err)
		return err
	}

	return nil
}

// Stop gracefully stops the gRPC server, bounded by ctx's deadline. If the
// deadline passes before the graceful stop completes, it force-terminates
// remaining connections.
func (s *Server) Stop(ctx context.Context) error {
	done := make(chan struct{})

	go func() {
		s.grpcServer.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		s.logger.Warn(ctx, "grpcx: graceful stop deadline exceeded, forcing stop")
		s.grpcServer.Stop()
		return ctx.Err()
	}
}
