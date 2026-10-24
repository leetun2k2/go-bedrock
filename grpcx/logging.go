package grpcx

import (
	"context"
	"sync/atomic"

	"github.com/leetun2k2/go-bedrock/logx"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

type canonicalRPCKey struct{}

type canonicalRPC struct {
	request  *logx.Request
	method   string
	stream   atomic.Bool
	panicked atomic.Bool
	trailer  atomic.Bool
}

type canonicalHandler struct {
	logger *logx.Logger
}

func (h *canonicalHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	ctx, request := logx.StartRequest(ctx)
	rpc := &canonicalRPC{request: request, method: info.FullMethodName}
	return context.WithValue(ctx, canonicalRPCKey{}, rpc)
}

func (h *canonicalHandler) HandleRPC(ctx context.Context, event stats.RPCStats) {
	rpc, ok := ctx.Value(canonicalRPCKey{}).(*canonicalRPC)
	if !ok {
		return
	}
	switch event := event.(type) {
	case *stats.Begin:
		rpc.stream.Store(event.IsClientStream || event.IsServerStream)
	case *stats.OutTrailer:
		rpc.trailer.Store(true)
	case *stats.End:
		if rpc.stream.Load() {
			return
		}
		code := status.Code(event.Error)
		fields := []any{"protocol", "grpc", "rpc_method", rpc.method}
		if event.Error == nil && !rpc.trailer.Load() {
			// An unwinding panic may reach End without a returned error or a
			// final status. Do not report a successful RPC in that case.
			code = codes.Unknown
			fields = append(fields, "error", "incomplete")
		}
		if event.Error != nil {
			// Status messages may contain credentials or request data. Keep only
			// the code; applications may add sanitized detail with AddFields.
			fields = append(fields, "error", code.String())
		}
		panicked := rpc.panicked.Load()
		if panicked {
			fields = append(fields, "error", "panic")
		}
		fields = append(fields, "grpc_code", code.String())
		level := grpcLogLevel(code)
		if panicked {
			level = zapcore.ErrorLevel
		}
		rpc.request.Complete(h.logger, level, fields...)
	}
}

func (h *canonicalHandler) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (h *canonicalHandler) HandleConn(context.Context, stats.ConnStats) {}

func (h *canonicalHandler) intercept(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	rpc, _ := ctx.Value(canonicalRPCKey{}).(*canonicalRPC)
	defer markPanic(rpc)
	logx.EnrichContext(ctx)
	return handler(ctx, req)
}

func markPanic(rpc *canonicalRPC) {
	value := recover()
	if value == nil {
		return
	}
	if rpc != nil {
		rpc.panicked.Store(true)
	}
	panic(value)
}

func grpcLogLevel(code codes.Code) zapcore.Level {
	switch code {
	case codes.OK:
		return zapcore.InfoLevel
	case codes.Internal, codes.Unknown, codes.DataLoss, codes.Unavailable:
		return zapcore.ErrorLevel
	default:
		return zapcore.WarnLevel
	}
}
