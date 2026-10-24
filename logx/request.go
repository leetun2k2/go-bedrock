package logx

import (
	"context"
	"crypto/rand"
	"sync"
	"time"

	"github.com/leetun2k2/go-bedrock/contextx"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type requestKey struct{}

// Request collects facts for one canonical summary. Do not copy it.
type Request struct {
	mu     sync.Mutex
	fields map[string]any
	start  time.Time
	done   bool
}

// StartRequest creates an isolated collector and supplies a request ID if absent.
// Pass the returned context to downstream handlers and goroutines.
func StartRequest(ctx context.Context) (context.Context, *Request) {
	if ctx == nil {
		ctx = context.Background()
	}
	if contextx.GetRequestID(ctx) == "" {
		ctx = contextx.SetRequestID(ctx, rand.Text())
	}

	r := &Request{
		fields: make(map[string]any),
		start:  time.Now(),
	}
	correlation := ContextFields(ctx)
	for i := 0; i < len(correlation); i += 2 {
		r.fields[correlation[i].(string)] = correlation[i+1]
	}
	ctx = context.WithValue(ctx, requestKey{}, r)
	EnrichContext(ctx)
	return ctx, r
}

// AddFields adds explicit, safe request facts. Reserved keys and invalid pairs
// are ignored. Repeated keys use the last value; empty identity IDs remove them.
// Values must remain immutable until completion. Calls after completion do nothing.
func AddFields(ctx context.Context, fields ...any) {
	r := requestFromContext(ctx)
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.done {
		return
	}

	for i := 0; i+1 < len(fields); i += 2 {
		key, ok := fields[i].(string)
		if !ok || key == "" {
			continue
		}
		if reservedField(key) {
			continue
		}
		value := fields[i+1]
		switch key {
		case "user_id", "session_id", "identity_id":
			id, ok := value.(string)
			if !ok {
				continue
			}
			if id == "" {
				delete(r.fields, key)
				continue
			}
		}
		r.fields[key] = value
	}
}

// EnrichContext copies trusted identity IDs and the active OpenTelemetry span
// into the collector. Call it after authentication or tracing derives a context.
// Missing IDs leave previously supplied values intact.
func EnrichContext(ctx context.Context) {
	r := requestFromContext(ctx)
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.done {
		return
	}
	for _, key := range []string{contextx.KeyUserID, contextx.KeySessionID, contextx.KeyIdentityID} {
		id := contextx.GetString(ctx, key)
		if id != "" {
			r.fields[key] = id
		}
	}
	span := trace.SpanContextFromContext(ctx)
	if span.IsValid() {
		r.fields["trace_id"] = span.TraceID().String()
		r.fields["span_id"] = span.SpanID().String()
		delete(r.fields, "trace_sampled")
		if span.IsSampled() {
			r.fields["trace_sampled"] = true
		}
	}
}

// Complete emits at most one unsampled summary. Completion fields are trusted
// transport facts; business facts belong in AddFields. Only info/warn/error
// levels are used. Logging failures never escape into request handling.
func (r *Request) Complete(logger *Logger, level zapcore.Level, fields ...any) {
	defer recoverLogging()
	r.mu.Lock()
	if r.done {
		r.mu.Unlock()
		return
	}
	r.done = true
	for i := 0; i+1 < len(fields); i += 2 {
		key, ok := fields[i].(string)
		if !ok {
			continue
		}
		if completionField(key) {
			r.fields[key] = fields[i+1]
		}
	}
	r.fields["event"] = "request_completed"
	r.fields["duration_ms"] = float64(time.Since(r.start)) / float64(time.Millisecond)
	data := r.fields
	r.fields = nil
	r.mu.Unlock()
	output := make([]zap.Field, 0, len(data))
	for key, value := range data {
		output = append(output, zap.Any(key, value))
	}

	switch level {
	case zapcore.WarnLevel:
		logger.canonical.Warn("request completed", output...)
	case zapcore.ErrorLevel:
		logger.canonical.Error("request completed", output...)
	default:
		logger.canonical.Info("request completed", output...)
	}
}

func requestFromContext(ctx context.Context) *Request {
	if ctx == nil {
		return nil
	}
	r, _ := ctx.Value(requestKey{}).(*Request)
	return r
}

func reservedField(key string) bool {
	switch key {
	case "event", "duration_ms", "request_id", "trace_id", "span_id", "trace_sampled",
		"protocol", "method", "route", "status_code", "rpc_method", "grpc_code",
		"level", "ts", "msg", "caller", "stacktrace", "logger",
		"L", "T", "M", "C", "S", "N":
		return true
	default:
		return false
	}
}

func completionField(key string) bool {
	switch key {
	case "protocol", "method", "route", "status_code", "rpc_method", "grpc_code", "error":
		return true
	default:
		return false
	}
}

func recoverLogging() {
	_ = recover()
}
