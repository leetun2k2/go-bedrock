package restapix

import (
	"bufio"
	"context"
	"net"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/leetun2k2/go-bedrock/logx"
	"go.uber.org/zap/zapcore"
)

const canonicalPanicKey = "restapix.canonical_panic"

type canonicalHTTPKey struct{}

type canonicalHTTP struct {
	route    string
	panicked bool
}

// ServeHTTP serves the Gin router, including canonical summaries when enabled.
// Use this handler rather than Engine().ServeHTTP to include router redirects.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !s.cfg.CanonicalLogging {
		s.engine.ServeHTTP(w, r)
		return
	}
	ctx, request := logx.StartRequest(r.Context())
	state := &canonicalHTTP{route: "unmatched"}
	ctx = context.WithValue(ctx, canonicalHTTPKey{}, state)
	writer := &canonicalWriter{ResponseWriter: w, status: http.StatusOK}
	defer completeRequest(state, request, s.logger, r.Method, writer)
	s.engine.ServeHTTP(writer, r.WithContext(ctx))
}

func canonicalMiddleware(c *gin.Context) {
	state, ok := c.Request.Context().Value(canonicalHTTPKey{}).(*canonicalHTTP)
	if ok {
		defer state.capture(c)
	}
	c.Next()
}

func (s *canonicalHTTP) capture(c *gin.Context) {
	if route := c.FullPath(); route != "" {
		s.route = route
	}
	s.panicked = c.GetBool(canonicalPanicKey)
}

func completeRequest(state *canonicalHTTP, request *logx.Request, logger *logx.Logger, method string, writer *canonicalWriter) {
	status := writer.status
	level := zapcore.InfoLevel
	switch {
	case state.panicked:
		level = zapcore.ErrorLevel
	case status >= http.StatusInternalServerError:
		level = zapcore.ErrorLevel
	case status >= http.StatusBadRequest:
		level = zapcore.WarnLevel
	}
	request.Complete(logger, level,
		"protocol", "http",
		"method", method,
		"route", state.route,
		"status_code", status,
	)
}

// canonicalWriter observes final status while preserving Gin's optional writer
// operations and ResponseController support for deadlines and full duplex.
type canonicalWriter struct {
	http.ResponseWriter
	status  int
	written bool
}

func (w *canonicalWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (w *canonicalWriter) WriteHeader(code int) {
	if w.written {
		return
	}
	w.ResponseWriter.WriteHeader(code)
	finalStatus := code >= http.StatusOK
	switchingProtocols := code == http.StatusSwitchingProtocols
	if finalStatus || switchingProtocols {
		w.status = code
		w.written = true
	}
}

func (w *canonicalWriter) Write(data []byte) (int, error) {
	if !w.written {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(data)
}

func (w *canonicalWriter) Flush() {
	_ = w.FlushError()
}

func (w *canonicalWriter) FlushError() error {
	if !w.written {
		w.WriteHeader(http.StatusOK)
	}
	return http.NewResponseController(w.ResponseWriter).Flush()
}

func (w *canonicalWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return http.NewResponseController(w.ResponseWriter).Hijack()
}

func (w *canonicalWriter) CloseNotify() <-chan bool {
	return w.ResponseWriter.(http.CloseNotifier).CloseNotify()
}

func (w *canonicalWriter) Push(target string, options *http.PushOptions) error {
	pusher, ok := w.ResponseWriter.(http.Pusher)
	if !ok {
		return http.ErrNotSupported
	}
	return pusher.Push(target, options)
}
