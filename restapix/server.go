// Package restapix adapts a Gin-routed *http.Server into a
// serverx.Component.
package restapix

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/leetun2k2/go-bedrock/logx"
	"github.com/leetun2k2/go-bedrock/serverx"
)

// Server wraps a *http.Server, routed by a *gin.Engine, and adapts it to
// serverx.Component.
type Server struct {
	cfg        Config
	logger     *logx.Logger
	engine     *gin.Engine
	httpServer *http.Server
}

// New constructs the underlying Gin engine and http.Server. It does not
// start listening yet — callers obtain the *gin.Engine via Engine and
// register routes, route groups, and global middleware before calling
// Start. New returns an error instead of a server if cfg is invalid.
func New(cfg Config, logger *logx.Logger) (*Server, error) {
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	if cfg.Production {
		gin.SetMode(gin.ReleaseMode)
	} else {
		gin.SetMode(gin.DebugMode)
	}

	engine := gin.New()
	if cfg.CanonicalLogging {
		engine.Use(canonicalMiddleware)
	}
	engine.Use(recoveryMiddleware(logger))
	engine.Use(sameSiteLaxMiddleware())

	if len(cfg.CORSAllowOrigins) > 0 {
		engine.Use(corsMiddleware(cfg.CORSAllowOrigins))
	}

	if err := engine.SetTrustedProxies(cfg.TrustedProxies); err != nil {
		return nil, fmt.Errorf("restapix: trusted proxies: %w", err)
	}

	server := &Server{
		cfg:    cfg,
		logger: logger,
		engine: engine,
		httpServer: &http.Server{
			Addr:              cfg.addr(),
			ReadHeaderTimeout: cfg.ReadHeaderTimeout,
			ReadTimeout:       cfg.ReadTimeout,
			WriteTimeout:      cfg.WriteTimeout,
			IdleTimeout:       cfg.IdleTimeout,
		},
	}
	server.httpServer.Handler = server
	return server, nil
}

// recoveryMiddleware recovers panics raised by downstream handlers, logs
// them through logx, and responds 500 instead of letting the panic
// terminate the process.
func recoveryMiddleware(logger *logx.Logger) gin.HandlerFunc {
	return gin.CustomRecoveryWithWriter(io.Discard, func(c *gin.Context, recovered any) {
		logx.AddFields(c.Request.Context(), "error", "panic")
		c.Set(canonicalPanicKey, true)
		logger.Error(c.Request.Context(), "restapix: panic recovered",
			"error", recovered, "path", c.Request.URL.Path)
		c.AbortWithStatus(http.StatusInternalServerError)
	})
}

// sameSiteLaxMiddleware sets the default SameSite mode applied to cookies
// set via gin.Context.SetCookie during the request, mitigating CSRF while
// still allowing top-level navigations (e.g. following a link) to carry the
// cookie.
func sameSiteLaxMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.SetSameSite(http.SameSiteLaxMode)
		c.Next()
	}
}

// corsMiddleware allows cross-origin requests from origins, with credentials
// (cookies, Authorization header) included.
func corsMiddleware(origins []string) gin.HandlerFunc {
	cfg := cors.DefaultConfig()
	cfg.AllowOrigins = origins
	cfg.AllowCredentials = true
	cfg.AllowHeaders = append(cfg.AllowHeaders, "Authorization")

	return cors.New(cfg)
}

// Engine exposes the underlying *gin.Engine so callers can register routes,
// route groups, and global middleware before calling Start.
func (s *Server) Engine() *gin.Engine {
	return s.engine
}

// Component adapts the Server to serverx.Component.
func (s *Server) Component() serverx.Component {
	return serverx.Component{
		Name:  "restapix",
		Start: s.Start,
		Stop:  s.Stop,
	}
}

// Start binds a TCP listener on cfg.Addr (default :5000) and blocks serving
// until ctx is canceled or the server fails.
func (s *Server) Start(ctx context.Context) error {
	addr := s.cfg.addr()

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		s.logger.Error(ctx, "restapix: failed to listen", "addr", addr, "error", err)
		return err
	}

	s.logger.Info(ctx, "restapix: listening", "addr", addr)

	if err := s.httpServer.Serve(lis); err != nil && err != http.ErrServerClosed {
		s.logger.Error(ctx, "restapix: serve failed", "addr", addr, "error", err)
		return err
	}

	return nil
}

// Stop gracefully shuts down the HTTP server, bounded by ctx's deadline.
func (s *Server) Stop(ctx context.Context) error {
	if err := s.httpServer.Shutdown(ctx); err != nil {
		s.logger.Error(ctx, "restapix: shutdown failed", "error", err)
		return err
	}

	return nil
}
