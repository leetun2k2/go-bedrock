package restapix

import (
	"fmt"
	"time"
)

// defaultAddr is the listen address used when Config.Addr is empty.
const defaultAddr = ":5000"

const (
	defaultReadHeaderTimeout = 5 * time.Second
	defaultIdleTimeout       = 60 * time.Second
)

// Config configures the restapix Server.
//
// Addr is optional: an empty value falls back to defaultAddr.
// ReadHeaderTimeout and IdleTimeout are optional: a zero value falls back to
// a sane default guarding against slow-client resource exhaustion.
// ReadTimeout and WriteTimeout are opt-in whole-request deadlines, left
// disabled (zero) by default since a global deadline can break uploads,
// downloads, SSE, and other long requests; set them explicitly where a hard
// per-request deadline is safe.
//
// TrustedProxies lists the proxy IPs/CIDRs allowed to set client-IP
// forwarding headers (X-Forwarded-For, X-Real-IP). An empty value trusts no
// proxy: the reported client IP is always the direct remote address.
//
// CORSAllowOrigins lists the origins allowed to make cross-origin requests.
// An empty value disables CORS entirely — no CORS middleware is installed.
type Config struct {
	Production bool   `json:"production" yaml:"production"`
	Addr       string `json:"addr" yaml:"addr"`

	// CanonicalLogging emits one unsampled summary per request. Disabled by default.
	CanonicalLogging bool `json:"canonicalLogging" yaml:"canonicalLogging"`

	ReadHeaderTimeout time.Duration `json:"readHeaderTimeout" yaml:"readHeaderTimeout"`
	ReadTimeout       time.Duration `json:"readTimeout" yaml:"readTimeout"`
	WriteTimeout      time.Duration `json:"writeTimeout" yaml:"writeTimeout"`
	IdleTimeout       time.Duration `json:"idleTimeout" yaml:"idleTimeout"`

	TrustedProxies []string `json:"trustedProxies" yaml:"trustedProxies"`

	CORSAllowOrigins []string `json:"corsAllowOrigins" yaml:"corsAllowOrigins"`
}

func (c Config) addr() string {
	if c.Addr == "" {
		return defaultAddr
	}

	return c.Addr
}

func (c Config) withDefaults() Config {
	if c.ReadHeaderTimeout == 0 {
		c.ReadHeaderTimeout = defaultReadHeaderTimeout
	}

	if c.IdleTimeout == 0 {
		c.IdleTimeout = defaultIdleTimeout
	}

	return c
}

// validate reports whether c holds a usable configuration. It is called
// after withDefaults, so a zero ReadTimeout/WriteTimeout here means
// "disabled", not "unset".
func (c Config) validate() error {
	if c.ReadHeaderTimeout < 0 {
		return fmt.Errorf("restapix: read header timeout must not be negative: %s", c.ReadHeaderTimeout)
	}

	if c.ReadTimeout < 0 {
		return fmt.Errorf("restapix: read timeout must not be negative: %s", c.ReadTimeout)
	}

	if c.WriteTimeout < 0 {
		return fmt.Errorf("restapix: write timeout must not be negative: %s", c.WriteTimeout)
	}

	if c.IdleTimeout < 0 {
		return fmt.Errorf("restapix: idle timeout must not be negative: %s", c.IdleTimeout)
	}

	return nil
}
