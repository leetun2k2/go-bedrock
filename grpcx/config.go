package grpcx

// defaultAddr is the listen address used when Config.Addr is empty.
const defaultAddr = ":6000"

// Config configures the grpcx Server.
//
// Addr is optional: an empty value falls back to defaultAddr.
type Config struct {
	Addr string `json:"addr" yaml:"addr"`

	// CanonicalLogging emits unsampled unary RPC summaries. Disabled by default.
	CanonicalLogging bool `json:"canonicalLogging" yaml:"canonicalLogging"`
}

func (c Config) addr() string {
	if c.Addr == "" {
		return defaultAddr
	}

	return c.Addr
}
