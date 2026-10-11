package pubsubx

// Config configures a pubsubx Client.
//
// Prefix namespaces every channel name so multiple applications can share
// one Redis database without colliding. Prefix is optional; when empty,
// channel names are used as-is.
type Config struct {
	Prefix string
}
