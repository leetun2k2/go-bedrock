# Using grpcx

## Install

```sh
go get github.com/leetun2k2/go-bedrock/grpcx@latest
```

## Configure

`Config.Addr` is the TCP listen address. It defaults to `:6000`.

Pass gRPC server options, such as credentials and interceptors, after the logger in `New`.

Set `Config.CanonicalLogging` to `true` to emit one unsampled summary per unary RPC. It is disabled by default; streaming RPCs are excluded. Existing unary interceptors and stats handlers still compose. The summary uses gRPC's completion status, including errors returned by outer authentication/recovery interceptors. Escaping handler panics still propagate.

After authentication or starting a tracing span, call `logx.EnrichContext(ctx)`. Use `logx.AddFields(ctx, "order_id", "order-456")` for safe business fields; see [logx usage](../logx/USAGE.md#canonical-request-summaries).

## Integrate

```go
package main

import (
	"context"
	"log"

	"github.com/leetun2k2/go-bedrock/grpcx"
	"github.com/leetun2k2/go-bedrock/logx"
	"github.com/leetun2k2/go-bedrock/serverx"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func main() {
	logger, err := logx.New(&logx.LoggerConfig{Production: true})
	if err != nil {
		log.Fatal(err)
	}

	grpcServer := grpcx.New(grpcx.Config{Addr: ":6000"}, logger)
	healthpb.RegisterHealthServer(grpcServer.GRPCServer(), health.NewServer())

	supervisor := serverx.New(serverx.Config{Name: "orders"}, logger)
	if err := supervisor.Register(grpcServer.Component()); err != nil {
		log.Fatal(err)
	}

	if err := supervisor.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
```

Replace the health service registration with the generated `RegisterXxxServer` calls for the application.

## Lifecycle

```mermaid
flowchart LR
    A[Create] --> B[Register services]
    B --> C[Register component]
    C --> D[Run supervisor]
    D --> E[Graceful stop]
```

When the shutdown deadline expires, `Stop` force-closes remaining connections and returns the context error.

## Avoid

- Do not register services after the server starts.
- Do not call `Start` directly when `serverx` owns the lifecycle.
- Do not omit transport credentials on an untrusted network.
- Do not pass a nil logger.
