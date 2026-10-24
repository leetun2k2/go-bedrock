# logx

`logx` provides structured Zap logging with request and trace correlation taken from `context.Context`.

It selects Zap's development or production logger and adds a request ID plus valid OpenTelemetry trace and span IDs to every log call.

Use it when service logs must be structured and linked to request or distributed trace data.

Opt-in canonical request summaries collect trusted identity and business fields and emit once at completion, without sampling. Ordinary logging remains immediate.

See [USAGE.md](USAGE.md) for installation and a complete example.
