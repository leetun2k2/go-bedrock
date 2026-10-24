package logx

import (
	"context"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Logger struct {
	logger    *zap.Logger
	sugar     *zap.SugaredLogger
	canonical *zap.Logger
}

// New creates a new Logger with the specified configuration.
func New(cfg *LoggerConfig) (*Logger, error) {
	cfgZap := zap.NewDevelopmentConfig()
	if cfg.Production {
		cfgZap = zap.NewProductionConfig()
	}

	sampling := cfgZap.Sampling
	cfgZap.Sampling = nil
	canonical, err := cfgZap.Build()
	if err != nil {
		return nil, err
	}

	logger := canonical
	if sampling != nil {
		wrap := func(core zapcore.Core) zapcore.Core {
			return zapcore.NewSamplerWithOptions(core, time.Second, sampling.Initial, sampling.Thereafter)
		}
		logger = canonical.WithOptions(zap.WrapCore(wrap))
	}

	sugar := logger.Sugar()

	return &Logger{
		logger:    logger,
		sugar:     sugar,
		canonical: canonical,
	}, nil
}

func (l *Logger) Debug(ctx context.Context, msg string, fields ...any) {
	l.sugar.With(ContextFields(ctx)...).Debugw(msg, fields...)
}

func (l *Logger) Info(ctx context.Context, msg string, fields ...any) {
	l.sugar.With(ContextFields(ctx)...).Infow(msg, fields...)
}

func (l *Logger) Warn(ctx context.Context, msg string, fields ...any) {
	l.sugar.With(ContextFields(ctx)...).Warnw(msg, fields...)
}

func (l *Logger) Error(ctx context.Context, msg string, fields ...any) {
	l.sugar.With(ContextFields(ctx)...).Errorw(msg, fields...)
}

func (l *Logger) Fatal(ctx context.Context, msg string, fields ...any) {
	l.sugar.With(ContextFields(ctx)...).Fatalw(msg, fields...)
}

func (l *Logger) Panic(ctx context.Context, msg string, fields ...any) {
	l.sugar.With(ContextFields(ctx)...).Panicw(msg, fields...)
}
