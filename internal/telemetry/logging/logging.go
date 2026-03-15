package logging

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/sahariardev/fluxGate/internal/config"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func Build(cfg *config.Config) (*zap.Logger, func() error, error) {
	enc := zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())

	if strings.ToLower(cfg.Logging.Format) == "console" {
		cfg := zap.NewProductionEncoderConfig()
		cfg.EncodeTime = zapcore.ISO8601TimeEncoder
		enc = zapcore.NewConsoleEncoder(cfg)
	}

	level := zapcore.InfoLevel

	switch strings.ToLower(cfg.Logging.Level) {
	case "debug":
		level = zapcore.DebugLevel
	case "info":
		level = zapcore.InfoLevel
	case "warn":
		level = zapcore.WarnLevel
	case "error":
		level = zapcore.ErrorLevel
	default:
		return nil, nil, fmt.Errorf("Invalid log level: %s", cfg.Logging.Level)

	}

	core := zapcore.NewCore(enc, zapcore.AddSync(os.Stdout), level)
	logger := zap.New(core, zap.AddCaller(), zap.AddCallerSkip(1))

	return logger, logger.Sync, nil
}

type ctxKey int

const (
	ctxLoggerKey ctxKey = iota
)

func WithLogger(ctx context.Context, logger *zap.Logger) context.Context {
	return context.WithValue(ctx, ctxLoggerKey, logger)
}

func FromContext(ctx context.Context, fallback *zap.Logger) *zap.Logger {
	if value := ctx.Value(ctxLoggerKey); value != nil {
		if logger, ok := value.(*zap.Logger); ok {
			return logger
		}
	}

	return fallback
}

func WithComponent(logger *zap.Logger, component string) *zap.Logger {
	if component == "" {
		return logger
	}

	return logger.With(zap.String("component", component))
}

func WithConID(logger *zap.Logger, conId string) *zap.Logger {
	if conId == "" {
		return logger
	}

	return logger.With(zap.String("conId", conId))
}
