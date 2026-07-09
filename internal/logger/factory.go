package logger

import (
	"fmt"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/vaishnav-sp/cluster-db/internal/config"
)

// New creates a structured logger from the supplied configuration.
// Call logger.Sync() during graceful shutdown to flush any buffered logs.
func New(cfg config.LoggingConfig) (*zap.Logger, error) {
	level, err := parseLevel(cfg.Level)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidLogLevel, err)
	}

	encoding, err := parseEncoding(cfg.Encoding)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidEncoding, err)
	}

	encoderCfg := zap.NewProductionEncoderConfig()
	if encoding == "console" {
		encoderCfg = zap.NewDevelopmentEncoderConfig()
	}

	var opts []zap.Option
	if cfg.Development {
		opts = append(opts, zap.Development())
	}

	loggerConfig := zap.Config{
		Level:            zap.NewAtomicLevelAt(level),
		Development:      cfg.Development,
		Encoding:         encoding,
		EncoderConfig:    encoderCfg,
		OutputPaths:      cfg.OutputPaths,
		ErrorOutputPaths: cfg.ErrorOutputPaths,
	}

	logger, err := loggerConfig.Build(opts...)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrLoggerInitialization, err)
	}

	return logger, nil
}

func parseLevel(level string) (zapcore.Level, error) {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return zapcore.DebugLevel, nil
	case "info":
		return zapcore.InfoLevel, nil
	case "warn", "warning":
		return zapcore.WarnLevel, nil
	case "error":
		return zapcore.ErrorLevel, nil
	case "panic":
		return zapcore.PanicLevel, nil
	case "fatal":
		return zapcore.FatalLevel, nil
	default:
		return zapcore.InfoLevel, fmt.Errorf("unsupported level %q", level)
	}
}

func parseEncoding(encoding string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(encoding)) {
	case "json":
		return "json", nil
	case "console":
		return "console", nil
	default:
		return "", fmt.Errorf("unsupported encoding %q", encoding)
	}
}
