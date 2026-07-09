package logger

import "errors"

var (
	// ErrInvalidLogLevel indicates that the requested log level is unsupported.
	ErrInvalidLogLevel = errors.New("invalid log level")
	// ErrInvalidEncoding indicates that the requested encoding is unsupported.
	ErrInvalidEncoding = errors.New("invalid encoding")
	// ErrLoggerInitialization indicates that the logger could not be initialized.
	ErrLoggerInitialization = errors.New("logger initialization failed")
)
