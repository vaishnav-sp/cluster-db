package app

import "errors"

var (
	// ErrConfigurationInitialization indicates configuration could not be loaded.
	ErrConfigurationInitialization = errors.New("configuration initialization failed")
	// ErrLoggerInitialization indicates the logger could not be initialized.
	ErrLoggerInitialization = errors.New("logger initialization failed")
	// ErrServerInitialization indicates the server could not be initialized.
	ErrServerInitialization = errors.New("server initialization failed")
	// ErrStorageInitialization indicates the storage manager could not be initialized.
	ErrStorageInitialization = errors.New("storage initialization failed")
)
