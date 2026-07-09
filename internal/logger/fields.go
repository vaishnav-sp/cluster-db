package logger

import (
	"time"

	"go.uber.org/zap"
)

// Service creates a service field.
func Service(name string) zap.Field {
	return zap.String("service", name)
}

// Node creates a node field.
func Node(id string) zap.Field {
	return zap.String("node_id", id)
}

// RequestID creates a request ID field.
func RequestID(id string) zap.Field {
	return zap.String("request_id", id)
}

// Component creates a component field.
func Component(name string) zap.Field {
	return zap.String("component", name)
}

// Address creates an address field.
func Address(addr string) zap.Field {
	return zap.String("address", addr)
}

// Duration creates a duration field.
func Duration(d time.Duration) zap.Field {
	return zap.Duration("duration", d)
}

// Error creates an error field.
func Error(err error) zap.Field {
	return zap.Error(err)
}
