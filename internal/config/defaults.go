package config

import "time"

// Defaults returns the default application configuration.
func Defaults() Config {
	return Config{
		Server: ServerConfig{
			Host:            "0.0.0.0",
			Port:            8080,
			ReadTimeout:     15 * time.Second,
			WriteTimeout:    15 * time.Second,
			IdleTimeout:     60 * time.Second,
			ShutdownTimeout: 10 * time.Second,
			Environment:     "development",
		},
		Cluster: ClusterConfig{
			NodeID:            "node-1",
			NodeAddress:       "127.0.0.1:9000",
			ReplicationFactor: 3,
			HeartbeatInterval: 5 * time.Second,
			FailureTimeout:    30 * time.Second,
			DiscoveryPort:     9001,
		},
		Storage: StorageConfig{
			DataDirectory:    "./data",
			SyncWrites:       true,
			MaxOpenFiles:     1000,
			ValueLogFileSize: 1024 * 1024 * 1024,
			Compression:      "snappy",
		},
		Logging: LoggingConfig{
			Level:            "info",
			Encoding:         "json",
			Development:      false,
			OutputPaths:      []string{"stdout"},
			ErrorOutputPaths: []string{"stderr"},
		},
		Metrics: MetricsConfig{
			Enabled: true,
			Port:    9090,
		},
		Authentication: AuthenticationConfig{
			JWTSecret:  "change-me-in-production",
			Issuer:     "clusterdb",
			Expiration: 24 * time.Hour,
		},
		Cache: CacheConfig{
			Enabled: false,
			Host:    "127.0.0.1",
			Port:    6379,
			TTL:     5 * time.Minute,
		},
		AI: AIConfig{
			EmbeddingModel:  "nomic-embed-text",
			OllamaURL:       "http://127.0.0.1:11434",
			LLMModel:        "llama3.1",
			VectorDimension: 768,
		},
	}
}
