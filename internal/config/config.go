package config

import "time"

// Config is the top-level application configuration used by all executables.
type Config struct {
	Server         ServerConfig         `mapstructure:"server"`
	Cluster        ClusterConfig        `mapstructure:"cluster"`
	Storage        StorageConfig        `mapstructure:"storage"`
	Logging        LoggingConfig        `mapstructure:"logging"`
	Metrics        MetricsConfig        `mapstructure:"metrics"`
	Authentication AuthenticationConfig `mapstructure:"authentication"`
	Cache          CacheConfig          `mapstructure:"cache"`
	AI             AIConfig             `mapstructure:"ai"`
}

// ServerConfig contains HTTP and process lifecycle settings.
type ServerConfig struct {
	Host            string        `mapstructure:"host"`
	Port            int           `mapstructure:"port"`
	ReadTimeout     time.Duration `mapstructure:"read_timeout"`
	WriteTimeout    time.Duration `mapstructure:"write_timeout"`
	IdleTimeout     time.Duration `mapstructure:"idle_timeout"`
	ShutdownTimeout time.Duration `mapstructure:"shutdown_timeout"`
	Environment     string        `mapstructure:"environment"`
}

// ClusterConfig contains distributed node and replication settings.
type ClusterConfig struct {
	NodeID            string        `mapstructure:"node_id"`
	NodeAddress       string        `mapstructure:"node_address"`
	ReplicationFactor int           `mapstructure:"replication_factor"`
	HeartbeatInterval time.Duration `mapstructure:"heartbeat_interval"`
	FailureTimeout    time.Duration `mapstructure:"failure_timeout"`
	DiscoveryPort     int           `mapstructure:"discovery_port"`
}

// StorageConfig contains persistence related settings.
type StorageConfig struct {
	// Engine selects the storage backend. Supported values: "memory".
	// Future values: "badger", "pebble", "rocksdb".
	Engine           string    `mapstructure:"engine"`
	DataDirectory    string    `mapstructure:"data_directory"`
	SyncWrites       bool      `mapstructure:"sync_writes"`
	MaxOpenFiles     int       `mapstructure:"max_open_files"`
	ValueLogFileSize int       `mapstructure:"value_log_file_size"`
	Compression      string    `mapstructure:"compression"`
	WAL              WALConfig `mapstructure:"wal"`
}

// WALConfig controls write-ahead-log persistence for storage engines that
// support it. It is disabled by default to preserve memory-only behaviour.
type WALConfig struct {
	Enabled     bool   `mapstructure:"enabled"`
	Path        string `mapstructure:"path"`
	SyncOnWrite bool   `mapstructure:"sync_on_write"`
}

// LoggingConfig contains log output settings.
type LoggingConfig struct {
	Level            string   `mapstructure:"level"`
	Encoding         string   `mapstructure:"encoding"`
	Development      bool     `mapstructure:"development"`
	OutputPaths      []string `mapstructure:"output_paths"`
	ErrorOutputPaths []string `mapstructure:"error_output_paths"`
}

// MetricsConfig contains observability settings.
type MetricsConfig struct {
	Enabled bool `mapstructure:"enabled"`
	Port    int  `mapstructure:"port"`
}

// AuthenticationConfig contains auth and token settings.
type AuthenticationConfig struct {
	JWTSecret  string        `mapstructure:"jwt_secret"`
	Issuer     string        `mapstructure:"issuer"`
	Expiration time.Duration `mapstructure:"expiration"`
}

// CacheConfig contains cache connection settings.
type CacheConfig struct {
	Enabled bool          `mapstructure:"enabled"`
	Host    string        `mapstructure:"host"`
	Port    int           `mapstructure:"port"`
	TTL     time.Duration `mapstructure:"ttl"`
}

// AIConfig contains AI service integration settings.
type AIConfig struct {
	EmbeddingModel  string `mapstructure:"embedding_model"`
	OllamaURL       string `mapstructure:"ollama_url"`
	LLMModel        string `mapstructure:"llm_model"`
	VectorDimension int    `mapstructure:"vector_dimension"`
}
