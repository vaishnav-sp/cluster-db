package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Load loads the configuration for the requested environment.
func Load() (*Config, error) {
	v := createViper()
	setDefaults(v)
	bindEnvironmentVariables(v)

	configName := resolveConfigName()
	if err := loadConfigurationFile(v, configName); err != nil {
		return nil, err
	}

	cfg, err := unmarshalConfiguration(v)
	if err != nil {
		return nil, err
	}

	if err := validateConfiguration(cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}

func createViper() *viper.Viper {
	v := viper.New()
	v.SetConfigType("yaml")
	v.AddConfigPath(filepath.Join("configs"))
	v.SetEnvPrefix("CLUSTERDB")
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	return v
}

func setDefaults(v *viper.Viper) {
	v.SetDefault("server.host", "0.0.0.0")
	v.SetDefault("server.port", 8080)
	v.SetDefault("server.read_timeout", 15*time.Second)
	v.SetDefault("server.write_timeout", 15*time.Second)
	v.SetDefault("server.idle_timeout", 60*time.Second)
	v.SetDefault("server.shutdown_timeout", 10*time.Second)
	v.SetDefault("server.environment", "development")

	v.SetDefault("cluster.node_id", "node-1")
	v.SetDefault("cluster.node_address", "127.0.0.1:9000")
	v.SetDefault("cluster.replication_factor", 3)
	v.SetDefault("cluster.heartbeat_interval", 5*time.Second)
	v.SetDefault("cluster.failure_timeout", 30*time.Second)
	v.SetDefault("cluster.discovery_port", 9001)

	v.SetDefault("storage.engine", "memory")
	v.SetDefault("storage.data_directory", "./data")
	v.SetDefault("storage.sync_writes", true)
	v.SetDefault("storage.max_open_files", 1000)
	v.SetDefault("storage.value_log_file_size", 1024*1024*1024)
	v.SetDefault("storage.compression", "snappy")
	v.SetDefault("storage.wal.enabled", false)
	v.SetDefault("storage.wal.path", "./data/clusterdb.wal")
	v.SetDefault("storage.wal.sync_on_write", true)

	v.SetDefault("logging.level", "info")
	v.SetDefault("logging.encoding", "json")
	v.SetDefault("logging.development", false)
	v.SetDefault("logging.output_paths", []string{"stdout"})
	v.SetDefault("logging.error_output_paths", []string{"stderr"})

	v.SetDefault("metrics.enabled", true)
	v.SetDefault("metrics.port", 9090)

	v.SetDefault("authentication.jwt_secret", "change-me-in-production-key-32-chars")
	v.SetDefault("authentication.issuer", "clusterdb")
	v.SetDefault("authentication.expiration", 24*time.Hour)

	v.SetDefault("cache.enabled", false)
	v.SetDefault("cache.host", "127.0.0.1")
	v.SetDefault("cache.port", 6379)
	v.SetDefault("cache.ttl", 5*time.Minute)

	v.SetDefault("ai.embedding_model", "nomic-embed-text")
	v.SetDefault("ai.ollama_url", "http://127.0.0.1:11434")
	v.SetDefault("ai.llm_model", "llama3.1")
	v.SetDefault("ai.vector_dimension", 768)
}

func bindEnvironmentVariables(v *viper.Viper) {
	bindings := []string{
		"server.host",
		"server.port",
		"server.read_timeout",
		"server.write_timeout",
		"server.idle_timeout",
		"server.shutdown_timeout",
		"server.environment",
		"cluster.node_id",
		"cluster.node_address",
		"cluster.replication_factor",
		"cluster.heartbeat_interval",
		"cluster.failure_timeout",
		"cluster.discovery_port",
		"storage.engine",
		"storage.data_directory",
		"storage.sync_writes",
		"storage.max_open_files",
		"storage.value_log_file_size",
		"storage.compression",
		"storage.wal.enabled",
		"storage.wal.path",
		"storage.wal.sync_on_write",
		"logging.level",
		"logging.encoding",
		"logging.development",
		"logging.output_paths",
		"logging.error_output_paths",
		"metrics.enabled",
		"metrics.port",
		"authentication.jwt_secret",
		"authentication.issuer",
		"authentication.expiration",
		"cache.enabled",
		"cache.host",
		"cache.port",
		"cache.ttl",
		"ai.embedding_model",
		"ai.ollama_url",
		"ai.llm_model",
		"ai.vector_dimension",
	}

	for _, key := range bindings {
		_ = v.BindEnv(key)
	}
}

func resolveConfigName() string {
	appEnv := strings.TrimSpace(os.Getenv("APP_ENV"))
	if appEnv == "" {
		appEnv = "development"
	}
	if !isSupportedEnvironment(appEnv) {
		return "development"
	}
	return appEnv
}

func loadConfigurationFile(v *viper.Viper, configName string) error {
	v.SetConfigName(configName)
	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			return fmt.Errorf("%w: %s", ErrConfigFileNotFound, configName)
		}
		return fmt.Errorf("read config: %w", err)
	}
	return nil
}

func unmarshalConfiguration(v *viper.Viper) (*Config, error) {
	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}
	return &cfg, nil
}

func validateConfiguration(cfg *Config) error {
	return Validate(*cfg)
}

func isSupportedEnvironment(env string) bool {
	switch env {
	case "development", "production", "docker":
		return true
	default:
		return false
	}
}
