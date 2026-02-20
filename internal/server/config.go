package server

import (
	commonconfig "github.com/openjobspec/ojs-go-backend-common/config"
)

// Config holds server configuration from environment variables.
type Config struct {
	commonconfig.BaseConfig
	RedisURL string
}

// LoadConfig reads configuration from environment variables with defaults.
func LoadConfig() Config {
	return Config{
		BaseConfig: commonconfig.LoadBaseConfig(),
		RedisURL:   commonconfig.GetEnv("REDIS_URL", "redis://localhost:6379"),
	}
}
