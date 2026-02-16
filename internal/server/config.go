package server

import "os"

// Config holds server configuration from environment variables.
type Config struct {
	Port     string
	GRPCPort string
	RedisURL string
}

// LoadConfig reads configuration from environment variables with defaults.
func LoadConfig() Config {
	return Config{
		Port:     getEnv("OJS_PORT", "8080"),
		GRPCPort: getEnv("OJS_GRPC_PORT", "9090"),
		RedisURL: getEnv("REDIS_URL", "redis://localhost:6379"),
	}
}

func getEnv(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return defaultVal
}

