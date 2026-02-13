package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/openjobspec/ojs-backend-redis/internal/core"
)

// RedisBackend implements core.Backend using Redis.
type RedisBackend struct {
	client    *redis.Client
	startTime time.Time
}

// New creates a new RedisBackend.
func New(redisURL string) (*RedisBackend, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("parsing redis URL: %w", err)
	}

	client := redis.NewClient(opts)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("connecting to redis: %w", err)
	}

	return &RedisBackend{
		client:    client,
		startTime: time.Now(),
	}, nil
}

// Close closes the Redis connection.
func (b *RedisBackend) Close() error {
	return b.client.Close()
}

// Health returns the health status.
func (b *RedisBackend) Health(ctx context.Context) (*core.HealthResponse, error) {
	start := time.Now()
	err := b.client.Ping(ctx).Err()
	latency := time.Since(start).Milliseconds()

	resp := &core.HealthResponse{
		Version:       core.OJSVersion,
		UptimeSeconds: int64(time.Since(b.startTime).Seconds()),
	}

	if err != nil {
		resp.Status = "degraded"
		resp.Backend = core.BackendHealth{
			Type:   "redis",
			Status: "disconnected",
			Error:  err.Error(),
		}
		return resp, err
	}

	resp.Status = "ok"
	resp.Backend = core.BackendHealth{
		Type:      "redis",
		Status:    "connected",
		LatencyMs: latency,
	}
	return resp, nil
}
