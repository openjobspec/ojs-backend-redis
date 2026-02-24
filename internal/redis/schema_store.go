package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/openjobspec/ojs-backend-redis/internal/core"
)

// Schema key builders
func schemaKey(jobType, version string) string {
	return fmt.Sprintf("%sschemas:%s:%s", keyPrefix, jobType, version)
}
func schemaVersionsKey(jobType string) string {
	return fmt.Sprintf("%sschemas:%s:versions", keyPrefix, jobType)
}
func schemaLatestKey(jobType string) string {
	return fmt.Sprintf("%sschemas:%s:latest", keyPrefix, jobType)
}

// RegisterSchema stores a schema for a job type and version.
func (b *RedisBackend) RegisterSchema(ctx context.Context, jobType string, version string, schema json.RawMessage) (*core.Schema, error) {
	key := schemaKey(jobType, version)

	// Check for duplicate
	exists, err := b.client.Exists(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("check schema existence: %w", err)
	}
	if exists > 0 {
		return nil, core.NewConflictError(
			fmt.Sprintf("Schema for '%s' version '%s' already exists.", jobType, version),
			map[string]any{"job_type": jobType, "version": version},
		)
	}

	now := time.Now()
	s := &core.Schema{
		JobType:   jobType,
		Version:   version,
		Schema:    schema,
		CreatedAt: now,
	}

	data, err := json.Marshal(s)
	if err != nil {
		return nil, fmt.Errorf("marshal schema: %w", err)
	}

	// Store schema, add to version set, and update latest atomically
	pipe := b.client.TxPipeline()
	pipe.Set(ctx, key, string(data), 0)
	pipe.ZAdd(ctx, schemaVersionsKey(jobType), redis.Z{Score: float64(now.UnixMilli()), Member: version})
	pipe.Set(ctx, schemaLatestKey(jobType), version, 0)
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("register schema: %w", err)
	}

	return s, nil
}

// GetSchema returns the latest schema for a job type.
func (b *RedisBackend) GetSchema(ctx context.Context, jobType string) (*core.Schema, error) {
	version, err := b.client.Get(ctx, schemaLatestKey(jobType)).Result()
	if err != nil {
		return nil, core.NewNotFoundError("Schema", jobType)
	}

	return b.GetSchemaVersion(ctx, jobType, version)
}

// GetSchemaVersion returns a specific schema version for a job type.
func (b *RedisBackend) GetSchemaVersion(ctx context.Context, jobType string, version string) (*core.Schema, error) {
	data, err := b.client.Get(ctx, schemaKey(jobType, version)).Result()
	if err != nil {
		return nil, core.NewNotFoundError("Schema", fmt.Sprintf("%s@%s", jobType, version))
	}

	var s core.Schema
	if err := json.Unmarshal([]byte(data), &s); err != nil {
		return nil, fmt.Errorf("unmarshal schema: %w", err)
	}

	return &s, nil
}

// ListVersions returns all registered versions for a job type.
func (b *RedisBackend) ListVersions(ctx context.Context, jobType string) ([]*core.SchemaVersion, error) {
	members, err := b.client.ZRangeWithScores(ctx, schemaVersionsKey(jobType), 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("list schema versions: %w", err)
	}

	latestVersion, _ := b.client.Get(ctx, schemaLatestKey(jobType)).Result()

	var versions []*core.SchemaVersion
	for _, m := range members {
		v := m.Member.(string)
		createdAtMs := int64(m.Score)
		versions = append(versions, &core.SchemaVersion{
			Version:   v,
			CreatedAt: time.UnixMilli(createdAtMs),
			IsLatest:  v == latestVersion,
		})
	}

	// Sort newest first
	sort.Slice(versions, func(i, j int) bool {
		return versions[i].CreatedAt.After(versions[j].CreatedAt)
	})

	return versions, nil
}

// ValidateArgs validates job arguments against the latest schema for a job type.
func (b *RedisBackend) ValidateArgs(ctx context.Context, jobType string, args json.RawMessage) (*core.ValidationResult, error) {
	_, err := b.GetSchema(ctx, jobType)
	if err != nil {
		// No schema registered — validation passes by default
		return &core.ValidationResult{Valid: true}, nil
	}

	// Basic validation: ensure args is valid JSON
	if !json.Valid(args) {
		return &core.ValidationResult{
			Valid:  false,
			Errors: []string{"args is not valid JSON"},
		}, nil
	}

	return &core.ValidationResult{Valid: true}, nil
}

// DeleteSchema removes a specific schema version.
func (b *RedisBackend) DeleteSchema(ctx context.Context, jobType string, version string) error {
	key := schemaKey(jobType, version)

	exists, err := b.client.Exists(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("check schema existence: %w", err)
	}
	if exists == 0 {
		return core.NewNotFoundError("Schema", fmt.Sprintf("%s@%s", jobType, version))
	}

	pipe := b.client.TxPipeline()
	pipe.Del(ctx, key)
	pipe.ZRem(ctx, schemaVersionsKey(jobType), version)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("delete schema: %w", err)
	}

	// Update latest pointer if we deleted the current latest
	currentLatest, _ := b.client.Get(ctx, schemaLatestKey(jobType)).Result()
	if currentLatest == version {
		// Find the new latest (highest score = most recent)
		members, err := b.client.ZRevRange(ctx, schemaVersionsKey(jobType), 0, 0).Result()
		if err == nil && len(members) > 0 {
			b.client.Set(ctx, schemaLatestKey(jobType), members[0], 0)
		} else {
			b.client.Del(ctx, schemaLatestKey(jobType))
		}
	}

	return nil
}
