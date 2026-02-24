package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/openjobspec/ojs-backend-redis/internal/core"
)

// Checkpoint key builder
func checkpointKey(jobID string) string {
	return fmt.Sprintf("%scheckpoints:%s", keyPrefix, jobID)
}

// SaveCheckpoint persists intermediate state for a running job.
func (b *RedisBackend) SaveCheckpoint(ctx context.Context, jobID string, state json.RawMessage) error {
	// Enforce 1MB size limit on state
	if len(state) > 1048576 {
		return core.NewInvalidRequestError(
			"Checkpoint state exceeds 1MB size limit.",
			map[string]any{"size_bytes": len(state), "max_bytes": 1048576},
		)
	}

	key := checkpointKey(jobID)
	now := time.Now()

	// Atomically increment sequence and update checkpoint
	pipe := b.client.TxPipeline()
	pipe.HIncrBy(ctx, key, "sequence", 1)
	pipe.HSet(ctx, key, map[string]any{
		"job_id":     jobID,
		"state":      string(state),
		"created_at": core.FormatTime(now),
	})
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}

	return nil
}

// GetCheckpoint retrieves the latest checkpoint for a job.
func (b *RedisBackend) GetCheckpoint(ctx context.Context, jobID string) (*core.Checkpoint, error) {
	key := checkpointKey(jobID)

	data, err := b.client.HGetAll(ctx, key).Result()
	if err != nil || len(data) == 0 {
		return nil, core.NewNotFoundError("Checkpoint", jobID)
	}

	stateStr, ok := data["state"]
	if !ok || stateStr == "" {
		return nil, core.NewNotFoundError("Checkpoint", jobID)
	}

	sequence := 0
	if seqStr, ok := data["sequence"]; ok {
		fmt.Sscanf(seqStr, "%d", &sequence)
	}

	createdAt := time.Time{}
	if caStr, ok := data["created_at"]; ok {
		createdAt, _ = time.Parse(core.TimeFormat, caStr)
	}

	return &core.Checkpoint{
		JobID:     jobID,
		State:     json.RawMessage(stateStr),
		Sequence:  sequence,
		CreatedAt: createdAt,
	}, nil
}

// DeleteCheckpoint removes the checkpoint for a job.
func (b *RedisBackend) DeleteCheckpoint(ctx context.Context, jobID string) error {
	key := checkpointKey(jobID)

	deleted, err := b.client.Del(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("delete checkpoint: %w", err)
	}
	if deleted == 0 {
		return core.NewNotFoundError("Checkpoint", jobID)
	}

	return nil
}
