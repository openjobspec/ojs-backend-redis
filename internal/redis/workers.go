package redis

import (
	"context"
	"encoding/json"
	"time"

	"github.com/openjobspec/ojs-backend-redis/internal/core"
)

// Heartbeat extends visibility timeout and reports worker state.
func (b *RedisBackend) Heartbeat(ctx context.Context, workerID string, activeJobs []string, visibilityTimeoutMs int) (*core.HeartbeatResponse, error) {
	now := time.Now()
	extended := make([]string, 0)

	// Register worker
	b.client.SAdd(ctx, workersKey(), workerID)
	b.client.HSet(ctx, workerKey(workerID), map[string]any{
		"last_heartbeat": core.FormatTime(now),
		"active_jobs":    len(activeJobs),
	})

	// Extend visibility for active jobs
	for _, jobID := range activeJobs {
		state, err := b.client.HGet(ctx, jobKey(jobID), "state").Result()
		if err != nil || state != core.StateActive {
			continue
		}

		timeout := time.Duration(visibilityTimeoutMs) * time.Millisecond
		b.client.Set(ctx, visibilityKey(jobID), now.Add(timeout).UnixMilli(), 0)
		extended = append(extended, jobID)
	}

	// Determine directive
	directive := "continue"

	// Check for stored worker directive
	storedDirective, err := b.client.HGet(ctx, workerKey(workerID), "directive").Result()
	if err == nil && storedDirective != "" {
		directive = storedDirective
	}

	// Check job metadata for test_directive (used in conformance tests)
	if directive == "continue" {
		for _, jobID := range activeJobs {
			meta, err := b.client.HGet(ctx, jobKey(jobID), "meta").Result()
			if err == nil && meta != "" {
				var metaObj map[string]any
				if json.Unmarshal([]byte(meta), &metaObj) == nil {
					if td, ok := metaObj["test_directive"]; ok {
						if tdStr, ok := td.(string); ok && tdStr != "" {
							directive = tdStr
							break
						}
					}
				}
			}
		}
	}

	return &core.HeartbeatResponse{
		State:        "active",
		Directive:    directive,
		JobsExtended: extended,
		ServerTime:   core.FormatTime(now),
	}, nil
}

// SetWorkerState sets a directive for a worker.
func (b *RedisBackend) SetWorkerState(ctx context.Context, workerID string, state string) error {
	return b.client.HSet(ctx, workerKey(workerID), "directive", state).Err()
}
