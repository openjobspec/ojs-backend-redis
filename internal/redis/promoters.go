package redis

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/openjobspec/ojs-backend-redis/internal/core"
)

// PromoteScheduled moves due scheduled jobs to their available queues using an atomic Lua script.
func (b *RedisBackend) PromoteScheduled(ctx context.Context) error {
	now := time.Now()
	_, err := promoteScript.Run(ctx, b.client,
		[]string{scheduledKey()},
		strconv.FormatInt(now.UnixMilli(), 10),
		core.FormatTime(now),
	).Result()
	if err != nil {
		return fmt.Errorf("promote scheduled: %w", err)
	}
	return nil
}

// PromoteRetries moves due retry jobs to their available queues using an atomic Lua script.
func (b *RedisBackend) PromoteRetries(ctx context.Context) error {
	now := time.Now()
	_, err := promoteScript.Run(ctx, b.client,
		[]string{retryKey()},
		strconv.FormatInt(now.UnixMilli(), 10),
		core.FormatTime(now),
	).Result()
	if err != nil {
		return fmt.Errorf("promote retries: %w", err)
	}
	return nil
}

// RequeueStalled finds and requeues jobs that exceeded their visibility timeout
// using per-job atomic Lua scripts.
func (b *RedisBackend) RequeueStalled(ctx context.Context) error {
	// Get all known queues
	queues, err := b.client.SMembers(ctx, queuesKey()).Result()
	if err != nil {
		return err
	}

	now := time.Now()
	nowMs := strconv.FormatInt(now.UnixMilli(), 10)
	nowFormatted := core.FormatTime(now)

	for _, queue := range queues {
		activeJobs, err := b.client.SMembers(ctx, queueActiveKey(queue)).Result()
		if err != nil {
			continue
		}

		for _, jobID := range activeJobs {
			if _, err := requeueStalledScript.Run(ctx, b.client, nil,
				jobID, queue, nowFormatted, nowMs,
			).Result(); err != nil {
				slog.Error("requeue-stalled: error running requeue script", "job_id", jobID, "error", err)
			}
		}
	}

	return nil
}
