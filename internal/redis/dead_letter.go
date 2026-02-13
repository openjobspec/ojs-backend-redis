package redis

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/openjobspec/ojs-backend-redis/internal/core"
)

// ListDeadLetter returns dead letter jobs.
func (b *RedisBackend) ListDeadLetter(ctx context.Context, limit, offset int) ([]*core.Job, int, error) {
	total, err := b.client.ZCard(ctx, deadKey()).Result()
	if err != nil {
		return nil, 0, err
	}

	ids, err := b.client.ZRevRange(ctx, deadKey(), int64(offset), int64(offset+limit-1)).Result()
	if err != nil {
		return nil, 0, err
	}

	var jobs []*core.Job
	for _, id := range ids {
		job, err := b.Info(ctx, id)
		if err == nil {
			jobs = append(jobs, job)
		}
	}

	return jobs, int(total), nil
}

// RetryDeadLetter retries a dead letter job using an atomic Lua script.
func (b *RedisBackend) RetryDeadLetter(ctx context.Context, jobID string) (*core.Job, error) {
	now := time.Now()

	res, err := retryDeadLetterScript.Run(ctx, b.client, nil,
		jobID,
		core.FormatTime(now),
		strconv.FormatInt(now.UnixMilli(), 10),
	).Result()
	if err != nil {
		return nil, fmt.Errorf("retry dead letter: %w", err)
	}

	status, _ := parseLuaResult(res)
	if status == luaStatusNotFound {
		return nil, core.NewNotFoundError("Dead letter job", jobID)
	}

	return b.Info(ctx, jobID)
}

// DeleteDeadLetter removes a job from the dead letter queue.
func (b *RedisBackend) DeleteDeadLetter(ctx context.Context, jobID string) error {
	removed, err := b.client.ZRem(ctx, deadKey(), jobID).Result()
	if err != nil {
		return err
	}
	if removed == 0 {
		return core.NewNotFoundError("Dead letter job", jobID)
	}
	return nil
}
