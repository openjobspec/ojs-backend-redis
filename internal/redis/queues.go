package redis

import (
	"context"
	"sort"

	"github.com/openjobspec/ojs-backend-redis/internal/core"
)

// ListQueues returns all known queues.
func (b *RedisBackend) ListQueues(ctx context.Context) ([]core.QueueInfo, error) {
	names, err := b.client.SMembers(ctx, queuesKey()).Result()
	if err != nil {
		return nil, err
	}

	sort.Strings(names)
	var queues []core.QueueInfo
	for _, name := range names {
		status := "active"
		if exists, _ := b.client.Exists(ctx, queuePausedKey(name)).Result(); exists > 0 {
			status = "paused"
		}
		queues = append(queues, core.QueueInfo{
			Name:   name,
			Status: status,
		})
	}
	return queues, nil
}

// QueueStats returns statistics for a queue.
func (b *RedisBackend) QueueStats(ctx context.Context, name string) (*core.QueueStats, error) {
	available, _ := b.client.ZCard(ctx, queueAvailableKey(name)).Result()
	active, _ := b.client.SCard(ctx, queueActiveKey(name)).Result()
	completed, _ := b.client.Get(ctx, queueCompletedKey(name)).Int64()

	status := "active"
	if exists, _ := b.client.Exists(ctx, queuePausedKey(name)).Result(); exists > 0 {
		status = "paused"
	}

	return &core.QueueStats{
		Queue:  name,
		Status: status,
		Stats: core.Stats{
			Available: int(available),
			Active:    int(active),
			Completed: int(completed),
		},
	}, nil
}

// PauseQueue pauses a queue.
func (b *RedisBackend) PauseQueue(ctx context.Context, name string) error {
	return b.client.Set(ctx, queuePausedKey(name), "1", 0).Err()
}

// ResumeQueue resumes a queue.
func (b *RedisBackend) ResumeQueue(ctx context.Context, name string) error {
	return b.client.Del(ctx, queuePausedKey(name)).Err()
}
