package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/openjobspec/ojs-backend-redis/internal/core"
)

const (
	historyKeyPrefix  = "ojs:history:"
	childrenKeyPrefix = "ojs:children:"
	defaultRetention  = 30 * 24 * time.Hour
	defaultMaxEvents  = 1000
)

// RecordEvent persists a history event for a job.
func (b *RedisBackend) RecordEvent(ctx context.Context, event *core.HistoryEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal history event: %w", err)
	}

	key := historyKeyPrefix + event.JobID
	score := float64(time.Now().UnixMilli())

	pipe := b.client.Pipeline()
	pipe.ZAdd(ctx, key, redis.Z{Score: score, Member: string(data)})
	pipe.Expire(ctx, key, defaultRetention)
	pipe.ZRemRangeByRank(ctx, key, 0, int64(-defaultMaxEvents-1))
	_, err = pipe.Exec(ctx)
	return err
}

// GetJobHistory returns the execution history for a specific job.
func (b *RedisBackend) GetJobHistory(ctx context.Context, jobID string, limit int, cursor string) (*core.HistoryPage, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}

	key := historyKeyPrefix + jobID

	minScore := "-inf"
	if cursor != "" {
		minScore = "(" + cursor // exclusive
	}

	results, err := b.client.ZRangeByScoreWithScores(ctx, key, &redis.ZRangeBy{
		Min:   minScore,
		Max:   "+inf",
		Count: int64(limit + 1),
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("query history: %w", err)
	}

	events := make([]*core.HistoryEvent, 0, min(len(results), limit))
	var nextCursor string
	for i, r := range results {
		if i >= limit {
			nextCursor = fmt.Sprintf("%.0f", r.Score)
			break
		}
		var event core.HistoryEvent
		if err := json.Unmarshal([]byte(r.Member.(string)), &event); err != nil {
			continue
		}
		events = append(events, &event)
	}

	total, _ := b.client.ZCard(ctx, key).Result()

	return &core.HistoryPage{
		Events:     events,
		NextCursor: nextCursor,
		Total:      int(total),
	}, nil
}

// GetJobLineage returns the parent-child relationship tree for a job.
func (b *RedisBackend) GetJobLineage(ctx context.Context, jobID string) (*core.JobLineage, error) {
	return b.buildLineage(ctx, jobID, 0, 5)
}

func (b *RedisBackend) buildLineage(ctx context.Context, jobID string, depth, maxDepth int) (*core.JobLineage, error) {
	if depth >= maxDepth {
		return &core.JobLineage{JobID: jobID}, nil
	}

	job, err := b.Info(ctx, jobID)
	if err != nil {
		return &core.JobLineage{JobID: jobID}, nil
	}

	lineage := &core.JobLineage{
		JobID:      job.ID,
		ParentID:   job.ParentID,
		WorkflowID: job.WorkflowID,
		RootID:     job.RootID,
	}

	childIDs, err := b.client.SMembers(ctx, childrenKeyPrefix+jobID).Result()
	if err == nil {
		for _, childID := range childIDs {
			child, _ := b.buildLineage(ctx, childID, depth+1, maxDepth)
			if child != nil {
				lineage.Children = append(lineage.Children, *child)
			}
		}
	}

	return lineage, nil
}

// PurgeJobHistory deletes all history events for a specific job.
func (b *RedisBackend) PurgeJobHistory(ctx context.Context, jobID string) error {
	return b.client.Del(ctx, historyKeyPrefix+jobID).Err()
}

// PurgeHistory deletes history events older than the given duration.
func (b *RedisBackend) PurgeHistory(ctx context.Context, olderThan time.Duration) (int, error) {
	cutoffMs := float64(time.Now().Add(-olderThan).UnixMilli())
	deleted := 0

	var scanCursor uint64
	for {
		keys, cursor, err := b.client.Scan(ctx, scanCursor, historyKeyPrefix+"*", 100).Result()
		if err != nil {
			return deleted, fmt.Errorf("scan history keys: %w", err)
		}

		for _, key := range keys {
			n, err := b.client.ZRemRangeByScore(ctx, key, "-inf", fmt.Sprintf("%.0f", cutoffMs)).Result()
			if err == nil {
				deleted += int(n)
			}
		}

		scanCursor = cursor
		if cursor == 0 {
			break
		}
	}
	return deleted, nil
}

