package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/openjobspec/ojs-backend-redis/internal/core"
)

// newCronParser creates a standard cron parser with the fields we support.
func newCronParser() cron.Parser {
	return cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
}

// RegisterCron registers a cron job.
func (b *RedisBackend) RegisterCron(ctx context.Context, cronJob *core.CronJob) (*core.CronJob, error) {
	// Use Expression field (or fall back to Schedule)
	expr := cronJob.Expression
	if expr == "" {
		expr = cronJob.Schedule
	}

	parser := newCronParser()

	var schedule cron.Schedule
	var err error

	if cronJob.Timezone != "" {
		// Parse with timezone
		loc, locErr := time.LoadLocation(cronJob.Timezone)
		if locErr != nil {
			return nil, core.NewInvalidRequestError(
				fmt.Sprintf("Invalid timezone: %s", cronJob.Timezone),
				map[string]any{"timezone": cronJob.Timezone},
			)
		}
		// Wrap expression with timezone prefix
		schedule, err = parser.Parse("CRON_TZ=" + loc.String() + " " + expr)
		if err != nil {
			// Try without CRON_TZ prefix for special expressions like @daily
			schedule, err = parser.Parse(expr)
		}
	} else {
		schedule, err = parser.Parse(expr)
	}

	if err != nil {
		return nil, core.NewInvalidRequestError(
			fmt.Sprintf("Invalid cron expression: %s", expr),
			map[string]any{"expression": expr, "error": err.Error()},
		)
	}

	now := time.Now()
	cronJob.CreatedAt = core.FormatTime(now)
	cronJob.NextRunAt = core.FormatTime(schedule.Next(now))
	cronJob.Schedule = expr
	cronJob.Expression = expr

	if cronJob.Queue == "" {
		cronJob.Queue = "default"
	}
	if cronJob.OverlapPolicy == "" {
		cronJob.OverlapPolicy = "allow"
	}
	cronJob.Enabled = true

	data, err := json.Marshal(cronJob)
	if err != nil {
		return nil, fmt.Errorf("marshal cron job: %w", err)
	}
	pipe := b.client.Pipeline()
	pipe.Set(ctx, cronKey(cronJob.Name), string(data), 0)
	pipe.SAdd(ctx, cronNamesKey(), cronJob.Name)
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("register cron: %w", err)
	}

	return cronJob, nil
}

// ListCron lists all registered cron jobs.
func (b *RedisBackend) ListCron(ctx context.Context) ([]*core.CronJob, error) {
	names, err := b.client.SMembers(ctx, cronNamesKey()).Result()
	if err != nil {
		return nil, err
	}

	sort.Strings(names)
	var crons []*core.CronJob
	for _, name := range names {
		data, err := b.client.Get(ctx, cronKey(name)).Result()
		if err != nil {
			continue
		}
		var cj core.CronJob
		if err := json.Unmarshal([]byte(data), &cj); err == nil {
			crons = append(crons, &cj)
		}
	}
	return crons, nil
}

// DeleteCron removes a cron job and returns it.
func (b *RedisBackend) DeleteCron(ctx context.Context, name string) (*core.CronJob, error) {
	data, err := b.client.Get(ctx, cronKey(name)).Result()
	if err != nil {
		return nil, core.NewNotFoundError("Cron job", name)
	}

	var cj core.CronJob
	if err := json.Unmarshal([]byte(data), &cj); err != nil {
		return nil, fmt.Errorf("unmarshal cron job %s: %w", name, err)
	}

	pipe := b.client.Pipeline()
	pipe.Del(ctx, cronKey(name))
	pipe.SRem(ctx, cronNamesKey(), name)
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("delete cron: %w", err)
	}

	return &cj, nil
}

// FireCronJobs checks cron schedules and fires due jobs.
func (b *RedisBackend) FireCronJobs(ctx context.Context) error {
	names, err := b.client.SMembers(ctx, cronNamesKey()).Result()
	if err != nil {
		return err
	}

	now := time.Now()
	parser := newCronParser()

	for _, name := range names {
		data, err := b.client.Get(ctx, cronKey(name)).Result()
		if err != nil {
			continue
		}

		var cronJob core.CronJob
		if err := json.Unmarshal([]byte(data), &cronJob); err != nil {
			continue
		}

		if cronJob.NextRunAt == "" {
			continue
		}

		nextRun, err := time.Parse(core.TimeFormat, cronJob.NextRunAt)
		if err != nil {
			continue
		}

		if now.Before(nextRun) {
			continue
		}

		// Leader election with SET NX
		lockKey := fmt.Sprintf("ojs:cron_lock:%s:%d", name, nextRun.Unix())
		acquired, err := b.client.SetNX(ctx, lockKey, "1", CronLockTTL).Result()
		if err != nil || !acquired {
			continue
		}

		// Extract job details from template
		var jobType string
		var args json.RawMessage
		var queue string
		var meta json.RawMessage
		if cronJob.JobTemplate != nil {
			jobType = cronJob.JobTemplate.Type
			args = cronJob.JobTemplate.Args
			if cronJob.JobTemplate.Options != nil {
				queue = cronJob.JobTemplate.Options.Queue
			}
		}
		if queue == "" {
			queue = "default"
		}

		// Check overlap policy
		if cronJob.OverlapPolicy == "skip" {
			if b.shouldSkipCronFiring(ctx, name) {
				b.updateCronNextRun(ctx, name, &cronJob, parser, now)
				continue
			}
		}

		// Fire the cron job
		cronVisTimeout := CronVisibilityTimeoutMs
		job := &core.Job{
			Type:                jobType,
			Args:                args,
			Queue:               queue,
			Meta:                meta,
			VisibilityTimeoutMs: &cronVisTimeout,
		}

		created, pushErr := b.Push(ctx, job)
		if pushErr != nil {
			slog.Error("cron: error firing job", "name", name, "error", pushErr)
			continue
		}

		// Track the instance for overlap checking
		if cronJob.OverlapPolicy == "skip" {
			if err := b.client.Set(ctx, cronInstanceKey(name), created.ID, 0).Err(); err != nil {
				slog.Error("cron: error tracking instance", "name", name, "error", err)
			}
		}

		b.updateCronNextRun(ctx, name, &cronJob, parser, now)
	}

	return nil
}

// shouldSkipCronFiring checks if a cron job should be skipped due to overlap policy.
func (b *RedisBackend) shouldSkipCronFiring(ctx context.Context, name string) bool {
	instanceJobID, err := b.client.Get(ctx, cronInstanceKey(name)).Result()
	if err != nil || instanceJobID == "" {
		return false
	}
	jobState, stateErr := b.client.HGet(ctx, jobKey(instanceJobID), "state").Result()
	return stateErr == nil && !core.IsTerminalState(jobState)
}

// updateCronNextRun updates the next run time for a cron job.
func (b *RedisBackend) updateCronNextRun(ctx context.Context, name string, cronJob *core.CronJob, parser cron.Parser, now time.Time) {
	expr := cronJob.Expression
	if expr == "" {
		expr = cronJob.Schedule
	}
	schedule, err := parser.Parse(expr)
	if err != nil {
		slog.Error("cron: error parsing expression", "name", name, "error", err)
		return
	}
	cronJob.LastRunAt = core.FormatTime(now)
	cronJob.NextRunAt = core.FormatTime(schedule.Next(now))
	cronData, marshalErr := json.Marshal(cronJob)
	if marshalErr != nil {
		slog.Error("cron: error marshaling job", "name", name, "error", marshalErr)
		return
	}
	if err := b.client.Set(ctx, cronKey(name), string(cronData), 0).Err(); err != nil {
		slog.Error("cron: error updating next run", "name", name, "error", err)
	}
}
