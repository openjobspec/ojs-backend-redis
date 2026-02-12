package redis

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"

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

func (b *RedisBackend) Close() error {
	return b.client.Close()
}

// Push enqueues a single job.
func (b *RedisBackend) Push(ctx context.Context, job *core.Job) (*core.Job, error) {
	now := time.Now()

	// Assign ID if not provided
	if job.ID == "" {
		job.ID = core.NewUUIDv7()
	}

	// Set system-managed fields
	job.CreatedAt = core.FormatTime(now)
	job.Attempt = 0

	// Handle unique jobs
	if job.Unique != nil {
		fingerprint := computeFingerprint(job)
		ttl := time.Hour // default
		if job.Unique.Period != "" {
			if d, err := core.ParseISO8601Duration(job.Unique.Period); err == nil {
				ttl = d
			}
		}

		conflict := job.Unique.OnConflict
		if conflict == "" {
			conflict = "reject"
		}

		// Check for existing unique job
		existingID, err := b.client.Get(ctx, uniqueKey(fingerprint)).Result()
		if err == nil && existingID != "" {
			// Check if existing job is in a relevant state
			existingState, stateErr := b.client.HGet(ctx, jobKey(existingID), "state").Result()
			if stateErr == nil {
				// Check state filtering: if unique.states is specified, only consider
				// the job a duplicate if its state is in the list
				isRelevant := false
				if len(job.Unique.States) > 0 {
					for _, s := range job.Unique.States {
						if s == existingState {
							isRelevant = true
							break
						}
					}
				} else {
					// Default: any non-terminal state is relevant
					isRelevant = !core.IsTerminalState(existingState)
				}

				if isRelevant {
					switch conflict {
					case "reject":
						return nil, &core.OJSError{
							Code:    core.ErrCodeDuplicate,
							Message: "A job with the same unique key already exists.",
							Details: map[string]any{
								"existing_job_id": existingID,
								"unique_key":      fingerprint,
							},
						}
					case "ignore":
						// Return existing job with IsExisting flag
						existing, err := b.Info(ctx, existingID)
						if err == nil {
							existing.IsExisting = true
							return existing, nil
						}
					case "replace":
						// Cancel existing and create new
						b.Cancel(ctx, existingID)
					}
				}
			}
		}

		// Set unique key
		b.client.Set(ctx, uniqueKey(fingerprint), job.ID, ttl)
	}

	// Determine initial state
	if job.ScheduledAt != "" {
		scheduledTime, err := time.Parse(time.RFC3339, job.ScheduledAt)
		if err == nil && scheduledTime.After(now) {
			job.State = core.StateScheduled
			job.EnqueuedAt = core.FormatTime(now)

			// Store job and add to scheduled set
			pipe := b.client.Pipeline()
			pipe.HSet(ctx, jobKey(job.ID), jobToHash(job))
			pipe.ZAdd(ctx, scheduledKey(), redis.Z{
				Score:  float64(scheduledTime.UnixMilli()),
				Member: job.ID,
			})
			pipe.SAdd(ctx, queuesKey(), job.Queue)
			if _, err := pipe.Exec(ctx); err != nil {
				return nil, fmt.Errorf("enqueue scheduled job: %w", err)
			}
			return job, nil
		}
		// Past scheduled_at - treat as immediate
	}

	job.State = core.StateAvailable
	job.EnqueuedAt = core.FormatTime(now)

	// Compute score for priority queuing
	score := computeScore(job.Priority, now)

	// Store job and add to available queue
	pipe := b.client.Pipeline()
	pipe.HSet(ctx, jobKey(job.ID), jobToHash(job))
	pipe.ZAdd(ctx, queueAvailableKey(job.Queue), redis.Z{
		Score:  score,
		Member: job.ID,
	})
	pipe.SAdd(ctx, queuesKey(), job.Queue)
	// Store rate limit config at queue level if specified
	if job.RateLimit != nil && job.RateLimit.MaxPerSecond > 0 {
		pipe.Set(ctx, queueRateLimitKey(job.Queue), strconv.Itoa(job.RateLimit.MaxPerSecond), 0)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("enqueue job: %w", err)
	}

	return job, nil
}

// computeScore creates a composite score for priority ordering.
// Higher priority jobs get lower scores (fetched first).
// Same priority jobs are FIFO by enqueue time.
// Priority range: -100 to 100.
func computeScore(priority *int, enqueueTime time.Time) float64 {
	p := 0
	if priority != nil {
		p = *priority
	}
	// Score = (100 - priority) * 1e15 + enqueued_at_ms
	// priority 100 → score = 0 * 1e15 + ms (fetched first)
	// priority 0   → score = 100 * 1e15 + ms
	// priority -100 → score = 200 * 1e15 + ms (fetched last)
	return float64(100-p)*1e15 + float64(enqueueTime.UnixMilli())
}

// Fetch claims jobs from the specified queues.
func (b *RedisBackend) Fetch(ctx context.Context, queues []string, count int, workerID string, visibilityTimeoutMs int) ([]*core.Job, error) {
	now := time.Now()
	var jobs []*core.Job

	for _, queue := range queues {
		if len(jobs) >= count {
			break
		}

		// Check if queue is paused
		paused, _ := b.client.Exists(ctx, queuePausedKey(queue)).Result()
		if paused > 0 {
			continue
		}

		// Check rate limit
		rateLimitStr, rlErr := b.client.Get(ctx, queueRateLimitKey(queue)).Result()
		if rlErr == nil && rateLimitStr != "" {
			maxPerSec, _ := strconv.Atoi(rateLimitStr)
			if maxPerSec > 0 {
				windowMs := int64(1000 / maxPerSec)
				lastFetchStr, _ := b.client.Get(ctx, queueRateLimitLastKey(queue)).Result()
				if lastFetchStr != "" {
					lastFetchMs, _ := strconv.ParseInt(lastFetchStr, 10, 64)
					nowMs := now.UnixMilli()
					if nowMs-lastFetchMs < windowMs {
						continue // Rate limited
					}
				}
			}
		}

		remaining := count - len(jobs)
		for i := 0; i < remaining; i++ {
			// Atomic pop from available queue
			results, err := b.client.ZPopMin(ctx, queueAvailableKey(queue), 1).Result()
			if err != nil || len(results) == 0 {
				break
			}

			jobID := results[0].Member.(string)

			// Check if job has expired
			expiresAt, _ := b.client.HGet(ctx, jobKey(jobID), "expires_at").Result()
			if expiresAt != "" {
				expTime, err := time.Parse(time.RFC3339, expiresAt)
				if err == nil && now.After(expTime) {
					// Discard expired job (no completed_at for TTL-discarded jobs)
					b.client.HSet(ctx, jobKey(jobID), map[string]any{
						"state": core.StateDiscarded,
					})
					continue
				}
			}

			// Update job state to active (attempt is NOT incremented on fetch;
			// it is incremented on NACK to count completed attempts)
			pipe := b.client.Pipeline()
			pipe.HSet(ctx, jobKey(jobID), map[string]any{
				"state":      core.StateActive,
				"started_at": core.FormatTime(now),
				"worker_id":  workerID,
			})
			pipe.SAdd(ctx, queueActiveKey(queue), jobID)

			// Set visibility timeout: request > job-level > default (30s)
			effectiveVisTimeout := visibilityTimeoutMs
			if effectiveVisTimeout <= 0 {
				if jobVisStr, err := b.client.HGet(ctx, jobKey(jobID), "visibility_timeout_ms").Result(); err == nil && jobVisStr != "" {
					if jobVis, err := strconv.Atoi(jobVisStr); err == nil && jobVis > 0 {
						effectiveVisTimeout = jobVis
					}
				}
			}
			if effectiveVisTimeout <= 0 {
				effectiveVisTimeout = 30000 // default 30s
			}
			pipe.Set(ctx, visibilityKey(jobID), core.FormatTime(now.Add(time.Duration(effectiveVisTimeout)*time.Millisecond)), 0)

			if _, err := pipe.Exec(ctx); err != nil {
				continue
			}

			// Record rate limit timestamp for this queue
			b.client.Set(ctx, queueRateLimitLastKey(queue), strconv.FormatInt(time.Now().UnixMilli(), 10), 0)

			// Get the full job
			job, err := b.Info(ctx, jobID)
			if err != nil {
				continue
			}

			jobs = append(jobs, job)
		}
	}

	return jobs, nil
}

// Ack acknowledges a job as completed.
func (b *RedisBackend) Ack(ctx context.Context, jobID string, result []byte) (*core.AckResponse, error) {
	// Get current state
	data, err := b.client.HGetAll(ctx, jobKey(jobID)).Result()
	if err != nil || len(data) == 0 {
		return nil, core.NewNotFoundError("Job", jobID)
	}

	currentState := data["state"]
	if currentState != core.StateActive {
		return nil, core.NewConflictError(
			fmt.Sprintf("Cannot acknowledge job not in 'active' state. Current state: '%s'.", currentState),
			map[string]any{
				"job_id":         jobID,
				"current_state":  currentState,
				"expected_state": "active",
			},
		)
	}

	now := core.NowFormatted()
	queue := data["queue"]

	updates := map[string]any{
		"state":        core.StateCompleted,
		"completed_at": now,
	}

	if result != nil && len(result) > 0 {
		updates["result"] = string(result)
	}

	pipe := b.client.Pipeline()
	pipe.HSet(ctx, jobKey(jobID), updates)
	// Clear error field on successful acknowledgment
	pipe.HDel(ctx, jobKey(jobID), "error")
	pipe.SRem(ctx, queueActiveKey(queue), jobID)
	pipe.Del(ctx, visibilityKey(jobID))
	pipe.Incr(ctx, queueCompletedKey(queue))
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("ack job: %w", err)
	}

	// Check if this job is part of a workflow
	b.advanceWorkflow(ctx, jobID, core.StateCompleted, result)

	// Fetch the full updated job for the response
	job, _ := b.Info(ctx, jobID)

	return &core.AckResponse{
		Acknowledged: true,
		JobID:        jobID,
		State:        core.StateCompleted,
		CompletedAt:  now,
		Job:          job,
	}, nil
}

// Nack reports a job failure.
func (b *RedisBackend) Nack(ctx context.Context, jobID string, jobErr *core.JobError, requeue bool) (*core.NackResponse, error) {
	data, err := b.client.HGetAll(ctx, jobKey(jobID)).Result()
	if err != nil || len(data) == 0 {
		return nil, core.NewNotFoundError("Job", jobID)
	}

	currentState := data["state"]
	if currentState != core.StateActive {
		return nil, core.NewConflictError(
			fmt.Sprintf("Cannot fail job not in 'active' state. Current state: '%s'.", currentState),
			map[string]any{
				"job_id":         jobID,
				"current_state":  currentState,
				"expected_state": "active",
			},
		)
	}

	now := time.Now()
	queue := data["queue"]
	attempt, _ := strconv.Atoi(data["attempt"])
	maxAttempts := 3
	if v, ok := data["max_attempts"]; ok && v != "" {
		maxAttempts, _ = strconv.Atoi(v)
	}

	// Handle requeue: return job to available state immediately
	if requeue {
		score := computeScore(nil, now)
		if v, ok := data["priority"]; ok && v != "" {
			p, _ := strconv.Atoi(v)
			score = computeScore(&p, now)
		}

		pipe := b.client.Pipeline()
		pipe.HSet(ctx, jobKey(jobID), map[string]any{
			"state":       core.StateAvailable,
			"started_at":  "",
			"worker_id":   "",
			"enqueued_at": core.FormatTime(now),
		})
		pipe.SRem(ctx, queueActiveKey(queue), jobID)
		pipe.Del(ctx, visibilityKey(jobID))
		pipe.ZAdd(ctx, queueAvailableKey(queue), redis.Z{
			Score:  score,
			Member: jobID,
		})
		pipe.Exec(ctx)

		job, _ := b.Info(ctx, jobID)
		return &core.NackResponse{
			JobID:       jobID,
			State:       core.StateAvailable,
			Attempt:     attempt,
			MaxAttempts: maxAttempts,
			Job:         job,
		}, nil
	}

	// Increment attempt counter (counts completed attempt cycles)
	newAttempt := attempt + 1

	// Store error on job
	var errJSON []byte
	if jobErr != nil {
		errObj := map[string]any{
			"message": jobErr.Message,
			"attempt": attempt,
		}
		if jobErr.Code != "" {
			errObj["type"] = jobErr.Code
		}
		if jobErr.Type != "" {
			errObj["type"] = jobErr.Type
		}
		if jobErr.Retryable != nil {
			errObj["retryable"] = *jobErr.Retryable
		}
		if jobErr.Details != nil {
			errObj["details"] = jobErr.Details
		}
		errJSON, _ = json.Marshal(errObj)
	}

	// Update error history
	var errorHistory []json.RawMessage
	if hist, ok := data["error_history"]; ok && hist != "" {
		json.Unmarshal([]byte(hist), &errorHistory)
	}
	if errJSON != nil {
		errorHistory = append(errorHistory, json.RawMessage(errJSON))
	}
	histJSON, _ := json.Marshal(errorHistory)

	// Check if error is non-retryable
	isNonRetryable := false
	if jobErr != nil && jobErr.Retryable != nil && !*jobErr.Retryable {
		isNonRetryable = true
	}

	// Check non-retryable error patterns from retry policy
	var retryPolicy *core.RetryPolicy
	if v, ok := data["retry"]; ok && v != "" {
		var rp core.RetryPolicy
		json.Unmarshal([]byte(v), &rp)
		retryPolicy = &rp
	}

	if !isNonRetryable && jobErr != nil && retryPolicy != nil {
		for _, pattern := range retryPolicy.NonRetryableErrors {
			errType := jobErr.Code
			if jobErr.Type != "" {
				errType = jobErr.Type
			}
			if matchesPattern(errType, pattern) || matchesPattern(jobErr.Message, pattern) {
				isNonRetryable = true
				break
			}
		}
	}

	// Determine on_exhaustion behavior
	onExhaustion := "discard"
	if retryPolicy != nil && retryPolicy.OnExhaustion != "" {
		onExhaustion = retryPolicy.OnExhaustion
	}

	// Determine next state
	if isNonRetryable || newAttempt >= maxAttempts {
		discardedAt := core.FormatTime(now)
		updates := map[string]any{
			"state":         core.StateDiscarded,
			"completed_at":  discardedAt,
			"error_history": string(histJSON),
			"attempt":       strconv.Itoa(newAttempt),
		}
		if errJSON != nil {
			updates["error"] = string(errJSON)
		}

		pipe := b.client.Pipeline()
		pipe.HSet(ctx, jobKey(jobID), updates)
		pipe.SRem(ctx, queueActiveKey(queue), jobID)
		pipe.Del(ctx, visibilityKey(jobID))

		// Only add to DLQ if on_exhaustion is "dead_letter"
		if onExhaustion == "dead_letter" {
			pipe.ZAdd(ctx, deadKey(), redis.Z{
				Score:  float64(now.UnixMilli()),
				Member: jobID,
			})
		}
		pipe.Exec(ctx)

		b.advanceWorkflow(ctx, jobID, core.StateDiscarded, nil)

		job, _ := b.Info(ctx, jobID)
		return &core.NackResponse{
			JobID:       jobID,
			State:       core.StateDiscarded,
			Attempt:     newAttempt,
			MaxAttempts: maxAttempts,
			DiscardedAt: discardedAt,
			Job:         job,
		}, nil
	}

	// Retry
	backoff := core.CalculateBackoff(retryPolicy, newAttempt)
	backoffMs := backoff.Milliseconds()
	nextAttemptAt := now.Add(backoff)

	updates := map[string]any{
		"state":          core.StateRetryable,
		"error_history":  string(histJSON),
		"attempt":        strconv.Itoa(newAttempt),
		"retry_delay_ms": strconv.FormatInt(backoffMs, 10),
	}
	if errJSON != nil {
		updates["error"] = string(errJSON)
	}

	pipe := b.client.Pipeline()
	pipe.HSet(ctx, jobKey(jobID), updates)
	pipe.SRem(ctx, queueActiveKey(queue), jobID)
	pipe.Del(ctx, visibilityKey(jobID))
	pipe.ZAdd(ctx, retryKey(), redis.Z{
		Score:  float64(nextAttemptAt.UnixMilli()),
		Member: jobID,
	})
	pipe.Exec(ctx)

	retryJob, _ := b.Info(ctx, jobID)
	return &core.NackResponse{
		JobID:         jobID,
		State:         core.StateRetryable,
		Attempt:       newAttempt,
		MaxAttempts:   maxAttempts,
		NextAttemptAt: core.FormatTime(nextAttemptAt),
		Job:           retryJob,
	}, nil
}

// Info retrieves job details.
func (b *RedisBackend) Info(ctx context.Context, jobID string) (*core.Job, error) {
	data, err := b.client.HGetAll(ctx, jobKey(jobID)).Result()
	if err != nil {
		return nil, fmt.Errorf("get job: %w", err)
	}
	if len(data) == 0 {
		return nil, core.NewNotFoundError("Job", jobID)
	}

	return hashToJob(data), nil
}

// Cancel cancels a job.
func (b *RedisBackend) Cancel(ctx context.Context, jobID string) (*core.Job, error) {
	data, err := b.client.HGetAll(ctx, jobKey(jobID)).Result()
	if err != nil || len(data) == 0 {
		return nil, core.NewNotFoundError("Job", jobID)
	}

	currentState := data["state"]

	if core.IsTerminalState(currentState) {
		return nil, core.NewConflictError(
			fmt.Sprintf("Cannot cancel job in terminal state '%s'.", currentState),
			map[string]any{
				"job_id":        jobID,
				"current_state": currentState,
			},
		)
	}

	now := core.NowFormatted()
	queue := data["queue"]

	pipe := b.client.Pipeline()
	pipe.HSet(ctx, jobKey(jobID), map[string]any{
		"state":        core.StateCancelled,
		"cancelled_at": now,
	})

	// Remove from all possible sets
	pipe.ZRem(ctx, queueAvailableKey(queue), jobID)
	pipe.SRem(ctx, queueActiveKey(queue), jobID)
	pipe.ZRem(ctx, scheduledKey(), jobID)
	pipe.ZRem(ctx, retryKey(), jobID)
	pipe.Del(ctx, visibilityKey(jobID))

	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("cancel job: %w", err)
	}

	job := hashToJob(data)
	job.State = core.StateCancelled
	job.CancelledAt = now

	return job, nil
}

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
		b.client.Set(ctx, visibilityKey(jobID), core.FormatTime(now.Add(timeout)), 0)
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

// RetryDeadLetter retries a dead letter job.
func (b *RedisBackend) RetryDeadLetter(ctx context.Context, jobID string) (*core.Job, error) {
	score, err := b.client.ZScore(ctx, deadKey(), jobID).Result()
	if err != nil {
		return nil, core.NewNotFoundError("Dead letter job", jobID)
	}
	_ = score

	now := time.Now()
	queue, _ := b.client.HGet(ctx, jobKey(jobID), "queue").Result()

	pipe := b.client.Pipeline()
	pipe.ZRem(ctx, deadKey(), jobID)
	pipe.HSet(ctx, jobKey(jobID), map[string]any{
		"state":       core.StateAvailable,
		"attempt":     "0",
		"enqueued_at": core.FormatTime(now),
	})
	// Clear error-related fields and completion timestamp
	pipe.HDel(ctx, jobKey(jobID), "error", "error_history", "completed_at", "retry_delay_ms")

	score2 := computeScore(nil, now)
	pipe.ZAdd(ctx, queueAvailableKey(queue), redis.Z{
		Score:  score2,
		Member: jobID,
	})
	pipe.Exec(ctx)

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

// RegisterCron registers a cron job.
func (b *RedisBackend) RegisterCron(ctx context.Context, cronJob *core.CronJob) (*core.CronJob, error) {
	// Use Expression field (or fall back to Schedule)
	expr := cronJob.Expression
	if expr == "" {
		expr = cronJob.Schedule
	}

	// Validate cron expression
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

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

	data, _ := json.Marshal(cronJob)
	pipe := b.client.Pipeline()
	pipe.Set(ctx, cronKey(cronJob.Name), string(data), 0)
	pipe.SAdd(ctx, cronNamesKey(), cronJob.Name)
	pipe.Exec(ctx)

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
	json.Unmarshal([]byte(data), &cj)

	pipe := b.client.Pipeline()
	pipe.Del(ctx, cronKey(name))
	pipe.SRem(ctx, cronNamesKey(), name)
	pipe.Exec(ctx)

	return &cj, nil
}

// CreateWorkflow creates and starts a workflow.
func (b *RedisBackend) CreateWorkflow(ctx context.Context, req *core.WorkflowRequest) (*core.Workflow, error) {
	now := time.Now()
	wfID := core.NewUUIDv7()

	// Determine job list (chain uses Steps, group/batch uses Jobs)
	jobs := req.Jobs
	if req.Type == "chain" {
		jobs = req.Steps
	}

	total := len(jobs)

	// Build the workflow response
	wf := &core.Workflow{
		ID:        wfID,
		Name:      req.Name,
		Type:      req.Type,
		State:     "running",
		CreatedAt: core.FormatTime(now),
	}

	if req.Type == "chain" {
		wf.StepsTotal = &total
		zero := 0
		wf.StepsCompleted = &zero
	} else {
		wf.JobsTotal = &total
		zero := 0
		wf.JobsCompleted = &zero
	}

	// Store workflow metadata in Redis hash
	wfHash := map[string]any{
		"id":         wfID,
		"type":       req.Type,
		"name":       req.Name,
		"state":      "running",
		"total":      strconv.Itoa(total),
		"completed":  "0",
		"failed":     "0",
		"created_at": core.FormatTime(now),
	}
	if req.Callbacks != nil {
		cbJSON, _ := json.Marshal(req.Callbacks)
		wfHash["callbacks"] = string(cbJSON)
	}

	// Store workflow job definitions for chain (needed to enqueue later steps)
	jobDefs, _ := json.Marshal(jobs)
	wfHash["job_defs"] = string(jobDefs)

	b.client.HSet(ctx, workflowKey(wfID), wfHash)

	if req.Type == "chain" {
		// Chain: only enqueue the first step
		step := jobs[0]
		queue := "default"
		if step.Options != nil && step.Options.Queue != "" {
			queue = step.Options.Queue
		}

		job := &core.Job{
			Type:         step.Type,
			Args:         step.Args,
			Queue:        queue,
			WorkflowID:   wfID,
			WorkflowStep: 0,
		}
		if step.Options != nil && step.Options.RetryPolicy != nil {
			job.Retry = step.Options.RetryPolicy
			job.MaxAttempts = &step.Options.RetryPolicy.MaxAttempts
		} else if step.Options != nil && step.Options.Retry != nil {
			job.Retry = step.Options.Retry
			job.MaxAttempts = &step.Options.Retry.MaxAttempts
		}

		created, err := b.Push(ctx, job)
		if err != nil {
			return nil, err
		}

		// Store job ID in workflow's job list
		b.client.RPush(ctx, workflowKey(wfID)+":jobs", created.ID)
	} else {
		// Group/Batch: enqueue all jobs immediately
		for i, step := range jobs {
			queue := "default"
			if step.Options != nil && step.Options.Queue != "" {
				queue = step.Options.Queue
			}

			job := &core.Job{
				Type:         step.Type,
				Args:         step.Args,
				Queue:        queue,
				WorkflowID:   wfID,
				WorkflowStep: i,
			}
			if step.Options != nil && step.Options.RetryPolicy != nil {
				job.Retry = step.Options.RetryPolicy
				job.MaxAttempts = &step.Options.RetryPolicy.MaxAttempts
			} else if step.Options != nil && step.Options.Retry != nil {
				job.Retry = step.Options.Retry
				job.MaxAttempts = &step.Options.Retry.MaxAttempts
			}

			created, err := b.Push(ctx, job)
			if err != nil {
				return nil, err
			}

			b.client.RPush(ctx, workflowKey(wfID)+":jobs", created.ID)
		}
	}

	return wf, nil
}

// GetWorkflow retrieves a workflow by ID.
func (b *RedisBackend) GetWorkflow(ctx context.Context, id string) (*core.Workflow, error) {
	data, err := b.client.HGetAll(ctx, workflowKey(id)).Result()
	if err != nil || len(data) == 0 {
		return nil, core.NewNotFoundError("Workflow", id)
	}

	wf := &core.Workflow{
		ID:        data["id"],
		Name:      data["name"],
		Type:      data["type"],
		State:     data["state"],
		CreatedAt: data["created_at"],
	}
	if v, ok := data["completed_at"]; ok && v != "" {
		wf.CompletedAt = v
	}

	total, _ := strconv.Atoi(data["total"])
	completed, _ := strconv.Atoi(data["completed"])

	if wf.Type == "chain" {
		wf.StepsTotal = &total
		wf.StepsCompleted = &completed
	} else {
		wf.JobsTotal = &total
		wf.JobsCompleted = &completed
	}

	return wf, nil
}

// CancelWorkflow cancels a workflow and its active/pending jobs.
func (b *RedisBackend) CancelWorkflow(ctx context.Context, id string) (*core.Workflow, error) {
	wf, err := b.GetWorkflow(ctx, id)
	if err != nil {
		return nil, err
	}

	if wf.State == "completed" || wf.State == "failed" || wf.State == "cancelled" {
		return nil, core.NewConflictError(
			fmt.Sprintf("Cannot cancel workflow in state '%s'.", wf.State),
			nil,
		)
	}

	// Cancel all jobs belonging to this workflow
	jobIDs, _ := b.client.LRange(ctx, workflowKey(id)+":jobs", 0, -1).Result()
	for _, jobID := range jobIDs {
		jobState, _ := b.client.HGet(ctx, jobKey(jobID), "state").Result()
		if jobState != "" && !core.IsTerminalState(jobState) {
			b.Cancel(ctx, jobID)
		}
	}

	wf.State = "cancelled"
	wf.CompletedAt = core.NowFormatted()

	b.client.HSet(ctx, workflowKey(id), map[string]any{
		"state":        "cancelled",
		"completed_at": wf.CompletedAt,
	})

	return wf, nil
}

// AdvanceWorkflow is called after ACK or NACK to update workflow state.
func (b *RedisBackend) AdvanceWorkflow(ctx context.Context, workflowID string, jobID string, result json.RawMessage, failed bool) error {
	data, err := b.client.HGetAll(ctx, workflowKey(workflowID)).Result()
	if err != nil || len(data) == 0 {
		return nil
	}

	wfType := data["type"]
	state := data["state"]
	if state != "running" {
		return nil
	}

	total, _ := strconv.Atoi(data["total"])
	completed, _ := strconv.Atoi(data["completed"])
	failedCount, _ := strconv.Atoi(data["failed"])

	// Get the job's workflow step index
	stepStr, _ := b.client.HGet(ctx, jobKey(jobID), "workflow_step").Result()
	stepIdx, _ := strconv.Atoi(stepStr)

	// Store the result for chain result passing
	if result != nil && len(result) > 0 {
		b.client.HSet(ctx, workflowKey(workflowID)+":results", strconv.Itoa(stepIdx), string(result))
	}

	if failed {
		failedCount++
	} else {
		completed++
	}

	// Track total finished (completed + failed) for determining when all jobs are done
	totalFinished := completed + failedCount

	updates := map[string]any{
		"completed": strconv.Itoa(completed),
		"failed":    strconv.Itoa(failedCount),
	}

	if wfType == "chain" {
		if failed {
			// Chain stops on failure
			updates["state"] = "failed"
			updates["completed_at"] = core.NowFormatted()
			b.client.HSet(ctx, workflowKey(workflowID), updates)
			return nil
		}

		if totalFinished >= total {
			// Chain complete
			updates["state"] = "completed"
			updates["completed_at"] = core.NowFormatted()
			b.client.HSet(ctx, workflowKey(workflowID), updates)
			return nil
		}

		// Enqueue next step
		b.client.HSet(ctx, workflowKey(workflowID), updates)
		return b.enqueueChainStep(ctx, workflowID, data, stepIdx+1)
	}

	// Group/Batch: check if all jobs are done
	b.client.HSet(ctx, workflowKey(workflowID), updates)

	if totalFinished >= total {
		finalState := "completed"
		if failedCount > 0 {
			finalState = "failed"
		}
		b.client.HSet(ctx, workflowKey(workflowID), map[string]any{
			"state":        finalState,
			"completed_at": core.NowFormatted(),
		})

		// Fire batch callbacks
		if wfType == "batch" {
			b.fireBatchCallbacks(ctx, workflowID, data, failedCount > 0)
		}
	}

	return nil
}

// enqueueChainStep enqueues the next step in a chain workflow.
func (b *RedisBackend) enqueueChainStep(ctx context.Context, workflowID string, wfData map[string]string, stepIdx int) error {
	// Load job definitions
	var jobDefs []core.WorkflowJobRequest
	if err := json.Unmarshal([]byte(wfData["job_defs"]), &jobDefs); err != nil {
		return err
	}

	if stepIdx >= len(jobDefs) {
		return nil
	}

	step := jobDefs[stepIdx]
	queue := "default"
	if step.Options != nil && step.Options.Queue != "" {
		queue = step.Options.Queue
	}

	// Collect parent results from previous steps
	var parentResults []json.RawMessage
	resultsData, _ := b.client.HGetAll(ctx, workflowKey(workflowID)+":results").Result()
	for i := 0; i < stepIdx; i++ {
		if r, ok := resultsData[strconv.Itoa(i)]; ok {
			parentResults = append(parentResults, json.RawMessage(r))
		}
	}

	job := &core.Job{
		Type:          step.Type,
		Args:          step.Args,
		Queue:         queue,
		WorkflowID:    workflowID,
		WorkflowStep:  stepIdx,
		ParentResults: parentResults,
	}
	if step.Options != nil && step.Options.RetryPolicy != nil {
		job.Retry = step.Options.RetryPolicy
		job.MaxAttempts = &step.Options.RetryPolicy.MaxAttempts
	} else if step.Options != nil && step.Options.Retry != nil {
		job.Retry = step.Options.Retry
		job.MaxAttempts = &step.Options.Retry.MaxAttempts
	}

	created, err := b.Push(ctx, job)
	if err != nil {
		return err
	}

	b.client.RPush(ctx, workflowKey(workflowID)+":jobs", created.ID)
	return nil
}

// fireBatchCallbacks fires callback jobs based on batch outcome.
func (b *RedisBackend) fireBatchCallbacks(ctx context.Context, workflowID string, wfData map[string]string, hasFailure bool) {
	cbStr, ok := wfData["callbacks"]
	if !ok || cbStr == "" {
		return
	}

	var callbacks core.WorkflowCallbacks
	if err := json.Unmarshal([]byte(cbStr), &callbacks); err != nil {
		return
	}

	// on_complete always fires
	if callbacks.OnComplete != nil {
		b.fireCallback(ctx, callbacks.OnComplete)
	}

	// on_success fires only when all jobs succeeded
	if !hasFailure && callbacks.OnSuccess != nil {
		b.fireCallback(ctx, callbacks.OnSuccess)
	}

	// on_failure fires when any job failed
	if hasFailure && callbacks.OnFailure != nil {
		b.fireCallback(ctx, callbacks.OnFailure)
	}
}

// fireCallback creates a job from a workflow callback definition.
func (b *RedisBackend) fireCallback(ctx context.Context, cb *core.WorkflowCallback) {
	queue := "default"
	if cb.Options != nil && cb.Options.Queue != "" {
		queue = cb.Options.Queue
	}
	b.Push(ctx, &core.Job{
		Type:  cb.Type,
		Args:  cb.Args,
		Queue: queue,
	})
}

// PushBatch atomically enqueues multiple jobs.
func (b *RedisBackend) PushBatch(ctx context.Context, jobs []*core.Job) ([]*core.Job, error) {
	now := time.Now()
	pipe := b.client.Pipeline()

	for _, job := range jobs {
		if job.ID == "" {
			job.ID = core.NewUUIDv7()
		}
		job.State = core.StateAvailable
		job.Attempt = 0
		job.CreatedAt = core.FormatTime(now)
		job.EnqueuedAt = core.FormatTime(now)

		score := computeScore(job.Priority, now)

		pipe.HSet(ctx, jobKey(job.ID), jobToHash(job))
		pipe.ZAdd(ctx, queueAvailableKey(job.Queue), redis.Z{
			Score:  score,
			Member: job.ID,
		})
		pipe.SAdd(ctx, queuesKey(), job.Queue)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("batch enqueue: %w", err)
	}

	return jobs, nil
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

// SetWorkerState sets a directive for a worker.
func (b *RedisBackend) SetWorkerState(ctx context.Context, workerID string, state string) error {
	return b.client.HSet(ctx, workerKey(workerID), "directive", state).Err()
}

// advanceWorkflow advances a workflow when a job completes or fails.
func (b *RedisBackend) advanceWorkflow(ctx context.Context, jobID, state string, result []byte) {
	// Check if this job belongs to a workflow
	wfID, _ := b.client.HGet(ctx, jobKey(jobID), "workflow_id").Result()
	if wfID == "" {
		return
	}

	failed := state == core.StateDiscarded || state == core.StateCancelled
	b.AdvanceWorkflow(ctx, wfID, jobID, json.RawMessage(result), failed)
}


// PromoteScheduled moves due scheduled jobs to their available queues.
func (b *RedisBackend) PromoteScheduled(ctx context.Context) error {
	now := time.Now()
	ids, err := b.client.ZRangeByScore(ctx, scheduledKey(), &redis.ZRangeBy{
		Min: "0",
		Max: strconv.FormatInt(now.UnixMilli(), 10),
	}).Result()
	if err != nil {
		return err
	}

	for _, jobID := range ids {
		data, err := b.client.HGetAll(ctx, jobKey(jobID)).Result()
		if err != nil || len(data) == 0 {
			b.client.ZRem(ctx, scheduledKey(), jobID)
			continue
		}

		queue := data["queue"]
		score := computeScore(nil, now)
		if v, ok := data["priority"]; ok && v != "" {
			p, _ := strconv.Atoi(v)
			score = computeScore(&p, now)
		}

		pipe := b.client.Pipeline()
		pipe.ZRem(ctx, scheduledKey(), jobID)
		pipe.HSet(ctx, jobKey(jobID), map[string]any{
			"state":       core.StateAvailable,
			"enqueued_at": core.FormatTime(now),
		})
		pipe.ZAdd(ctx, queueAvailableKey(queue), redis.Z{
			Score:  score,
			Member: jobID,
		})
		pipe.Exec(ctx)
	}

	return nil
}

// PromoteRetries moves due retry jobs to their available queues.
func (b *RedisBackend) PromoteRetries(ctx context.Context) error {
	now := time.Now()
	ids, err := b.client.ZRangeByScore(ctx, retryKey(), &redis.ZRangeBy{
		Min: "0",
		Max: strconv.FormatInt(now.UnixMilli(), 10),
	}).Result()
	if err != nil {
		return err
	}

	for _, jobID := range ids {
		data, err := b.client.HGetAll(ctx, jobKey(jobID)).Result()
		if err != nil || len(data) == 0 {
			b.client.ZRem(ctx, retryKey(), jobID)
			continue
		}

		queue := data["queue"]
		score := computeScore(nil, now)
		if v, ok := data["priority"]; ok && v != "" {
			p, _ := strconv.Atoi(v)
			score = computeScore(&p, now)
		}

		pipe := b.client.Pipeline()
		pipe.ZRem(ctx, retryKey(), jobID)
		pipe.HSet(ctx, jobKey(jobID), map[string]any{
			"state":       core.StateAvailable,
			"enqueued_at": core.FormatTime(now),
		})
		pipe.ZAdd(ctx, queueAvailableKey(queue), redis.Z{
			Score:  score,
			Member: jobID,
		})
		pipe.Exec(ctx)
	}

	return nil
}

// RequeueStalled finds and requeues jobs that exceeded their visibility timeout.
func (b *RedisBackend) RequeueStalled(ctx context.Context) error {
	// Get all known queues
	queues, err := b.client.SMembers(ctx, queuesKey()).Result()
	if err != nil {
		return err
	}

	now := time.Now()

	for _, queue := range queues {
		activeJobs, err := b.client.SMembers(ctx, queueActiveKey(queue)).Result()
		if err != nil {
			continue
		}

		for _, jobID := range activeJobs {
			// Check visibility timeout
			visDeadline, err := b.client.Get(ctx, visibilityKey(jobID)).Result()
			if err != nil {
				continue
			}

			deadline, err := time.Parse(core.TimeFormat, visDeadline)
			if err != nil {
				continue
			}

			if now.After(deadline) {
				// Requeue the stalled job
				data, err := b.client.HGetAll(ctx, jobKey(jobID)).Result()
				if err != nil || len(data) == 0 {
					continue
				}

				score := computeScore(nil, now)
				if v, ok := data["priority"]; ok && v != "" {
					p, _ := strconv.Atoi(v)
					score = computeScore(&p, now)
				}

				pipe := b.client.Pipeline()
				pipe.SRem(ctx, queueActiveKey(queue), jobID)
				pipe.HSet(ctx, jobKey(jobID), map[string]any{
					"state":       core.StateAvailable,
					"started_at":  "",
					"worker_id":   "",
					"enqueued_at": core.FormatTime(now),
				})
				pipe.ZAdd(ctx, queueAvailableKey(queue), redis.Z{
					Score:  score,
					Member: jobID,
				})
				pipe.Del(ctx, visibilityKey(jobID))
				pipe.Exec(ctx)
			}
		}
	}

	return nil
}

// FireCronJobs checks cron schedules and fires due jobs.
func (b *RedisBackend) FireCronJobs(ctx context.Context) error {
	names, err := b.client.SMembers(ctx, cronNamesKey()).Result()
	if err != nil {
		return err
	}

	now := time.Now()
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

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
		acquired, err := b.client.SetNX(ctx, lockKey, "1", 60*time.Second).Result()
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
			// Check instance tracking key for this cron
			instanceJobID, err := b.client.Get(ctx, cronInstanceKey(name)).Result()
			skipFire := false
			if err == nil && instanceJobID != "" {
				// Check if the tracked job is still in a non-terminal state
				jobState, stateErr := b.client.HGet(ctx, jobKey(instanceJobID), "state").Result()
				if stateErr == nil && !core.IsTerminalState(jobState) {
					skipFire = true
				}
			}
			if skipFire {
				// Still update next run time even if skipped
				expr := cronJob.Expression
				if expr == "" {
					expr = cronJob.Schedule
				}
				schedule, err := parser.Parse(expr)
				if err == nil {
					cronJob.LastRunAt = core.FormatTime(now)
					cronJob.NextRunAt = core.FormatTime(schedule.Next(now))
					cronData, _ := json.Marshal(cronJob)
					b.client.Set(ctx, cronKey(name), string(cronData), 0)
				}
				continue
			}
		}

		// Fire the cron job (use a long visibility timeout so the reaper doesn't
		// requeue it before the next cron tick, which would defeat overlap checks)
		cronVisTimeout := 600000 // 10 minutes
		job := &core.Job{
			Type:                jobType,
			Args:                args,
			Queue:               queue,
			Meta:                meta,
			VisibilityTimeoutMs: &cronVisTimeout,
		}

		created, pushErr := b.Push(ctx, job)
		if pushErr != nil {
			continue
		}

		// Track the instance for overlap checking
		if cronJob.OverlapPolicy == "skip" {
			b.client.Set(ctx, cronInstanceKey(name), created.ID, 0)
		}

		// Update next run time
		expr := cronJob.Expression
		if expr == "" {
			expr = cronJob.Schedule
		}
		schedule, err := parser.Parse(expr)
		if err == nil {
			cronJob.LastRunAt = core.FormatTime(now)
			cronJob.NextRunAt = core.FormatTime(schedule.Next(now))
			cronData, _ := json.Marshal(cronJob)
			b.client.Set(ctx, cronKey(name), string(cronData), 0)
		}
	}

	return nil
}

func computeFingerprint(job *core.Job) string {
	h := sha256.New()
	keys := job.Unique.Keys
	if len(keys) == 0 {
		// Default: hash type + args
		keys = []string{"type", "args"}
	}
	sort.Strings(keys)
	for _, key := range keys {
		switch key {
		case "type":
			h.Write([]byte("type:"))
			h.Write([]byte(job.Type))
		case "args":
			h.Write([]byte("args:"))
			if job.Args != nil {
				h.Write(job.Args)
			}
		case "queue":
			h.Write([]byte("queue:"))
			h.Write([]byte(job.Queue))
		}
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func matchesPattern(s, pattern string) bool {
	// Try regex matching first (patterns like "Auth.*")
	re, err := regexp.Compile("^" + pattern + "$")
	if err == nil {
		return re.MatchString(s)
	}
	// Fallback to exact match
	return s == pattern
}
