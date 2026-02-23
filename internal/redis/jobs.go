package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	ojsotel "github.com/openjobspec/ojs-go-backend-common/otel"

	"github.com/openjobspec/ojs-backend-redis/internal/core"
)

// Push enqueues a single job.
func (b *RedisBackend) Push(ctx context.Context, job *core.Job) (*core.Job, error) {
	ctx, span := ojsotel.StartJobSpan(ctx, "push", job.ID, job.Type, job.Queue)
	defer span.End()

	now := time.Now()

	// Assign ID if not provided
	if job.ID == "" {
		job.ID = core.NewUUIDv7()
	}

	// Set system-managed fields
	job.CreatedAt = core.FormatTime(now)
	job.Attempt = 0

	// Handle unique jobs via Lua script for atomic dedup
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

		statesJSON := ""
		if len(job.Unique.States) > 0 {
			j, err := json.Marshal(job.Unique.States)
			if err != nil {
				return nil, fmt.Errorf("marshal unique states: %w", err)
			}
			statesJSON = string(j)
		}

		res, err := uniqueCheckScript.Run(ctx, b.client, nil,
			fingerprint,
			job.ID,
			strconv.FormatInt(int64(ttl.Seconds()), 10),
			conflict,
			statesJSON,
		).Result()
		if err != nil {
			slog.Error("push: error checking unique key", "job_id", job.ID, "error", err)
		} else {
			_, data := parseLuaResult(res)
			if len(data) > 0 {
				action, ok := data[0].(string)
				if !ok {
					slog.Error("push: unexpected type in unique check result", "job_id", job.ID, "value", data[0])
				}
				switch action {
				case "reject":
					existingID := ""
					fp := fingerprint
					if len(data) > 1 {
						existingID, _ = data[1].(string)
					}
					if len(data) > 2 {
						fp, _ = data[2].(string)
					}
					return nil, &core.OJSError{
						Code:    core.ErrCodeDuplicate,
						Message: "A job with the same unique key already exists.",
						Details: map[string]any{
							"existing_job_id": existingID,
							"unique_key":      fp,
						},
					}
				case "ignore":
					existingID := ""
					if len(data) > 1 {
						existingID, _ = data[1].(string)
					}
					existing, err := b.Info(ctx, existingID)
					if err == nil {
						existing.IsExisting = true
						return existing, nil
					}
				case "replace":
					existingID := ""
					if len(data) > 1 {
						existingID, _ = data[1].(string)
					}
					if _, err := b.Cancel(ctx, existingID); err != nil {
						slog.Error("push: error cancelling existing job during replace", "job_id", existingID, "error", err)
					}
				// "proceed": continue with normal enqueue
				}
			}
		}
	}

	// Determine initial state
	if job.ScheduledAt != "" {
		scheduledTime, err := time.Parse(time.RFC3339, job.ScheduledAt)
		if err == nil && scheduledTime.After(now) {
			job.State = core.StateScheduled
			job.EnqueuedAt = core.FormatTime(now)

			// Store job and add to scheduled set
			hash, hashErr := jobToHash(job)
			if hashErr != nil {
				return nil, fmt.Errorf("serialize job: %w", hashErr)
			}
			pipe := b.client.Pipeline()
			pipe.HSet(ctx, jobKey(job.ID), hash)
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
	hash, hashErr := jobToHash(job)
	if hashErr != nil {
		return nil, fmt.Errorf("serialize job: %w", hashErr)
	}
	pipe := b.client.Pipeline()
	pipe.HSet(ctx, jobKey(job.ID), hash)
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

// PushBatch enqueues multiple jobs, delegating to Push for full feature parity.
func (b *RedisBackend) PushBatch(ctx context.Context, jobs []*core.Job) ([]*core.Job, error) {
	// Pre-validate all jobs before any writes to avoid partial batch failures
	for _, job := range jobs {
		if err := core.ValidateEnqueueRequest(&core.EnqueueRequest{
			Type: job.Type,
			Args: job.Args,
		}); err != nil {
			return nil, err
		}
	}

	var results []*core.Job
	for _, job := range jobs {
		created, err := b.Push(ctx, job)
		if err != nil {
			return nil, fmt.Errorf("batch enqueue job %s: %w", job.Type, err)
		}
		results = append(results, created)
	}
	return results, nil
}

// Fetch claims jobs from the specified queues using atomic Lua scripts.
func (b *RedisBackend) Fetch(ctx context.Context, queues []string, count int, workerID string, visibilityTimeoutMs int) ([]*core.Job, error) {
	ctx, span := ojsotel.StartStorageSpan(ctx, "fetch", "redis")
	defer span.End()

	now := time.Now()
	nowFormatted := core.FormatTime(now)
	nowMs := strconv.FormatInt(now.UnixMilli(), 10)
	visMs := strconv.Itoa(visibilityTimeoutMs)
	defaultVisMs := strconv.Itoa(core.DefaultVisibilityTimeoutMs)
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
					if now.UnixMilli()-lastFetchMs < windowMs {
						continue // Rate limited
					}
				}
			}
		}

		remaining := count - len(jobs)
		for i := 0; i < remaining; i++ {
			// Atomic pop + expiry check + state transition via Lua
			res, err := fetchScript.Run(ctx, b.client, nil,
				queue, nowFormatted, workerID, nowMs, visMs, defaultVisMs,
			).Result()
			if err != nil {
				break
			}

			status, data := parseLuaResult(res)
			if status == luaStatusNotFound {
				break // No more jobs in this queue
			}
			if status == luaStatusExpired {
				continue // Job was expired and discarded, try next
			}
			if status != luaStatusOK || len(data) == 0 {
				break
			}

			jobID, ok := data[0].(string)
			if !ok {
				continue
			}

			// Record rate limit timestamp for this queue
			if err := b.client.Set(ctx, queueRateLimitLastKey(queue), strconv.FormatInt(time.Now().UnixMilli(), 10), 0).Err(); err != nil {
				slog.Error("fetch: error recording rate limit timestamp", "queue", queue, "error", err)
			}

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

// Ack acknowledges a job as completed using an atomic Lua script.
func (b *RedisBackend) Ack(ctx context.Context, jobID string, result []byte) (*core.AckResponse, error) {
	ctx, span := ojsotel.StartJobSpan(ctx, "ack", jobID, "", "")
	defer span.End()

	now := core.NowFormatted()

	resultStr := ""
	if len(result) > 0 {
		resultStr = string(result)
	}

	res, err := ackScript.Run(ctx, b.client, nil, jobID, now, resultStr).Result()
	if err != nil {
		return nil, fmt.Errorf("ack job: %w", err)
	}

	status, data := parseLuaResult(res)
	switch status {
	case luaStatusNotFound:
		return nil, core.NewNotFoundError("Job", jobID)
	case luaStatusConflict:
		currentState := ""
		if len(data) > 0 {
			currentState, _ = data[0].(string)
		}
		return nil, core.NewConflictError(
			fmt.Sprintf("Cannot acknowledge job not in 'active' state. Current state: '%s'.", currentState),
			map[string]any{
				"job_id":         jobID,
				"current_state":  currentState,
				"expected_state": "active",
			},
		)
	}

	// Check if this job is part of a workflow
	if err := b.advanceWorkflow(ctx, jobID, core.StateCompleted, result); err != nil {
		slog.Error("ack: error advancing workflow", "job_id", jobID, "error", err)
	}

	// Fetch the full updated job for the response (best-effort; state already committed by Lua)
	job, infoErr := b.Info(ctx, jobID)
	if infoErr != nil {
		slog.Warn("ack: failed to fetch job after completion", "job_id", jobID, "error", infoErr)
	}

	return &core.AckResponse{
		Acknowledged: true,
		JobID:        jobID,
		State:        core.StateCompleted,
		CompletedAt:  now,
		Job:          job,
	}, nil
}

// Nack reports a job failure. Retry decision logic stays in Go;
// state transitions use atomic Lua scripts.
func (b *RedisBackend) Nack(ctx context.Context, jobID string, jobErr *core.JobError, requeue bool) (*core.NackResponse, error) {
	ctx, span := ojsotel.StartJobSpan(ctx, "nack", jobID, "", "")
	defer span.End()

	// Read job data for retry decision logic
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
	attempt, _ := strconv.Atoi(data["attempt"])
	maxAttempts := 3
	if v, ok := data["max_attempts"]; ok && v != "" {
		maxAttempts, _ = strconv.Atoi(v)
	}

	// Handle requeue: return job to available state immediately via Lua
	if requeue {
		nowMs := strconv.FormatInt(now.UnixMilli(), 10)

		res, scriptErr := nackRequeueScript.Run(ctx, b.client, nil,
			jobID, core.FormatTime(now), nowMs,
		).Result()
		if scriptErr != nil {
			return nil, fmt.Errorf("requeue job: %w", scriptErr)
		}

		status, luaData := parseLuaResult(res)
		if status == luaStatusNotFound {
			return nil, core.NewNotFoundError("Job", jobID)
		}
		if status == luaStatusConflict {
			curState := ""
			if len(luaData) > 0 {
				curState, _ = luaData[0].(string)
			}
			return nil, core.NewConflictError(
				fmt.Sprintf("Cannot fail job not in 'active' state. Current state: '%s'.", curState),
				map[string]any{"job_id": jobID, "current_state": curState, "expected_state": "active"},
			)
		}

		job, infoErr := b.Info(ctx, jobID)
		if infoErr != nil {
			slog.Warn("nack-requeue: failed to fetch job after requeue", "job_id", jobID, "error", infoErr)
		}
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
		errJSON, err = json.Marshal(errObj)
		if err != nil {
			return nil, fmt.Errorf("marshal job error: %w", err)
		}
	}

	// Update error history
	var errorHistory []json.RawMessage
	if hist, ok := data["error_history"]; ok && hist != "" {
		if err := json.Unmarshal([]byte(hist), &errorHistory); err != nil {
			slog.Error("nack: error parsing error history", "job_id", jobID, "error", err)
		}
	}
	if errJSON != nil {
		errorHistory = append(errorHistory, json.RawMessage(errJSON))
	}
	histJSON, err := json.Marshal(errorHistory)
	if err != nil {
		slog.Error("nack: error marshaling error history", "job_id", jobID, "error", err)
	}

	// Check if error is non-retryable
	isNonRetryable := false
	if jobErr != nil && jobErr.Retryable != nil && !*jobErr.Retryable {
		isNonRetryable = true
	}

	// Check non-retryable error patterns from retry policy
	var retryPolicy *core.RetryPolicy
	if v, ok := data["retry"]; ok && v != "" {
		var rp core.RetryPolicy
		if err := json.Unmarshal([]byte(v), &rp); err != nil {
			slog.Error("nack: error parsing retry policy", "job_id", jobID, "error", err)
		} else {
			retryPolicy = &rp
		}
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

	// Discard path: non-retryable or max attempts reached
	if isNonRetryable || newAttempt >= maxAttempts {
		discardedAt := core.FormatTime(now)
		errJSONStr := ""
		if errJSON != nil {
			errJSONStr = string(errJSON)
		}
		nowMs := strconv.FormatInt(now.UnixMilli(), 10)

		res, scriptErr := nackDiscardScript.Run(ctx, b.client, nil,
			jobID,
			strconv.Itoa(newAttempt),
			errJSONStr,
			string(histJSON),
			discardedAt,
			onExhaustion,
			nowMs,
		).Result()
		if scriptErr != nil {
			return nil, fmt.Errorf("discard job: %w", scriptErr)
		}

		status, _ := parseLuaResult(res)
		if status != luaStatusOK {
			return nil, fmt.Errorf("discard job: unexpected status %d", status)
		}

		if err := b.advanceWorkflow(ctx, jobID, core.StateDiscarded, nil); err != nil {
			slog.Error("nack: error advancing workflow for discarded job", "job_id", jobID, "error", err)
		}

		job, infoErr := b.Info(ctx, jobID)
		if infoErr != nil {
			slog.Warn("nack-discard: failed to fetch job after discard", "job_id", jobID, "error", infoErr)
		}
		return &core.NackResponse{
			JobID:       jobID,
			State:       core.StateDiscarded,
			Attempt:     newAttempt,
			MaxAttempts: maxAttempts,
			DiscardedAt: discardedAt,
			Job:         job,
		}, nil
	}

	// Retry path
	backoff := core.CalculateBackoff(retryPolicy, newAttempt)
	backoffMs := backoff.Milliseconds()
	nextAttemptAt := now.Add(backoff)

	errJSONStr := ""
	if errJSON != nil {
		errJSONStr = string(errJSON)
	}

	res, scriptErr := nackRetryScript.Run(ctx, b.client, nil,
		jobID,
		strconv.Itoa(newAttempt),
		errJSONStr,
		string(histJSON),
		strconv.FormatInt(backoffMs, 10),
		strconv.FormatInt(nextAttemptAt.UnixMilli(), 10),
	).Result()
	if scriptErr != nil {
		return nil, fmt.Errorf("retry job: %w", scriptErr)
	}

	status, _ := parseLuaResult(res)
	if status != luaStatusOK {
		return nil, fmt.Errorf("retry job: unexpected status %d", status)
	}

	retryJob, infoErr := b.Info(ctx, jobID)
	if infoErr != nil {
		slog.Warn("nack-retry: failed to fetch job after retry scheduling", "job_id", jobID, "error", infoErr)
	}
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

// Cancel cancels a job using an atomic Lua script.
func (b *RedisBackend) Cancel(ctx context.Context, jobID string) (*core.Job, error) {
	now := core.NowFormatted()

	result, err := cancelScript.Run(ctx, b.client, nil, jobID, now).Result()
	if err != nil {
		return nil, fmt.Errorf("cancel job: %w", err)
	}

	status, data := parseLuaResult(result)
	switch status {
	case luaStatusNotFound:
		return nil, core.NewNotFoundError("Job", jobID)
	case luaStatusConflict:
		currentState := ""
		if len(data) > 0 {
			currentState, _ = data[0].(string)
		}
		return nil, core.NewConflictError(
			fmt.Sprintf("Cannot cancel job in terminal state '%s'.", currentState),
			map[string]any{
				"job_id":        jobID,
				"current_state": currentState,
			},
		)
	}

	return b.Info(ctx, jobID)
}
