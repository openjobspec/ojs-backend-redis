package redis

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/openjobspec/ojs-backend-redis/internal/core"
)

// jobToHash converts a Job to a map for Redis HSET.
func jobToHash(job *core.Job) (map[string]any, error) {
	h := map[string]any{
		"id":      job.ID,
		"type":    job.Type,
		"state":   job.State,
		"queue":   job.Queue,
		"attempt": strconv.Itoa(job.Attempt),
	}

	if job.Args != nil {
		h["args"] = string(job.Args)
	}
	if job.WorkerID != "" {
		h["worker_id"] = job.WorkerID
	}
	if len(job.Meta) > 0 {
		h["meta"] = string(job.Meta)
	}
	if job.Priority != nil {
		h["priority"] = strconv.Itoa(*job.Priority)
	}
	if job.MaxAttempts != nil {
		h["max_attempts"] = strconv.Itoa(*job.MaxAttempts)
	}
	if job.TimeoutMs != nil {
		h["timeout_ms"] = strconv.Itoa(*job.TimeoutMs)
	}
	if job.CreatedAt != "" {
		h["created_at"] = job.CreatedAt
	}
	if job.EnqueuedAt != "" {
		h["enqueued_at"] = job.EnqueuedAt
	}
	if job.StartedAt != "" {
		h["started_at"] = job.StartedAt
	}
	if job.CompletedAt != "" {
		h["completed_at"] = job.CompletedAt
	}
	if job.CancelledAt != "" {
		h["cancelled_at"] = job.CancelledAt
	}
	if job.ScheduledAt != "" {
		h["scheduled_at"] = job.ScheduledAt
	}
	if len(job.Result) > 0 {
		h["result"] = string(job.Result)
	}
	if len(job.Error) > 0 {
		h["error"] = string(job.Error)
	}
	if len(job.Tags) > 0 {
		tagsJSON, err := json.Marshal(job.Tags)
		if err != nil {
			return nil, fmt.Errorf("marshal tags: %w", err)
		}
		h["tags"] = string(tagsJSON)
	}
	if job.Retry != nil {
		retryJSON, err := json.Marshal(job.Retry)
		if err != nil {
			return nil, fmt.Errorf("marshal retry policy: %w", err)
		}
		h["retry"] = string(retryJSON)
	}
	if job.Unique != nil {
		uniqueJSON, err := json.Marshal(job.Unique)
		if err != nil {
			return nil, fmt.Errorf("marshal unique policy: %w", err)
		}
		h["unique"] = string(uniqueJSON)
	}
	if job.ExpiresAt != "" {
		h["expires_at"] = job.ExpiresAt
	}
	if job.VisibilityTimeoutMs != nil {
		h["visibility_timeout_ms"] = strconv.Itoa(*job.VisibilityTimeoutMs)
	}
	if job.WorkflowID != "" {
		h["workflow_id"] = job.WorkflowID
	}
	if job.WorkflowStep >= 0 && job.WorkflowID != "" {
		h["workflow_step"] = strconv.Itoa(job.WorkflowStep)
	}
	if len(job.ParentResults) > 0 {
		prJSON, err := json.Marshal(job.ParentResults)
		if err != nil {
			return nil, fmt.Errorf("marshal parent results: %w", err)
		}
		h["parent_results"] = string(prJSON)
	}

	// Store unknown fields
	for k, v := range job.UnknownFields {
		h["x:"+k] = string(v)
	}

	return h, nil
}

// hashToJob converts a Redis hash map to a Job.
func hashToJob(data map[string]string) *core.Job {
	if len(data) == 0 {
		return nil
	}

	job := &core.Job{
		ID:    data["id"],
		Type:  data["type"],
		State: data["state"],
		Queue: data["queue"],
	}

	if v, ok := data["args"]; ok {
		job.Args = json.RawMessage(v)
	}
	if v, ok := data["worker_id"]; ok && v != "" {
		job.WorkerID = v
	}
	if v, ok := data["meta"]; ok && v != "" {
		job.Meta = json.RawMessage(v)
	}
	if v, ok := data["priority"]; ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			job.Priority = &n
		}
	}
	if v, ok := data["attempt"]; ok {
		job.Attempt, _ = strconv.Atoi(v)
	}
	if v, ok := data["max_attempts"]; ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			job.MaxAttempts = &n
		}
	}
	if v, ok := data["timeout_ms"]; ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			job.TimeoutMs = &n
		}
	}
	if v, ok := data["created_at"]; ok {
		job.CreatedAt = v
	}
	if v, ok := data["enqueued_at"]; ok {
		job.EnqueuedAt = v
	}
	if v, ok := data["started_at"]; ok && v != "" {
		job.StartedAt = v
	}
	if v, ok := data["completed_at"]; ok && v != "" {
		job.CompletedAt = v
	}
	if v, ok := data["cancelled_at"]; ok && v != "" {
		job.CancelledAt = v
	}
	if v, ok := data["scheduled_at"]; ok && v != "" {
		job.ScheduledAt = v
	}
	if v, ok := data["result"]; ok && v != "" {
		job.Result = json.RawMessage(v)
	}
	if v, ok := data["error"]; ok && v != "" {
		job.Error = json.RawMessage(v)
	}
	if v, ok := data["tags"]; ok && v != "" {
		var tags []string
		if err := json.Unmarshal([]byte(v), &tags); err != nil {
			slog.Error("codec: error parsing tags", "job_id", job.ID, "error", err)
		} else {
			job.Tags = tags
		}
	}
	if v, ok := data["retry"]; ok && v != "" {
		var retry core.RetryPolicy
		if err := json.Unmarshal([]byte(v), &retry); err != nil {
			slog.Error("codec: error parsing retry policy", "job_id", job.ID, "error", err)
		} else {
			job.Retry = &retry
		}
	}
	if v, ok := data["unique"]; ok && v != "" {
		var unique core.UniquePolicy
		if err := json.Unmarshal([]byte(v), &unique); err != nil {
			slog.Error("codec: error parsing unique policy", "job_id", job.ID, "error", err)
		} else {
			job.Unique = &unique
		}
	}
	if v, ok := data["expires_at"]; ok && v != "" {
		job.ExpiresAt = v
	}
	if v, ok := data["error_history"]; ok && v != "" {
		var errors []json.RawMessage
		if json.Unmarshal([]byte(v), &errors) == nil {
			job.Errors = errors
		}
	}
	if v, ok := data["retry_delay_ms"]; ok && v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			job.RetryDelayMs = &n
		}
	}
	if v, ok := data["visibility_timeout_ms"]; ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			job.VisibilityTimeoutMs = &n
		}
	}
	if v, ok := data["workflow_id"]; ok && v != "" {
		job.WorkflowID = v
	}
	if v, ok := data["workflow_step"]; ok && v != "" {
		job.WorkflowStep, _ = strconv.Atoi(v)
	}
	if v, ok := data["parent_results"]; ok && v != "" {
		var pr []json.RawMessage
		if json.Unmarshal([]byte(v), &pr) == nil {
			job.ParentResults = pr
		}
	}

	// Restore unknown fields
	job.UnknownFields = make(map[string]json.RawMessage)
	for k, v := range data {
		if len(k) > 2 && k[:2] == "x:" {
			job.UnknownFields[k[2:]] = json.RawMessage(v)
		}
	}
	if len(job.UnknownFields) == 0 {
		job.UnknownFields = nil
	}

	return job
}
