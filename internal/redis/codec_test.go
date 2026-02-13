package redis

import (
	"encoding/json"
	"strconv"
	"testing"

	"github.com/openjobspec/ojs-backend-redis/internal/core"
)

func TestJobToHash(t *testing.T) {
	tests := []struct {
		name     string
		job      *core.Job
		wantKeys map[string]string // expected key→value pairs (values converted to string for comparison)
		omitKeys []string          // keys that must NOT be present
	}{
		{
			name: "minimal job with required fields only",
			job: &core.Job{
				ID:      "job-001",
				Type:    "email.send",
				State:   "available",
				Queue:   "default",
				Attempt: 0,
			},
			wantKeys: map[string]string{
				"id":      "job-001",
				"type":    "email.send",
				"state":   "available",
				"queue":   "default",
				"attempt": "0",
			},
			omitKeys: []string{
				"args", "meta", "priority", "max_attempts", "timeout_ms",
				"created_at", "enqueued_at", "started_at", "completed_at",
				"cancelled_at", "scheduled_at", "result", "error",
				"tags", "retry", "unique", "expires_at",
				"visibility_timeout_ms", "workflow_id", "workflow_step",
				"parent_results",
			},
		},
		{
			name: "all optional fields populated",
			job: &core.Job{
				ID:          "job-002",
				Type:        "report.generate",
				State:       "active",
				Queue:       "critical",
				Attempt:     2,
				Args:        json.RawMessage(`["arg1","arg2"]`),
				Meta:        json.RawMessage(`{"user":"admin"}`),
				Priority:    intPtr(10),
				MaxAttempts: intPtr(5),
				TimeoutMs:   intPtr(30000),
				CreatedAt:   "2025-01-01T00:00:00.000Z",
				EnqueuedAt:  "2025-01-01T00:00:01.000Z",
				StartedAt:   "2025-01-01T00:00:02.000Z",
				CompletedAt: "2025-01-01T00:00:03.000Z",
				CancelledAt: "2025-01-01T00:00:04.000Z",
				ScheduledAt: "2025-01-01T00:00:05.000Z",
				Result:      json.RawMessage(`{"status":"ok"}`),
				Error:       json.RawMessage(`{"message":"failed"}`),
				Tags:        []string{"urgent", "billing"},
				Retry: &core.RetryPolicy{
					MaxAttempts: 3,
					BackoffType: "exponential",
				},
				Unique: &core.UniquePolicy{
					Keys:   []string{"type", "args"},
					Period: "5m",
				},
				ExpiresAt:           "2025-01-02T00:00:00.000Z",
				VisibilityTimeoutMs: intPtr(60000),
				WorkflowID:          "wf-100",
				WorkflowStep:        2,
				ParentResults:       []json.RawMessage{json.RawMessage(`{"ok":true}`)},
			},
			wantKeys: map[string]string{
				"id":                    "job-002",
				"type":                  "report.generate",
				"state":                 "active",
				"queue":                 "critical",
				"attempt":              "2",
				"args":                  `["arg1","arg2"]`,
				"meta":                  `{"user":"admin"}`,
				"priority":             "10",
				"max_attempts":          "5",
				"timeout_ms":            "30000",
				"created_at":            "2025-01-01T00:00:00.000Z",
				"enqueued_at":           "2025-01-01T00:00:01.000Z",
				"started_at":            "2025-01-01T00:00:02.000Z",
				"completed_at":          "2025-01-01T00:00:03.000Z",
				"cancelled_at":          "2025-01-01T00:00:04.000Z",
				"scheduled_at":          "2025-01-01T00:00:05.000Z",
				"result":                `{"status":"ok"}`,
				"error":                 `{"message":"failed"}`,
				"expires_at":            "2025-01-02T00:00:00.000Z",
				"visibility_timeout_ms": "60000",
				"workflow_id":           "wf-100",
				"workflow_step":         "2",
			},
		},
		{
			name: "unknown fields stored with x: prefix",
			job: &core.Job{
				ID:      "job-003",
				Type:    "test",
				State:   "available",
				Queue:   "default",
				Attempt: 0,
				UnknownFields: map[string]json.RawMessage{
					"custom_field":  json.RawMessage(`"custom_value"`),
					"another_field": json.RawMessage(`42`),
				},
			},
			wantKeys: map[string]string{
				"id":              "job-003",
				"type":            "test",
				"state":           "available",
				"queue":           "default",
				"attempt":         "0",
				"x:custom_field":  `"custom_value"`,
				"x:another_field": "42",
			},
		},
		{
			name: "nil optional pointer fields omitted from map",
			job: &core.Job{
				ID:          "job-004",
				Type:        "noop",
				State:       "completed",
				Queue:       "low",
				Attempt:     1,
				Priority:    nil,
				MaxAttempts: nil,
				TimeoutMs:   nil,
				Args:        nil,
				Meta:        nil,
				Result:      nil,
				Error:       nil,
				Tags:        nil,
				Retry:       nil,
				Unique:      nil,
				VisibilityTimeoutMs: nil,
			},
			wantKeys: map[string]string{
				"id":      "job-004",
				"type":    "noop",
				"state":   "completed",
				"queue":   "low",
				"attempt": "1",
			},
			omitKeys: []string{
				"priority", "max_attempts", "timeout_ms",
				"args", "meta", "result", "error",
				"tags", "retry", "unique", "visibility_timeout_ms",
				"workflow_id", "workflow_step", "parent_results",
			},
		},
		{
			name: "tags marshaled as JSON array",
			job: &core.Job{
				ID:      "job-005",
				Type:    "tag-test",
				State:   "available",
				Queue:   "default",
				Attempt: 0,
				Tags:    []string{"alpha", "beta", "gamma"},
			},
			wantKeys: map[string]string{
				"tags": `["alpha","beta","gamma"]`,
			},
		},
		{
			name: "retry policy marshaled as JSON",
			job: &core.Job{
				ID:      "job-006",
				Type:    "retry-test",
				State:   "available",
				Queue:   "default",
				Attempt: 0,
				Retry: &core.RetryPolicy{
					MaxAttempts:        5,
					InitialInterval:    "1s",
					BackoffCoefficient: 2.0,
					MaxInterval:        "30s",
					Jitter:             true,
				},
			},
		},
		{
			name: "unique policy marshaled as JSON",
			job: &core.Job{
				ID:      "job-007",
				Type:    "unique-test",
				State:   "available",
				Queue:   "default",
				Attempt: 0,
				Unique: &core.UniquePolicy{
					Keys:       []string{"type", "args"},
					Period:     "10m",
					OnConflict: "reject",
				},
			},
		},
		{
			name: "parent results marshaled as JSON array",
			job: &core.Job{
				ID:      "job-008",
				Type:    "workflow-child",
				State:   "available",
				Queue:   "default",
				Attempt: 0,
				ParentResults: []json.RawMessage{
					json.RawMessage(`{"step":1,"ok":true}`),
					json.RawMessage(`{"step":2,"ok":false}`),
				},
			},
		},
		{
			name: "workflow step zero with workflow ID is stored",
			job: &core.Job{
				ID:           "job-009",
				Type:         "wf-step",
				State:        "available",
				Queue:        "default",
				Attempt:      0,
				WorkflowID:   "wf-200",
				WorkflowStep: 0,
			},
			wantKeys: map[string]string{
				"workflow_id":   "wf-200",
				"workflow_step": "0",
			},
		},
		{
			name: "workflow step without workflow ID is not stored",
			job: &core.Job{
				ID:           "job-010",
				Type:         "no-wf",
				State:        "available",
				Queue:        "default",
				Attempt:      0,
				WorkflowID:   "",
				WorkflowStep: 3,
			},
			omitKeys: []string{"workflow_id", "workflow_step"},
		},
		{
			name: "empty meta byte slice omitted",
			job: &core.Job{
				ID:      "job-011",
				Type:    "meta-test",
				State:   "available",
				Queue:   "default",
				Attempt: 0,
				Meta:    json.RawMessage{},
			},
			omitKeys: []string{"meta"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := jobToHash(tt.job)

			// Verify expected key-value pairs
			for key, wantVal := range tt.wantKeys {
				got, ok := h[key]
				if !ok {
					t.Errorf("missing key %q in hash", key)
					continue
				}
				gotStr, isStr := got.(string)
				if !isStr {
					t.Errorf("key %q: expected string value, got %T", key, got)
					continue
				}
				if gotStr != wantVal {
					t.Errorf("key %q: got %q, want %q", key, gotStr, wantVal)
				}
			}

			// Verify omitted keys
			for _, key := range tt.omitKeys {
				if _, ok := h[key]; ok {
					t.Errorf("key %q should be omitted from hash but is present", key)
				}
			}
		})
	}
}

func TestJobToHash_RetryJSON(t *testing.T) {
	job := &core.Job{
		ID: "job-retry", Type: "t", State: "available", Queue: "q",
		Retry: &core.RetryPolicy{
			MaxAttempts:        5,
			InitialInterval:    "1s",
			BackoffCoefficient: 2.0,
		},
	}
	h := jobToHash(job)
	raw, ok := h["retry"]
	if !ok {
		t.Fatal("retry key missing")
	}
	var rp core.RetryPolicy
	if err := json.Unmarshal([]byte(raw.(string)), &rp); err != nil {
		t.Fatalf("retry unmarshal error: %v", err)
	}
	if rp.MaxAttempts != 5 {
		t.Errorf("retry max_attempts: got %d, want 5", rp.MaxAttempts)
	}
	if rp.InitialInterval != "1s" {
		t.Errorf("retry initial_interval: got %q, want %q", rp.InitialInterval, "1s")
	}
	if rp.BackoffCoefficient != 2.0 {
		t.Errorf("retry backoff_coefficient: got %f, want 2.0", rp.BackoffCoefficient)
	}
}

func TestJobToHash_UniqueJSON(t *testing.T) {
	job := &core.Job{
		ID: "job-unique", Type: "t", State: "available", Queue: "q",
		Unique: &core.UniquePolicy{
			Keys:       []string{"type", "args"},
			Period:     "10m",
			OnConflict: "reject",
		},
	}
	h := jobToHash(job)
	raw, ok := h["unique"]
	if !ok {
		t.Fatal("unique key missing")
	}
	var up core.UniquePolicy
	if err := json.Unmarshal([]byte(raw.(string)), &up); err != nil {
		t.Fatalf("unique unmarshal error: %v", err)
	}
	if len(up.Keys) != 2 || up.Keys[0] != "type" || up.Keys[1] != "args" {
		t.Errorf("unique keys: got %v, want [type args]", up.Keys)
	}
	if up.Period != "10m" {
		t.Errorf("unique period: got %q, want %q", up.Period, "10m")
	}
	if up.OnConflict != "reject" {
		t.Errorf("unique on_conflict: got %q, want %q", up.OnConflict, "reject")
	}
}

func TestHashToJob(t *testing.T) {
	tests := []struct {
		name   string
		data   map[string]string
		verify func(t *testing.T, job *core.Job)
		isNil  bool
	}{
		{
			name:  "empty map returns nil",
			data:  map[string]string{},
			isNil: true,
		},
		{
			name: "minimal fields produce correct Job struct",
			data: map[string]string{
				"id":    "job-100",
				"type":  "email.send",
				"state": "available",
				"queue": "default",
			},
			verify: func(t *testing.T, job *core.Job) {
				if job.ID != "job-100" {
					t.Errorf("ID: got %q, want %q", job.ID, "job-100")
				}
				if job.Type != "email.send" {
					t.Errorf("Type: got %q, want %q", job.Type, "email.send")
				}
				if job.State != "available" {
					t.Errorf("State: got %q, want %q", job.State, "available")
				}
				if job.Queue != "default" {
					t.Errorf("Queue: got %q, want %q", job.Queue, "default")
				}
				if job.Args != nil {
					t.Errorf("Args: expected nil, got %s", string(job.Args))
				}
				if job.Priority != nil {
					t.Errorf("Priority: expected nil, got %d", *job.Priority)
				}
				if job.MaxAttempts != nil {
					t.Errorf("MaxAttempts: expected nil, got %d", *job.MaxAttempts)
				}
				if job.TimeoutMs != nil {
					t.Errorf("TimeoutMs: expected nil, got %d", *job.TimeoutMs)
				}
				if job.UnknownFields != nil {
					t.Errorf("UnknownFields: expected nil, got %v", job.UnknownFields)
				}
			},
		},
		{
			name: "all fields populated produce complete Job struct",
			data: map[string]string{
				"id":                    "job-200",
				"type":                  "report.generate",
				"state":                 "completed",
				"queue":                 "critical",
				"attempt":              "3",
				"args":                  `["a","b"]`,
				"meta":                  `{"k":"v"}`,
				"priority":             "5",
				"max_attempts":          "10",
				"timeout_ms":            "45000",
				"created_at":            "2025-01-01T00:00:00.000Z",
				"enqueued_at":           "2025-01-01T00:00:01.000Z",
				"started_at":            "2025-01-01T00:00:02.000Z",
				"completed_at":          "2025-01-01T00:00:03.000Z",
				"cancelled_at":          "2025-01-01T00:00:04.000Z",
				"scheduled_at":          "2025-01-01T00:00:05.000Z",
				"result":                `{"status":"done"}`,
				"error":                 `{"message":"oops"}`,
				"tags":                  `["tag1","tag2"]`,
				"retry":                 `{"max_attempts":3,"backoff_type":"exponential"}`,
				"unique":                `{"keys":["type"],"period":"5m"}`,
				"expires_at":            "2025-01-02T00:00:00.000Z",
				"visibility_timeout_ms": "60000",
				"workflow_id":           "wf-500",
				"workflow_step":         "4",
				"parent_results":        `[{"ok":true}]`,
				"error_history":         `[{"message":"err1"},{"message":"err2"}]`,
				"retry_delay_ms":        "5000",
			},
			verify: func(t *testing.T, job *core.Job) {
				if job.ID != "job-200" {
					t.Errorf("ID: got %q, want %q", job.ID, "job-200")
				}
				if job.Type != "report.generate" {
					t.Errorf("Type: got %q, want %q", job.Type, "report.generate")
				}
				if job.State != "completed" {
					t.Errorf("State: got %q, want %q", job.State, "completed")
				}
				if job.Queue != "critical" {
					t.Errorf("Queue: got %q, want %q", job.Queue, "critical")
				}
				if job.Attempt != 3 {
					t.Errorf("Attempt: got %d, want 3", job.Attempt)
				}
				if string(job.Args) != `["a","b"]` {
					t.Errorf("Args: got %s, want %s", string(job.Args), `["a","b"]`)
				}
				if string(job.Meta) != `{"k":"v"}` {
					t.Errorf("Meta: got %s, want %s", string(job.Meta), `{"k":"v"}`)
				}
				if job.Priority == nil || *job.Priority != 5 {
					t.Errorf("Priority: got %v, want 5", job.Priority)
				}
				if job.MaxAttempts == nil || *job.MaxAttempts != 10 {
					t.Errorf("MaxAttempts: got %v, want 10", job.MaxAttempts)
				}
				if job.TimeoutMs == nil || *job.TimeoutMs != 45000 {
					t.Errorf("TimeoutMs: got %v, want 45000", job.TimeoutMs)
				}
				if job.CreatedAt != "2025-01-01T00:00:00.000Z" {
					t.Errorf("CreatedAt: got %q", job.CreatedAt)
				}
				if job.EnqueuedAt != "2025-01-01T00:00:01.000Z" {
					t.Errorf("EnqueuedAt: got %q", job.EnqueuedAt)
				}
				if job.StartedAt != "2025-01-01T00:00:02.000Z" {
					t.Errorf("StartedAt: got %q", job.StartedAt)
				}
				if job.CompletedAt != "2025-01-01T00:00:03.000Z" {
					t.Errorf("CompletedAt: got %q", job.CompletedAt)
				}
				if job.CancelledAt != "2025-01-01T00:00:04.000Z" {
					t.Errorf("CancelledAt: got %q", job.CancelledAt)
				}
				if job.ScheduledAt != "2025-01-01T00:00:05.000Z" {
					t.Errorf("ScheduledAt: got %q", job.ScheduledAt)
				}
				if string(job.Result) != `{"status":"done"}` {
					t.Errorf("Result: got %s", string(job.Result))
				}
				if string(job.Error) != `{"message":"oops"}` {
					t.Errorf("Error: got %s", string(job.Error))
				}
				if len(job.Tags) != 2 || job.Tags[0] != "tag1" || job.Tags[1] != "tag2" {
					t.Errorf("Tags: got %v, want [tag1 tag2]", job.Tags)
				}
				if job.Retry == nil {
					t.Fatal("Retry: expected non-nil")
				}
				if job.Retry.MaxAttempts != 3 {
					t.Errorf("Retry.MaxAttempts: got %d, want 3", job.Retry.MaxAttempts)
				}
				if job.Retry.BackoffType != "exponential" {
					t.Errorf("Retry.BackoffType: got %q, want %q", job.Retry.BackoffType, "exponential")
				}
				if job.Unique == nil {
					t.Fatal("Unique: expected non-nil")
				}
				if len(job.Unique.Keys) != 1 || job.Unique.Keys[0] != "type" {
					t.Errorf("Unique.Keys: got %v, want [type]", job.Unique.Keys)
				}
				if job.Unique.Period != "5m" {
					t.Errorf("Unique.Period: got %q, want %q", job.Unique.Period, "5m")
				}
				if job.ExpiresAt != "2025-01-02T00:00:00.000Z" {
					t.Errorf("ExpiresAt: got %q", job.ExpiresAt)
				}
				if job.VisibilityTimeoutMs == nil || *job.VisibilityTimeoutMs != 60000 {
					t.Errorf("VisibilityTimeoutMs: got %v, want 60000", job.VisibilityTimeoutMs)
				}
				if job.WorkflowID != "wf-500" {
					t.Errorf("WorkflowID: got %q, want %q", job.WorkflowID, "wf-500")
				}
				if job.WorkflowStep != 4 {
					t.Errorf("WorkflowStep: got %d, want 4", job.WorkflowStep)
				}
				if len(job.ParentResults) != 1 {
					t.Fatalf("ParentResults: got len %d, want 1", len(job.ParentResults))
				}
				if string(job.ParentResults[0]) != `{"ok":true}` {
					t.Errorf("ParentResults[0]: got %s", string(job.ParentResults[0]))
				}
				if len(job.Errors) != 2 {
					t.Fatalf("Errors: got len %d, want 2", len(job.Errors))
				}
				if string(job.Errors[0]) != `{"message":"err1"}` {
					t.Errorf("Errors[0]: got %s", string(job.Errors[0]))
				}
				if string(job.Errors[1]) != `{"message":"err2"}` {
					t.Errorf("Errors[1]: got %s", string(job.Errors[1]))
				}
				if job.RetryDelayMs == nil || *job.RetryDelayMs != 5000 {
					t.Errorf("RetryDelayMs: got %v, want 5000", job.RetryDelayMs)
				}
			},
		},
		{
			name: "unknown fields with x: prefix restored in UnknownFields",
			data: map[string]string{
				"id":              "job-300",
				"type":            "test",
				"state":           "available",
				"queue":           "default",
				"x:custom_field":  `"hello"`,
				"x:another_field": `{"nested":true}`,
			},
			verify: func(t *testing.T, job *core.Job) {
				if job.UnknownFields == nil {
					t.Fatal("UnknownFields: expected non-nil")
				}
				if len(job.UnknownFields) != 2 {
					t.Errorf("UnknownFields: got len %d, want 2", len(job.UnknownFields))
				}
				if string(job.UnknownFields["custom_field"]) != `"hello"` {
					t.Errorf("UnknownFields[custom_field]: got %s, want %s", string(job.UnknownFields["custom_field"]), `"hello"`)
				}
				if string(job.UnknownFields["another_field"]) != `{"nested":true}` {
					t.Errorf("UnknownFields[another_field]: got %s, want %s", string(job.UnknownFields["another_field"]), `{"nested":true}`)
				}
			},
		},
		{
			name: "priority parsed from string",
			data: map[string]string{
				"id":       "job-400",
				"type":     "t",
				"state":    "available",
				"queue":    "q",
				"priority": "7",
			},
			verify: func(t *testing.T, job *core.Job) {
				if job.Priority == nil || *job.Priority != 7 {
					t.Errorf("Priority: got %v, want 7", job.Priority)
				}
			},
		},
		{
			name: "max_attempts parsed from string",
			data: map[string]string{
				"id":           "job-401",
				"type":         "t",
				"state":        "available",
				"queue":        "q",
				"max_attempts": "25",
			},
			verify: func(t *testing.T, job *core.Job) {
				if job.MaxAttempts == nil || *job.MaxAttempts != 25 {
					t.Errorf("MaxAttempts: got %v, want 25", job.MaxAttempts)
				}
			},
		},
		{
			name: "timeout_ms parsed from string",
			data: map[string]string{
				"id":         "job-402",
				"type":       "t",
				"state":      "available",
				"queue":      "q",
				"timeout_ms": "120000",
			},
			verify: func(t *testing.T, job *core.Job) {
				if job.TimeoutMs == nil || *job.TimeoutMs != 120000 {
					t.Errorf("TimeoutMs: got %v, want 120000", job.TimeoutMs)
				}
			},
		},
		{
			name: "tags parsed from JSON string",
			data: map[string]string{
				"id":    "job-403",
				"type":  "t",
				"state": "available",
				"queue": "q",
				"tags":  `["a","b","c"]`,
			},
			verify: func(t *testing.T, job *core.Job) {
				want := []string{"a", "b", "c"}
				if len(job.Tags) != len(want) {
					t.Fatalf("Tags length: got %d, want %d", len(job.Tags), len(want))
				}
				for i, v := range want {
					if job.Tags[i] != v {
						t.Errorf("Tags[%d]: got %q, want %q", i, job.Tags[i], v)
					}
				}
			},
		},
		{
			name: "retry parsed from JSON string",
			data: map[string]string{
				"id":    "job-404",
				"type":  "t",
				"state": "available",
				"queue": "q",
				"retry": `{"max_attempts":8,"initial_interval":"2s","backoff_coefficient":1.5,"jitter":true}`,
			},
			verify: func(t *testing.T, job *core.Job) {
				if job.Retry == nil {
					t.Fatal("Retry: expected non-nil")
				}
				if job.Retry.MaxAttempts != 8 {
					t.Errorf("Retry.MaxAttempts: got %d, want 8", job.Retry.MaxAttempts)
				}
				if job.Retry.InitialInterval != "2s" {
					t.Errorf("Retry.InitialInterval: got %q, want %q", job.Retry.InitialInterval, "2s")
				}
				if job.Retry.BackoffCoefficient != 1.5 {
					t.Errorf("Retry.BackoffCoefficient: got %f, want 1.5", job.Retry.BackoffCoefficient)
				}
				if !job.Retry.Jitter {
					t.Errorf("Retry.Jitter: got false, want true")
				}
			},
		},
		{
			name: "error_history parsed from JSON string",
			data: map[string]string{
				"id":            "job-405",
				"type":          "t",
				"state":         "retryable",
				"queue":         "q",
				"error_history": `[{"message":"timeout"},{"message":"connection refused"}]`,
			},
			verify: func(t *testing.T, job *core.Job) {
				if len(job.Errors) != 2 {
					t.Fatalf("Errors length: got %d, want 2", len(job.Errors))
				}
				if string(job.Errors[0]) != `{"message":"timeout"}` {
					t.Errorf("Errors[0]: got %s, want %s", string(job.Errors[0]), `{"message":"timeout"}`)
				}
				if string(job.Errors[1]) != `{"message":"connection refused"}` {
					t.Errorf("Errors[1]: got %s, want %s", string(job.Errors[1]), `{"message":"connection refused"}`)
				}
			},
		},
		{
			name: "retry_delay_ms parsed from string",
			data: map[string]string{
				"id":             "job-406",
				"type":           "t",
				"state":          "retryable",
				"queue":          "q",
				"retry_delay_ms": "15000",
			},
			verify: func(t *testing.T, job *core.Job) {
				if job.RetryDelayMs == nil || *job.RetryDelayMs != 15000 {
					t.Errorf("RetryDelayMs: got %v, want 15000", job.RetryDelayMs)
				}
			},
		},
		{
			name: "no unknown fields results in nil UnknownFields",
			data: map[string]string{
				"id":    "job-500",
				"type":  "t",
				"state": "available",
				"queue": "q",
			},
			verify: func(t *testing.T, job *core.Job) {
				if job.UnknownFields != nil {
					t.Errorf("UnknownFields: expected nil when no x: keys, got %v", job.UnknownFields)
				}
			},
		},
		{
			name: "empty string values for optional fields treated as absent",
			data: map[string]string{
				"id":           "job-600",
				"type":         "t",
				"state":        "available",
				"queue":        "q",
				"priority":     "",
				"max_attempts": "",
				"timeout_ms":   "",
				"started_at":   "",
				"completed_at": "",
				"cancelled_at": "",
				"scheduled_at": "",
				"result":       "",
				"error":        "",
				"tags":         "",
				"retry":        "",
				"unique":       "",
				"meta":         "",
				"expires_at":   "",
			},
			verify: func(t *testing.T, job *core.Job) {
				if job.Priority != nil {
					t.Errorf("Priority: expected nil for empty string")
				}
				if job.MaxAttempts != nil {
					t.Errorf("MaxAttempts: expected nil for empty string")
				}
				if job.TimeoutMs != nil {
					t.Errorf("TimeoutMs: expected nil for empty string")
				}
				if job.StartedAt != "" {
					t.Errorf("StartedAt: expected empty for empty string")
				}
				if job.CompletedAt != "" {
					t.Errorf("CompletedAt: expected empty for empty string")
				}
				if job.CancelledAt != "" {
					t.Errorf("CancelledAt: expected empty for empty string")
				}
				if job.ScheduledAt != "" {
					t.Errorf("ScheduledAt: expected empty for empty string")
				}
				if job.Result != nil {
					t.Errorf("Result: expected nil for empty string")
				}
				if job.Error != nil {
					t.Errorf("Error: expected nil for empty string")
				}
				if job.Tags != nil {
					t.Errorf("Tags: expected nil for empty string")
				}
				if job.Retry != nil {
					t.Errorf("Retry: expected nil for empty string")
				}
				if job.Unique != nil {
					t.Errorf("Unique: expected nil for empty string")
				}
				if job.Meta != nil {
					t.Errorf("Meta: expected nil for empty string")
				}
				if job.ExpiresAt != "" {
					t.Errorf("ExpiresAt: expected empty for empty string")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job := hashToJob(tt.data)
			if tt.isNil {
				if job != nil {
					t.Errorf("expected nil job, got %+v", job)
				}
				return
			}
			if job == nil {
				t.Fatal("expected non-nil job, got nil")
			}
			if tt.verify != nil {
				tt.verify(t, job)
			}
		})
	}
}

func TestRoundtrip(t *testing.T) {
	tests := []struct {
		name string
		job  *core.Job
	}{
		{
			name: "minimal job roundtrips correctly",
			job: &core.Job{
				ID:      "rt-001",
				Type:    "email.send",
				State:   "available",
				Queue:   "default",
				Attempt: 0,
			},
		},
		{
			name: "job with all scalar fields roundtrips correctly",
			job: &core.Job{
				ID:          "rt-002",
				Type:        "report.generate",
				State:       "active",
				Queue:       "high",
				Attempt:     3,
				Args:        json.RawMessage(`["x","y","z"]`),
				Meta:        json.RawMessage(`{"trace_id":"abc"}`),
				Priority:    intPtr(8),
				MaxAttempts: intPtr(10),
				TimeoutMs:   intPtr(60000),
				CreatedAt:   "2025-06-01T12:00:00.000Z",
				EnqueuedAt:  "2025-06-01T12:00:01.000Z",
				StartedAt:   "2025-06-01T12:00:02.000Z",
				CompletedAt: "2025-06-01T12:00:03.000Z",
				CancelledAt: "2025-06-01T12:00:04.000Z",
				ScheduledAt: "2025-06-01T12:00:05.000Z",
				Result:      json.RawMessage(`{"rows":42}`),
				Error:       json.RawMessage(`{"message":"fail"}`),
				ExpiresAt:   "2025-06-02T12:00:00.000Z",
				VisibilityTimeoutMs: intPtr(30000),
				WorkflowID:          "wf-rt",
				WorkflowStep:        1,
			},
		},
		{
			name: "job with tags roundtrips correctly",
			job: &core.Job{
				ID:      "rt-003",
				Type:    "t",
				State:   "available",
				Queue:   "q",
				Attempt: 0,
				Tags:    []string{"alpha", "beta"},
			},
		},
		{
			name: "job with retry policy roundtrips correctly",
			job: &core.Job{
				ID:      "rt-004",
				Type:    "t",
				State:   "available",
				Queue:   "q",
				Attempt: 0,
				Retry: &core.RetryPolicy{
					MaxAttempts:        5,
					InitialInterval:    "1s",
					BackoffCoefficient: 2.0,
					MaxInterval:        "30s",
					Jitter:             true,
					BackoffType:        "exponential",
				},
			},
		},
		{
			name: "job with unique policy roundtrips correctly",
			job: &core.Job{
				ID:      "rt-005",
				Type:    "t",
				State:   "available",
				Queue:   "q",
				Attempt: 0,
				Unique: &core.UniquePolicy{
					Keys:       []string{"type", "args"},
					Period:     "10m",
					OnConflict: "replace",
					States:     []string{"available", "active"},
				},
			},
		},
		{
			name: "job with unknown fields roundtrips correctly",
			job: &core.Job{
				ID:      "rt-006",
				Type:    "t",
				State:   "available",
				Queue:   "q",
				Attempt: 0,
				UnknownFields: map[string]json.RawMessage{
					"vendor_ext":  json.RawMessage(`"value123"`),
					"numeric_ext": json.RawMessage(`99`),
				},
			},
		},
		{
			name: "job with parent results roundtrips correctly",
			job: &core.Job{
				ID:         "rt-007",
				Type:       "t",
				State:      "available",
				Queue:      "q",
				Attempt:    0,
				WorkflowID: "wf-pr",
				ParentResults: []json.RawMessage{
					json.RawMessage(`{"step":0,"data":"ok"}`),
					json.RawMessage(`{"step":1,"data":"done"}`),
				},
			},
		},
		{
			name: "job with zero priority roundtrips correctly",
			job: &core.Job{
				ID:       "rt-008",
				Type:     "t",
				State:    "available",
				Queue:    "q",
				Attempt:  0,
				Priority: intPtr(0),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Convert Job -> hash
			h := jobToHash(tt.job)

			// Convert hash values to map[string]string (as Redis stores them)
			strMap := make(map[string]string, len(h))
			for k, v := range h {
				strMap[k] = v.(string)
			}

			// Convert string map -> Job
			got := hashToJob(strMap)
			if got == nil {
				t.Fatal("hashToJob returned nil for non-empty hash")
			}

			// Verify core fields
			if got.ID != tt.job.ID {
				t.Errorf("ID: got %q, want %q", got.ID, tt.job.ID)
			}
			if got.Type != tt.job.Type {
				t.Errorf("Type: got %q, want %q", got.Type, tt.job.Type)
			}
			if got.State != tt.job.State {
				t.Errorf("State: got %q, want %q", got.State, tt.job.State)
			}
			if got.Queue != tt.job.Queue {
				t.Errorf("Queue: got %q, want %q", got.Queue, tt.job.Queue)
			}
			if got.Attempt != tt.job.Attempt {
				t.Errorf("Attempt: got %d, want %d", got.Attempt, tt.job.Attempt)
			}

			// Args
			if string(got.Args) != string(tt.job.Args) {
				t.Errorf("Args: got %s, want %s", string(got.Args), string(tt.job.Args))
			}
			// Meta
			if string(got.Meta) != string(tt.job.Meta) {
				t.Errorf("Meta: got %s, want %s", string(got.Meta), string(tt.job.Meta))
			}

			// Priority
			if (tt.job.Priority == nil) != (got.Priority == nil) {
				t.Errorf("Priority nil mismatch: got %v, want %v", got.Priority, tt.job.Priority)
			} else if tt.job.Priority != nil && *got.Priority != *tt.job.Priority {
				t.Errorf("Priority: got %d, want %d", *got.Priority, *tt.job.Priority)
			}

			// MaxAttempts
			if (tt.job.MaxAttempts == nil) != (got.MaxAttempts == nil) {
				t.Errorf("MaxAttempts nil mismatch: got %v, want %v", got.MaxAttempts, tt.job.MaxAttempts)
			} else if tt.job.MaxAttempts != nil && *got.MaxAttempts != *tt.job.MaxAttempts {
				t.Errorf("MaxAttempts: got %d, want %d", *got.MaxAttempts, *tt.job.MaxAttempts)
			}

			// TimeoutMs
			if (tt.job.TimeoutMs == nil) != (got.TimeoutMs == nil) {
				t.Errorf("TimeoutMs nil mismatch: got %v, want %v", got.TimeoutMs, tt.job.TimeoutMs)
			} else if tt.job.TimeoutMs != nil && *got.TimeoutMs != *tt.job.TimeoutMs {
				t.Errorf("TimeoutMs: got %d, want %d", *got.TimeoutMs, *tt.job.TimeoutMs)
			}

			// Timestamps
			if got.CreatedAt != tt.job.CreatedAt {
				t.Errorf("CreatedAt: got %q, want %q", got.CreatedAt, tt.job.CreatedAt)
			}
			if got.EnqueuedAt != tt.job.EnqueuedAt {
				t.Errorf("EnqueuedAt: got %q, want %q", got.EnqueuedAt, tt.job.EnqueuedAt)
			}
			if got.StartedAt != tt.job.StartedAt {
				t.Errorf("StartedAt: got %q, want %q", got.StartedAt, tt.job.StartedAt)
			}
			if got.CompletedAt != tt.job.CompletedAt {
				t.Errorf("CompletedAt: got %q, want %q", got.CompletedAt, tt.job.CompletedAt)
			}
			if got.CancelledAt != tt.job.CancelledAt {
				t.Errorf("CancelledAt: got %q, want %q", got.CancelledAt, tt.job.CancelledAt)
			}
			if got.ScheduledAt != tt.job.ScheduledAt {
				t.Errorf("ScheduledAt: got %q, want %q", got.ScheduledAt, tt.job.ScheduledAt)
			}

			// Result & Error
			if string(got.Result) != string(tt.job.Result) {
				t.Errorf("Result: got %s, want %s", string(got.Result), string(tt.job.Result))
			}
			if string(got.Error) != string(tt.job.Error) {
				t.Errorf("Error: got %s, want %s", string(got.Error), string(tt.job.Error))
			}

			// ExpiresAt
			if got.ExpiresAt != tt.job.ExpiresAt {
				t.Errorf("ExpiresAt: got %q, want %q", got.ExpiresAt, tt.job.ExpiresAt)
			}

			// VisibilityTimeoutMs
			if (tt.job.VisibilityTimeoutMs == nil) != (got.VisibilityTimeoutMs == nil) {
				t.Errorf("VisibilityTimeoutMs nil mismatch: got %v, want %v", got.VisibilityTimeoutMs, tt.job.VisibilityTimeoutMs)
			} else if tt.job.VisibilityTimeoutMs != nil && *got.VisibilityTimeoutMs != *tt.job.VisibilityTimeoutMs {
				t.Errorf("VisibilityTimeoutMs: got %d, want %d", *got.VisibilityTimeoutMs, *tt.job.VisibilityTimeoutMs)
			}

			// WorkflowID
			if got.WorkflowID != tt.job.WorkflowID {
				t.Errorf("WorkflowID: got %q, want %q", got.WorkflowID, tt.job.WorkflowID)
			}

			// Tags
			if len(got.Tags) != len(tt.job.Tags) {
				t.Errorf("Tags length: got %d, want %d", len(got.Tags), len(tt.job.Tags))
			} else {
				for i, v := range tt.job.Tags {
					if got.Tags[i] != v {
						t.Errorf("Tags[%d]: got %q, want %q", i, got.Tags[i], v)
					}
				}
			}

			// Retry
			if (tt.job.Retry == nil) != (got.Retry == nil) {
				t.Errorf("Retry nil mismatch: got %v, want %v", got.Retry, tt.job.Retry)
			} else if tt.job.Retry != nil {
				wantJSON, _ := json.Marshal(tt.job.Retry)
				gotJSON, _ := json.Marshal(got.Retry)
				if string(gotJSON) != string(wantJSON) {
					t.Errorf("Retry: got %s, want %s", string(gotJSON), string(wantJSON))
				}
			}

			// Unique
			if (tt.job.Unique == nil) != (got.Unique == nil) {
				t.Errorf("Unique nil mismatch: got %v, want %v", got.Unique, tt.job.Unique)
			} else if tt.job.Unique != nil {
				wantJSON, _ := json.Marshal(tt.job.Unique)
				gotJSON, _ := json.Marshal(got.Unique)
				if string(gotJSON) != string(wantJSON) {
					t.Errorf("Unique: got %s, want %s", string(gotJSON), string(wantJSON))
				}
			}

			// UnknownFields
			if len(tt.job.UnknownFields) != len(got.UnknownFields) {
				t.Errorf("UnknownFields length: got %d, want %d", len(got.UnknownFields), len(tt.job.UnknownFields))
			} else {
				for k, wantVal := range tt.job.UnknownFields {
					gotVal, ok := got.UnknownFields[k]
					if !ok {
						t.Errorf("UnknownFields: missing key %q", k)
					} else if string(gotVal) != string(wantVal) {
						t.Errorf("UnknownFields[%q]: got %s, want %s", k, string(gotVal), string(wantVal))
					}
				}
			}

			// ParentResults
			if len(got.ParentResults) != len(tt.job.ParentResults) {
				t.Errorf("ParentResults length: got %d, want %d", len(got.ParentResults), len(tt.job.ParentResults))
			} else {
				for i, wantPR := range tt.job.ParentResults {
					if string(got.ParentResults[i]) != string(wantPR) {
						t.Errorf("ParentResults[%d]: got %s, want %s", i, string(got.ParentResults[i]), string(wantPR))
					}
				}
			}
		})
	}
}

// TestHashToJob_NumericStringParsing verifies that numeric fields encoded as
// strings in the Redis hash are correctly parsed back into Go integer types.
func TestHashToJob_NumericStringParsing(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
		check func(t *testing.T, job *core.Job)
	}{
		{
			name:  "priority positive",
			key:   "priority",
			value: "100",
			check: func(t *testing.T, job *core.Job) {
				if job.Priority == nil || *job.Priority != 100 {
					t.Errorf("Priority: got %v, want 100", job.Priority)
				}
			},
		},
		{
			name:  "priority zero",
			key:   "priority",
			value: "0",
			check: func(t *testing.T, job *core.Job) {
				if job.Priority == nil || *job.Priority != 0 {
					t.Errorf("Priority: got %v, want 0", job.Priority)
				}
			},
		},
		{
			name:  "priority negative",
			key:   "priority",
			value: "-5",
			check: func(t *testing.T, job *core.Job) {
				if job.Priority == nil || *job.Priority != -5 {
					t.Errorf("Priority: got %v, want -5", job.Priority)
				}
			},
		},
		{
			name:  "max_attempts large value",
			key:   "max_attempts",
			value: "999",
			check: func(t *testing.T, job *core.Job) {
				if job.MaxAttempts == nil || *job.MaxAttempts != 999 {
					t.Errorf("MaxAttempts: got %v, want 999", job.MaxAttempts)
				}
			},
		},
		{
			name:  "timeout_ms large value",
			key:   "timeout_ms",
			value: "3600000",
			check: func(t *testing.T, job *core.Job) {
				if job.TimeoutMs == nil || *job.TimeoutMs != 3600000 {
					t.Errorf("TimeoutMs: got %v, want 3600000", job.TimeoutMs)
				}
			},
		},
		{
			name:  "attempt parsed correctly",
			key:   "attempt",
			value: "7",
			check: func(t *testing.T, job *core.Job) {
				if job.Attempt != 7 {
					t.Errorf("Attempt: got %d, want 7", job.Attempt)
				}
			},
		},
		{
			name:  "visibility_timeout_ms parsed correctly",
			key:   "visibility_timeout_ms",
			value: "90000",
			check: func(t *testing.T, job *core.Job) {
				if job.VisibilityTimeoutMs == nil || *job.VisibilityTimeoutMs != 90000 {
					t.Errorf("VisibilityTimeoutMs: got %v, want 90000", job.VisibilityTimeoutMs)
				}
			},
		},
		{
			name:  "workflow_step parsed correctly",
			key:   "workflow_step",
			value: "3",
			check: func(t *testing.T, job *core.Job) {
				if job.WorkflowStep != 3 {
					t.Errorf("WorkflowStep: got %d, want 3", job.WorkflowStep)
				}
			},
		},
		{
			name:  "retry_delay_ms parsed as int64",
			key:   "retry_delay_ms",
			value: strconv.FormatInt(int64(1<<40), 10), // large int64 value
			check: func(t *testing.T, job *core.Job) {
				want := int64(1 << 40)
				if job.RetryDelayMs == nil || *job.RetryDelayMs != want {
					t.Errorf("RetryDelayMs: got %v, want %d", job.RetryDelayMs, want)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := map[string]string{
				"id":    "num-test",
				"type":  "t",
				"state": "available",
				"queue": "q",
			}
			data[tt.key] = tt.value

			job := hashToJob(data)
			if job == nil {
				t.Fatal("expected non-nil job")
			}
			tt.check(t, job)
		})
	}
}

// TestHashToJob_JSONFieldParsing verifies that fields stored as JSON strings
// in the Redis hash are correctly deserialized into their Go types.
func TestHashToJob_JSONFieldParsing(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
		check func(t *testing.T, job *core.Job)
	}{
		{
			name:  "tags single element",
			key:   "tags",
			value: `["only"]`,
			check: func(t *testing.T, job *core.Job) {
				if len(job.Tags) != 1 || job.Tags[0] != "only" {
					t.Errorf("Tags: got %v, want [only]", job.Tags)
				}
			},
		},
		{
			name:  "tags empty array",
			key:   "tags",
			value: `[]`,
			check: func(t *testing.T, job *core.Job) {
				if len(job.Tags) != 0 {
					t.Errorf("Tags: got %v, want empty", job.Tags)
				}
			},
		},
		{
			name:  "retry with on_exhaustion",
			key:   "retry",
			value: `{"max_attempts":3,"on_exhaustion":"dead_letter"}`,
			check: func(t *testing.T, job *core.Job) {
				if job.Retry == nil {
					t.Fatal("Retry: expected non-nil")
				}
				if job.Retry.OnExhaustion != "dead_letter" {
					t.Errorf("Retry.OnExhaustion: got %q, want %q", job.Retry.OnExhaustion, "dead_letter")
				}
			},
		},
		{
			name:  "retry with non_retryable_errors",
			key:   "retry",
			value: `{"max_attempts":2,"non_retryable_errors":["InvalidInput","AuthError"]}`,
			check: func(t *testing.T, job *core.Job) {
				if job.Retry == nil {
					t.Fatal("Retry: expected non-nil")
				}
				if len(job.Retry.NonRetryableErrors) != 2 {
					t.Fatalf("NonRetryableErrors length: got %d, want 2", len(job.Retry.NonRetryableErrors))
				}
				if job.Retry.NonRetryableErrors[0] != "InvalidInput" {
					t.Errorf("NonRetryableErrors[0]: got %q, want %q", job.Retry.NonRetryableErrors[0], "InvalidInput")
				}
			},
		},
		{
			name:  "unique with states",
			key:   "unique",
			value: `{"keys":["type"],"states":["available","active","completed"]}`,
			check: func(t *testing.T, job *core.Job) {
				if job.Unique == nil {
					t.Fatal("Unique: expected non-nil")
				}
				if len(job.Unique.States) != 3 {
					t.Fatalf("Unique.States length: got %d, want 3", len(job.Unique.States))
				}
				if job.Unique.States[2] != "completed" {
					t.Errorf("Unique.States[2]: got %q, want %q", job.Unique.States[2], "completed")
				}
			},
		},
		{
			name:  "error_history with complex error objects",
			key:   "error_history",
			value: `[{"message":"timeout","code":"E001"},{"message":"retry limit","code":"E002","details":{"attempt":3}}]`,
			check: func(t *testing.T, job *core.Job) {
				if len(job.Errors) != 2 {
					t.Fatalf("Errors length: got %d, want 2", len(job.Errors))
				}
				// Verify each entry is valid JSON
				for i, e := range job.Errors {
					if !json.Valid(e) {
						t.Errorf("Errors[%d]: invalid JSON: %s", i, string(e))
					}
				}
			},
		},
		{
			name:  "parent_results with multiple entries",
			key:   "parent_results",
			value: `[{"count":10},{"count":20},{"count":30}]`,
			check: func(t *testing.T, job *core.Job) {
				if len(job.ParentResults) != 3 {
					t.Fatalf("ParentResults length: got %d, want 3", len(job.ParentResults))
				}
				if string(job.ParentResults[1]) != `{"count":20}` {
					t.Errorf("ParentResults[1]: got %s, want %s", string(job.ParentResults[1]), `{"count":20}`)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := map[string]string{
				"id":    "json-test",
				"type":  "t",
				"state": "available",
				"queue": "q",
			}
			data[tt.key] = tt.value

			job := hashToJob(data)
			if job == nil {
				t.Fatal("expected non-nil job")
			}
			tt.check(t, job)
		})
	}
}

// TestJobToHash_AllValuesAreStrings verifies that every value in the hash
// returned by jobToHash is a string, since Redis stores hash field values
// as strings.
func TestJobToHash_AllValuesAreStrings(t *testing.T) {
	job := &core.Job{
		ID:          "str-check",
		Type:        "t",
		State:       "available",
		Queue:       "q",
		Attempt:     5,
		Args:        json.RawMessage(`[1,2,3]`),
		Meta:        json.RawMessage(`{"k":"v"}`),
		Priority:    intPtr(3),
		MaxAttempts: intPtr(10),
		TimeoutMs:   intPtr(5000),
		CreatedAt:   "2025-01-01T00:00:00.000Z",
		Tags:        []string{"a"},
		Retry:       &core.RetryPolicy{MaxAttempts: 3},
		Unique:      &core.UniquePolicy{Keys: []string{"type"}},
		VisibilityTimeoutMs: intPtr(1000),
		WorkflowID:          "wf",
		WorkflowStep:        1,
		ParentResults:       []json.RawMessage{json.RawMessage(`{}`)},
		UnknownFields: map[string]json.RawMessage{
			"ext": json.RawMessage(`"val"`),
		},
	}

	h := jobToHash(job)
	for k, v := range h {
		if _, ok := v.(string); !ok {
			t.Errorf("key %q: value type is %T, want string", k, v)
		}
	}
}
