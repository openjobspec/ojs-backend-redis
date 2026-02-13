package core

import (
	"encoding/json"
	"testing"
	"time"
)

func TestFormatTime(t *testing.T) {
	tests := []struct {
		name     string
		input    time.Time
		expected string
	}{
		{
			name:     "known time formats correctly",
			input:    time.Date(2024, 6, 15, 12, 30, 45, 123000000, time.UTC),
			expected: "2024-06-15T12:30:45.123Z",
		},
		{
			name:     "zero milliseconds shown as three zeros",
			input:    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			expected: "2024-01-01T00:00:00.000Z",
		},
		{
			name:     "non-UTC time is converted to UTC",
			input:    time.Date(2024, 6, 15, 20, 0, 0, 0, time.FixedZone("EST", -5*60*60)),
			expected: "2024-06-16T01:00:00.000Z",
		},
		{
			name:     "sub-millisecond precision is truncated",
			input:    time.Date(2024, 3, 10, 8, 15, 30, 999999999, time.UTC),
			expected: "2024-03-10T08:15:30.999Z",
		},
		{
			name:     "millisecond precision with leading zeros",
			input:    time.Date(2024, 12, 31, 23, 59, 59, 7000000, time.UTC),
			expected: "2024-12-31T23:59:59.007Z",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FormatTime(tt.input)
			if got != tt.expected {
				t.Errorf("FormatTime(%v) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestParseEnqueueRequest(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantErr   bool
		check     func(t *testing.T, req *EnqueueRequest)
	}{
		{
			name:  "basic valid JSON with type and args",
			input: `{"type":"email.send","args":["user@example.com","Welcome"]}`,
			check: func(t *testing.T, req *EnqueueRequest) {
				if req.Type != "email.send" {
					t.Errorf("Type = %q, want %q", req.Type, "email.send")
				}
				var args []string
				if err := json.Unmarshal(req.Args, &args); err != nil {
					t.Fatalf("failed to unmarshal args: %v", err)
				}
				if len(args) != 2 || args[0] != "user@example.com" || args[1] != "Welcome" {
					t.Errorf("Args = %v, want [user@example.com Welcome]", args)
				}
			},
		},
		{
			name:  "preserves unknown fields",
			input: `{"type":"test","args":[],"x_custom":"value","x_trace_id":"abc-123"}`,
			check: func(t *testing.T, req *EnqueueRequest) {
				if len(req.UnknownFields) != 2 {
					t.Fatalf("UnknownFields length = %d, want 2", len(req.UnknownFields))
				}
				var customVal string
				if err := json.Unmarshal(req.UnknownFields["x_custom"], &customVal); err != nil {
					t.Fatalf("failed to unmarshal x_custom: %v", err)
				}
				if customVal != "value" {
					t.Errorf("x_custom = %q, want %q", customVal, "value")
				}
				var traceID string
				if err := json.Unmarshal(req.UnknownFields["x_trace_id"], &traceID); err != nil {
					t.Fatalf("failed to unmarshal x_trace_id: %v", err)
				}
				if traceID != "abc-123" {
					t.Errorf("x_trace_id = %q, want %q", traceID, "abc-123")
				}
			},
		},
		{
			name:  "HasID true when id is present",
			input: `{"id":"550e8400-e29b-41d4-a716-446655440000","type":"test","args":[]}`,
			check: func(t *testing.T, req *EnqueueRequest) {
				if !req.HasID {
					t.Error("HasID = false, want true")
				}
				if req.ID != "550e8400-e29b-41d4-a716-446655440000" {
					t.Errorf("ID = %q, want %q", req.ID, "550e8400-e29b-41d4-a716-446655440000")
				}
			},
		},
		{
			name:  "HasID false when id is absent",
			input: `{"type":"test","args":[]}`,
			check: func(t *testing.T, req *EnqueueRequest) {
				if req.HasID {
					t.Error("HasID = true, want false")
				}
			},
		},
		{
			name:    "invalid JSON returns error",
			input:   `{not valid json}`,
			wantErr: true,
		},
		{
			name:    "empty input returns error",
			input:   ``,
			wantErr: true,
		},
		{
			name:  "with options queue priority and retry",
			input: `{"type":"report.generate","args":[1],"options":{"queue":"critical","priority":10,"retry":{"max_attempts":5}}}`,
			check: func(t *testing.T, req *EnqueueRequest) {
				if req.Type != "report.generate" {
					t.Errorf("Type = %q, want %q", req.Type, "report.generate")
				}
				if req.Options == nil {
					t.Fatal("Options is nil")
				}
				if req.Options.Queue != "critical" {
					t.Errorf("Options.Queue = %q, want %q", req.Options.Queue, "critical")
				}
				if req.Options.Priority == nil {
					t.Fatal("Options.Priority is nil")
				}
				if *req.Options.Priority != 10 {
					t.Errorf("Options.Priority = %d, want 10", *req.Options.Priority)
				}
				if req.Options.Retry == nil {
					t.Fatal("Options.Retry is nil")
				}
				if req.Options.Retry.MaxAttempts != 5 {
					t.Errorf("Options.Retry.MaxAttempts = %d, want 5", req.Options.Retry.MaxAttempts)
				}
			},
		},
		{
			name:  "with meta field",
			input: `{"type":"test","args":[],"meta":{"tenant":"acme"}}`,
			check: func(t *testing.T, req *EnqueueRequest) {
				if req.Meta == nil {
					t.Fatal("Meta is nil")
				}
				var meta map[string]string
				if err := json.Unmarshal(req.Meta, &meta); err != nil {
					t.Fatalf("failed to unmarshal meta: %v", err)
				}
				if meta["tenant"] != "acme" {
					t.Errorf("meta[tenant] = %q, want %q", meta["tenant"], "acme")
				}
			},
		},
		{
			name:  "with schema field",
			input: `{"type":"test","args":[],"schema":"https://example.com/schema.json"}`,
			check: func(t *testing.T, req *EnqueueRequest) {
				if req.Schema != "https://example.com/schema.json" {
					t.Errorf("Schema = %q, want %q", req.Schema, "https://example.com/schema.json")
				}
			},
		},
		{
			name:  "known fields are not in UnknownFields",
			input: `{"id":"abc","type":"test","args":[1],"meta":{"k":"v"},"schema":"s","options":{"queue":"q"}}`,
			check: func(t *testing.T, req *EnqueueRequest) {
				if len(req.UnknownFields) != 0 {
					t.Errorf("UnknownFields length = %d, want 0; fields: %v", len(req.UnknownFields), req.UnknownFields)
				}
			},
		},
		{
			name:  "unknown fields with complex values",
			input: `{"type":"test","args":[],"x_nested":{"a":1,"b":[2,3]}}`,
			check: func(t *testing.T, req *EnqueueRequest) {
				raw, ok := req.UnknownFields["x_nested"]
				if !ok {
					t.Fatal("x_nested not found in UnknownFields")
				}
				var nested map[string]json.RawMessage
				if err := json.Unmarshal(raw, &nested); err != nil {
					t.Fatalf("failed to unmarshal x_nested: %v", err)
				}
				if _, ok := nested["a"]; !ok {
					t.Error("x_nested missing key 'a'")
				}
				if _, ok := nested["b"]; !ok {
					t.Error("x_nested missing key 'b'")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := ParseEnqueueRequest([]byte(tt.input))
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.check != nil {
				tt.check(t, req)
			}
		})
	}
}

func TestJobMarshalJSON(t *testing.T) {
	intPtr := func(v int) *int { return &v }
	int64Ptr := func(v int64) *int64 { return &v }

	tests := []struct {
		name  string
		job   Job
		check func(t *testing.T, data map[string]json.RawMessage)
	}{
		{
			name: "includes known fields",
			job: Job{
				ID:        "job-001",
				Type:      "email.send",
				State:     "available",
				Queue:     "default",
				Args:      json.RawMessage(`["hello"]`),
				Attempt:   1,
				CreatedAt: "2024-06-15T12:00:00.000Z",
			},
			check: func(t *testing.T, data map[string]json.RawMessage) {
				assertJSONString(t, data, "id", "job-001")
				assertJSONString(t, data, "type", "email.send")
				assertJSONString(t, data, "state", "available")
				assertJSONString(t, data, "queue", "default")
				assertJSONNumber(t, data, "attempt", 1)
				assertJSONString(t, data, "created_at", "2024-06-15T12:00:00.000Z")

				var args []string
				if err := json.Unmarshal(data["args"], &args); err != nil {
					t.Fatalf("failed to unmarshal args: %v", err)
				}
				if len(args) != 1 || args[0] != "hello" {
					t.Errorf("args = %v, want [hello]", args)
				}
			},
		},
		{
			name: "includes unknown fields",
			job: Job{
				ID:      "job-002",
				Type:    "test",
				State:   "active",
				Queue:   "default",
				Attempt: 0,
				UnknownFields: map[string]json.RawMessage{
					"x_custom":   json.RawMessage(`"custom_value"`),
					"x_trace_id": json.RawMessage(`"trace-abc"`),
				},
			},
			check: func(t *testing.T, data map[string]json.RawMessage) {
				var customVal string
				if err := json.Unmarshal(data["x_custom"], &customVal); err != nil {
					t.Fatalf("failed to unmarshal x_custom: %v", err)
				}
				if customVal != "custom_value" {
					t.Errorf("x_custom = %q, want %q", customVal, "custom_value")
				}
				var traceID string
				if err := json.Unmarshal(data["x_trace_id"], &traceID); err != nil {
					t.Fatalf("failed to unmarshal x_trace_id: %v", err)
				}
				if traceID != "trace-abc" {
					t.Errorf("x_trace_id = %q, want %q", traceID, "trace-abc")
				}
			},
		},
		{
			name: "omits empty optional fields",
			job: Job{
				ID:      "job-003",
				Type:    "test",
				State:   "available",
				Queue:   "default",
				Attempt: 0,
				// All optional fields left at zero values
			},
			check: func(t *testing.T, data map[string]json.RawMessage) {
				omittedFields := []string{
					"meta", "priority", "max_attempts", "timeout_ms",
					"enqueued_at", "started_at", "completed_at", "cancelled_at",
					"scheduled_at", "result", "error", "tags", "errors",
					"expires_at", "retry_delay_ms", "parent_results",
				}
				for _, field := range omittedFields {
					if _, ok := data[field]; ok {
						t.Errorf("field %q should be omitted but is present", field)
					}
				}
				// Required fields should still be present
				requiredFields := []string{"id", "type", "state", "queue", "attempt"}
				for _, field := range requiredFields {
					if _, ok := data[field]; !ok {
						t.Errorf("required field %q is missing", field)
					}
				}
			},
		},
		{
			name: "priority included when set",
			job: Job{
				ID:       "job-004",
				Type:     "test",
				State:    "available",
				Queue:    "default",
				Attempt:  0,
				Priority: intPtr(5),
			},
			check: func(t *testing.T, data map[string]json.RawMessage) {
				assertJSONNumber(t, data, "priority", 5)
			},
		},
		{
			name: "priority omitted when nil",
			job: Job{
				ID:       "job-005",
				Type:     "test",
				State:    "available",
				Queue:    "default",
				Attempt:  0,
				Priority: nil,
			},
			check: func(t *testing.T, data map[string]json.RawMessage) {
				if _, ok := data["priority"]; ok {
					t.Error("priority should be omitted when nil")
				}
			},
		},
		{
			name: "max_attempts and timeout_ms included when set",
			job: Job{
				ID:          "job-006",
				Type:        "test",
				State:       "available",
				Queue:       "default",
				Attempt:     0,
				MaxAttempts: intPtr(3),
				TimeoutMs:   intPtr(30000),
			},
			check: func(t *testing.T, data map[string]json.RawMessage) {
				assertJSONNumber(t, data, "max_attempts", 3)
				assertJSONNumber(t, data, "timeout_ms", 30000)
			},
		},
		{
			name: "timestamp fields included when non-empty",
			job: Job{
				ID:          "job-007",
				Type:        "test",
				State:       "completed",
				Queue:       "default",
				Attempt:     1,
				CreatedAt:   "2024-06-15T12:00:00.000Z",
				EnqueuedAt:  "2024-06-15T12:00:00.100Z",
				StartedAt:   "2024-06-15T12:00:01.000Z",
				CompletedAt: "2024-06-15T12:00:02.000Z",
				ScheduledAt: "2024-06-15T12:00:00.000Z",
			},
			check: func(t *testing.T, data map[string]json.RawMessage) {
				assertJSONString(t, data, "created_at", "2024-06-15T12:00:00.000Z")
				assertJSONString(t, data, "enqueued_at", "2024-06-15T12:00:00.100Z")
				assertJSONString(t, data, "started_at", "2024-06-15T12:00:01.000Z")
				assertJSONString(t, data, "completed_at", "2024-06-15T12:00:02.000Z")
				assertJSONString(t, data, "scheduled_at", "2024-06-15T12:00:00.000Z")
			},
		},
		{
			name: "meta and result included when non-empty",
			job: Job{
				ID:      "job-008",
				Type:    "test",
				State:   "completed",
				Queue:   "default",
				Attempt: 1,
				Meta:    json.RawMessage(`{"tenant":"acme"}`),
				Result:  json.RawMessage(`{"status":"ok"}`),
			},
			check: func(t *testing.T, data map[string]json.RawMessage) {
				if _, ok := data["meta"]; !ok {
					t.Error("meta should be present")
				}
				if _, ok := data["result"]; !ok {
					t.Error("result should be present")
				}
			},
		},
		{
			name: "error field included when non-empty",
			job: Job{
				ID:      "job-009",
				Type:    "test",
				State:   "retryable",
				Queue:   "default",
				Attempt: 1,
				Error:   json.RawMessage(`{"message":"timeout"}`),
			},
			check: func(t *testing.T, data map[string]json.RawMessage) {
				if _, ok := data["error"]; !ok {
					t.Error("error should be present")
				}
			},
		},
		{
			name: "tags included when non-empty",
			job: Job{
				ID:      "job-010",
				Type:    "test",
				State:   "available",
				Queue:   "default",
				Attempt: 0,
				Tags:    []string{"urgent", "billing"},
			},
			check: func(t *testing.T, data map[string]json.RawMessage) {
				var tags []string
				if err := json.Unmarshal(data["tags"], &tags); err != nil {
					t.Fatalf("failed to unmarshal tags: %v", err)
				}
				if len(tags) != 2 || tags[0] != "urgent" || tags[1] != "billing" {
					t.Errorf("tags = %v, want [urgent billing]", tags)
				}
			},
		},
		{
			name: "errors array included when non-empty",
			job: Job{
				ID:      "job-011",
				Type:    "test",
				State:   "discarded",
				Queue:   "default",
				Attempt: 3,
				Errors: []json.RawMessage{
					json.RawMessage(`{"message":"first failure"}`),
					json.RawMessage(`{"message":"second failure"}`),
				},
			},
			check: func(t *testing.T, data map[string]json.RawMessage) {
				var errors []json.RawMessage
				if err := json.Unmarshal(data["errors"], &errors); err != nil {
					t.Fatalf("failed to unmarshal errors: %v", err)
				}
				if len(errors) != 2 {
					t.Errorf("errors length = %d, want 2", len(errors))
				}
			},
		},
		{
			name: "retry_delay_ms included when set",
			job: Job{
				ID:           "job-012",
				Type:         "test",
				State:        "retryable",
				Queue:        "default",
				Attempt:      1,
				RetryDelayMs: int64Ptr(5000),
			},
			check: func(t *testing.T, data map[string]json.RawMessage) {
				assertJSONNumber(t, data, "retry_delay_ms", 5000)
			},
		},
		{
			name: "expires_at included when non-empty",
			job: Job{
				ID:        "job-013",
				Type:      "test",
				State:     "available",
				Queue:     "default",
				Attempt:   0,
				ExpiresAt: "2024-12-31T23:59:59.000Z",
			},
			check: func(t *testing.T, data map[string]json.RawMessage) {
				assertJSONString(t, data, "expires_at", "2024-12-31T23:59:59.000Z")
			},
		},
		{
			name: "parent_results included when non-empty",
			job: Job{
				ID:      "job-014",
				Type:    "test",
				State:   "active",
				Queue:   "default",
				Attempt: 1,
				ParentResults: []json.RawMessage{
					json.RawMessage(`{"data":"from_parent"}`),
				},
			},
			check: func(t *testing.T, data map[string]json.RawMessage) {
				var results []json.RawMessage
				if err := json.Unmarshal(data["parent_results"], &results); err != nil {
					t.Fatalf("failed to unmarshal parent_results: %v", err)
				}
				if len(results) != 1 {
					t.Errorf("parent_results length = %d, want 1", len(results))
				}
			},
		},
		{
			name: "internal fields are not serialized",
			job: Job{
				ID:                  "job-015",
				Type:                "test",
				State:               "active",
				Queue:               "default",
				Attempt:             1,
				VisibilityTimeoutMs: intPtr(60000),
				WorkflowID:          "wf-001",
				WorkflowStep:        2,
				IsExisting:          true,
				RateLimit:           &RateLimitPolicy{MaxPerSecond: 10},
			},
			check: func(t *testing.T, data map[string]json.RawMessage) {
				internalFields := []string{
					"visibility_timeout_ms", "workflow_id", "workflow_step",
					"is_existing", "rate_limit",
				}
				for _, field := range internalFields {
					if _, ok := data[field]; ok {
						t.Errorf("internal field %q should not be serialized but is present", field)
					}
				}
			},
		},
		{
			name: "cancelled_at included when non-empty",
			job: Job{
				ID:          "job-016",
				Type:        "test",
				State:       "cancelled",
				Queue:       "default",
				Attempt:     0,
				CancelledAt: "2024-06-15T13:00:00.000Z",
			},
			check: func(t *testing.T, data map[string]json.RawMessage) {
				assertJSONString(t, data, "cancelled_at", "2024-06-15T13:00:00.000Z")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw, err := json.Marshal(&tt.job)
			if err != nil {
				t.Fatalf("MarshalJSON error: %v", err)
			}

			var data map[string]json.RawMessage
			if err := json.Unmarshal(raw, &data); err != nil {
				t.Fatalf("failed to unmarshal marshaled JSON: %v", err)
			}

			tt.check(t, data)
		})
	}
}

// assertJSONString checks that a field in the JSON map has the expected string value.
func assertJSONString(t *testing.T, data map[string]json.RawMessage, field string, expected string) {
	t.Helper()
	raw, ok := data[field]
	if !ok {
		t.Errorf("field %q is missing", field)
		return
	}
	var got string
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Errorf("field %q is not a valid string: %v", field, err)
		return
	}
	if got != expected {
		t.Errorf("field %q = %q, want %q", field, got, expected)
	}
}

// assertJSONNumber checks that a field in the JSON map has the expected numeric value.
func assertJSONNumber(t *testing.T, data map[string]json.RawMessage, field string, expected float64) {
	t.Helper()
	raw, ok := data[field]
	if !ok {
		t.Errorf("field %q is missing", field)
		return
	}
	var got float64
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Errorf("field %q is not a valid number: %v", field, err)
		return
	}
	if got != expected {
		t.Errorf("field %q = %v, want %v", field, got, expected)
	}
}
