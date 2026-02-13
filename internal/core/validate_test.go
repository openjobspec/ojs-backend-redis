package core

import (
	"encoding/json"
	"strings"
	"testing"
)

func intPtr(v int) *int {
	return &v
}

func TestValidateEnqueueRequest(t *testing.T) {
	// Generate a valid UUIDv7 for reuse across test cases.
	validUUIDv7 := NewUUIDv7()

	tests := []struct {
		name      string
		req       *EnqueueRequest
		wantErr   bool
		wantCode  string
		wantField string // expected field value in error details, if any
	}{
		// -----------------------------------------------------------
		// Happy path
		// -----------------------------------------------------------
		{
			name: "valid minimal request with type and args array",
			req: &EnqueueRequest{
				Type: "email.send",
				Args: json.RawMessage(`["hello"]`),
			},
			wantErr: false,
		},
		{
			name: "valid request with empty args array",
			req: &EnqueueRequest{
				Type: "process",
				Args: json.RawMessage(`[]`),
			},
			wantErr: false,
		},
		{
			name: "valid request with complex args array",
			req: &EnqueueRequest{
				Type: "batch-import",
				Args: json.RawMessage(`[1, "two", true, null, {"nested": "obj"}, [1,2,3]]`),
			},
			wantErr: false,
		},
		{
			name: "valid request with options",
			req: &EnqueueRequest{
				Type: "report.generate",
				Args: json.RawMessage(`["quarterly"]`),
				Options: &EnqueueOptions{
					Queue:    "reports",
					Priority: intPtr(10),
				},
			},
			wantErr: false,
		},
		{
			name: "valid request with all retry policy fields",
			req: &EnqueueRequest{
				Type: "webhook.deliver",
				Args: json.RawMessage(`["https://example.com"]`),
				Options: &EnqueueOptions{
					Retry: &RetryPolicy{
						MaxAttempts:        5,
						InitialInterval:    "PT1S",
						BackoffCoefficient: 2.0,
						MaxInterval:        "PT5M",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "valid request with retry_policy alias field",
			req: &EnqueueRequest{
				Type: "webhook.deliver",
				Args: json.RawMessage(`["https://example.com"]`),
				Options: &EnqueueOptions{
					RetryPolicy: &RetryPolicy{
						MaxAttempts:        3,
						InitialInterval:    "PT10S",
						BackoffCoefficient: 1.5,
						MaxInterval:        "PT1H",
					},
				},
			},
			wantErr: false,
		},

		// -----------------------------------------------------------
		// Type field validation
		// -----------------------------------------------------------
		{
			name: "missing type returns error",
			req: &EnqueueRequest{
				Type: "",
				Args: json.RawMessage(`["x"]`),
			},
			wantErr:   true,
			wantCode:  ErrCodeInvalidRequest,
			wantField: "type",
		},
		{
			name: "type with uppercase letters is invalid",
			req: &EnqueueRequest{
				Type: "EmailSend",
				Args: json.RawMessage(`["x"]`),
			},
			wantErr:   true,
			wantCode:  ErrCodeInvalidRequest,
			wantField: "type",
		},
		{
			name: "type starting with digit is invalid",
			req: &EnqueueRequest{
				Type: "1email",
				Args: json.RawMessage(`["x"]`),
			},
			wantErr:   true,
			wantCode:  ErrCodeInvalidRequest,
			wantField: "type",
		},
		{
			name: "type with spaces is invalid",
			req: &EnqueueRequest{
				Type: "email send",
				Args: json.RawMessage(`["x"]`),
			},
			wantErr:   true,
			wantCode:  ErrCodeInvalidRequest,
			wantField: "type",
		},
		{
			name: "type starting with underscore is invalid",
			req: &EnqueueRequest{
				Type: "_email",
				Args: json.RawMessage(`["x"]`),
			},
			wantErr:   true,
			wantCode:  ErrCodeInvalidRequest,
			wantField: "type",
		},
		{
			name: "type starting with hyphen is invalid",
			req: &EnqueueRequest{
				Type: "-email",
				Args: json.RawMessage(`["x"]`),
			},
			wantErr:   true,
			wantCode:  ErrCodeInvalidRequest,
			wantField: "type",
		},
		{
			name: "type with dot segment starting with digit is invalid",
			req: &EnqueueRequest{
				Type: "email.1send",
				Args: json.RawMessage(`["x"]`),
			},
			wantErr:   true,
			wantCode:  ErrCodeInvalidRequest,
			wantField: "type",
		},
		{
			name: "type with trailing dot is invalid",
			req: &EnqueueRequest{
				Type: "email.",
				Args: json.RawMessage(`["x"]`),
			},
			wantErr:   true,
			wantCode:  ErrCodeInvalidRequest,
			wantField: "type",
		},
		{
			name: "type with leading dot is invalid",
			req: &EnqueueRequest{
				Type: ".email",
				Args: json.RawMessage(`["x"]`),
			},
			wantErr:   true,
			wantCode:  ErrCodeInvalidRequest,
			wantField: "type",
		},
		{
			name: "type with consecutive dots is invalid",
			req: &EnqueueRequest{
				Type: "email..send",
				Args: json.RawMessage(`["x"]`),
			},
			wantErr:   true,
			wantCode:  ErrCodeInvalidRequest,
			wantField: "type",
		},
		{
			name: "valid dotted type",
			req: &EnqueueRequest{
				Type: "email.send.notification",
				Args: json.RawMessage(`["x"]`),
			},
			wantErr: false,
		},
		{
			name: "valid type with hyphens and underscores",
			req: &EnqueueRequest{
				Type: "batch-import_v2",
				Args: json.RawMessage(`["x"]`),
			},
			wantErr: false,
		},
		{
			name: "valid single-character type",
			req: &EnqueueRequest{
				Type: "a",
				Args: json.RawMessage(`["x"]`),
			},
			wantErr: false,
		},
		{
			name: "type at max length 255 is valid",
			req: &EnqueueRequest{
				Type: strings.Repeat("a", 255),
				Args: json.RawMessage(`["x"]`),
			},
			wantErr: false,
		},
		{
			name: "type exceeding max length 255 returns error",
			req: &EnqueueRequest{
				Type: strings.Repeat("a", 256),
				Args: json.RawMessage(`["x"]`),
			},
			wantErr:   true,
			wantCode:  ErrCodeInvalidRequest,
			wantField: "type",
		},

		// -----------------------------------------------------------
		// Args field validation
		// -----------------------------------------------------------
		{
			name: "nil args returns error",
			req: &EnqueueRequest{
				Type: "test",
				Args: nil,
			},
			wantErr:   true,
			wantCode:  ErrCodeInvalidRequest,
			wantField: "args",
		},
		{
			name: "args is JSON null returns error",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`null`),
			},
			wantErr:   true,
			wantCode:  ErrCodeInvalidRequest,
			wantField: "args",
		},
		{
			name: "args is JSON object returns error",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`{"key": "value"}`),
			},
			wantErr:   true,
			wantCode:  ErrCodeInvalidRequest,
			wantField: "args",
		},
		{
			name: "args is JSON string returns error",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`"hello"`),
			},
			wantErr:   true,
			wantCode:  ErrCodeInvalidRequest,
			wantField: "args",
		},
		{
			name: "args is JSON number returns error",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`42`),
			},
			wantErr:   true,
			wantCode:  ErrCodeInvalidRequest,
			wantField: "args",
		},
		{
			name: "args is JSON boolean returns error",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`true`),
			},
			wantErr:   true,
			wantCode:  ErrCodeInvalidRequest,
			wantField: "args",
		},
		{
			name: "args is invalid JSON returns error",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`{not-json`),
			},
			wantErr:   true,
			wantCode:  ErrCodeInvalidRequest,
			wantField: "args",
		},

		// -----------------------------------------------------------
		// ID / UUIDv7 validation
		// -----------------------------------------------------------
		{
			name: "valid UUIDv7 with HasID true",
			req: &EnqueueRequest{
				Type:  "test",
				Args:  json.RawMessage(`["x"]`),
				ID:    validUUIDv7,
				HasID: true,
			},
			wantErr: false,
		},
		{
			name: "empty ID with HasID false does not validate",
			req: &EnqueueRequest{
				Type:  "test",
				Args:  json.RawMessage(`["x"]`),
				ID:    "",
				HasID: false,
			},
			wantErr: false,
		},
		{
			name: "empty ID with HasID true returns error",
			req: &EnqueueRequest{
				Type:  "test",
				Args:  json.RawMessage(`["x"]`),
				ID:    "",
				HasID: true,
			},
			wantErr:   true,
			wantCode:  ErrCodeInvalidRequest,
			wantField: "id",
		},
		{
			name: "invalid ID format with HasID true returns error",
			req: &EnqueueRequest{
				Type:  "test",
				Args:  json.RawMessage(`["x"]`),
				ID:    "not-a-uuid",
				HasID: true,
			},
			wantErr:   true,
			wantCode:  ErrCodeInvalidRequest,
			wantField: "id",
		},
		{
			name: "UUIDv4 with HasID true returns error (not v7)",
			req: &EnqueueRequest{
				Type:  "test",
				Args:  json.RawMessage(`["x"]`),
				ID:    "550e8400-e29b-41d4-a716-446655440000", // v4 UUID
				HasID: true,
			},
			wantErr:   true,
			wantCode:  ErrCodeInvalidRequest,
			wantField: "id",
		},
		{
			name: "garbage ID with HasID false is ignored",
			req: &EnqueueRequest{
				Type:  "test",
				Args:  json.RawMessage(`["x"]`),
				ID:    "garbage",
				HasID: false,
			},
			wantErr: false,
		},

		// -----------------------------------------------------------
		// Queue name validation
		// -----------------------------------------------------------
		{
			name: "valid queue name",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`["x"]`),
				Options: &EnqueueOptions{
					Queue: "default",
				},
			},
			wantErr: false,
		},
		{
			name: "valid queue name with hyphens and dots",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`["x"]`),
				Options: &EnqueueOptions{
					Queue: "my-queue.v2",
				},
			},
			wantErr: false,
		},
		{
			name: "valid queue name starting with digit",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`["x"]`),
				Options: &EnqueueOptions{
					Queue: "1priority",
				},
			},
			wantErr: false,
		},
		{
			name: "empty queue name is allowed (not specified)",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`["x"]`),
				Options: &EnqueueOptions{
					Queue: "",
				},
			},
			wantErr: false,
		},
		{
			name: "queue name with uppercase is invalid",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`["x"]`),
				Options: &EnqueueOptions{
					Queue: "MyQueue",
				},
			},
			wantErr:   true,
			wantCode:  ErrCodeInvalidRequest,
			wantField: "queue",
		},
		{
			name: "queue name with spaces is invalid",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`["x"]`),
				Options: &EnqueueOptions{
					Queue: "my queue",
				},
			},
			wantErr:   true,
			wantCode:  ErrCodeInvalidRequest,
			wantField: "queue",
		},
		{
			name: "queue name starting with hyphen is invalid",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`["x"]`),
				Options: &EnqueueOptions{
					Queue: "-queue",
				},
			},
			wantErr:   true,
			wantCode:  ErrCodeInvalidRequest,
			wantField: "queue",
		},
		{
			name: "queue name starting with dot is invalid",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`["x"]`),
				Options: &EnqueueOptions{
					Queue: ".queue",
				},
			},
			wantErr:   true,
			wantCode:  ErrCodeInvalidRequest,
			wantField: "queue",
		},
		{
			name: "queue name with underscores is invalid",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`["x"]`),
				Options: &EnqueueOptions{
					Queue: "my_queue",
				},
			},
			wantErr:   true,
			wantCode:  ErrCodeInvalidRequest,
			wantField: "queue",
		},
		{
			name: "queue name at max length 128 is valid",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`["x"]`),
				Options: &EnqueueOptions{
					Queue: strings.Repeat("a", 128),
				},
			},
			wantErr: false,
		},
		{
			name: "queue name exceeding max length 128 returns error",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`["x"]`),
				Options: &EnqueueOptions{
					Queue: strings.Repeat("a", 129),
				},
			},
			wantErr:   true,
			wantCode:  ErrCodeInvalidRequest,
			wantField: "queue",
		},

		// -----------------------------------------------------------
		// Priority validation
		// -----------------------------------------------------------
		{
			name: "priority at lower bound -100 is valid",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`["x"]`),
				Options: &EnqueueOptions{
					Priority: intPtr(-100),
				},
			},
			wantErr: false,
		},
		{
			name: "priority at upper bound 100 is valid",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`["x"]`),
				Options: &EnqueueOptions{
					Priority: intPtr(100),
				},
			},
			wantErr: false,
		},
		{
			name: "priority zero is valid",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`["x"]`),
				Options: &EnqueueOptions{
					Priority: intPtr(0),
				},
			},
			wantErr: false,
		},
		{
			name: "priority nil (not set) is valid",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`["x"]`),
				Options: &EnqueueOptions{
					Priority: nil,
				},
			},
			wantErr: false,
		},
		{
			name: "priority below -100 returns error",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`["x"]`),
				Options: &EnqueueOptions{
					Priority: intPtr(-101),
				},
			},
			wantErr:   true,
			wantCode:  ErrCodeInvalidRequest,
			wantField: "priority",
		},
		{
			name: "priority above 100 returns error",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`["x"]`),
				Options: &EnqueueOptions{
					Priority: intPtr(101),
				},
			},
			wantErr:   true,
			wantCode:  ErrCodeInvalidRequest,
			wantField: "priority",
		},
		{
			name: "priority far below range returns error",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`["x"]`),
				Options: &EnqueueOptions{
					Priority: intPtr(-1000),
				},
			},
			wantErr:   true,
			wantCode:  ErrCodeInvalidRequest,
			wantField: "priority",
		},
		{
			name: "priority far above range returns error",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`["x"]`),
				Options: &EnqueueOptions{
					Priority: intPtr(1000),
				},
			},
			wantErr:   true,
			wantCode:  ErrCodeInvalidRequest,
			wantField: "priority",
		},

		// -----------------------------------------------------------
		// Retry policy: max_attempts
		// -----------------------------------------------------------
		{
			name: "retry max_attempts zero is valid",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`["x"]`),
				Options: &EnqueueOptions{
					Retry: &RetryPolicy{
						MaxAttempts: 0,
					},
				},
			},
			wantErr: false,
		},
		{
			name: "retry max_attempts positive is valid",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`["x"]`),
				Options: &EnqueueOptions{
					Retry: &RetryPolicy{
						MaxAttempts: 10,
					},
				},
			},
			wantErr: false,
		},
		{
			name: "retry max_attempts negative returns error",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`["x"]`),
				Options: &EnqueueOptions{
					Retry: &RetryPolicy{
						MaxAttempts: -1,
					},
				},
			},
			wantErr:   true,
			wantCode:  ErrCodeValidationError,
			wantField: "retry.max_attempts",
		},
		{
			name: "retry max_attempts very negative returns error",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`["x"]`),
				Options: &EnqueueOptions{
					Retry: &RetryPolicy{
						MaxAttempts: -100,
					},
				},
			},
			wantErr:   true,
			wantCode:  ErrCodeValidationError,
			wantField: "retry.max_attempts",
		},

		// -----------------------------------------------------------
		// Retry policy: backoff_coefficient
		// -----------------------------------------------------------
		{
			name: "retry backoff_coefficient zero (unset) is valid",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`["x"]`),
				Options: &EnqueueOptions{
					Retry: &RetryPolicy{
						MaxAttempts:        3,
						BackoffCoefficient: 0,
					},
				},
			},
			wantErr: false,
		},
		{
			name: "retry backoff_coefficient exactly 1.0 is valid",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`["x"]`),
				Options: &EnqueueOptions{
					Retry: &RetryPolicy{
						MaxAttempts:        3,
						BackoffCoefficient: 1.0,
					},
				},
			},
			wantErr: false,
		},
		{
			name: "retry backoff_coefficient 2.0 is valid",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`["x"]`),
				Options: &EnqueueOptions{
					Retry: &RetryPolicy{
						MaxAttempts:        3,
						BackoffCoefficient: 2.0,
					},
				},
			},
			wantErr: false,
		},
		{
			name: "retry backoff_coefficient 0.5 returns error",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`["x"]`),
				Options: &EnqueueOptions{
					Retry: &RetryPolicy{
						MaxAttempts:        3,
						BackoffCoefficient: 0.5,
					},
				},
			},
			wantErr:   true,
			wantCode:  ErrCodeValidationError,
			wantField: "retry.backoff_coefficient",
		},
		{
			name: "retry backoff_coefficient 0.99 returns error",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`["x"]`),
				Options: &EnqueueOptions{
					Retry: &RetryPolicy{
						MaxAttempts:        3,
						BackoffCoefficient: 0.99,
					},
				},
			},
			wantErr:   true,
			wantCode:  ErrCodeValidationError,
			wantField: "retry.backoff_coefficient",
		},
		{
			name: "retry backoff_coefficient very small positive returns error",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`["x"]`),
				Options: &EnqueueOptions{
					Retry: &RetryPolicy{
						MaxAttempts:        3,
						BackoffCoefficient: 0.001,
					},
				},
			},
			wantErr:   true,
			wantCode:  ErrCodeValidationError,
			wantField: "retry.backoff_coefficient",
		},

		// -----------------------------------------------------------
		// Retry policy: initial_interval
		// -----------------------------------------------------------
		{
			name: "retry valid initial_interval",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`["x"]`),
				Options: &EnqueueOptions{
					Retry: &RetryPolicy{
						MaxAttempts:     3,
						InitialInterval: "PT1S",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "retry initial_interval with minutes",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`["x"]`),
				Options: &EnqueueOptions{
					Retry: &RetryPolicy{
						MaxAttempts:     3,
						InitialInterval: "PT5M",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "retry initial_interval empty string is valid (not specified)",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`["x"]`),
				Options: &EnqueueOptions{
					Retry: &RetryPolicy{
						MaxAttempts:     3,
						InitialInterval: "",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "retry invalid initial_interval returns error",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`["x"]`),
				Options: &EnqueueOptions{
					Retry: &RetryPolicy{
						MaxAttempts:     3,
						InitialInterval: "not-a-duration",
					},
				},
			},
			wantErr:   true,
			wantCode:  ErrCodeInvalidRequest,
			wantField: "retry.initial_interval",
		},
		{
			name: "retry initial_interval plain number returns error",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`["x"]`),
				Options: &EnqueueOptions{
					Retry: &RetryPolicy{
						MaxAttempts:     3,
						InitialInterval: "5000",
					},
				},
			},
			wantErr:   true,
			wantCode:  ErrCodeInvalidRequest,
			wantField: "retry.initial_interval",
		},
		{
			name: "retry initial_interval zero duration returns error",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`["x"]`),
				Options: &EnqueueOptions{
					Retry: &RetryPolicy{
						MaxAttempts:     3,
						InitialInterval: "PT0S",
					},
				},
			},
			wantErr:   true,
			wantCode:  ErrCodeInvalidRequest,
			wantField: "retry.initial_interval",
		},

		// -----------------------------------------------------------
		// Retry policy: max_interval
		// -----------------------------------------------------------
		{
			name: "retry valid max_interval",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`["x"]`),
				Options: &EnqueueOptions{
					Retry: &RetryPolicy{
						MaxAttempts: 3,
						MaxInterval: "PT5M",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "retry max_interval with hours",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`["x"]`),
				Options: &EnqueueOptions{
					Retry: &RetryPolicy{
						MaxAttempts: 3,
						MaxInterval: "PT1H",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "retry max_interval empty string is valid (not specified)",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`["x"]`),
				Options: &EnqueueOptions{
					Retry: &RetryPolicy{
						MaxAttempts: 3,
						MaxInterval: "",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "retry invalid max_interval returns error",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`["x"]`),
				Options: &EnqueueOptions{
					Retry: &RetryPolicy{
						MaxAttempts: 3,
						MaxInterval: "invalid",
					},
				},
			},
			wantErr:   true,
			wantCode:  ErrCodeInvalidRequest,
			wantField: "retry.max_interval",
		},
		{
			name: "retry max_interval with day component returns error",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`["x"]`),
				Options: &EnqueueOptions{
					Retry: &RetryPolicy{
						MaxAttempts: 3,
						MaxInterval: "P1D",
					},
				},
			},
			wantErr:   true,
			wantCode:  ErrCodeInvalidRequest,
			wantField: "retry.max_interval",
		},
		{
			name: "retry max_interval zero duration returns error",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`["x"]`),
				Options: &EnqueueOptions{
					Retry: &RetryPolicy{
						MaxAttempts: 3,
						MaxInterval: "PT0S",
					},
				},
			},
			wantErr:   true,
			wantCode:  ErrCodeInvalidRequest,
			wantField: "retry.max_interval",
		},

		// -----------------------------------------------------------
		// Retry policy via RetryPolicy alias field
		// -----------------------------------------------------------
		{
			name: "retry_policy alias with negative max_attempts returns error",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`["x"]`),
				Options: &EnqueueOptions{
					RetryPolicy: &RetryPolicy{
						MaxAttempts: -5,
					},
				},
			},
			wantErr:   true,
			wantCode:  ErrCodeValidationError,
			wantField: "retry.max_attempts",
		},
		{
			name: "retry_policy alias with invalid initial_interval returns error",
			req: &EnqueueRequest{
				Type: "test",
				Args: json.RawMessage(`["x"]`),
				Options: &EnqueueOptions{
					RetryPolicy: &RetryPolicy{
						MaxAttempts:     3,
						InitialInterval: "bad",
					},
				},
			},
			wantErr:   true,
			wantCode:  ErrCodeInvalidRequest,
			wantField: "retry.initial_interval",
		},

		// -----------------------------------------------------------
		// Nil options is valid
		// -----------------------------------------------------------
		{
			name: "nil options is valid",
			req: &EnqueueRequest{
				Type:    "test",
				Args:    json.RawMessage(`["x"]`),
				Options: nil,
			},
			wantErr: false,
		},

		// -----------------------------------------------------------
		// Multiple option fields combined
		// -----------------------------------------------------------
		{
			name: "valid request with queue priority and retry",
			req: &EnqueueRequest{
				Type: "email.send",
				Args: json.RawMessage(`["user@example.com", "Welcome!"]`),
				Options: &EnqueueOptions{
					Queue:    "email",
					Priority: intPtr(50),
					Retry: &RetryPolicy{
						MaxAttempts:        5,
						InitialInterval:    "PT2S",
						BackoffCoefficient: 1.5,
						MaxInterval:        "PT10M",
					},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateEnqueueRequest(tt.req)

			if tt.wantErr {
				if err == nil {
					t.Fatal("ValidateEnqueueRequest() returned nil, want error")
				}
				if tt.wantCode != "" && err.Code != tt.wantCode {
					t.Errorf("error code = %q, want %q", err.Code, tt.wantCode)
				}
				if tt.wantField != "" {
					field, ok := err.Details["field"]
					if !ok {
						t.Errorf("error details missing 'field' key, got details: %v", err.Details)
					} else if field != tt.wantField {
						t.Errorf("error details field = %q, want %q", field, tt.wantField)
					}
				}
			} else if err != nil {
				t.Errorf("ValidateEnqueueRequest() returned unexpected error: %v", err)
			}
		})
	}
}
