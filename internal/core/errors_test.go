package core

import (
	"testing"
)

func TestOJSError_Error(t *testing.T) {
	tests := []struct {
		name     string
		err      *OJSError
		expected string
	}{
		{
			name:     "standard code and message",
			err:      &OJSError{Code: "not_found", Message: "job not found"},
			expected: "[not_found] job not found",
		},
		{
			name:     "empty code and message",
			err:      &OJSError{Code: "", Message: ""},
			expected: "[] ",
		},
		{
			name:     "code with empty message",
			err:      &OJSError{Code: "internal_error", Message: ""},
			expected: "[internal_error] ",
		},
		{
			name:     "empty code with message",
			err:      &OJSError{Code: "", Message: "something went wrong"},
			expected: "[] something went wrong",
		},
		{
			name:     "message containing special characters",
			err:      &OJSError{Code: "validation_error", Message: "field 'name' is required [must not be empty]"},
			expected: "[validation_error] field 'name' is required [must not be empty]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.err.Error()
			if got != tt.expected {
				t.Errorf("Error() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestOJSError_ImplementsErrorInterface(t *testing.T) {
	var _ error = (*OJSError)(nil)
}

func TestNewInvalidRequestError(t *testing.T) {
	tests := []struct {
		name            string
		message         string
		details         map[string]any
		expectedCode    string
		expectedMessage string
		expectedRetry   bool
		expectNilDetail bool
	}{
		{
			name:            "with details",
			message:         "missing required field",
			details:         map[string]any{"field": "queue"},
			expectedCode:    ErrCodeInvalidRequest,
			expectedMessage: "missing required field",
			expectedRetry:   false,
			expectNilDetail: false,
		},
		{
			name:            "with nil details",
			message:         "bad request body",
			details:         nil,
			expectedCode:    ErrCodeInvalidRequest,
			expectedMessage: "bad request body",
			expectedRetry:   false,
			expectNilDetail: true,
		},
		{
			name:            "with multiple details",
			message:         "multiple validation failures",
			details:         map[string]any{"field1": "queue", "field2": "type"},
			expectedCode:    ErrCodeInvalidRequest,
			expectedMessage: "multiple validation failures",
			expectedRetry:   false,
			expectNilDetail: false,
		},
		{
			name:            "with empty message",
			message:         "",
			details:         map[string]any{},
			expectedCode:    ErrCodeInvalidRequest,
			expectedMessage: "",
			expectedRetry:   false,
			expectNilDetail: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NewInvalidRequestError(tt.message, tt.details)

			if err.Code != tt.expectedCode {
				t.Errorf("Code = %q, want %q", err.Code, tt.expectedCode)
			}
			if err.Message != tt.expectedMessage {
				t.Errorf("Message = %q, want %q", err.Message, tt.expectedMessage)
			}
			if err.Retryable != tt.expectedRetry {
				t.Errorf("Retryable = %v, want %v", err.Retryable, tt.expectedRetry)
			}
			if tt.expectNilDetail && err.Details != nil {
				t.Errorf("Details = %v, want nil", err.Details)
			}
			if !tt.expectNilDetail && err.Details == nil {
				t.Error("Details is nil, want non-nil")
			}
			if err.Type != "" {
				t.Errorf("Type = %q, want empty string", err.Type)
			}
			if err.RequestID != "" {
				t.Errorf("RequestID = %q, want empty string", err.RequestID)
			}

			// Verify details content when provided
			if tt.details != nil {
				for k, v := range tt.details {
					got, ok := err.Details[k]
					if !ok {
						t.Errorf("Details missing key %q", k)
					} else if got != v {
						t.Errorf("Details[%q] = %v, want %v", k, got, v)
					}
				}
			}
		})
	}
}

func TestNewNotFoundError(t *testing.T) {
	tests := []struct {
		name             string
		resourceType     string
		resourceID       string
		expectedMessage  string
		expectedCode     string
		expectedRetry    bool
		expectedResType  string
		expectedResID    string
	}{
		{
			name:            "job not found",
			resourceType:    "job",
			resourceID:      "01234567-89ab-cdef-0123-456789abcdef",
			expectedMessage: "job '01234567-89ab-cdef-0123-456789abcdef' not found.",
			expectedCode:    ErrCodeNotFound,
			expectedRetry:   false,
			expectedResType: "job",
			expectedResID:   "01234567-89ab-cdef-0123-456789abcdef",
		},
		{
			name:            "queue not found",
			resourceType:    "queue",
			resourceID:      "default",
			expectedMessage: "queue 'default' not found.",
			expectedCode:    ErrCodeNotFound,
			expectedRetry:   false,
			expectedResType: "queue",
			expectedResID:   "default",
		},
		{
			name:            "workflow not found",
			resourceType:    "workflow",
			resourceID:      "wf-abc-123",
			expectedMessage: "workflow 'wf-abc-123' not found.",
			expectedCode:    ErrCodeNotFound,
			expectedRetry:   false,
			expectedResType: "workflow",
			expectedResID:   "wf-abc-123",
		},
		{
			name:            "empty resource type and id",
			resourceType:    "",
			resourceID:      "",
			expectedMessage: " '' not found.",
			expectedCode:    ErrCodeNotFound,
			expectedRetry:   false,
			expectedResType: "",
			expectedResID:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NewNotFoundError(tt.resourceType, tt.resourceID)

			if err.Code != tt.expectedCode {
				t.Errorf("Code = %q, want %q", err.Code, tt.expectedCode)
			}
			if err.Message != tt.expectedMessage {
				t.Errorf("Message = %q, want %q", err.Message, tt.expectedMessage)
			}
			if err.Retryable != tt.expectedRetry {
				t.Errorf("Retryable = %v, want %v", err.Retryable, tt.expectedRetry)
			}
			if err.Type != "" {
				t.Errorf("Type = %q, want empty string", err.Type)
			}
			if err.RequestID != "" {
				t.Errorf("RequestID = %q, want empty string", err.RequestID)
			}

			// Verify details contain resource_type and resource_id
			if err.Details == nil {
				t.Fatal("Details is nil, want non-nil map with resource_type and resource_id")
			}
			if got, ok := err.Details["resource_type"]; !ok {
				t.Error("Details missing key \"resource_type\"")
			} else if got != tt.expectedResType {
				t.Errorf("Details[\"resource_type\"] = %v, want %v", got, tt.expectedResType)
			}
			if got, ok := err.Details["resource_id"]; !ok {
				t.Error("Details missing key \"resource_id\"")
			} else if got != tt.expectedResID {
				t.Errorf("Details[\"resource_id\"] = %v, want %v", got, tt.expectedResID)
			}
		})
	}
}

func TestNewConflictError(t *testing.T) {
	tests := []struct {
		name            string
		message         string
		details         map[string]any
		expectedCode    string
		expectedMessage string
		expectedRetry   bool
		expectNilDetail bool
	}{
		{
			name:            "state transition conflict",
			message:         "job cannot transition from completed to active",
			details:         map[string]any{"current_state": "completed", "target_state": "active"},
			expectedCode:    ErrCodeConflict,
			expectedMessage: "job cannot transition from completed to active",
			expectedRetry:   false,
			expectNilDetail: false,
		},
		{
			name:            "with nil details",
			message:         "resource conflict",
			details:         nil,
			expectedCode:    ErrCodeConflict,
			expectedMessage: "resource conflict",
			expectedRetry:   false,
			expectNilDetail: true,
		},
		{
			name:            "with empty message",
			message:         "",
			details:         map[string]any{},
			expectedCode:    ErrCodeConflict,
			expectedMessage: "",
			expectedRetry:   false,
			expectNilDetail: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NewConflictError(tt.message, tt.details)

			if err.Code != tt.expectedCode {
				t.Errorf("Code = %q, want %q", err.Code, tt.expectedCode)
			}
			if err.Message != tt.expectedMessage {
				t.Errorf("Message = %q, want %q", err.Message, tt.expectedMessage)
			}
			if err.Retryable != tt.expectedRetry {
				t.Errorf("Retryable = %v, want %v", err.Retryable, tt.expectedRetry)
			}
			if tt.expectNilDetail && err.Details != nil {
				t.Errorf("Details = %v, want nil", err.Details)
			}
			if !tt.expectNilDetail && err.Details == nil {
				t.Error("Details is nil, want non-nil")
			}
			if err.Type != "" {
				t.Errorf("Type = %q, want empty string", err.Type)
			}
			if err.RequestID != "" {
				t.Errorf("RequestID = %q, want empty string", err.RequestID)
			}

			// Verify details content when provided
			if tt.details != nil {
				for k, v := range tt.details {
					got, ok := err.Details[k]
					if !ok {
						t.Errorf("Details missing key %q", k)
					} else if got != v {
						t.Errorf("Details[%q] = %v, want %v", k, got, v)
					}
				}
			}
		})
	}
}

func TestNewValidationError(t *testing.T) {
	tests := []struct {
		name            string
		message         string
		details         map[string]any
		expectedCode    string
		expectedType    string
		expectedMessage string
		expectedRetry   bool
		expectNilDetail bool
	}{
		{
			name:            "field validation failure",
			message:         "field 'queue' is required",
			details:         map[string]any{"field": "queue"},
			expectedCode:    ErrCodeValidationError,
			expectedType:    ErrCodeValidationError,
			expectedMessage: "field 'queue' is required",
			expectedRetry:   false,
			expectNilDetail: false,
		},
		{
			name:            "type field matches code",
			message:         "invalid args format",
			details:         map[string]any{"expected": "array", "got": "object"},
			expectedCode:    ErrCodeValidationError,
			expectedType:    ErrCodeValidationError,
			expectedMessage: "invalid args format",
			expectedRetry:   false,
			expectNilDetail: false,
		},
		{
			name:            "with nil details",
			message:         "validation failed",
			details:         nil,
			expectedCode:    ErrCodeValidationError,
			expectedType:    ErrCodeValidationError,
			expectedMessage: "validation failed",
			expectedRetry:   false,
			expectNilDetail: true,
		},
		{
			name:            "with empty message",
			message:         "",
			details:         map[string]any{},
			expectedCode:    ErrCodeValidationError,
			expectedType:    ErrCodeValidationError,
			expectedMessage: "",
			expectedRetry:   false,
			expectNilDetail: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NewValidationError(tt.message, tt.details)

			if err.Code != tt.expectedCode {
				t.Errorf("Code = %q, want %q", err.Code, tt.expectedCode)
			}
			if err.Type != tt.expectedType {
				t.Errorf("Type = %q, want %q", err.Type, tt.expectedType)
			}
			if err.Message != tt.expectedMessage {
				t.Errorf("Message = %q, want %q", err.Message, tt.expectedMessage)
			}
			if err.Retryable != tt.expectedRetry {
				t.Errorf("Retryable = %v, want %v", err.Retryable, tt.expectedRetry)
			}
			if tt.expectNilDetail && err.Details != nil {
				t.Errorf("Details = %v, want nil", err.Details)
			}
			if !tt.expectNilDetail && err.Details == nil {
				t.Error("Details is nil, want non-nil")
			}
			if err.RequestID != "" {
				t.Errorf("RequestID = %q, want empty string", err.RequestID)
			}

			// Verify details content when provided
			if tt.details != nil {
				for k, v := range tt.details {
					got, ok := err.Details[k]
					if !ok {
						t.Errorf("Details missing key %q", k)
					} else if got != v {
						t.Errorf("Details[%q] = %v, want %v", k, got, v)
					}
				}
			}
		})
	}
}

func TestNewInternalError(t *testing.T) {
	tests := []struct {
		name            string
		message         string
		expectedCode    string
		expectedMessage string
		expectedRetry   bool
	}{
		{
			name:            "database connection failure",
			message:         "failed to connect to Redis",
			expectedCode:    ErrCodeInternalError,
			expectedMessage: "failed to connect to Redis",
			expectedRetry:   true,
		},
		{
			name:            "generic internal error",
			message:         "an unexpected error occurred",
			expectedCode:    ErrCodeInternalError,
			expectedMessage: "an unexpected error occurred",
			expectedRetry:   true,
		},
		{
			name:            "empty message",
			message:         "",
			expectedCode:    ErrCodeInternalError,
			expectedMessage: "",
			expectedRetry:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NewInternalError(tt.message)

			if err.Code != tt.expectedCode {
				t.Errorf("Code = %q, want %q", err.Code, tt.expectedCode)
			}
			if err.Message != tt.expectedMessage {
				t.Errorf("Message = %q, want %q", err.Message, tt.expectedMessage)
			}
			if err.Retryable != tt.expectedRetry {
				t.Errorf("Retryable = %v, want %v", err.Retryable, tt.expectedRetry)
			}
			if err.Details != nil {
				t.Errorf("Details = %v, want nil", err.Details)
			}
			if err.Type != "" {
				t.Errorf("Type = %q, want empty string", err.Type)
			}
			if err.RequestID != "" {
				t.Errorf("RequestID = %q, want empty string", err.RequestID)
			}
		})
	}
}

func TestErrorConstants(t *testing.T) {
	tests := []struct {
		name     string
		constant string
		expected string
	}{
		{name: "ErrCodeInvalidRequest", constant: ErrCodeInvalidRequest, expected: "invalid_request"},
		{name: "ErrCodeValidationError", constant: ErrCodeValidationError, expected: "validation_error"},
		{name: "ErrCodeNotFound", constant: ErrCodeNotFound, expected: "not_found"},
		{name: "ErrCodeConflict", constant: ErrCodeConflict, expected: "conflict"},
		{name: "ErrCodeDuplicate", constant: ErrCodeDuplicate, expected: "duplicate"},
		{name: "ErrCodeInternalError", constant: ErrCodeInternalError, expected: "internal_error"},
		{name: "ErrCodeUnsupported", constant: ErrCodeUnsupported, expected: "unsupported"},
		{name: "ErrCodeQueuePaused", constant: ErrCodeQueuePaused, expected: "queue_paused"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.constant != tt.expected {
				t.Errorf("%s = %q, want %q", tt.name, tt.constant, tt.expected)
			}
		})
	}
}
