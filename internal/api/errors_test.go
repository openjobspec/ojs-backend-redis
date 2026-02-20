package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/openjobspec/ojs-backend-redis/internal/core"
)

func TestWriteJSON(t *testing.T) {
	type payload struct {
		Name  string `json:"name"`
		Count int    `json:"count"`
	}

	tests := []struct {
		name       string
		status     int
		data       any
		wantStatus int
	}{
		{
			name:       "200 OK with struct body",
			status:     http.StatusOK,
			data:       payload{Name: "test", Count: 42},
			wantStatus: http.StatusOK,
		},
		{
			name:       "201 Created with map body",
			status:     http.StatusCreated,
			data:       map[string]string{"id": "abc-123"},
			wantStatus: http.StatusCreated,
		},
		{
			name:       "204-range status with nil body",
			status:     http.StatusNoContent,
			data:       nil,
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "200 OK with slice body",
			status:     http.StatusOK,
			data:       []string{"a", "b", "c"},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()

			WriteJSON(w, tt.status, tt.data)

			resp := w.Result()
			defer resp.Body.Close()

			// Check status code.
			if resp.StatusCode != tt.wantStatus {
				t.Errorf("status code = %d, want %d", resp.StatusCode, tt.wantStatus)
			}

			// Check Content-Type header.
			ct := resp.Header.Get("Content-Type")
			if ct != core.OJSMediaType {
				t.Errorf("Content-Type = %q, want %q", ct, core.OJSMediaType)
			}

			// Check body is valid JSON.
			var decoded any
			if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
				t.Errorf("failed to decode response body as JSON: %v", err)
			}
		})
	}
}

func TestWriteError(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		ojsErr     *core.OJSError
		wantStatus int
		wantCode   string
		wantMsg    string
	}{
		{
			name:   "400 invalid_request error",
			status: http.StatusBadRequest,
			ojsErr: &core.OJSError{
				Code:    core.ErrCodeInvalidRequest,
				Message: "missing required field",
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   core.ErrCodeInvalidRequest,
			wantMsg:    "missing required field",
		},
		{
			name:   "404 not_found error",
			status: http.StatusNotFound,
			ojsErr: &core.OJSError{
				Code:    core.ErrCodeNotFound,
				Message: "job 'xyz' not found.",
			},
			wantStatus: http.StatusNotFound,
			wantCode:   core.ErrCodeNotFound,
			wantMsg:    "job 'xyz' not found.",
		},
		{
			name:   "500 internal_error with retryable flag",
			status: http.StatusInternalServerError,
			ojsErr: &core.OJSError{
				Code:      core.ErrCodeInternalError,
				Message:   "connection lost",
				Retryable: true,
			},
			wantStatus: http.StatusInternalServerError,
			wantCode:   core.ErrCodeInternalError,
			wantMsg:    "connection lost",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()

			WriteError(w, tt.status, tt.ojsErr)

			resp := w.Result()
			defer resp.Body.Close()

			// Check status code.
			if resp.StatusCode != tt.wantStatus {
				t.Errorf("status code = %d, want %d", resp.StatusCode, tt.wantStatus)
			}

			// Check Content-Type header.
			ct := resp.Header.Get("Content-Type")
			if ct != core.OJSMediaType {
				t.Errorf("Content-Type = %q, want %q", ct, core.OJSMediaType)
			}

			// Decode and verify the error envelope.
			var errResp ErrorResponse
			if err := json.NewDecoder(resp.Body).Decode(&errResp); err != nil {
				t.Fatalf("failed to decode error response: %v", err)
			}

			if errResp.Error == nil {
				t.Fatal("expected error field in response, got nil")
			}
			if errResp.Error.Code != tt.wantCode {
				t.Errorf("error.code = %q, want %q", errResp.Error.Code, tt.wantCode)
			}
			if errResp.Error.Message != tt.wantMsg {
				t.Errorf("error.message = %q, want %q", errResp.Error.Message, tt.wantMsg)
			}
		})
	}
}

func TestWriteError_IncludesRequestID(t *testing.T) {
	tests := []struct {
		name        string
		requestID   string
		wantInBody  bool
		wantReqID   string
	}{
		{
			name:       "X-Request-Id present is included in error body",
			requestID:  "req-abc-123",
			wantInBody: true,
			wantReqID:  "req-abc-123",
		},
		{
			name:       "X-Request-Id with UUID format",
			requestID:  "550e8400-e29b-41d4-a716-446655440000",
			wantInBody: true,
			wantReqID:  "550e8400-e29b-41d4-a716-446655440000",
		},
		{
			name:       "no X-Request-Id header leaves request_id empty",
			requestID:  "",
			wantInBody: false,
			wantReqID:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()

			if tt.requestID != "" {
				w.Header().Set("X-Request-Id", tt.requestID)
			}

			ojsErr := &core.OJSError{
				Code:    core.ErrCodeInternalError,
				Message: "something went wrong",
			}
			WriteError(w, http.StatusInternalServerError, ojsErr)

			resp := w.Result()
			defer resp.Body.Close()

			var errResp ErrorResponse
			if err := json.NewDecoder(resp.Body).Decode(&errResp); err != nil {
				t.Fatalf("failed to decode error response: %v", err)
			}

			if errResp.Error == nil {
				t.Fatal("expected error field in response, got nil")
			}

			if tt.wantInBody {
				if errResp.Error.RequestID != tt.wantReqID {
					t.Errorf("error.request_id = %q, want %q", errResp.Error.RequestID, tt.wantReqID)
				}
			} else {
				if errResp.Error.RequestID != "" {
					t.Errorf("error.request_id = %q, want empty", errResp.Error.RequestID)
				}
			}
		})
	}
}

func TestWriteOJSError(t *testing.T) {
	tests := []struct {
		name       string
		ojsErr     *core.OJSError
		wantStatus int
	}{
		{
			name: "not_found maps to 404",
			ojsErr: &core.OJSError{
				Code:    core.ErrCodeNotFound,
				Message: "resource not found",
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "conflict maps to 409",
			ojsErr: &core.OJSError{
				Code:    core.ErrCodeConflict,
				Message: "state conflict",
			},
			wantStatus: http.StatusConflict,
		},
		{
			name: "duplicate maps to 409",
			ojsErr: &core.OJSError{
				Code:    core.ErrCodeDuplicate,
				Message: "duplicate job",
			},
			wantStatus: http.StatusConflict,
		},
		{
			name: "invalid_request maps to 400",
			ojsErr: &core.OJSError{
				Code:    core.ErrCodeInvalidRequest,
				Message: "bad input",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "validation_error maps to 422",
			ojsErr: &core.OJSError{
				Code:    core.ErrCodeValidationError,
				Message: "field validation failed",
			},
			wantStatus: http.StatusUnprocessableEntity,
		},
		{
			name: "queue_paused maps to 409",
			ojsErr: &core.OJSError{
				Code:    core.ErrCodeQueuePaused,
				Message: "queue is paused",
			},
			wantStatus: http.StatusConflict,
		},
		{
			name: "internal_error maps to 500",
			ojsErr: &core.OJSError{
				Code:      core.ErrCodeInternalError,
				Message:   "unexpected failure",
				Retryable: true,
			},
			wantStatus: http.StatusInternalServerError,
		},
		{
			name: "unknown error code defaults to 500",
			ojsErr: &core.OJSError{
				Code:    "some_unknown_code",
				Message: "unknown error type",
			},
			wantStatus: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()

			WriteOJSError(w, tt.ojsErr)

			resp := w.Result()
			defer resp.Body.Close()

			// Check status code.
			if resp.StatusCode != tt.wantStatus {
				t.Errorf("status code = %d, want %d", resp.StatusCode, tt.wantStatus)
			}

			// Check Content-Type header.
			ct := resp.Header.Get("Content-Type")
			if ct != core.OJSMediaType {
				t.Errorf("Content-Type = %q, want %q", ct, core.OJSMediaType)
			}

			// Decode and verify the error envelope.
			var errResp ErrorResponse
			if err := json.NewDecoder(resp.Body).Decode(&errResp); err != nil {
				t.Fatalf("failed to decode error response: %v", err)
			}

			if errResp.Error == nil {
				t.Fatal("expected error field in response, got nil")
			}
			if errResp.Error.Code != tt.ojsErr.Code {
				t.Errorf("error.code = %q, want %q", errResp.Error.Code, tt.ojsErr.Code)
			}
			if errResp.Error.Message != tt.ojsErr.Message {
				t.Errorf("error.message = %q, want %q", errResp.Error.Message, tt.ojsErr.Message)
			}
		})
	}
}

func TestHandleError_WithOJSError(t *testing.T) {
	tests := []struct {
		name       string
		err        *core.OJSError
		wantStatus int
		wantCode   string
	}{
		{
			name:       "OJSError not_found yields 404",
			err:        core.NewNotFoundError("job", "job-123"),
			wantStatus: http.StatusNotFound,
			wantCode:   core.ErrCodeNotFound,
		},
		{
			name:       "OJSError conflict yields 409",
			err:        core.NewConflictError("already exists", nil),
			wantStatus: http.StatusConflict,
			wantCode:   core.ErrCodeConflict,
		},
		{
			name:       "OJSError invalid_request yields 400",
			err:        core.NewInvalidRequestError("bad payload", nil),
			wantStatus: http.StatusBadRequest,
			wantCode:   core.ErrCodeInvalidRequest,
		},
		{
			name:       "OJSError validation_error yields 422",
			err:        core.NewValidationError("invalid field", nil),
			wantStatus: http.StatusUnprocessableEntity,
			wantCode:   core.ErrCodeValidationError,
		},
		{
			name:       "OJSError internal_error yields 500",
			err:        core.NewInternalError("database timeout"),
			wantStatus: http.StatusInternalServerError,
			wantCode:   core.ErrCodeInternalError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()

			HandleError(w, tt.err)

			resp := w.Result()
			defer resp.Body.Close()

			// Check status code.
			if resp.StatusCode != tt.wantStatus {
				t.Errorf("status code = %d, want %d", resp.StatusCode, tt.wantStatus)
			}

			// Check Content-Type header.
			ct := resp.Header.Get("Content-Type")
			if ct != core.OJSMediaType {
				t.Errorf("Content-Type = %q, want %q", ct, core.OJSMediaType)
			}

			// Decode and verify the error envelope.
			var errResp ErrorResponse
			if err := json.NewDecoder(resp.Body).Decode(&errResp); err != nil {
				t.Fatalf("failed to decode error response: %v", err)
			}

			if errResp.Error == nil {
				t.Fatal("expected error field in response, got nil")
			}
			if errResp.Error.Code != tt.wantCode {
				t.Errorf("error.code = %q, want %q", errResp.Error.Code, tt.wantCode)
			}
		})
	}
}

func TestHandleError_WithGenericError(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		wantMsg string
	}{
		{
			name:    "plain error wraps as internal_error",
			err:     errors.New("something broke"),
			wantMsg: "an internal error occurred",
		},
		{
			name:    "formatted error wraps as internal_error",
			err:     errors.New("connection refused: dial tcp 127.0.0.1:6379"),
			wantMsg: "an internal error occurred",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()

			HandleError(w, tt.err)

			resp := w.Result()
			defer resp.Body.Close()

			// Generic errors always map to 500.
			if resp.StatusCode != http.StatusInternalServerError {
				t.Errorf("status code = %d, want %d", resp.StatusCode, http.StatusInternalServerError)
			}

			// Check Content-Type header.
			ct := resp.Header.Get("Content-Type")
			if ct != core.OJSMediaType {
				t.Errorf("Content-Type = %q, want %q", ct, core.OJSMediaType)
			}

			// Decode and verify the error envelope.
			var errResp ErrorResponse
			if err := json.NewDecoder(resp.Body).Decode(&errResp); err != nil {
				t.Fatalf("failed to decode error response: %v", err)
			}

			if errResp.Error == nil {
				t.Fatal("expected error field in response, got nil")
			}
			if errResp.Error.Code != core.ErrCodeInternalError {
				t.Errorf("error.code = %q, want %q", errResp.Error.Code, core.ErrCodeInternalError)
			}
			if errResp.Error.Message != tt.wantMsg {
				t.Errorf("error.message = %q, want %q", errResp.Error.Message, tt.wantMsg)
			}
			if !errResp.Error.Retryable {
				t.Error("expected retryable = true for internal errors")
			}
		})
	}
}
