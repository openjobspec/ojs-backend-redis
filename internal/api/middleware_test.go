package api

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/openjobspec/ojs-backend-redis/internal/core"
)

// passthrough is a simple handler that writes a 200 OK response.
// It is used as the "next" handler in middleware tests.
var passthrough = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
})

func TestOJSHeaders(t *testing.T) {
	tests := []struct {
		name            string
		requestID       string // value to set for X-Request-Id on the incoming request; empty means omit
		wantVersion     string
		wantContentType string
		wantRequestID   string // exact match; empty means check prefix only
		wantReqIDPrefix string // non-empty means verify this prefix instead of exact match
	}{
		{
			name:            "sets OJS-Version header",
			requestID:       "test-id-123",
			wantVersion:     core.OJSVersion,
			wantContentType: core.OJSMediaType,
			wantRequestID:   "test-id-123",
		},
		{
			name:            "sets Content-Type header",
			requestID:       "ct-check",
			wantVersion:     core.OJSVersion,
			wantContentType: core.OJSMediaType,
			wantRequestID:   "ct-check",
		},
		{
			name:            "echoes provided X-Request-Id",
			requestID:       "my-custom-request-id",
			wantVersion:     core.OJSVersion,
			wantContentType: core.OJSMediaType,
			wantRequestID:   "my-custom-request-id",
		},
		{
			name:            "generates X-Request-Id starting with req_ when none provided",
			requestID:       "",
			wantVersion:     core.OJSVersion,
			wantContentType: core.OJSMediaType,
			wantReqIDPrefix: "req_",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if tt.requestID != "" {
				req.Header.Set("X-Request-Id", tt.requestID)
			}
			rr := httptest.NewRecorder()

			handler := OJSHeaders(passthrough)
			handler.ServeHTTP(rr, req)

			if got := rr.Header().Get("OJS-Version"); got != tt.wantVersion {
				t.Errorf("OJS-Version = %q, want %q", got, tt.wantVersion)
			}

			if got := rr.Header().Get("Content-Type"); got != tt.wantContentType {
				t.Errorf("Content-Type = %q, want %q", got, tt.wantContentType)
			}

			gotReqID := rr.Header().Get("X-Request-Id")
			if tt.wantRequestID != "" {
				if gotReqID != tt.wantRequestID {
					t.Errorf("X-Request-Id = %q, want %q", gotReqID, tt.wantRequestID)
				}
			}
			if tt.wantReqIDPrefix != "" {
				if !strings.HasPrefix(gotReqID, tt.wantReqIDPrefix) {
					t.Errorf("X-Request-Id = %q, want prefix %q", gotReqID, tt.wantReqIDPrefix)
				}
				// The generated ID should be longer than just the prefix (it includes a UUID).
				if len(gotReqID) <= len(tt.wantReqIDPrefix) {
					t.Errorf("X-Request-Id = %q is too short; expected prefix %q followed by a UUID", gotReqID, tt.wantReqIDPrefix)
				}
			}
		})
	}
}

func TestValidateContentType(t *testing.T) {
	tests := []struct {
		name        string
		method      string
		contentType string // empty means do not set the header
		wantStatus  int
	}{
		{
			name:        "POST with application/json passes through",
			method:      http.MethodPost,
			contentType: "application/json",
			wantStatus:  http.StatusOK,
		},
		{
			name:        "POST with application/openjobspec+json passes through",
			method:      http.MethodPost,
			contentType: core.OJSMediaType,
			wantStatus:  http.StatusOK,
		},
		{
			name:        "POST with text/plain returns 400",
			method:      http.MethodPost,
			contentType: "text/plain",
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "POST with application/json charset=utf-8 passes through",
			method:      http.MethodPost,
			contentType: "application/json; charset=utf-8",
			wantStatus:  http.StatusOK,
		},
		{
			name:        "POST with no Content-Type passes through",
			method:      http.MethodPost,
			contentType: "",
			wantStatus:  http.StatusOK,
		},
		{
			name:        "GET with invalid Content-Type passes through",
			method:      http.MethodGet,
			contentType: "text/plain",
			wantStatus:  http.StatusOK,
		},
		{
			name:        "PUT with application/json passes through",
			method:      http.MethodPut,
			contentType: "application/json",
			wantStatus:  http.StatusOK,
		},
		{
			name:        "PUT with text/xml returns 400",
			method:      http.MethodPut,
			contentType: "text/xml",
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "PATCH with application/openjobspec+json passes through",
			method:      http.MethodPatch,
			contentType: core.OJSMediaType,
			wantStatus:  http.StatusOK,
		},
		{
			name:        "PATCH with multipart/form-data returns 400",
			method:      http.MethodPatch,
			contentType: "multipart/form-data",
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "DELETE with invalid Content-Type passes through",
			method:      http.MethodDelete,
			contentType: "text/plain",
			wantStatus:  http.StatusOK,
		},
		{
			name:        "POST with openjobspec+json and charset param passes through",
			method:      http.MethodPost,
			contentType: "application/openjobspec+json; charset=utf-8",
			wantStatus:  http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, "/", nil)
			if tt.contentType != "" {
				req.Header.Set("Content-Type", tt.contentType)
			}
			rr := httptest.NewRecorder()

			handler := ValidateContentType(passthrough)
			handler.ServeHTTP(rr, req)

			if rr.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d", rr.Code, tt.wantStatus)
			}

			// When 400 is returned, the response body should contain an error message.
			if tt.wantStatus == http.StatusBadRequest {
				body := rr.Body.String()
				if !strings.Contains(body, "invalid_request") {
					t.Errorf("expected error body to contain %q, got %q", "invalid_request", body)
				}
				if !strings.Contains(body, "Unsupported Content-Type") {
					t.Errorf("expected error body to contain %q, got %q", "Unsupported Content-Type", body)
				}
			}
		})
	}
}

func TestLimitRequestBody(t *testing.T) {
	tests := []struct {
		name        string
		method      string
		body        string
		wantWrapped bool // whether r.Body should be wrapped with MaxBytesReader
	}{
		{
			name:        "POST with small body passes through",
			method:      http.MethodPost,
			body:        `{"type":"test"}`,
			wantWrapped: true,
		},
		{
			name:        "POST body is wrapped with MaxBytesReader",
			method:      http.MethodPost,
			body:        "hello",
			wantWrapped: true,
		},
		{
			name:        "GET request body not limited",
			method:      http.MethodGet,
			body:        "some body",
			wantWrapped: false,
		},
		{
			name:        "PUT body is wrapped with MaxBytesReader",
			method:      http.MethodPut,
			body:        `{"data":"value"}`,
			wantWrapped: true,
		},
		{
			name:        "PATCH body is wrapped with MaxBytesReader",
			method:      http.MethodPatch,
			body:        `{"update":true}`,
			wantWrapped: true,
		},
		{
			name:        "DELETE request body not limited",
			method:      http.MethodDelete,
			body:        "delete body",
			wantWrapped: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, "/", strings.NewReader(tt.body))
			rr := httptest.NewRecorder()

			var capturedBodyType string
			var capturedBody string

			// Use a capturing handler to inspect the request body after middleware runs.
			capturing := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				capturedBodyType = fmt.Sprintf("%T", r.Body)
				data, err := io.ReadAll(r.Body)
				if err != nil {
					t.Fatalf("unexpected error reading body: %v", err)
				}
				capturedBody = string(data)
				w.WriteHeader(http.StatusOK)
			})

			handler := LimitRequestBody(capturing)
			handler.ServeHTTP(rr, req)

			if rr.Code != http.StatusOK {
				t.Errorf("status = %d, want %d", rr.Code, http.StatusOK)
			}

			if capturedBody != tt.body {
				t.Errorf("body = %q, want %q", capturedBody, tt.body)
			}

			// http.MaxBytesReader wraps the body in an unexported *http.maxBytesReader type.
			// We check the type string to verify whether wrapping occurred.
			isWrapped := strings.Contains(capturedBodyType, "maxBytesReader")
			if tt.wantWrapped && !isWrapped {
				t.Errorf("expected body to be wrapped by MaxBytesReader, got type %s", capturedBodyType)
			}
			if !tt.wantWrapped && isWrapped {
				t.Errorf("expected body NOT to be wrapped by MaxBytesReader for %s, got type %s", tt.method, capturedBodyType)
			}
		})
	}
}
