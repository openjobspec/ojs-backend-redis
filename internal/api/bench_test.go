package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/openjobspec/ojs-backend-redis/internal/core"
)

// BenchmarkJobCreate benchmarks the job creation endpoint (POST /ojs/v1/jobs).
func BenchmarkJobCreate(b *testing.B) {
	router := newTestRouter(&mockBackend{})
	body := `{"type":"email.send","args":["user@example.com"],"options":{"queue":"default"}}`

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPost, "/ojs/v1/jobs", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
	}
}

// BenchmarkJobGet benchmarks the job retrieval endpoint (GET /ojs/v1/jobs/{id}).
func BenchmarkJobGet(b *testing.B) {
	router := newTestRouter(&mockBackend{})

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodGet, "/ojs/v1/jobs/test-id", nil)
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
	}
}

// BenchmarkWorkerFetch benchmarks the worker fetch endpoint (POST /ojs/v1/workers/fetch).
func BenchmarkWorkerFetch(b *testing.B) {
	backend := &mockBackend{
		fetchFunc: func(ctx context.Context, queues []string, count int, workerID string, visMs int) ([]*core.Job, error) {
			return []*core.Job{{ID: "job-1", Type: "test", State: "active", Queue: "default"}}, nil
		},
	}
	router := newTestRouter(backend)
	body := `{"queues":["default"],"worker_id":"w-1"}`

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workers/fetch", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
	}
}

// BenchmarkWorkerAck benchmarks the worker ack endpoint (POST /ojs/v1/workers/ack).
func BenchmarkWorkerAck(b *testing.B) {
	router := newTestRouter(&mockBackend{})
	body := `{"job_id":"job-1","result":{"ok":true}}`

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workers/ack", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
	}
}

// BenchmarkBatchCreate benchmarks the batch job creation endpoint (POST /ojs/v1/jobs/batch).
func BenchmarkBatchCreate(b *testing.B) {
	router := newTestRouter(&mockBackend{})
	body := `{"jobs":[{"type":"email.send","args":["a@b.com"]},{"type":"email.send","args":["c@d.com"]},{"type":"email.send","args":["e@f.com"]}]}`

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPost, "/ojs/v1/jobs/batch", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
	}
}

// BenchmarkWorkerNack benchmarks the worker nack endpoint (POST /ojs/v1/workers/nack).
func BenchmarkWorkerNack(b *testing.B) {
	router := newTestRouter(&mockBackend{})
	body := `{"job_id":"job-1","error":{"message":"timeout"}}`

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workers/nack", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
	}
}

// BenchmarkQueueStats benchmarks the queue stats endpoint (GET /ojs/v1/queues/{name}/stats).
func BenchmarkQueueStats(b *testing.B) {
	router := newTestRouter(&mockBackend{})

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodGet, "/ojs/v1/queues/default/stats", nil)
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
	}
}

// BenchmarkWorkflowCreate benchmarks the workflow creation endpoint (POST /ojs/v1/workflows).
func BenchmarkWorkflowCreate(b *testing.B) {
	router := newTestRouter(&mockBackend{})
	body := `{"steps":[{"type":"chain","jobs":[{"type":"step1","args":[]},{"type":"step2","args":[]}]}]}`

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workflows", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
	}
}

// BenchmarkHealthCheck benchmarks the health endpoint (GET /ojs/v1/health).
func BenchmarkHealthCheck(b *testing.B) {
	router := newTestRouter(&mockBackend{})

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodGet, "/ojs/v1/health", nil)
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
	}
}

