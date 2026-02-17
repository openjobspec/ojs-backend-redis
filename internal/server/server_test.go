package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/openjobspec/ojs-backend-redis/internal/core"
	"github.com/openjobspec/ojs-backend-redis/internal/metrics"
)

type testBackend struct{}

func (b *testBackend) Push(ctx context.Context, job *core.Job) (*core.Job, error) {
	job.ID = "job-1"
	job.State = core.StateAvailable
	return job, nil
}
func (b *testBackend) Fetch(ctx context.Context, queues []string, count int, workerID string, visibilityTimeoutMs int) ([]*core.Job, error) {
	return []*core.Job{}, nil
}
func (b *testBackend) Ack(ctx context.Context, jobID string, result []byte) (*core.AckResponse, error) {
	return &core.AckResponse{Acknowledged: true, JobID: jobID, State: core.StateCompleted}, nil
}
func (b *testBackend) Nack(ctx context.Context, jobID string, jobErr *core.JobError, requeue bool) (*core.NackResponse, error) {
	return &core.NackResponse{JobID: jobID, State: core.StateRetryable}, nil
}
func (b *testBackend) Info(ctx context.Context, jobID string) (*core.Job, error) {
	return &core.Job{ID: jobID, Type: "test", State: core.StateAvailable, Queue: "default"}, nil
}
func (b *testBackend) Cancel(ctx context.Context, jobID string) (*core.Job, error) {
	return &core.Job{ID: jobID, Type: "test", State: core.StateCancelled, Queue: "default"}, nil
}
func (b *testBackend) ListQueues(ctx context.Context) ([]core.QueueInfo, error) {
	return []core.QueueInfo{{Name: "default", Status: "active"}}, nil
}
func (b *testBackend) Health(ctx context.Context) (*core.HealthResponse, error) {
	return &core.HealthResponse{
		Status:  "ok",
		Version: core.OJSVersion,
		Backend: core.BackendHealth{Type: "redis", Status: "connected"},
	}, nil
}
func (b *testBackend) Heartbeat(ctx context.Context, workerID string, activeJobs []string, visibilityTimeoutMs int) (*core.HeartbeatResponse, error) {
	return &core.HeartbeatResponse{State: "active", Directive: "continue"}, nil
}
func (b *testBackend) ListDeadLetter(ctx context.Context, limit, offset int) ([]*core.Job, int, error) {
	return []*core.Job{}, 0, nil
}
func (b *testBackend) RetryDeadLetter(ctx context.Context, jobID string) (*core.Job, error) {
	return &core.Job{ID: jobID, State: core.StateAvailable, Queue: "default"}, nil
}
func (b *testBackend) DeleteDeadLetter(ctx context.Context, jobID string) error { return nil }
func (b *testBackend) RegisterCron(ctx context.Context, cron *core.CronJob) (*core.CronJob, error) {
	return cron, nil
}
func (b *testBackend) ListCron(ctx context.Context) ([]*core.CronJob, error) {
	return []*core.CronJob{}, nil
}
func (b *testBackend) DeleteCron(ctx context.Context, name string) (*core.CronJob, error) {
	return &core.CronJob{Name: name}, nil
}
func (b *testBackend) CreateWorkflow(ctx context.Context, req *core.WorkflowRequest) (*core.Workflow, error) {
	return &core.Workflow{ID: "wf-1", Type: req.Type, State: "running", CreatedAt: core.NowFormatted()}, nil
}
func (b *testBackend) GetWorkflow(ctx context.Context, id string) (*core.Workflow, error) {
	return &core.Workflow{ID: id, Type: "chain", State: "running", CreatedAt: core.NowFormatted()}, nil
}
func (b *testBackend) CancelWorkflow(ctx context.Context, id string) (*core.Workflow, error) {
	return &core.Workflow{ID: id, Type: "chain", State: "cancelled", CreatedAt: core.NowFormatted()}, nil
}
func (b *testBackend) AdvanceWorkflow(ctx context.Context, workflowID string, jobID string, result json.RawMessage, failed bool) error {
	return nil
}
func (b *testBackend) PushBatch(ctx context.Context, jobs []*core.Job) ([]*core.Job, error) {
	return jobs, nil
}
func (b *testBackend) QueueStats(ctx context.Context, name string) (*core.QueueStats, error) {
	return &core.QueueStats{Queue: name, Status: "active"}, nil
}
func (b *testBackend) PauseQueue(ctx context.Context, name string) error  { return nil }
func (b *testBackend) ResumeQueue(ctx context.Context, name string) error { return nil }
func (b *testBackend) ListJobs(ctx context.Context, filters core.JobListFilters, limit, offset int) ([]*core.Job, int, error) {
	return []*core.Job{}, 0, nil
}
func (b *testBackend) ListWorkers(ctx context.Context, limit, offset int) ([]*core.WorkerInfo, core.WorkerSummary, error) {
	return []*core.WorkerInfo{}, core.WorkerSummary{}, nil
}
func (b *testBackend) SetWorkerState(ctx context.Context, workerID string, state string) error {
	return nil
}
func (b *testBackend) Close() error { return nil }

func TestNewRouter_SystemAndAdminRoutes(t *testing.T) {
	router := NewRouter(&testBackend{}, Config{})

	tests := []struct {
		method string
		path   string
		status int
	}{
		{method: http.MethodGet, path: "/ojs/manifest", status: http.StatusOK},
		{method: http.MethodGet, path: "/ojs/v1/health", status: http.StatusOK},
		{method: http.MethodGet, path: "/metrics", status: http.StatusOK},
		{method: http.MethodGet, path: "/ojs/v1/admin/jobs", status: http.StatusOK},
		{method: http.MethodGet, path: "/ojs/v1/admin/workers", status: http.StatusOK},
		{method: http.MethodGet, path: "/ojs/admin", status: http.StatusMovedPermanently},
	}

	for _, tt := range tests {
		req := httptest.NewRequest(tt.method, tt.path, nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != tt.status {
			t.Errorf("%s %s: expected %d, got %d", tt.method, tt.path, tt.status, w.Code)
		}
	}
}

func TestMetricsMiddleware_PreservesHandlerResponse(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("ok"))
	})

	handler := metricsMiddleware(next)
	req := httptest.NewRequest(http.MethodGet, "/test/path", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", w.Code)
	}
	if w.Body.String() != "ok" {
		t.Fatalf("unexpected body: %q", w.Body.String())
	}
}

func TestMetricsMiddleware_UsesRoutePatternLabel(t *testing.T) {
	router := NewRouter(&testBackend{}, Config{})

	templatePath := "/ojs/v1/jobs/{id}"
	concretePath := "/ojs/v1/jobs/job-123"

	beforeTemplate := testutil.ToFloat64(metrics.HTTPRequestsTotal.WithLabelValues(http.MethodGet, templatePath, "200"))
	beforeConcrete := testutil.ToFloat64(metrics.HTTPRequestsTotal.WithLabelValues(http.MethodGet, concretePath, "200"))

	req := httptest.NewRequest(http.MethodGet, concretePath, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	afterTemplate := testutil.ToFloat64(metrics.HTTPRequestsTotal.WithLabelValues(http.MethodGet, templatePath, "200"))
	afterConcrete := testutil.ToFloat64(metrics.HTTPRequestsTotal.WithLabelValues(http.MethodGet, concretePath, "200"))

	if afterTemplate != beforeTemplate+1 {
		t.Fatalf("template metric delta = %f, want +1", afterTemplate-beforeTemplate)
	}
	if afterConcrete != beforeConcrete {
		t.Fatalf("concrete-path metric should not increase: before=%f after=%f", beforeConcrete, afterConcrete)
	}
}
