package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/openjobspec/ojs-backend-redis/internal/core"
)

// mockBackend implements core.Backend for handler testing.
type mockBackend struct {
	pushFunc             func(ctx context.Context, job *core.Job) (*core.Job, error)
	fetchFunc            func(ctx context.Context, queues []string, count int, workerID string, visMs int) ([]*core.Job, error)
	ackFunc              func(ctx context.Context, jobID string, result []byte) (*core.AckResponse, error)
	nackFunc             func(ctx context.Context, jobID string, jobErr *core.JobError, requeue bool) (*core.NackResponse, error)
	infoFunc             func(ctx context.Context, jobID string) (*core.Job, error)
	cancelFunc           func(ctx context.Context, jobID string) (*core.Job, error)
	listQueuesFunc       func(ctx context.Context) ([]core.QueueInfo, error)
	healthFunc           func(ctx context.Context) (*core.HealthResponse, error)
	heartbeatFunc        func(ctx context.Context, workerID string, activeJobs []string, visMs int) (*core.HeartbeatResponse, error)
	listDeadLetterFunc   func(ctx context.Context, limit, offset int) ([]*core.Job, int, error)
	retryDeadLetterFunc  func(ctx context.Context, jobID string) (*core.Job, error)
	deleteDeadLetterFunc func(ctx context.Context, jobID string) error
	registerCronFunc     func(ctx context.Context, cron *core.CronJob) (*core.CronJob, error)
	listCronFunc         func(ctx context.Context) ([]*core.CronJob, error)
	deleteCronFunc       func(ctx context.Context, name string) (*core.CronJob, error)
	createWorkflowFunc   func(ctx context.Context, req *core.WorkflowRequest) (*core.Workflow, error)
	getWorkflowFunc      func(ctx context.Context, id string) (*core.Workflow, error)
	cancelWorkflowFunc   func(ctx context.Context, id string) (*core.Workflow, error)
	advanceWorkflowFunc  func(ctx context.Context, workflowID string, jobID string, result json.RawMessage, failed bool) error
	pushBatchFunc        func(ctx context.Context, jobs []*core.Job) ([]*core.Job, error)
	queueStatsFunc       func(ctx context.Context, name string) (*core.QueueStats, error)
	pauseQueueFunc       func(ctx context.Context, name string) error
	resumeQueueFunc      func(ctx context.Context, name string) error
	listJobsFunc         func(ctx context.Context, filters core.JobListFilters, limit, offset int) ([]*core.Job, int, error)
	listWorkersFunc      func(ctx context.Context, limit, offset int) ([]*core.WorkerInfo, core.WorkerSummary, error)
	setWorkerStateFunc   func(ctx context.Context, workerID string, state string) error
}

func (m *mockBackend) Push(ctx context.Context, job *core.Job) (*core.Job, error) {
	if m.pushFunc != nil {
		return m.pushFunc(ctx, job)
	}
	job.ID = "test-id"
	job.State = "available"
	job.CreatedAt = "2025-01-01T00:00:00.000Z"
	job.EnqueuedAt = "2025-01-01T00:00:00.000Z"
	return job, nil
}

func (m *mockBackend) Fetch(ctx context.Context, queues []string, count int, workerID string, visMs int) ([]*core.Job, error) {
	if m.fetchFunc != nil {
		return m.fetchFunc(ctx, queues, count, workerID, visMs)
	}
	return []*core.Job{}, nil
}

func (m *mockBackend) Ack(ctx context.Context, jobID string, result []byte) (*core.AckResponse, error) {
	if m.ackFunc != nil {
		return m.ackFunc(ctx, jobID, result)
	}
	return &core.AckResponse{Acknowledged: true, JobID: jobID, State: "completed"}, nil
}

func (m *mockBackend) Nack(ctx context.Context, jobID string, jobErr *core.JobError, requeue bool) (*core.NackResponse, error) {
	if m.nackFunc != nil {
		return m.nackFunc(ctx, jobID, jobErr, requeue)
	}
	return &core.NackResponse{JobID: jobID, State: "retryable", Attempt: 1, MaxAttempts: 3}, nil
}

func (m *mockBackend) Info(ctx context.Context, jobID string) (*core.Job, error) {
	if m.infoFunc != nil {
		return m.infoFunc(ctx, jobID)
	}
	return &core.Job{ID: jobID, Type: "test", State: "available", Queue: "default"}, nil
}

func (m *mockBackend) Cancel(ctx context.Context, jobID string) (*core.Job, error) {
	if m.cancelFunc != nil {
		return m.cancelFunc(ctx, jobID)
	}
	return &core.Job{ID: jobID, Type: "test", State: "cancelled", Queue: "default"}, nil
}

func (m *mockBackend) ListQueues(ctx context.Context) ([]core.QueueInfo, error) {
	if m.listQueuesFunc != nil {
		return m.listQueuesFunc(ctx)
	}
	return []core.QueueInfo{{Name: "default", Status: "active"}}, nil
}

func (m *mockBackend) Health(ctx context.Context) (*core.HealthResponse, error) {
	if m.healthFunc != nil {
		return m.healthFunc(ctx)
	}
	return &core.HealthResponse{Status: "ok", Version: "1.0"}, nil
}

func (m *mockBackend) Heartbeat(ctx context.Context, workerID string, activeJobs []string, visMs int) (*core.HeartbeatResponse, error) {
	if m.heartbeatFunc != nil {
		return m.heartbeatFunc(ctx, workerID, activeJobs, visMs)
	}
	return &core.HeartbeatResponse{State: "active", Directive: "continue"}, nil
}

func (m *mockBackend) ListDeadLetter(ctx context.Context, limit, offset int) ([]*core.Job, int, error) {
	if m.listDeadLetterFunc != nil {
		return m.listDeadLetterFunc(ctx, limit, offset)
	}
	return []*core.Job{}, 0, nil
}

func (m *mockBackend) RetryDeadLetter(ctx context.Context, jobID string) (*core.Job, error) {
	if m.retryDeadLetterFunc != nil {
		return m.retryDeadLetterFunc(ctx, jobID)
	}
	return &core.Job{ID: jobID, Type: "test", State: "available", Queue: "default"}, nil
}

func (m *mockBackend) DeleteDeadLetter(ctx context.Context, jobID string) error {
	if m.deleteDeadLetterFunc != nil {
		return m.deleteDeadLetterFunc(ctx, jobID)
	}
	return nil
}

func (m *mockBackend) RegisterCron(ctx context.Context, cron *core.CronJob) (*core.CronJob, error) {
	if m.registerCronFunc != nil {
		return m.registerCronFunc(ctx, cron)
	}
	cron.CreatedAt = "2025-01-01T00:00:00.000Z"
	cron.Enabled = true
	return cron, nil
}

func (m *mockBackend) ListCron(ctx context.Context) ([]*core.CronJob, error) {
	if m.listCronFunc != nil {
		return m.listCronFunc(ctx)
	}
	return []*core.CronJob{}, nil
}

func (m *mockBackend) DeleteCron(ctx context.Context, name string) (*core.CronJob, error) {
	if m.deleteCronFunc != nil {
		return m.deleteCronFunc(ctx, name)
	}
	return &core.CronJob{Name: name}, nil
}

func (m *mockBackend) CreateWorkflow(ctx context.Context, req *core.WorkflowRequest) (*core.Workflow, error) {
	if m.createWorkflowFunc != nil {
		return m.createWorkflowFunc(ctx, req)
	}
	total := len(req.Steps)
	if req.Type != "chain" {
		total = len(req.Jobs)
	}
	zero := 0
	wf := &core.Workflow{ID: "wf-test", Type: req.Type, State: "running", CreatedAt: "2025-01-01T00:00:00.000Z"}
	if req.Type == "chain" {
		wf.StepsTotal = &total
		wf.StepsCompleted = &zero
	} else {
		wf.JobsTotal = &total
		wf.JobsCompleted = &zero
	}
	return wf, nil
}

func (m *mockBackend) GetWorkflow(ctx context.Context, id string) (*core.Workflow, error) {
	if m.getWorkflowFunc != nil {
		return m.getWorkflowFunc(ctx, id)
	}
	total := 3
	completed := 1
	return &core.Workflow{ID: id, Type: "chain", State: "running", StepsTotal: &total, StepsCompleted: &completed}, nil
}

func (m *mockBackend) CancelWorkflow(ctx context.Context, id string) (*core.Workflow, error) {
	if m.cancelWorkflowFunc != nil {
		return m.cancelWorkflowFunc(ctx, id)
	}
	return &core.Workflow{ID: id, Type: "chain", State: "cancelled"}, nil
}

func (m *mockBackend) AdvanceWorkflow(ctx context.Context, workflowID string, jobID string, result json.RawMessage, failed bool) error {
	if m.advanceWorkflowFunc != nil {
		return m.advanceWorkflowFunc(ctx, workflowID, jobID, result, failed)
	}
	return nil
}

func (m *mockBackend) PushBatch(ctx context.Context, jobs []*core.Job) ([]*core.Job, error) {
	if m.pushBatchFunc != nil {
		return m.pushBatchFunc(ctx, jobs)
	}
	for i, j := range jobs {
		j.ID = fmt.Sprintf("batch-%d", i)
		j.State = "available"
		j.CreatedAt = "2025-01-01T00:00:00.000Z"
	}
	return jobs, nil
}

func (m *mockBackend) QueueStats(ctx context.Context, name string) (*core.QueueStats, error) {
	if m.queueStatsFunc != nil {
		return m.queueStatsFunc(ctx, name)
	}
	return &core.QueueStats{Queue: name, Status: "active", Stats: core.Stats{Available: 5, Active: 2}}, nil
}

func (m *mockBackend) PauseQueue(ctx context.Context, name string) error {
	if m.pauseQueueFunc != nil {
		return m.pauseQueueFunc(ctx, name)
	}
	return nil
}

func (m *mockBackend) ResumeQueue(ctx context.Context, name string) error {
	if m.resumeQueueFunc != nil {
		return m.resumeQueueFunc(ctx, name)
	}
	return nil
}

func (m *mockBackend) ListJobs(ctx context.Context, filters core.JobListFilters, limit, offset int) ([]*core.Job, int, error) {
	if m.listJobsFunc != nil {
		return m.listJobsFunc(ctx, filters, limit, offset)
	}
	return []*core.Job{}, 0, nil
}

func (m *mockBackend) ListWorkers(ctx context.Context, limit, offset int) ([]*core.WorkerInfo, core.WorkerSummary, error) {
	if m.listWorkersFunc != nil {
		return m.listWorkersFunc(ctx, limit, offset)
	}
	return []*core.WorkerInfo{}, core.WorkerSummary{}, nil
}

func (m *mockBackend) SetWorkerState(ctx context.Context, workerID string, state string) error {
	if m.setWorkerStateFunc != nil {
		return m.setWorkerStateFunc(ctx, workerID, state)
	}
	return nil
}

func (m *mockBackend) Close() error { return nil }

// newTestRouter creates a chi router wired to the given mock backend.
func newTestRouter(backend *mockBackend) *chi.Mux {
	r := chi.NewRouter()

	jobHandler := NewJobHandler(backend)
	workerHandler := NewWorkerHandler(backend)
	systemHandler := NewSystemHandler(backend)
	queueHandler := NewQueueHandler(backend)
	deadLetterHandler := NewDeadLetterHandler(backend)
	cronHandler := NewCronHandler(backend)
	workflowHandler := NewWorkflowHandler(backend)
	batchHandler := NewBatchHandler(backend)

	r.Get("/ojs/manifest", systemHandler.Manifest)
	r.Get("/ojs/v1/health", systemHandler.Health)
	r.Post("/ojs/v1/jobs", jobHandler.Create)
	r.Get("/ojs/v1/jobs/{id}", jobHandler.Get)
	r.Delete("/ojs/v1/jobs/{id}", jobHandler.Cancel)
	r.Post("/ojs/v1/jobs/batch", batchHandler.Create)
	r.Post("/ojs/v1/workers/fetch", workerHandler.Fetch)
	r.Post("/ojs/v1/workers/ack", workerHandler.Ack)
	r.Post("/ojs/v1/workers/nack", workerHandler.Nack)
	r.Post("/ojs/v1/workers/heartbeat", workerHandler.Heartbeat)
	r.Get("/ojs/v1/queues", queueHandler.List)
	r.Get("/ojs/v1/queues/{name}/stats", queueHandler.Stats)
	r.Post("/ojs/v1/queues/{name}/pause", queueHandler.Pause)
	r.Post("/ojs/v1/queues/{name}/resume", queueHandler.Resume)
	r.Get("/ojs/v1/dead-letter", deadLetterHandler.List)
	r.Post("/ojs/v1/dead-letter/{id}/retry", deadLetterHandler.Retry)
	r.Delete("/ojs/v1/dead-letter/{id}", deadLetterHandler.Delete)
	r.Get("/ojs/v1/cron", cronHandler.List)
	r.Post("/ojs/v1/cron", cronHandler.Register)
	r.Delete("/ojs/v1/cron/{name}", cronHandler.Delete)
	r.Post("/ojs/v1/workflows", workflowHandler.Create)
	r.Get("/ojs/v1/workflows/{id}", workflowHandler.Get)
	r.Delete("/ojs/v1/workflows/{id}", workflowHandler.Cancel)

	return r
}

// --- System handler tests ---

func TestManifest(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	req := httptest.NewRequest(http.MethodGet, "/ojs/manifest", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)

	if resp["specversion"] != core.OJSVersion {
		t.Errorf("specversion: got %v, want %s", resp["specversion"], core.OJSVersion)
	}
	impl, _ := resp["implementation"].(map[string]any)
	if impl["version"] != core.OJSVersion {
		t.Errorf("implementation.version: got %v, want %s", impl["version"], core.OJSVersion)
	}
	if impl["backend"] != "redis" {
		t.Errorf("backend: got %v, want redis", impl["backend"])
	}
}

func TestHealth_OK(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	req := httptest.NewRequest(http.MethodGet, "/ojs/v1/health", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["status"] != "ok" {
		t.Errorf("status: got %v, want ok", resp["status"])
	}
}

func TestHealth_Degraded(t *testing.T) {
	backend := &mockBackend{
		healthFunc: func(ctx context.Context) (*core.HealthResponse, error) {
			return &core.HealthResponse{Status: "degraded"}, fmt.Errorf("redis down")
		},
	}
	router := newTestRouter(backend)
	req := httptest.NewRequest(http.MethodGet, "/ojs/v1/health", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", w.Code)
	}
}

// --- Job handler tests ---

func TestCreateJob_Success(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	body := `{"type":"email.send","args":["hello"]}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/jobs", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	job, _ := resp["job"].(map[string]any)
	if job["state"] != "available" {
		t.Errorf("state: got %v, want available", job["state"])
	}
	if w.Header().Get("Location") == "" {
		t.Error("expected Location header")
	}
}

func TestCreateJob_InvalidJSON(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	body := `{invalid`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/jobs", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestCreateJob_MissingType(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	body := `{"args":["hello"]}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/jobs", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCreateJob_BackendError(t *testing.T) {
	backend := &mockBackend{
		pushFunc: func(ctx context.Context, job *core.Job) (*core.Job, error) {
			return nil, core.NewConflictError("duplicate", nil)
		},
	}
	router := newTestRouter(backend)
	body := `{"type":"email.send","args":["hello"]}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/jobs", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", w.Code)
	}
}

func TestCreateJob_ExistingJob(t *testing.T) {
	backend := &mockBackend{
		pushFunc: func(ctx context.Context, job *core.Job) (*core.Job, error) {
			job.ID = "existing-id"
			job.State = "available"
			job.IsExisting = true
			return job, nil
		},
	}
	router := newTestRouter(backend)
	body := `{"type":"email.send","args":["hello"]}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/jobs", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for existing job, got %d", w.Code)
	}
}

func TestGetJob_Success(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	req := httptest.NewRequest(http.MethodGet, "/ojs/v1/jobs/job-123", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	job, _ := resp["job"].(map[string]any)
	if job["id"] != "job-123" {
		t.Errorf("id: got %v, want job-123", job["id"])
	}
}

func TestGetJob_NotFound(t *testing.T) {
	backend := &mockBackend{
		infoFunc: func(ctx context.Context, jobID string) (*core.Job, error) {
			return nil, core.NewNotFoundError("Job", jobID)
		},
	}
	router := newTestRouter(backend)
	req := httptest.NewRequest(http.MethodGet, "/ojs/v1/jobs/nonexistent", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestCancelJob_Success(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	req := httptest.NewRequest(http.MethodDelete, "/ojs/v1/jobs/job-123", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	job, _ := resp["job"].(map[string]any)
	if job["state"] != "cancelled" {
		t.Errorf("state: got %v, want cancelled", job["state"])
	}
}

func TestCancelJob_Conflict(t *testing.T) {
	backend := &mockBackend{
		cancelFunc: func(ctx context.Context, jobID string) (*core.Job, error) {
			return nil, core.NewConflictError("already completed", nil)
		},
	}
	router := newTestRouter(backend)
	req := httptest.NewRequest(http.MethodDelete, "/ojs/v1/jobs/job-123", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", w.Code)
	}
}

// --- Worker handler tests ---

func TestFetch_Success(t *testing.T) {
	backend := &mockBackend{
		fetchFunc: func(ctx context.Context, queues []string, count int, workerID string, visMs int) ([]*core.Job, error) {
			return []*core.Job{{ID: "fetched-1", Type: "test", State: "active", Queue: "default"}}, nil
		},
	}
	router := newTestRouter(backend)
	body := `{"queues":["default"],"worker_id":"w-1"}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workers/fetch", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	jobs, _ := resp["jobs"].([]any)
	if len(jobs) != 1 {
		t.Errorf("expected 1 job, got %d", len(jobs))
	}
	if resp["job"] == nil {
		t.Error("expected 'job' shorthand field when jobs returned")
	}
}

func TestFetch_EmptyQueues(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	body := `{"queues":[]}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workers/fetch", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestFetch_NoJobs(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	body := `{"queues":["default"]}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workers/fetch", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	jobs, _ := resp["jobs"].([]any)
	if len(jobs) != 0 {
		t.Errorf("expected 0 jobs, got %d", len(jobs))
	}
}

func TestAck_Success(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	body := `{"job_id":"job-1","result":{"ok":true}}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workers/ack", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["acknowledged"] != true {
		t.Errorf("expected acknowledged=true, got %v", resp["acknowledged"])
	}
}

func TestAck_MissingJobID(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	body := `{}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workers/ack", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestNack_Success(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	body := `{"job_id":"job-1","error":{"message":"timeout"}}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workers/nack", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestNack_MissingJobID(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	body := `{"error":{"message":"timeout"}}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workers/nack", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHeartbeat_Success(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	body := `{"worker_id":"w-1","active_jobs":["job-1"]}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workers/heartbeat", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["directive"] != "continue" {
		t.Errorf("directive: got %v, want continue", resp["directive"])
	}
}

func TestHeartbeat_MissingWorkerID(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	body := `{"active_jobs":["job-1"]}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workers/heartbeat", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

// --- Queue handler tests ---

func TestListQueues_Success(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	req := httptest.NewRequest(http.MethodGet, "/ojs/v1/queues", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	queues, _ := resp["queues"].([]any)
	if len(queues) != 1 {
		t.Errorf("expected 1 queue, got %d", len(queues))
	}
	if resp["pagination"] == nil {
		t.Error("expected pagination field")
	}
}

func TestQueueStats_Success(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	req := httptest.NewRequest(http.MethodGet, "/ojs/v1/queues/default/stats", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	queue, _ := resp["queue"].(map[string]any)
	if queue["name"] != "default" {
		t.Errorf("name: got %v, want default", queue["name"])
	}
	if queue["available"] != float64(5) {
		t.Errorf("available: got %v, want 5", queue["available"])
	}
}

func TestPauseQueue_Success(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/queues/default/pause", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	queue, _ := resp["queue"].(map[string]any)
	if queue["paused"] != true {
		t.Errorf("paused: got %v, want true", queue["paused"])
	}
}

func TestResumeQueue_Success(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/queues/default/resume", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	queue, _ := resp["queue"].(map[string]any)
	if queue["paused"] != false {
		t.Errorf("paused: got %v, want false", queue["paused"])
	}
}

// --- Dead letter handler tests ---

func TestListDeadLetter_Success(t *testing.T) {
	backend := &mockBackend{
		listDeadLetterFunc: func(ctx context.Context, limit, offset int) ([]*core.Job, int, error) {
			jobs := []*core.Job{{ID: "dead-1", Type: "test", State: "discarded", Queue: "default"}}
			return jobs, 1, nil
		},
	}
	router := newTestRouter(backend)
	req := httptest.NewRequest(http.MethodGet, "/ojs/v1/dead-letter", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	jobs, _ := resp["jobs"].([]any)
	if len(jobs) != 1 {
		t.Errorf("expected 1 dead letter job, got %d", len(jobs))
	}
	pagination, _ := resp["pagination"].(map[string]any)
	if pagination["total"] != float64(1) {
		t.Errorf("total: got %v, want 1", pagination["total"])
	}
}

func TestListDeadLetter_LimitCap(t *testing.T) {
	var capturedLimit int
	backend := &mockBackend{
		listDeadLetterFunc: func(ctx context.Context, limit, offset int) ([]*core.Job, int, error) {
			capturedLimit = limit
			return []*core.Job{}, 0, nil
		},
	}
	router := newTestRouter(backend)
	req := httptest.NewRequest(http.MethodGet, "/ojs/v1/dead-letter?limit=99999", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if capturedLimit > 1000 {
		t.Errorf("limit should be capped at 1000, got %d", capturedLimit)
	}
}

func TestRetryDeadLetter_Success(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/dead-letter/dead-1/retry", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestRetryDeadLetter_NotFound(t *testing.T) {
	backend := &mockBackend{
		retryDeadLetterFunc: func(ctx context.Context, jobID string) (*core.Job, error) {
			return nil, core.NewNotFoundError("Job", jobID)
		},
	}
	router := newTestRouter(backend)
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/dead-letter/nonexistent/retry", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestDeleteDeadLetter_Success(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	req := httptest.NewRequest(http.MethodDelete, "/ojs/v1/dead-letter/dead-1", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["deleted"] != true {
		t.Errorf("deleted: got %v, want true", resp["deleted"])
	}
}

// --- Cron handler tests ---

func TestListCron_Success(t *testing.T) {
	backend := &mockBackend{
		listCronFunc: func(ctx context.Context) ([]*core.CronJob, error) {
			return []*core.CronJob{{Name: "daily", Expression: "0 9 * * *", Enabled: true}}, nil
		},
	}
	router := newTestRouter(backend)
	req := httptest.NewRequest(http.MethodGet, "/ojs/v1/cron", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	crons, _ := resp["crons"].([]any)
	if len(crons) != 1 {
		t.Errorf("expected 1 cron, got %d", len(crons))
	}
}

func TestListCron_Empty(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	req := httptest.NewRequest(http.MethodGet, "/ojs/v1/cron", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	crons, _ := resp["crons"].([]any)
	if len(crons) != 0 {
		t.Errorf("expected 0 crons, got %d", len(crons))
	}
}

func TestRegisterCron_Success(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	body := `{"name":"daily","expression":"0 9 * * *","job_template":{"type":"reports.generate","options":{"queue":"reports"}}}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/cron", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
	}
}

func TestRegisterCron_MissingName(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	body := `{"expression":"0 9 * * *","job_template":{"type":"test"}}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/cron", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestRegisterCron_MissingExpression(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	body := `{"name":"daily","job_template":{"type":"test"}}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/cron", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestRegisterCron_MissingJobTemplate(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	body := `{"name":"daily","expression":"0 9 * * *"}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/cron", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestDeleteCron_Success(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	req := httptest.NewRequest(http.MethodDelete, "/ojs/v1/cron/daily", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestDeleteCron_NotFound(t *testing.T) {
	backend := &mockBackend{
		deleteCronFunc: func(ctx context.Context, name string) (*core.CronJob, error) {
			return nil, core.NewNotFoundError("Cron job", name)
		},
	}
	router := newTestRouter(backend)
	req := httptest.NewRequest(http.MethodDelete, "/ojs/v1/cron/nonexistent", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

// --- Workflow handler tests ---

func TestCreateWorkflow_Chain(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	body := `{"type":"chain","steps":[{"type":"step1","args":[]},{"type":"step2","args":[]}]}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workflows", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	wf, _ := resp["workflow"].(map[string]any)
	if wf["type"] != "chain" {
		t.Errorf("type: got %v, want chain", wf["type"])
	}
	if wf["state"] != "running" {
		t.Errorf("state: got %v, want running", wf["state"])
	}
}

func TestCreateWorkflow_Group(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	body := `{"type":"group","jobs":[{"type":"job1","args":[]},{"type":"job2","args":[]}]}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workflows", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCreateWorkflow_MissingType(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	body := `{"steps":[{"type":"step1"}]}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workflows", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestCreateWorkflow_InvalidType(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	body := `{"type":"invalid","steps":[{"type":"step1"}]}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workflows", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestCreateWorkflow_ChainMissingSteps(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	body := `{"type":"chain","steps":[]}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workflows", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestGetWorkflow_Success(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	req := httptest.NewRequest(http.MethodGet, "/ojs/v1/workflows/wf-123", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	wf, _ := resp["workflow"].(map[string]any)
	if wf["id"] != "wf-123" {
		t.Errorf("id: got %v, want wf-123", wf["id"])
	}
}

func TestGetWorkflow_NotFound(t *testing.T) {
	backend := &mockBackend{
		getWorkflowFunc: func(ctx context.Context, id string) (*core.Workflow, error) {
			return nil, core.NewNotFoundError("Workflow", id)
		},
	}
	router := newTestRouter(backend)
	req := httptest.NewRequest(http.MethodGet, "/ojs/v1/workflows/nonexistent", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestCancelWorkflow_Success(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	req := httptest.NewRequest(http.MethodDelete, "/ojs/v1/workflows/wf-123", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	wf, _ := resp["workflow"].(map[string]any)
	if wf["state"] != "cancelled" {
		t.Errorf("state: got %v, want cancelled", wf["state"])
	}
}

func TestCancelWorkflow_Conflict(t *testing.T) {
	backend := &mockBackend{
		cancelWorkflowFunc: func(ctx context.Context, id string) (*core.Workflow, error) {
			return nil, core.NewConflictError("already completed", nil)
		},
	}
	router := newTestRouter(backend)
	req := httptest.NewRequest(http.MethodDelete, "/ojs/v1/workflows/wf-123", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", w.Code)
	}
}

// --- Batch handler tests ---

func TestBatchCreate_Success(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	body := `{"jobs":[{"type":"email.send","args":["a"]},{"type":"email.send","args":["b"]}]}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/jobs/batch", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["count"] != float64(2) {
		t.Errorf("count: got %v, want 2", resp["count"])
	}
}

func TestBatchCreate_EmptyJobs(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	body := `{"jobs":[]}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/jobs/batch", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestBatchCreate_InvalidJobInBatch(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	body := `{"jobs":[{"type":"ok","args":[]},{"args":[]}]}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/jobs/batch", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Second job missing type should fail validation
	if w.Code != http.StatusUnprocessableEntity && w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 or 422, got %d: %s", w.Code, w.Body.String())
	}
}

func TestBatchCreate_BackendError(t *testing.T) {
	backend := &mockBackend{
		pushBatchFunc: func(ctx context.Context, jobs []*core.Job) ([]*core.Job, error) {
			return nil, fmt.Errorf("redis connection lost")
		},
	}
	router := newTestRouter(backend)
	body := `{"jobs":[{"type":"email.send","args":["a"]}]}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/jobs/batch", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", w.Code)
	}
}

// --- Invalid JSON body tests ---

func TestFetch_InvalidJSON(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workers/fetch", strings.NewReader("{bad"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestAck_InvalidJSON(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workers/ack", strings.NewReader("{bad"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestNack_InvalidJSON(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workers/nack", strings.NewReader("{bad"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHeartbeat_InvalidJSON(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workers/heartbeat", strings.NewReader("{bad"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestRegisterCron_InvalidJSON(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/cron", strings.NewReader("{bad"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestCreateWorkflow_InvalidJSON(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workflows", strings.NewReader("{bad"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestBatchCreate_InvalidJSON(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/jobs/batch", strings.NewReader("{bad"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}
