package redis

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/openjobspec/ojs-backend-redis/internal/core"
)

// testBackend returns a RedisBackend connected to a test Redis instance.
// It flushes the database before returning. The test is skipped if REDIS_URL
// is not set or Redis is unreachable.
func testBackend(t *testing.T) *RedisBackend {
	t.Helper()
	url := os.Getenv("REDIS_URL")
	if url == "" {
		t.Skip("REDIS_URL not set, skipping integration test")
	}
	b, err := New(url)
	if err != nil {
		t.Skipf("cannot connect to Redis: %v", err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		b.client.FlushDB(ctx)
		b.Close()
	})
	// Flush before test to start clean
	b.client.FlushDB(context.Background())
	return b
}

func TestIntegration_PushAndInfo(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	job := &core.Job{
		Type:  "email.send",
		Args:  json.RawMessage(`{"to":"user@example.com"}`),
		Queue: "default",
	}

	created, err := b.Push(ctx, job)
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	if created.ID == "" {
		t.Fatal("expected job ID to be set")
	}
	if created.State != core.StateAvailable {
		t.Errorf("state = %q, want %q", created.State, core.StateAvailable)
	}
	if created.CreatedAt == "" {
		t.Error("expected created_at to be set")
	}
	if created.EnqueuedAt == "" {
		t.Error("expected enqueued_at to be set")
	}

	// Info should return the same job
	info, err := b.Info(ctx, created.ID)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if info.ID != created.ID {
		t.Errorf("Info ID = %q, want %q", info.ID, created.ID)
	}
	if info.Type != "email.send" {
		t.Errorf("Info Type = %q, want %q", info.Type, "email.send")
	}
	if info.State != core.StateAvailable {
		t.Errorf("Info State = %q, want %q", info.State, core.StateAvailable)
	}
}

func TestIntegration_PushWithCustomID(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	job := &core.Job{
		ID:    "custom-id-123",
		Type:  "test.job",
		Args:  json.RawMessage(`{}`),
		Queue: "default",
	}

	created, err := b.Push(ctx, job)
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	if created.ID != "custom-id-123" {
		t.Errorf("ID = %q, want %q", created.ID, "custom-id-123")
	}
}

func TestIntegration_PushScheduledFuture(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	future := time.Now().Add(1 * time.Hour).UTC().Format(time.RFC3339)
	job := &core.Job{
		Type:        "scheduled.job",
		Args:        json.RawMessage(`{}`),
		Queue:       "default",
		ScheduledAt: future,
	}

	created, err := b.Push(ctx, job)
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	if created.State != core.StateScheduled {
		t.Errorf("state = %q, want %q", created.State, core.StateScheduled)
	}
}

func TestIntegration_PushScheduledPast(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	past := time.Now().Add(-1 * time.Hour).UTC().Format(time.RFC3339)
	job := &core.Job{
		Type:        "scheduled.past",
		Args:        json.RawMessage(`{}`),
		Queue:       "default",
		ScheduledAt: past,
	}

	created, err := b.Push(ctx, job)
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	// Past scheduled_at should be treated as immediate
	if created.State != core.StateAvailable {
		t.Errorf("state = %q, want %q", created.State, core.StateAvailable)
	}
}

func TestIntegration_InfoNotFound(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	_, err := b.Info(ctx, "nonexistent-id")
	if err == nil {
		t.Fatal("expected error for nonexistent job")
	}
	ojsErr, ok := err.(*core.OJSError)
	if !ok {
		t.Fatalf("expected OJSError, got %T", err)
	}
	if ojsErr.Code != core.ErrCodeNotFound {
		t.Errorf("error code = %q, want %q", ojsErr.Code, core.ErrCodeNotFound)
	}
}

func TestIntegration_FetchAndAck(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	// Push a job
	job := &core.Job{
		Type:  "process.data",
		Args:  json.RawMessage(`{"key":"value"}`),
		Queue: "work",
	}
	created, err := b.Push(ctx, job)
	if err != nil {
		t.Fatalf("Push: %v", err)
	}

	// Fetch it
	jobs, err := b.Fetch(ctx, []string{"work"}, 1, "worker-1", DefaultVisibilityTimeoutMs)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("fetched %d jobs, want 1", len(jobs))
	}
	fetched := jobs[0]
	if fetched.ID != created.ID {
		t.Errorf("fetched ID = %q, want %q", fetched.ID, created.ID)
	}
	if fetched.State != core.StateActive {
		t.Errorf("fetched state = %q, want %q", fetched.State, core.StateActive)
	}

	// Ack it
	ackResp, err := b.Ack(ctx, fetched.ID, json.RawMessage(`{"status":"done"}`))
	if err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if !ackResp.Acknowledged {
		t.Error("expected acknowledged = true")
	}
	if ackResp.State != core.StateCompleted {
		t.Errorf("ack state = %q, want %q", ackResp.State, core.StateCompleted)
	}

	// Verify final state
	info, err := b.Info(ctx, fetched.ID)
	if err != nil {
		t.Fatalf("Info after ack: %v", err)
	}
	if info.State != core.StateCompleted {
		t.Errorf("final state = %q, want %q", info.State, core.StateCompleted)
	}
	if info.CompletedAt == "" {
		t.Error("expected completed_at to be set")
	}
}

func TestIntegration_FetchEmptyQueue(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	jobs, err := b.Fetch(ctx, []string{"empty-queue"}, 5, "worker-1", DefaultVisibilityTimeoutMs)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(jobs) != 0 {
		t.Errorf("fetched %d jobs from empty queue, want 0", len(jobs))
	}
}

func TestIntegration_FetchRespectsPriority(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	low := 1
	high := 10

	// Push low priority first
	_, err := b.Push(ctx, &core.Job{
		Type:     "low.priority",
		Args:     json.RawMessage(`{}`),
		Queue:    "priority-test",
		Priority: &low,
	})
	if err != nil {
		t.Fatalf("Push low: %v", err)
	}

	// Push high priority second
	_, err = b.Push(ctx, &core.Job{
		Type:     "high.priority",
		Args:     json.RawMessage(`{}`),
		Queue:    "priority-test",
		Priority: &high,
	})
	if err != nil {
		t.Fatalf("Push high: %v", err)
	}

	// Fetch should return high priority first
	jobs, err := b.Fetch(ctx, []string{"priority-test"}, 2, "worker-1", DefaultVisibilityTimeoutMs)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(jobs) != 2 {
		t.Fatalf("fetched %d jobs, want 2", len(jobs))
	}
	if jobs[0].Type != "high.priority" {
		t.Errorf("first job type = %q, want %q", jobs[0].Type, "high.priority")
	}
	if jobs[1].Type != "low.priority" {
		t.Errorf("second job type = %q, want %q", jobs[1].Type, "low.priority")
	}
}

func TestIntegration_NackRequeue(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	// Push and fetch
	created, err := b.Push(ctx, &core.Job{
		Type:  "requeue.test",
		Args:  json.RawMessage(`{}`),
		Queue: "nack-test",
	})
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	jobs, err := b.Fetch(ctx, []string{"nack-test"}, 1, "worker-1", DefaultVisibilityTimeoutMs)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("Fetch: err=%v, count=%d", err, len(jobs))
	}

	// Nack with requeue
	resp, err := b.Nack(ctx, created.ID, &core.JobError{Message: "temporary"}, true)
	if err != nil {
		t.Fatalf("Nack: %v", err)
	}
	if resp.State != core.StateAvailable {
		t.Errorf("nack state = %q, want %q", resp.State, core.StateAvailable)
	}

	// Should be fetchable again
	jobs, err = b.Fetch(ctx, []string{"nack-test"}, 1, "worker-1", DefaultVisibilityTimeoutMs)
	if err != nil {
		t.Fatalf("Fetch after requeue: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("fetched %d jobs after requeue, want 1", len(jobs))
	}
}

func TestIntegration_NackDiscard(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	maxAtt := 1
	created, err := b.Push(ctx, &core.Job{
		Type:        "discard.test",
		Args:        json.RawMessage(`{}`),
		Queue:       "discard-q",
		MaxAttempts: &maxAtt,
	})
	if err != nil {
		t.Fatalf("Push: %v", err)
	}

	_, err = b.Fetch(ctx, []string{"discard-q"}, 1, "worker-1", DefaultVisibilityTimeoutMs)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	// Nack without requeue — max_attempts=1, attempt becomes 1, so it should discard
	resp, err := b.Nack(ctx, created.ID, &core.JobError{Message: "fatal error"}, false)
	if err != nil {
		t.Fatalf("Nack: %v", err)
	}
	if resp.State != core.StateDiscarded {
		t.Errorf("nack state = %q, want %q", resp.State, core.StateDiscarded)
	}

	info, err := b.Info(ctx, created.ID)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if info.State != core.StateDiscarded {
		t.Errorf("final state = %q, want %q", info.State, core.StateDiscarded)
	}
}

func TestIntegration_NackNonRetryable(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	maxAtt := 10
	created, err := b.Push(ctx, &core.Job{
		Type:        "nonretryable.test",
		Args:        json.RawMessage(`{}`),
		Queue:       "nr-q",
		MaxAttempts: &maxAtt,
	})
	if err != nil {
		t.Fatalf("Push: %v", err)
	}

	_, err = b.Fetch(ctx, []string{"nr-q"}, 1, "worker-1", DefaultVisibilityTimeoutMs)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	// Non-retryable error should discard even with attempts remaining
	notRetryable := false
	resp, err := b.Nack(ctx, created.ID, &core.JobError{
		Message:   "permanent failure",
		Retryable: &notRetryable,
	}, false)
	if err != nil {
		t.Fatalf("Nack: %v", err)
	}
	if resp.State != core.StateDiscarded {
		t.Errorf("state = %q, want %q", resp.State, core.StateDiscarded)
	}
}

func TestIntegration_Cancel(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	created, err := b.Push(ctx, &core.Job{
		Type:  "cancel.test",
		Args:  json.RawMessage(`{}`),
		Queue: "cancel-q",
	})
	if err != nil {
		t.Fatalf("Push: %v", err)
	}

	cancelled, err := b.Cancel(ctx, created.ID)
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if cancelled.State != core.StateCancelled {
		t.Errorf("state = %q, want %q", cancelled.State, core.StateCancelled)
	}
	if cancelled.CancelledAt == "" {
		t.Error("expected cancelled_at to be set")
	}
}

func TestIntegration_CancelNotFound(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	_, err := b.Cancel(ctx, "nonexistent-cancel-id")
	if err == nil {
		t.Fatal("expected error for nonexistent job")
	}
	ojsErr, ok := err.(*core.OJSError)
	if !ok {
		t.Fatalf("expected OJSError, got %T", err)
	}
	if ojsErr.Code != core.ErrCodeNotFound {
		t.Errorf("error code = %q, want %q", ojsErr.Code, core.ErrCodeNotFound)
	}
}

func TestIntegration_CancelCompletedConflict(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	// Push, fetch, ack
	created, _ := b.Push(ctx, &core.Job{
		Type:  "conflict.test",
		Args:  json.RawMessage(`{}`),
		Queue: "conflict-q",
	})
	b.Fetch(ctx, []string{"conflict-q"}, 1, "worker-1", DefaultVisibilityTimeoutMs)
	b.Ack(ctx, created.ID, nil)

	// Try to cancel completed job
	_, err := b.Cancel(ctx, created.ID)
	if err == nil {
		t.Fatal("expected conflict error for completed job")
	}
	ojsErr, ok := err.(*core.OJSError)
	if !ok {
		t.Fatalf("expected OJSError, got %T", err)
	}
	if ojsErr.Code != core.ErrCodeConflict {
		t.Errorf("error code = %q, want %q", ojsErr.Code, core.ErrCodeConflict)
	}
}

func TestIntegration_AckNotActive(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	created, _ := b.Push(ctx, &core.Job{
		Type:  "ack.notactive",
		Args:  json.RawMessage(`{}`),
		Queue: "ack-q",
	})

	// Try to ack a job that's still available (not active)
	_, err := b.Ack(ctx, created.ID, nil)
	if err == nil {
		t.Fatal("expected conflict error for non-active job")
	}
	ojsErr, ok := err.(*core.OJSError)
	if !ok {
		t.Fatalf("expected OJSError, got %T", err)
	}
	if ojsErr.Code != core.ErrCodeConflict {
		t.Errorf("error code = %q, want %q", ojsErr.Code, core.ErrCodeConflict)
	}
}

func TestIntegration_PushBatch(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	jobs := []*core.Job{
		{Type: "batch.1", Args: json.RawMessage(`{}`), Queue: "batch-q"},
		{Type: "batch.2", Args: json.RawMessage(`{}`), Queue: "batch-q"},
		{Type: "batch.3", Args: json.RawMessage(`{}`), Queue: "batch-q"},
	}

	results, err := b.PushBatch(ctx, jobs)
	if err != nil {
		t.Fatalf("PushBatch: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("pushed %d jobs, want 3", len(results))
	}
	for i, r := range results {
		if r.ID == "" {
			t.Errorf("job[%d] has empty ID", i)
		}
		if r.State != core.StateAvailable {
			t.Errorf("job[%d] state = %q, want %q", i, r.State, core.StateAvailable)
		}
	}
}

func TestIntegration_FetchMultipleQueues(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	// Push to different queues
	b.Push(ctx, &core.Job{Type: "q1.job", Args: json.RawMessage(`{}`), Queue: "queue-a"})
	b.Push(ctx, &core.Job{Type: "q2.job", Args: json.RawMessage(`{}`), Queue: "queue-b"})

	// Fetch from both
	jobs, err := b.Fetch(ctx, []string{"queue-a", "queue-b"}, 5, "worker-1", DefaultVisibilityTimeoutMs)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(jobs) != 2 {
		t.Errorf("fetched %d jobs, want 2", len(jobs))
	}
}

func TestIntegration_JobPreservesFields(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	priority := 5
	maxAtt := 3
	timeoutMs := 5000
	job := &core.Job{
		Type:        "fields.test",
		Args:        json.RawMessage(`{"key":"value"}`),
		Meta:        json.RawMessage(`{"source":"test"}`),
		Queue:       "fields-q",
		Priority:    &priority,
		MaxAttempts: &maxAtt,
		TimeoutMs:   &timeoutMs,
		Tags:        []string{"tag1", "tag2"},
	}

	created, err := b.Push(ctx, job)
	if err != nil {
		t.Fatalf("Push: %v", err)
	}

	info, err := b.Info(ctx, created.ID)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}

	if string(info.Args) != `{"key":"value"}` {
		t.Errorf("Args = %s, want %s", info.Args, `{"key":"value"}`)
	}
	if string(info.Meta) != `{"source":"test"}` {
		t.Errorf("Meta = %s, want %s", info.Meta, `{"source":"test"}`)
	}
	if info.Priority == nil || *info.Priority != 5 {
		t.Errorf("Priority = %v, want 5", info.Priority)
	}
	if info.MaxAttempts == nil || *info.MaxAttempts != 3 {
		t.Errorf("MaxAttempts = %v, want 3", info.MaxAttempts)
	}
	if info.TimeoutMs == nil || *info.TimeoutMs != 5000 {
		t.Errorf("TimeoutMs = %v, want 5000", info.TimeoutMs)
	}
	if len(info.Tags) != 2 || info.Tags[0] != "tag1" || info.Tags[1] != "tag2" {
		t.Errorf("Tags = %v, want [tag1 tag2]", info.Tags)
	}
}

func TestIntegration_Health(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	resp, err := b.Health(ctx)
	if err != nil {
		t.Fatalf("Health: %v", err)
	}
	if resp.Status != "ok" {
		t.Errorf("status = %q, want %q", resp.Status, "ok")
	}
	if resp.Backend.Type != "redis" {
		t.Errorf("backend type = %q, want %q", resp.Backend.Type, "redis")
	}
	if resp.Backend.Status != "connected" {
		t.Errorf("backend status = %q, want %q", resp.Backend.Status, "connected")
	}
}

func TestIntegration_NackRetry(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	maxAtt := 3
	created, err := b.Push(ctx, &core.Job{
		Type:        "retry.test",
		Args:        json.RawMessage(`{}`),
		Queue:       "retry-q",
		MaxAttempts: &maxAtt,
	})
	if err != nil {
		t.Fatalf("Push: %v", err)
	}

	// Fetch and nack (should retry since max_attempts=3 and this is attempt 1)
	_, err = b.Fetch(ctx, []string{"retry-q"}, 1, "worker-1", DefaultVisibilityTimeoutMs)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	resp, err := b.Nack(ctx, created.ID, &core.JobError{Message: "temp error"}, false)
	if err != nil {
		t.Fatalf("Nack: %v", err)
	}
	if resp.State != core.StateRetryable {
		t.Errorf("state = %q, want %q", resp.State, core.StateRetryable)
	}
	if resp.Attempt != 1 {
		t.Errorf("attempt = %d, want 1", resp.Attempt)
	}

	// Verify job info shows retryable state
	info, err := b.Info(ctx, created.ID)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if info.State != core.StateRetryable {
		t.Errorf("info state = %q, want %q", info.State, core.StateRetryable)
	}
}

func TestIntegration_SetWorkerStateAffectsHeartbeatDirective(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	if err := b.SetWorkerState(ctx, "worker-1", "quiet"); err != nil {
		t.Fatalf("SetWorkerState: %v", err)
	}

	resp, err := b.Heartbeat(ctx, "worker-1", nil, DefaultVisibilityTimeoutMs)
	if err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	if resp.Directive != "quiet" {
		t.Errorf("directive = %q, want %q", resp.Directive, "quiet")
	}
}

func TestIntegration_ListJobsFiltersAndPagination(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	_, err := b.Push(ctx, &core.Job{Type: "email.send", Args: json.RawMessage(`{}`), Queue: "q-a"})
	if err != nil {
		t.Fatalf("Push job1: %v", err)
	}
	_, err = b.Push(ctx, &core.Job{Type: "report.generate", Args: json.RawMessage(`{}`), Queue: "q-b"})
	if err != nil {
		t.Fatalf("Push job2: %v", err)
	}
	_, err = b.Push(ctx, &core.Job{Type: "email.send", Args: json.RawMessage(`{}`), Queue: "q-a"})
	if err != nil {
		t.Fatalf("Push job3: %v", err)
	}

	_, err = b.Fetch(ctx, []string{"q-a"}, 1, "worker-1", DefaultVisibilityTimeoutMs)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	queueJobs, queueTotal, err := b.ListJobs(ctx, core.JobListFilters{Queue: "q-a"}, 10, 0)
	if err != nil {
		t.Fatalf("ListJobs queue filter: %v", err)
	}
	if queueTotal != 2 || len(queueJobs) != 2 {
		t.Fatalf("queue filtered jobs: total=%d len=%d, want total=2 len=2", queueTotal, len(queueJobs))
	}

	activeJobs, activeTotal, err := b.ListJobs(ctx, core.JobListFilters{State: core.StateActive, WorkerID: "worker-1"}, 10, 0)
	if err != nil {
		t.Fatalf("ListJobs state+worker filter: %v", err)
	}
	if activeTotal != 1 || len(activeJobs) != 1 {
		t.Fatalf("active filtered jobs: total=%d len=%d, want total=1 len=1", activeTotal, len(activeJobs))
	}
	if activeJobs[0].WorkerID != "worker-1" {
		t.Errorf("worker_id = %q, want %q", activeJobs[0].WorkerID, "worker-1")
	}

	page, total, err := b.ListJobs(ctx, core.JobListFilters{}, 1, 1)
	if err != nil {
		t.Fatalf("ListJobs pagination: %v", err)
	}
	if total != 3 || len(page) != 1 {
		t.Fatalf("paginated jobs: total=%d len=%d, want total=3 len=1", total, len(page))
	}
}

func TestIntegration_ListWorkersSummaryAndPagination(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	if _, err := b.Heartbeat(ctx, "worker-1", nil, DefaultVisibilityTimeoutMs); err != nil {
		t.Fatalf("Heartbeat worker-1: %v", err)
	}
	if _, err := b.Heartbeat(ctx, "worker-2", nil, DefaultVisibilityTimeoutMs); err != nil {
		t.Fatalf("Heartbeat worker-2: %v", err)
	}
	if err := b.SetWorkerState(ctx, "worker-2", "quiet"); err != nil {
		t.Fatalf("SetWorkerState worker-2: %v", err)
	}

	staleAt := time.Now().Add(-2 * workerStaleThreshold)
	if err := b.client.SAdd(ctx, workersKey(), "worker-3").Err(); err != nil {
		t.Fatalf("SAdd stale worker: %v", err)
	}
	if err := b.client.HSet(ctx, workerKey("worker-3"), map[string]any{
		"last_heartbeat": core.FormatTime(staleAt),
		"active_jobs":    "0",
	}).Err(); err != nil {
		t.Fatalf("HSet stale worker: %v", err)
	}

	workers, summary, err := b.ListWorkers(ctx, 2, 0)
	if err != nil {
		t.Fatalf("ListWorkers: %v", err)
	}
	if len(workers) != 2 {
		t.Fatalf("workers page len = %d, want 2", len(workers))
	}
	if summary.Total != 3 || summary.Running != 1 || summary.Quiet != 1 || summary.Stale != 1 {
		t.Fatalf("unexpected summary: %+v", summary)
	}
}
