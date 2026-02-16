//go:build integration

package redis

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	redismodule "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/openjobspec/ojs-backend-redis/internal/core"
)

// setupTestBackend starts a Redis testcontainer and returns a connected RedisBackend.
func setupTestBackend(t *testing.T) *RedisBackend {
	t.Helper()
	ctx := context.Background()

	redisC, err := redismodule.Run(ctx, "redis:7-alpine")
	require.NoError(t, err)
	t.Cleanup(func() { redisC.Terminate(ctx) })

	endpoint, err := redisC.Endpoint(ctx, "")
	require.NoError(t, err)

	backend, err := New("redis://" + endpoint)
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })

	return backend
}

// pushJob is a test helper that pushes a job and asserts no error.
func pushJob(t *testing.T, b *RedisBackend, jobType, queue string) *core.Job {
	t.Helper()
	job, err := b.Push(context.Background(), &core.Job{
		Type:  jobType,
		Args:  json.RawMessage(`{}`),
		Queue: queue,
	})
	require.NoError(t, err)
	return job
}

// fetchAndAck fetches one job from the queue and acks it.
func fetchAndAck(t *testing.T, b *RedisBackend, queue string) *core.Job {
	t.Helper()
	ctx := context.Background()
	jobs, err := b.Fetch(ctx, []string{queue}, 1, "worker-test", DefaultVisibilityTimeoutMs)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	_, err = b.Ack(ctx, jobs[0].ID, nil)
	require.NoError(t, err)
	return jobs[0]
}

// --- Queue Operations ---

func TestQueueOperations(t *testing.T) {
	t.Run("ListQueues_returns_all_queues_with_jobs", func(t *testing.T) {
		b := setupTestBackend(t)
		ctx := context.Background()

		pushJob(t, b, "job.a", "queue-alpha")
		pushJob(t, b, "job.b", "queue-beta")
		pushJob(t, b, "job.c", "queue-gamma")

		queues, err := b.ListQueues(ctx)
		require.NoError(t, err)

		names := make([]string, len(queues))
		for i, q := range queues {
			names[i] = q.Name
		}
		assert.Contains(t, names, "queue-alpha")
		assert.Contains(t, names, "queue-beta")
		assert.Contains(t, names, "queue-gamma")
	})

	t.Run("QueueStats_returns_accurate_counts", func(t *testing.T) {
		b := setupTestBackend(t)
		ctx := context.Background()

		pushJob(t, b, "stats.a", "stats-q")
		pushJob(t, b, "stats.b", "stats-q")
		pushJob(t, b, "stats.c", "stats-q")

		// Fetch one to make it active
		jobs, err := b.Fetch(ctx, []string{"stats-q"}, 1, "worker-1", DefaultVisibilityTimeoutMs)
		require.NoError(t, err)
		require.Len(t, jobs, 1)

		stats, err := b.QueueStats(ctx, "stats-q")
		require.NoError(t, err)
		assert.Equal(t, "stats-q", stats.Queue)
		assert.Equal(t, "active", stats.Status)
		assert.Equal(t, 2, stats.Stats.Available)
		assert.Equal(t, 1, stats.Stats.Active)
	})

	t.Run("PauseQueue_prevents_fetch", func(t *testing.T) {
		b := setupTestBackend(t)
		ctx := context.Background()

		pushJob(t, b, "pause.test", "pause-q")

		err := b.PauseQueue(ctx, "pause-q")
		require.NoError(t, err)

		// Verify queue status shows paused
		stats, err := b.QueueStats(ctx, "pause-q")
		require.NoError(t, err)
		assert.Equal(t, "paused", stats.Status)

		// Fetch should return nothing from a paused queue
		jobs, err := b.Fetch(ctx, []string{"pause-q"}, 1, "worker-1", DefaultVisibilityTimeoutMs)
		require.NoError(t, err)
		assert.Empty(t, jobs)
	})

	t.Run("ResumeQueue_re-enables_fetch", func(t *testing.T) {
		b := setupTestBackend(t)
		ctx := context.Background()

		pushJob(t, b, "resume.test", "resume-q")

		// Pause then resume
		require.NoError(t, b.PauseQueue(ctx, "resume-q"))
		require.NoError(t, b.ResumeQueue(ctx, "resume-q"))

		// Verify queue shows active
		stats, err := b.QueueStats(ctx, "resume-q")
		require.NoError(t, err)
		assert.Equal(t, "active", stats.Status)

		// Fetch should work
		jobs, err := b.Fetch(ctx, []string{"resume-q"}, 1, "worker-1", DefaultVisibilityTimeoutMs)
		require.NoError(t, err)
		assert.Len(t, jobs, 1)
	})

	t.Run("ListQueues_shows_paused_status", func(t *testing.T) {
		b := setupTestBackend(t)
		ctx := context.Background()

		pushJob(t, b, "status.a", "active-q")
		pushJob(t, b, "status.b", "paused-q")
		require.NoError(t, b.PauseQueue(ctx, "paused-q"))

		queues, err := b.ListQueues(ctx)
		require.NoError(t, err)

		statusMap := make(map[string]string)
		for _, q := range queues {
			statusMap[q.Name] = q.Status
		}
		assert.Equal(t, "active", statusMap["active-q"])
		assert.Equal(t, "paused", statusMap["paused-q"])
	})
}

// --- Dead Letter Operations ---

func TestDeadLetterOperations(t *testing.T) {
	t.Run("exhausted_retries_with_dead_letter_policy", func(t *testing.T) {
		b := setupTestBackend(t)
		ctx := context.Background()

		maxAtt := 1
		job, err := b.Push(ctx, &core.Job{
			Type:        "dead.test",
			Args:        json.RawMessage(`{}`),
			Queue:       "dead-q",
			MaxAttempts: &maxAtt,
			Retry: &core.RetryPolicy{
				MaxAttempts:  1,
				OnExhaustion: "dead_letter",
			},
		})
		require.NoError(t, err)

		// Fetch to make active
		_, err = b.Fetch(ctx, []string{"dead-q"}, 1, "worker-1", DefaultVisibilityTimeoutMs)
		require.NoError(t, err)

		// Nack to exhaust retries — should go to dead letter
		resp, err := b.Nack(ctx, job.ID, &core.JobError{Message: "fatal"}, false)
		require.NoError(t, err)
		assert.Equal(t, core.StateDiscarded, resp.State)

		// Verify job is in dead letter queue
		deadJobs, total, err := b.ListDeadLetter(ctx, 10, 0)
		require.NoError(t, err)
		assert.Equal(t, 1, total)
		require.Len(t, deadJobs, 1)
		assert.Equal(t, job.ID, deadJobs[0].ID)
	})

	t.Run("ListDeadLetter_returns_dead_jobs_with_correct_count", func(t *testing.T) {
		b := setupTestBackend(t)
		ctx := context.Background()

		// Push and move 3 jobs to dead letter
		var jobIDs []string
		for i := 0; i < 3; i++ {
			maxAtt := 1
			j, err := b.Push(ctx, &core.Job{
				Type:        "dead.batch",
				Args:        json.RawMessage(`{}`),
				Queue:       "dead-batch-q",
				MaxAttempts: &maxAtt,
				Retry: &core.RetryPolicy{
					MaxAttempts:  1,
					OnExhaustion: "dead_letter",
				},
			})
			require.NoError(t, err)
			jobIDs = append(jobIDs, j.ID)

			_, err = b.Fetch(ctx, []string{"dead-batch-q"}, 1, "worker-1", DefaultVisibilityTimeoutMs)
			require.NoError(t, err)
			_, err = b.Nack(ctx, j.ID, &core.JobError{Message: "fatal"}, false)
			require.NoError(t, err)
		}

		deadJobs, total, err := b.ListDeadLetter(ctx, 10, 0)
		require.NoError(t, err)
		assert.Equal(t, 3, total)
		assert.Len(t, deadJobs, 3)

		// Test pagination
		page1, total1, err := b.ListDeadLetter(ctx, 2, 0)
		require.NoError(t, err)
		assert.Equal(t, 3, total1)
		assert.Len(t, page1, 2)

		page2, _, err := b.ListDeadLetter(ctx, 2, 2)
		require.NoError(t, err)
		assert.Len(t, page2, 1)
	})

	t.Run("RetryDeadLetter_moves_job_back_to_available", func(t *testing.T) {
		b := setupTestBackend(t)
		ctx := context.Background()

		maxAtt := 1
		job, err := b.Push(ctx, &core.Job{
			Type:        "dead.retry",
			Args:        json.RawMessage(`{}`),
			Queue:       "dead-retry-q",
			MaxAttempts: &maxAtt,
			Retry: &core.RetryPolicy{
				MaxAttempts:  1,
				OnExhaustion: "dead_letter",
			},
		})
		require.NoError(t, err)

		_, err = b.Fetch(ctx, []string{"dead-retry-q"}, 1, "worker-1", DefaultVisibilityTimeoutMs)
		require.NoError(t, err)
		_, err = b.Nack(ctx, job.ID, &core.JobError{Message: "fatal"}, false)
		require.NoError(t, err)

		// Retry from dead letter
		retried, err := b.RetryDeadLetter(ctx, job.ID)
		require.NoError(t, err)
		assert.Equal(t, core.StateAvailable, retried.State)

		// Should be fetchable again
		jobs, err := b.Fetch(ctx, []string{"dead-retry-q"}, 1, "worker-1", DefaultVisibilityTimeoutMs)
		require.NoError(t, err)
		assert.Len(t, jobs, 1)
		assert.Equal(t, job.ID, jobs[0].ID)

		// Should no longer be in dead letter
		_, total, err := b.ListDeadLetter(ctx, 10, 0)
		require.NoError(t, err)
		assert.Equal(t, 0, total)
	})

	t.Run("DeleteDeadLetter_permanently_removes_job", func(t *testing.T) {
		b := setupTestBackend(t)
		ctx := context.Background()

		maxAtt := 1
		job, err := b.Push(ctx, &core.Job{
			Type:        "dead.delete",
			Args:        json.RawMessage(`{}`),
			Queue:       "dead-delete-q",
			MaxAttempts: &maxAtt,
			Retry: &core.RetryPolicy{
				MaxAttempts:  1,
				OnExhaustion: "dead_letter",
			},
		})
		require.NoError(t, err)

		_, err = b.Fetch(ctx, []string{"dead-delete-q"}, 1, "worker-1", DefaultVisibilityTimeoutMs)
		require.NoError(t, err)
		_, err = b.Nack(ctx, job.ID, &core.JobError{Message: "fatal"}, false)
		require.NoError(t, err)

		// Delete from dead letter
		err = b.DeleteDeadLetter(ctx, job.ID)
		require.NoError(t, err)

		_, total, err := b.ListDeadLetter(ctx, 10, 0)
		require.NoError(t, err)
		assert.Equal(t, 0, total)

		// Deleting again should return not found
		err = b.DeleteDeadLetter(ctx, job.ID)
		require.Error(t, err)
		ojsErr, ok := err.(*core.OJSError)
		require.True(t, ok)
		assert.Equal(t, core.ErrCodeNotFound, ojsErr.Code)
	})

	t.Run("RetryDeadLetter_not_found", func(t *testing.T) {
		b := setupTestBackend(t)
		ctx := context.Background()

		_, err := b.RetryDeadLetter(ctx, "nonexistent-dead-id")
		require.Error(t, err)
		ojsErr, ok := err.(*core.OJSError)
		require.True(t, ok)
		assert.Equal(t, core.ErrCodeNotFound, ojsErr.Code)
	})
}

// --- Cron Operations ---

func TestCronOperations(t *testing.T) {
	t.Run("RegisterCron_creates_a_cron_entry", func(t *testing.T) {
		b := setupTestBackend(t)
		ctx := context.Background()

		cron, err := b.RegisterCron(ctx, &core.CronJob{
			Name:       "test-cron",
			Expression: "*/5 * * * *",
			JobTemplate: &core.CronJobTemplate{
				Type: "cron.job",
				Args: json.RawMessage(`{"key":"value"}`),
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "test-cron", cron.Name)
		assert.Equal(t, "*/5 * * * *", cron.Expression)
		assert.True(t, cron.Enabled)
		assert.NotEmpty(t, cron.CreatedAt)
		assert.NotEmpty(t, cron.NextRunAt)
		assert.Equal(t, "default", cron.Queue)
	})

	t.Run("RegisterCron_with_custom_queue_and_timezone", func(t *testing.T) {
		b := setupTestBackend(t)
		ctx := context.Background()

		cron, err := b.RegisterCron(ctx, &core.CronJob{
			Name:       "tz-cron",
			Expression: "0 9 * * *",
			Timezone:   "America/New_York",
			Queue:      "custom-q",
			JobTemplate: &core.CronJobTemplate{
				Type: "cron.tz",
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "tz-cron", cron.Name)
		assert.NotEmpty(t, cron.NextRunAt)
	})

	t.Run("RegisterCron_invalid_expression_returns_error", func(t *testing.T) {
		b := setupTestBackend(t)
		ctx := context.Background()

		_, err := b.RegisterCron(ctx, &core.CronJob{
			Name:       "bad-cron",
			Expression: "not a valid expression",
		})
		require.Error(t, err)
	})

	t.Run("ListCron_returns_all_registered_entries", func(t *testing.T) {
		b := setupTestBackend(t)
		ctx := context.Background()

		_, err := b.RegisterCron(ctx, &core.CronJob{
			Name:       "cron-a",
			Expression: "*/10 * * * *",
			JobTemplate: &core.CronJobTemplate{
				Type: "cron.a",
			},
		})
		require.NoError(t, err)

		_, err = b.RegisterCron(ctx, &core.CronJob{
			Name:       "cron-b",
			Expression: "@hourly",
			JobTemplate: &core.CronJobTemplate{
				Type: "cron.b",
			},
		})
		require.NoError(t, err)

		crons, err := b.ListCron(ctx)
		require.NoError(t, err)
		assert.Len(t, crons, 2)

		names := make([]string, len(crons))
		for i, c := range crons {
			names[i] = c.Name
		}
		assert.Contains(t, names, "cron-a")
		assert.Contains(t, names, "cron-b")
	})

	t.Run("DeleteCron_removes_entry", func(t *testing.T) {
		b := setupTestBackend(t)
		ctx := context.Background()

		_, err := b.RegisterCron(ctx, &core.CronJob{
			Name:       "deletable-cron",
			Expression: "*/15 * * * *",
			JobTemplate: &core.CronJobTemplate{
				Type: "cron.del",
			},
		})
		require.NoError(t, err)

		deleted, err := b.DeleteCron(ctx, "deletable-cron")
		require.NoError(t, err)
		assert.Equal(t, "deletable-cron", deleted.Name)

		// Should no longer appear in list
		crons, err := b.ListCron(ctx)
		require.NoError(t, err)
		assert.Empty(t, crons)
	})

	t.Run("DeleteCron_not_found", func(t *testing.T) {
		b := setupTestBackend(t)
		ctx := context.Background()

		_, err := b.DeleteCron(ctx, "nonexistent-cron")
		require.Error(t, err)
		ojsErr, ok := err.(*core.OJSError)
		require.True(t, ok)
		assert.Equal(t, core.ErrCodeNotFound, ojsErr.Code)
	})
}

// --- Workflow Operations ---

func TestWorkflowOperations(t *testing.T) {
	t.Run("CreateWorkflow_group_creates_and_returns_workflow", func(t *testing.T) {
		b := setupTestBackend(t)
		ctx := context.Background()

		wf, err := b.CreateWorkflow(ctx, &core.WorkflowRequest{
			Type: "group",
			Name: "test-group",
			Jobs: []core.WorkflowJobRequest{
				{Name: "step-1", Type: "wf.job.a", Args: json.RawMessage(`{}`)},
				{Name: "step-2", Type: "wf.job.b", Args: json.RawMessage(`{}`)},
			},
		})
		require.NoError(t, err)
		assert.NotEmpty(t, wf.ID)
		assert.Equal(t, "test-group", wf.Name)
		assert.Equal(t, "group", wf.Type)
		assert.Equal(t, "running", wf.State)
		require.NotNil(t, wf.JobsTotal)
		assert.Equal(t, 2, *wf.JobsTotal)
		require.NotNil(t, wf.JobsCompleted)
		assert.Equal(t, 0, *wf.JobsCompleted)
		assert.NotEmpty(t, wf.CreatedAt)
	})

	t.Run("CreateWorkflow_chain_creates_and_returns_workflow", func(t *testing.T) {
		b := setupTestBackend(t)
		ctx := context.Background()

		wf, err := b.CreateWorkflow(ctx, &core.WorkflowRequest{
			Type: "chain",
			Name: "test-chain",
			Steps: []core.WorkflowJobRequest{
				{Name: "step-1", Type: "chain.a", Args: json.RawMessage(`{}`)},
				{Name: "step-2", Type: "chain.b", Args: json.RawMessage(`{}`)},
				{Name: "step-3", Type: "chain.c", Args: json.RawMessage(`{}`)},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "chain", wf.Type)
		assert.Equal(t, "running", wf.State)
		require.NotNil(t, wf.StepsTotal)
		assert.Equal(t, 3, *wf.StepsTotal)
		require.NotNil(t, wf.StepsCompleted)
		assert.Equal(t, 0, *wf.StepsCompleted)
	})

	t.Run("GetWorkflow_retrieves_by_ID", func(t *testing.T) {
		b := setupTestBackend(t)
		ctx := context.Background()

		created, err := b.CreateWorkflow(ctx, &core.WorkflowRequest{
			Type: "group",
			Name: "get-test",
			Jobs: []core.WorkflowJobRequest{
				{Name: "job-1", Type: "get.job", Args: json.RawMessage(`{}`)},
			},
		})
		require.NoError(t, err)

		retrieved, err := b.GetWorkflow(ctx, created.ID)
		require.NoError(t, err)
		assert.Equal(t, created.ID, retrieved.ID)
		assert.Equal(t, "get-test", retrieved.Name)
		assert.Equal(t, "group", retrieved.Type)
		assert.Equal(t, "running", retrieved.State)
	})

	t.Run("GetWorkflow_not_found", func(t *testing.T) {
		b := setupTestBackend(t)
		ctx := context.Background()

		_, err := b.GetWorkflow(ctx, "nonexistent-wf")
		require.Error(t, err)
		ojsErr, ok := err.(*core.OJSError)
		require.True(t, ok)
		assert.Equal(t, core.ErrCodeNotFound, ojsErr.Code)
	})

	t.Run("CancelWorkflow_transitions_to_cancelled", func(t *testing.T) {
		b := setupTestBackend(t)
		ctx := context.Background()

		wf, err := b.CreateWorkflow(ctx, &core.WorkflowRequest{
			Type: "group",
			Name: "cancel-wf",
			Jobs: []core.WorkflowJobRequest{
				{Name: "job-1", Type: "cancel.job.a", Args: json.RawMessage(`{}`)},
				{Name: "job-2", Type: "cancel.job.b", Args: json.RawMessage(`{}`)},
			},
		})
		require.NoError(t, err)

		cancelled, err := b.CancelWorkflow(ctx, wf.ID)
		require.NoError(t, err)
		assert.Equal(t, "cancelled", cancelled.State)
		assert.NotEmpty(t, cancelled.CompletedAt)

		// Verify via GetWorkflow
		retrieved, err := b.GetWorkflow(ctx, wf.ID)
		require.NoError(t, err)
		assert.Equal(t, "cancelled", retrieved.State)
	})

	t.Run("CancelWorkflow_already_cancelled_returns_conflict", func(t *testing.T) {
		b := setupTestBackend(t)
		ctx := context.Background()

		wf, err := b.CreateWorkflow(ctx, &core.WorkflowRequest{
			Type: "group",
			Name: "double-cancel",
			Jobs: []core.WorkflowJobRequest{
				{Name: "job-1", Type: "dc.job", Args: json.RawMessage(`{}`)},
			},
		})
		require.NoError(t, err)

		_, err = b.CancelWorkflow(ctx, wf.ID)
		require.NoError(t, err)

		// Second cancel should conflict
		_, err = b.CancelWorkflow(ctx, wf.ID)
		require.Error(t, err)
		ojsErr, ok := err.(*core.OJSError)
		require.True(t, ok)
		assert.Equal(t, core.ErrCodeConflict, ojsErr.Code)
	})

	t.Run("CancelWorkflow_cancels_child_jobs", func(t *testing.T) {
		b := setupTestBackend(t)
		ctx := context.Background()

		wf, err := b.CreateWorkflow(ctx, &core.WorkflowRequest{
			Type: "group",
			Name: "cancel-children",
			Jobs: []core.WorkflowJobRequest{
				{Name: "child-1", Type: "child.a", Args: json.RawMessage(`{}`)},
				{Name: "child-2", Type: "child.b", Args: json.RawMessage(`{}`)},
			},
		})
		require.NoError(t, err)

		// Get the child job IDs from the workflow jobs list
		jobIDs, err := b.client.LRange(ctx, workflowKey(wf.ID)+":jobs", 0, -1).Result()
		require.NoError(t, err)
		require.Len(t, jobIDs, 2)

		_, err = b.CancelWorkflow(ctx, wf.ID)
		require.NoError(t, err)

		// Verify child jobs are cancelled
		for _, id := range jobIDs {
			info, err := b.Info(ctx, id)
			require.NoError(t, err)
			assert.Equal(t, core.StateCancelled, info.State)
		}
	})
}

// --- Concurrent Operations ---

func TestConcurrentOperations(t *testing.T) {
	t.Run("concurrent_fetch_no_double_delivery", func(t *testing.T) {
		b := setupTestBackend(t)
		ctx := context.Background()

		// Push 10 jobs
		const numJobs = 10
		for i := 0; i < numJobs; i++ {
			pushJob(t, b, "concurrent.fetch", "conc-fetch-q")
		}

		// Spin up multiple goroutines each trying to fetch
		const numWorkers = 5
		var mu sync.Mutex
		allFetched := make(map[string]bool)
		var wg sync.WaitGroup

		for w := 0; w < numWorkers; w++ {
			wg.Add(1)
			workerID := "worker-" + string(rune('A'+w))
			go func(wid string) {
				defer wg.Done()
				jobs, err := b.Fetch(ctx, []string{"conc-fetch-q"}, numJobs, wid, DefaultVisibilityTimeoutMs)
				if err != nil {
					return
				}
				mu.Lock()
				defer mu.Unlock()
				for _, j := range jobs {
					assert.False(t, allFetched[j.ID], "job %s was double-delivered", j.ID)
					allFetched[j.ID] = true
				}
			}(workerID)
		}

		wg.Wait()
		assert.Equal(t, numJobs, len(allFetched), "all jobs should be fetched exactly once")
	})

	t.Run("concurrent_push_and_fetch_race_safety", func(t *testing.T) {
		b := setupTestBackend(t)
		ctx := context.Background()

		const numOps = 20
		var wg sync.WaitGroup
		var mu sync.Mutex
		pushedIDs := make(map[string]bool)
		fetchedIDs := make(map[string]bool)

		// Concurrent pushes
		for i := 0; i < numOps; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				job, err := b.Push(ctx, &core.Job{
					Type:  "race.test",
					Args:  json.RawMessage(`{}`),
					Queue: "race-q",
				})
				if err != nil {
					return
				}
				mu.Lock()
				pushedIDs[job.ID] = true
				mu.Unlock()
			}()
		}

		// Concurrent fetches (start after a short delay to overlap with pushes)
		for i := 0; i < numOps; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				workerID := "race-worker-" + string(rune('A'+idx%5))
				jobs, err := b.Fetch(ctx, []string{"race-q"}, 1, workerID, DefaultVisibilityTimeoutMs)
				if err != nil || len(jobs) == 0 {
					return
				}
				mu.Lock()
				for _, j := range jobs {
					assert.False(t, fetchedIDs[j.ID], "job %s was double-fetched", j.ID)
					fetchedIDs[j.ID] = true
				}
				mu.Unlock()
			}(i)
		}

		wg.Wait()

		// Every fetched job must have been pushed
		for id := range fetchedIDs {
			assert.True(t, pushedIDs[id], "fetched job %s was never pushed", id)
		}
	})

	t.Run("batch_push_atomicity", func(t *testing.T) {
		b := setupTestBackend(t)
		ctx := context.Background()

		jobs := []*core.Job{
			{Type: "batch.1", Args: json.RawMessage(`{"n":1}`), Queue: "batch-atom-q"},
			{Type: "batch.2", Args: json.RawMessage(`{"n":2}`), Queue: "batch-atom-q"},
			{Type: "batch.3", Args: json.RawMessage(`{"n":3}`), Queue: "batch-atom-q"},
			{Type: "batch.4", Args: json.RawMessage(`{"n":4}`), Queue: "batch-atom-q"},
			{Type: "batch.5", Args: json.RawMessage(`{"n":5}`), Queue: "batch-atom-q"},
		}

		results, err := b.PushBatch(ctx, jobs)
		require.NoError(t, err)
		require.Len(t, results, 5)

		// Verify all have unique IDs and are available
		idSet := make(map[string]bool)
		for _, r := range results {
			assert.NotEmpty(t, r.ID)
			assert.Equal(t, core.StateAvailable, r.State)
			assert.False(t, idSet[r.ID], "duplicate ID in batch")
			idSet[r.ID] = true
		}

		// Verify queue stats match
		stats, err := b.QueueStats(ctx, "batch-atom-q")
		require.NoError(t, err)
		assert.Equal(t, 5, stats.Stats.Available)
	})

	t.Run("concurrent_batch_pushes", func(t *testing.T) {
		b := setupTestBackend(t)
		ctx := context.Background()

		const numBatches = 5
		const batchSize = 3
		var wg sync.WaitGroup
		var mu sync.Mutex
		allIDs := make(map[string]bool)

		for i := 0; i < numBatches; i++ {
			wg.Add(1)
			go func(batch int) {
				defer wg.Done()
				jobs := make([]*core.Job, batchSize)
				for j := 0; j < batchSize; j++ {
					jobs[j] = &core.Job{
						Type:  "conc.batch",
						Args:  json.RawMessage(`{}`),
						Queue: "conc-batch-q",
					}
				}
				results, err := b.PushBatch(ctx, jobs)
				if err != nil {
					return
				}
				mu.Lock()
				for _, r := range results {
					allIDs[r.ID] = true
				}
				mu.Unlock()
			}(i)
		}

		wg.Wait()
		assert.Equal(t, numBatches*batchSize, len(allIDs), "all batch jobs should have unique IDs")
	})
}

// --- Lua Script Edge Cases ---

func TestLuaScriptEdgeCases(t *testing.T) {
	t.Run("ack_nonexistent_job", func(t *testing.T) {
		b := setupTestBackend(t)
		ctx := context.Background()

		_, err := b.Ack(ctx, "nonexistent-ack-id", nil)
		require.Error(t, err)
		ojsErr, ok := err.(*core.OJSError)
		require.True(t, ok)
		assert.Equal(t, core.ErrCodeNotFound, ojsErr.Code)
	})

	t.Run("nack_nonexistent_job", func(t *testing.T) {
		b := setupTestBackend(t)
		ctx := context.Background()

		_, err := b.Nack(ctx, "nonexistent-nack-id", &core.JobError{Message: "err"}, false)
		require.Error(t, err)
		ojsErr, ok := err.(*core.OJSError)
		require.True(t, ok)
		assert.Equal(t, core.ErrCodeNotFound, ojsErr.Code)
	})

	t.Run("double_ack_returns_conflict", func(t *testing.T) {
		b := setupTestBackend(t)
		ctx := context.Background()

		job := pushJob(t, b, "double.ack", "dack-q")
		_, err := b.Fetch(ctx, []string{"dack-q"}, 1, "worker-1", DefaultVisibilityTimeoutMs)
		require.NoError(t, err)

		_, err = b.Ack(ctx, job.ID, nil)
		require.NoError(t, err)

		// Second ack should conflict
		_, err = b.Ack(ctx, job.ID, nil)
		require.Error(t, err)
		ojsErr, ok := err.(*core.OJSError)
		require.True(t, ok)
		assert.Equal(t, core.ErrCodeConflict, ojsErr.Code)
	})

	t.Run("nack_with_requeue_then_fetch_again", func(t *testing.T) {
		b := setupTestBackend(t)
		ctx := context.Background()

		job := pushJob(t, b, "requeue.lua", "requeue-lua-q")
		_, err := b.Fetch(ctx, []string{"requeue-lua-q"}, 1, "worker-1", DefaultVisibilityTimeoutMs)
		require.NoError(t, err)

		resp, err := b.Nack(ctx, job.ID, &core.JobError{Message: "retry me"}, true)
		require.NoError(t, err)
		assert.Equal(t, core.StateAvailable, resp.State)

		// Should be fetchable again
		jobs, err := b.Fetch(ctx, []string{"requeue-lua-q"}, 1, "worker-2", DefaultVisibilityTimeoutMs)
		require.NoError(t, err)
		require.Len(t, jobs, 1)
		assert.Equal(t, job.ID, jobs[0].ID)
	})

	t.Run("cancel_from_various_states", func(t *testing.T) {
		b := setupTestBackend(t)
		ctx := context.Background()

		// Cancel from available
		availJob := pushJob(t, b, "cancel.avail", "cancel-states-q")
		cancelled, err := b.Cancel(ctx, availJob.ID)
		require.NoError(t, err)
		assert.Equal(t, core.StateCancelled, cancelled.State)

		// Cancel from active (fetch first)
		activeJob := pushJob(t, b, "cancel.active", "cancel-states-q")
		_, err = b.Fetch(ctx, []string{"cancel-states-q"}, 1, "worker-1", DefaultVisibilityTimeoutMs)
		require.NoError(t, err)
		cancelled, err = b.Cancel(ctx, activeJob.ID)
		require.NoError(t, err)
		assert.Equal(t, core.StateCancelled, cancelled.State)
	})

	t.Run("ack_with_result_data", func(t *testing.T) {
		b := setupTestBackend(t)
		ctx := context.Background()

		job := pushJob(t, b, "ack.result", "ack-result-q")
		_, err := b.Fetch(ctx, []string{"ack-result-q"}, 1, "worker-1", DefaultVisibilityTimeoutMs)
		require.NoError(t, err)

		resultData := json.RawMessage(`{"output":"success","count":42}`)
		ackResp, err := b.Ack(ctx, job.ID, resultData)
		require.NoError(t, err)
		assert.True(t, ackResp.Acknowledged)
		assert.Equal(t, core.StateCompleted, ackResp.State)

		// Verify result is stored
		info, err := b.Info(ctx, job.ID)
		require.NoError(t, err)
		assert.Equal(t, core.StateCompleted, info.State)
	})

	t.Run("nack_retry_increments_attempt", func(t *testing.T) {
		b := setupTestBackend(t)
		ctx := context.Background()

		maxAtt := 5
		job, err := b.Push(ctx, &core.Job{
			Type:        "retry.attempt",
			Args:        json.RawMessage(`{}`),
			Queue:       "retry-inc-q",
			MaxAttempts: &maxAtt,
		})
		require.NoError(t, err)

		// First cycle: fetch + nack
		_, err = b.Fetch(ctx, []string{"retry-inc-q"}, 1, "worker-1", DefaultVisibilityTimeoutMs)
		require.NoError(t, err)

		resp, err := b.Nack(ctx, job.ID, &core.JobError{Message: "err1"}, false)
		require.NoError(t, err)
		assert.Equal(t, core.StateRetryable, resp.State)
		assert.Equal(t, 1, resp.Attempt)
		assert.Equal(t, 5, resp.MaxAttempts)
	})
}
