package redis

import (
	"context"
	"fmt"
	"testing"
	"time"

	redisclient "github.com/redis/go-redis/v9"
)

// ---------------------------------------------------------------------------
// Lua Script Unit Tests
//
// These tests exercise each Lua script in isolation by setting up known Redis
// state, running the script via the pre-compiled *redis.Script objects, and
// verifying both the return value and side-effects on Redis keys.
//
// Requires REDIS_URL to be set (reuses testBackend from storage_test.go).
// ---------------------------------------------------------------------------

// --- fetch.lua ---

func TestLuaFetch_Success(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	jobID := "fetch-job-1"
	queue := "fetch-q"
	nowFmt := time.Now().UTC().Format(time.RFC3339)
	nowMs := time.Now().UnixMilli()

	// Setup: create job hash and add to available sorted set
	b.client.HSet(ctx, jobKey(jobID), map[string]interface{}{
		"state":    "available",
		"queue":    queue,
		"type":     "test.fetch",
		"priority": "0",
	})
	score := float64(100)*PriorityScoreMultiplier + float64(nowMs)
	b.client.ZAdd(ctx, queueAvailableKey(queue), redisclient.Z{Score: score, Member: jobID})

	// Execute fetch.lua
	result, err := fetchScript.Run(ctx, b.client, nil,
		queue,                                  // ARGV[1] queue name
		nowFmt,                                 // ARGV[2] now_formatted
		"worker-1",                             // ARGV[3] worker_id
		fmt.Sprintf("%d", nowMs),               // ARGV[4] now_ms
		"0",                                    // ARGV[5] vis_timeout_ms (request-level)
		fmt.Sprintf("%d", DefaultVisibilityTimeoutMs), // ARGV[6] default_vis_timeout_ms
	).Result()
	if err != nil {
		t.Fatalf("fetch script: %v", err)
	}

	status, data := parseLuaResult(result)
	if status != luaStatusOK {
		t.Fatalf("expected status %d, got %d", luaStatusOK, status)
	}
	if len(data) < 1 {
		t.Fatal("expected job_id in result data")
	}
	returnedID, ok := data[0].(string)
	if !ok {
		t.Fatalf("expected string job_id, got %T", data[0])
	}
	if returnedID != jobID {
		t.Errorf("returned job_id = %q, want %q", returnedID, jobID)
	}

	// Verify state changed to active
	state, err := b.client.HGet(ctx, jobKey(jobID), "state").Result()
	if err != nil {
		t.Fatalf("HGet state: %v", err)
	}
	if state != "active" {
		t.Errorf("state = %q, want %q", state, "active")
	}

	// Verify removed from available set
	avail, _ := b.client.ZScore(ctx, queueAvailableKey(queue), jobID).Result()
	if avail != 0 {
		t.Error("job should be removed from available set")
	}

	// Verify added to active set
	isMember, _ := b.client.SIsMember(ctx, queueActiveKey(queue), jobID).Result()
	if !isMember {
		t.Error("job should be in active set")
	}

	// Verify visibility key set
	vis, err := b.client.Get(ctx, visibilityKey(jobID)).Result()
	if err != nil {
		t.Fatalf("visibility key: %v", err)
	}
	if vis == "" {
		t.Error("expected visibility key to be set")
	}
}

func TestLuaFetch_EmptyQueue(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	nowFmt := time.Now().UTC().Format(time.RFC3339)
	nowMs := time.Now().UnixMilli()

	result, err := fetchScript.Run(ctx, b.client, nil,
		"empty-q", nowFmt, "worker-1",
		fmt.Sprintf("%d", nowMs), "0",
		fmt.Sprintf("%d", DefaultVisibilityTimeoutMs),
	).Result()
	if err != nil {
		t.Fatalf("fetch script: %v", err)
	}

	status, _ := parseLuaResult(result)
	if status != 1 { // status 1 = no job available
		t.Errorf("expected status 1 (no job), got %d", status)
	}
}

func TestLuaFetch_ExpiredJob(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	jobID := "fetch-expired-1"
	queue := "fetch-exp-q"
	pastTime := time.Now().Add(-1 * time.Hour).UTC().Format(time.RFC3339)
	nowFmt := time.Now().UTC().Format(time.RFC3339)
	nowMs := time.Now().UnixMilli()

	b.client.HSet(ctx, jobKey(jobID), map[string]interface{}{
		"state":      "available",
		"queue":      queue,
		"type":       "test.expired",
		"priority":   "0",
		"expires_at": pastTime,
	})
	score := float64(100)*PriorityScoreMultiplier + float64(nowMs)
	b.client.ZAdd(ctx, queueAvailableKey(queue), redisclient.Z{Score: score, Member: jobID})

	result, err := fetchScript.Run(ctx, b.client, nil,
		queue, nowFmt, "worker-1",
		fmt.Sprintf("%d", nowMs), "0",
		fmt.Sprintf("%d", DefaultVisibilityTimeoutMs),
	).Result()
	if err != nil {
		t.Fatalf("fetch script: %v", err)
	}

	status, data := parseLuaResult(result)
	if status != luaStatusExpired {
		t.Fatalf("expected status %d (expired), got %d", luaStatusExpired, status)
	}
	if len(data) < 1 {
		t.Fatal("expected job_id in expired result")
	}
	if data[0].(string) != jobID {
		t.Errorf("returned job_id = %q, want %q", data[0], jobID)
	}

	// Verify state changed to discarded
	state, _ := b.client.HGet(ctx, jobKey(jobID), "state").Result()
	if state != "discarded" {
		t.Errorf("state = %q, want %q", state, "discarded")
	}
}

func TestLuaFetch_JobLevelVisibilityTimeout(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	jobID := "fetch-vis-1"
	queue := "fetch-vis-q"
	nowFmt := time.Now().UTC().Format(time.RFC3339)
	nowMs := time.Now().UnixMilli()

	b.client.HSet(ctx, jobKey(jobID), map[string]interface{}{
		"state":                 "available",
		"queue":                 queue,
		"type":                  "test.vis",
		"priority":              "0",
		"visibility_timeout_ms": "60000",
	})
	score := float64(100)*PriorityScoreMultiplier + float64(nowMs)
	b.client.ZAdd(ctx, queueAvailableKey(queue), redisclient.Z{Score: score, Member: jobID})

	result, err := fetchScript.Run(ctx, b.client, nil,
		queue, nowFmt, "worker-1",
		fmt.Sprintf("%d", nowMs),
		"0", // request-level = 0, so job-level 60000 should be used
		fmt.Sprintf("%d", DefaultVisibilityTimeoutMs),
	).Result()
	if err != nil {
		t.Fatalf("fetch script: %v", err)
	}

	status, _ := parseLuaResult(result)
	if status != luaStatusOK {
		t.Fatalf("expected status %d, got %d", luaStatusOK, status)
	}

	// Verify visibility key uses job-level timeout
	vis, _ := b.client.Get(ctx, visibilityKey(jobID)).Result()
	expectedVis := fmt.Sprintf("%d", nowMs+60000)
	if vis != expectedVis {
		t.Errorf("visibility = %q, want %q", vis, expectedVis)
	}
}

// --- ack.lua ---

func TestLuaAck_Success(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	jobID := "ack-job-1"
	queue := "ack-q"
	completedAt := time.Now().UTC().Format(time.RFC3339)

	// Setup: active job with visibility key
	b.client.HSet(ctx, jobKey(jobID), map[string]interface{}{
		"state": "active",
		"queue": queue,
		"type":  "test.ack",
	})
	b.client.SAdd(ctx, queueActiveKey(queue), jobID)
	b.client.Set(ctx, visibilityKey(jobID), "999999999", 0)

	result, err := ackScript.Run(ctx, b.client, nil,
		jobID, completedAt, `{"output":"done"}`,
	).Result()
	if err != nil {
		t.Fatalf("ack script: %v", err)
	}

	status, data := parseLuaResult(result)
	if status != luaStatusOK {
		t.Fatalf("expected status %d, got %d", luaStatusOK, status)
	}
	if len(data) < 1 || data[0].(string) != queue {
		t.Errorf("expected queue %q in result", queue)
	}

	// Verify state = completed
	state, _ := b.client.HGet(ctx, jobKey(jobID), "state").Result()
	if state != "completed" {
		t.Errorf("state = %q, want %q", state, "completed")
	}

	// Verify result stored
	res, _ := b.client.HGet(ctx, jobKey(jobID), "result").Result()
	if res != `{"output":"done"}` {
		t.Errorf("result = %q, want %q", res, `{"output":"done"}`)
	}

	// Verify removed from active set
	isMember, _ := b.client.SIsMember(ctx, queueActiveKey(queue), jobID).Result()
	if isMember {
		t.Error("job should be removed from active set")
	}

	// Verify visibility key deleted
	exists, _ := b.client.Exists(ctx, visibilityKey(jobID)).Result()
	if exists != 0 {
		t.Error("visibility key should be deleted")
	}

	// Verify completed counter incremented
	count, _ := b.client.Get(ctx, queueCompletedKey(queue)).Result()
	if count != "1" {
		t.Errorf("completed counter = %q, want %q", count, "1")
	}
}

func TestLuaAck_NotFound(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	result, err := ackScript.Run(ctx, b.client, nil,
		"nonexistent-id", time.Now().UTC().Format(time.RFC3339), "",
	).Result()
	if err != nil {
		t.Fatalf("ack script: %v", err)
	}

	status, _ := parseLuaResult(result)
	if status != luaStatusNotFound {
		t.Errorf("expected status %d, got %d", luaStatusNotFound, status)
	}
}

func TestLuaAck_ConflictWrongState(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	jobID := "ack-conflict-1"
	b.client.HSet(ctx, jobKey(jobID), map[string]interface{}{
		"state": "available",
		"queue": "ack-conflict-q",
	})

	result, err := ackScript.Run(ctx, b.client, nil,
		jobID, time.Now().UTC().Format(time.RFC3339), "",
	).Result()
	if err != nil {
		t.Fatalf("ack script: %v", err)
	}

	status, data := parseLuaResult(result)
	if status != luaStatusConflict {
		t.Errorf("expected status %d, got %d", luaStatusConflict, status)
	}
	if len(data) < 1 || data[0].(string) != "available" {
		t.Errorf("expected current state 'available' in conflict data, got %v", data)
	}
}

func TestLuaAck_EmptyResult(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	jobID := "ack-noresult-1"
	queue := "ack-noresult-q"

	b.client.HSet(ctx, jobKey(jobID), map[string]interface{}{
		"state": "active",
		"queue": queue,
		"error": "previous error",
	})
	b.client.SAdd(ctx, queueActiveKey(queue), jobID)

	result, err := ackScript.Run(ctx, b.client, nil,
		jobID, time.Now().UTC().Format(time.RFC3339), "",
	).Result()
	if err != nil {
		t.Fatalf("ack script: %v", err)
	}

	status, _ := parseLuaResult(result)
	if status != luaStatusOK {
		t.Fatalf("expected status %d, got %d", luaStatusOK, status)
	}

	// Verify error field cleared
	errorVal, err := b.client.HGet(ctx, jobKey(jobID), "error").Result()
	if err == nil && errorVal != "" {
		t.Errorf("error field should be cleared, got %q", errorVal)
	}

	// Verify result not set
	resultExists, _ := b.client.HExists(ctx, jobKey(jobID), "result").Result()
	if resultExists {
		t.Error("result should not be set when empty string passed")
	}
}

// --- nack_retry.lua ---

func TestLuaNackRetry_Success(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	jobID := "retry-job-1"
	queue := "retry-q"
	nextAttemptMs := time.Now().Add(5 * time.Second).UnixMilli()

	b.client.HSet(ctx, jobKey(jobID), map[string]interface{}{
		"state": "active",
		"queue": queue,
	})
	b.client.SAdd(ctx, queueActiveKey(queue), jobID)
	b.client.Set(ctx, visibilityKey(jobID), "999999999", 0)

	result, err := nackRetryScript.Run(ctx, b.client, nil,
		jobID,                                 // ARGV[1]
		"1",                                   // ARGV[2] new_attempt
		`{"message":"temp error"}`,            // ARGV[3] error_json
		`[{"message":"temp error","at":"now"}]`, // ARGV[4] error_history_json
		"5000",                                // ARGV[5] retry_delay_ms
		fmt.Sprintf("%d", nextAttemptMs),      // ARGV[6] next_attempt_at_ms
	).Result()
	if err != nil {
		t.Fatalf("nack_retry script: %v", err)
	}

	status, data := parseLuaResult(result)
	if status != luaStatusOK {
		t.Fatalf("expected status %d, got %d", luaStatusOK, status)
	}
	if len(data) < 1 || data[0].(string) != queue {
		t.Errorf("expected queue %q in result", queue)
	}

	// Verify state = retryable
	state, _ := b.client.HGet(ctx, jobKey(jobID), "state").Result()
	if state != "retryable" {
		t.Errorf("state = %q, want %q", state, "retryable")
	}

	// Verify attempt incremented
	attempt, _ := b.client.HGet(ctx, jobKey(jobID), "attempt").Result()
	if attempt != "1" {
		t.Errorf("attempt = %q, want %q", attempt, "1")
	}

	// Verify removed from active
	isMember, _ := b.client.SIsMember(ctx, queueActiveKey(queue), jobID).Result()
	if isMember {
		t.Error("job should be removed from active set")
	}

	// Verify visibility key deleted
	exists, _ := b.client.Exists(ctx, visibilityKey(jobID)).Result()
	if exists != 0 {
		t.Error("visibility key should be deleted")
	}

	// Verify added to retry sorted set
	retryScore, err := b.client.ZScore(ctx, retryKey(), jobID).Result()
	if err != nil {
		t.Fatalf("ZScore retry: %v", err)
	}
	if int64(retryScore) != nextAttemptMs {
		t.Errorf("retry score = %v, want %v", int64(retryScore), nextAttemptMs)
	}
}

func TestLuaNackRetry_NotFound(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	result, err := nackRetryScript.Run(ctx, b.client, nil,
		"nonexistent-id", "1", "", "[]", "5000", "999999",
	).Result()
	if err != nil {
		t.Fatalf("nack_retry script: %v", err)
	}

	status, _ := parseLuaResult(result)
	if status != luaStatusNotFound {
		t.Errorf("expected status %d, got %d", luaStatusNotFound, status)
	}
}

func TestLuaNackRetry_ConflictWrongState(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	jobID := "retry-conflict-1"
	b.client.HSet(ctx, jobKey(jobID), map[string]interface{}{
		"state": "completed",
		"queue": "retry-conflict-q",
	})

	result, err := nackRetryScript.Run(ctx, b.client, nil,
		jobID, "1", "", "[]", "5000", "999999",
	).Result()
	if err != nil {
		t.Fatalf("nack_retry script: %v", err)
	}

	status, data := parseLuaResult(result)
	if status != luaStatusConflict {
		t.Errorf("expected status %d, got %d", luaStatusConflict, status)
	}
	if len(data) < 1 || data[0].(string) != "completed" {
		t.Errorf("expected current state 'completed' in conflict data")
	}
}

// --- nack_discard.lua ---

func TestLuaNackDiscard_Success(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	jobID := "discard-job-1"
	queue := "discard-q"
	completedAt := time.Now().UTC().Format(time.RFC3339)

	b.client.HSet(ctx, jobKey(jobID), map[string]interface{}{
		"state": "active",
		"queue": queue,
	})
	b.client.SAdd(ctx, queueActiveKey(queue), jobID)
	b.client.Set(ctx, visibilityKey(jobID), "999999999", 0)

	result, err := nackDiscardScript.Run(ctx, b.client, nil,
		jobID,               // ARGV[1]
		"1",                 // ARGV[2] new_attempt
		`{"message":"fatal"}`, // ARGV[3] error_json
		`[{"message":"fatal","at":"now"}]`, // ARGV[4] error_history_json
		completedAt,         // ARGV[5] completed_at
		"discard",           // ARGV[6] on_exhaustion
		"0",                 // ARGV[7] now_ms
	).Result()
	if err != nil {
		t.Fatalf("nack_discard script: %v", err)
	}

	status, data := parseLuaResult(result)
	if status != luaStatusOK {
		t.Fatalf("expected status %d, got %d", luaStatusOK, status)
	}
	if len(data) < 1 || data[0].(string) != queue {
		t.Errorf("expected queue %q in result", queue)
	}

	// Verify state = discarded
	state, _ := b.client.HGet(ctx, jobKey(jobID), "state").Result()
	if state != "discarded" {
		t.Errorf("state = %q, want %q", state, "discarded")
	}

	// Verify removed from active set
	isMember, _ := b.client.SIsMember(ctx, queueActiveKey(queue), jobID).Result()
	if isMember {
		t.Error("job should be removed from active set")
	}

	// Verify NOT in dead letter queue (on_exhaustion = "discard")
	deadScore, err := b.client.ZScore(ctx, deadKey(), jobID).Result()
	if err == nil && deadScore > 0 {
		t.Error("job should not be in dead letter queue when on_exhaustion=discard")
	}
}

func TestLuaNackDiscard_DeadLetter(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	jobID := "discard-dl-1"
	queue := "discard-dl-q"
	nowMs := time.Now().UnixMilli()

	b.client.HSet(ctx, jobKey(jobID), map[string]interface{}{
		"state": "active",
		"queue": queue,
	})
	b.client.SAdd(ctx, queueActiveKey(queue), jobID)

	result, err := nackDiscardScript.Run(ctx, b.client, nil,
		jobID, "1", `{"message":"fatal"}`, `[]`,
		time.Now().UTC().Format(time.RFC3339),
		"dead_letter",
		fmt.Sprintf("%d", nowMs),
	).Result()
	if err != nil {
		t.Fatalf("nack_discard script: %v", err)
	}

	status, _ := parseLuaResult(result)
	if status != luaStatusOK {
		t.Fatalf("expected status %d, got %d", luaStatusOK, status)
	}

	// Verify added to dead letter queue
	deadScore, err := b.client.ZScore(ctx, deadKey(), jobID).Result()
	if err != nil {
		t.Fatalf("ZScore dead: %v", err)
	}
	if int64(deadScore) != nowMs {
		t.Errorf("dead letter score = %v, want %v", int64(deadScore), nowMs)
	}
}

func TestLuaNackDiscard_NotFound(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	result, err := nackDiscardScript.Run(ctx, b.client, nil,
		"nonexistent-id", "1", "", "[]",
		time.Now().UTC().Format(time.RFC3339), "discard", "0",
	).Result()
	if err != nil {
		t.Fatalf("nack_discard script: %v", err)
	}

	status, _ := parseLuaResult(result)
	if status != luaStatusNotFound {
		t.Errorf("expected status %d, got %d", luaStatusNotFound, status)
	}
}

// --- nack_requeue.lua ---

func TestLuaNackRequeue_Success(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	jobID := "requeue-job-1"
	queue := "requeue-q"
	nowMs := time.Now().UnixMilli()
	enqueuedAt := time.Now().UTC().Format(time.RFC3339)

	b.client.HSet(ctx, jobKey(jobID), map[string]interface{}{
		"state":    "active",
		"queue":    queue,
		"priority": "5",
	})
	b.client.SAdd(ctx, queueActiveKey(queue), jobID)
	b.client.Set(ctx, visibilityKey(jobID), "999999999", 0)

	result, err := nackRequeueScript.Run(ctx, b.client, nil,
		jobID, enqueuedAt, fmt.Sprintf("%d", nowMs),
	).Result()
	if err != nil {
		t.Fatalf("nack_requeue script: %v", err)
	}

	status, data := parseLuaResult(result)
	if status != luaStatusOK {
		t.Fatalf("expected status %d, got %d", luaStatusOK, status)
	}
	if len(data) < 1 || data[0].(string) != queue {
		t.Errorf("expected queue %q in result", queue)
	}

	// Verify state = available
	state, _ := b.client.HGet(ctx, jobKey(jobID), "state").Result()
	if state != "available" {
		t.Errorf("state = %q, want %q", state, "available")
	}

	// Verify removed from active set
	isMember, _ := b.client.SIsMember(ctx, queueActiveKey(queue), jobID).Result()
	if isMember {
		t.Error("job should be removed from active set")
	}

	// Verify visibility key deleted
	exists, _ := b.client.Exists(ctx, visibilityKey(jobID)).Result()
	if exists != 0 {
		t.Error("visibility key should be deleted")
	}

	// Verify added to available queue with priority-based score
	availScore, err := b.client.ZScore(ctx, queueAvailableKey(queue), jobID).Result()
	if err != nil {
		t.Fatalf("ZScore available: %v", err)
	}
	expectedScore := float64(100-5)*1e15 + float64(nowMs)
	if availScore != expectedScore {
		t.Errorf("available score = %v, want %v", availScore, expectedScore)
	}

	// Verify worker_id and started_at cleared
	workerID, _ := b.client.HGet(ctx, jobKey(jobID), "worker_id").Result()
	if workerID != "" {
		t.Errorf("worker_id = %q, want empty", workerID)
	}
}

func TestLuaNackRequeue_ConflictWrongState(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	jobID := "requeue-conflict-1"
	b.client.HSet(ctx, jobKey(jobID), map[string]interface{}{
		"state": "available",
		"queue": "requeue-conflict-q",
	})

	result, err := nackRequeueScript.Run(ctx, b.client, nil,
		jobID, time.Now().UTC().Format(time.RFC3339), "0",
	).Result()
	if err != nil {
		t.Fatalf("nack_requeue script: %v", err)
	}

	status, _ := parseLuaResult(result)
	if status != luaStatusConflict {
		t.Errorf("expected status %d, got %d", luaStatusConflict, status)
	}
}

// --- cancel.lua ---

func TestLuaCancel_FromAvailable(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	jobID := "cancel-avail-1"
	queue := "cancel-q"
	cancelledAt := time.Now().UTC().Format(time.RFC3339)
	nowMs := time.Now().UnixMilli()

	b.client.HSet(ctx, jobKey(jobID), map[string]interface{}{
		"state": "available",
		"queue": queue,
	})
	score := float64(100)*PriorityScoreMultiplier + float64(nowMs)
	b.client.ZAdd(ctx, queueAvailableKey(queue), redisclient.Z{Score: score, Member: jobID})

	result, err := cancelScript.Run(ctx, b.client, nil,
		jobID, cancelledAt,
	).Result()
	if err != nil {
		t.Fatalf("cancel script: %v", err)
	}

	status, data := parseLuaResult(result)
	if status != luaStatusOK {
		t.Fatalf("expected status %d, got %d", luaStatusOK, status)
	}
	if len(data) < 1 || data[0].(string) != queue {
		t.Errorf("expected queue %q in result", queue)
	}

	// Verify state = cancelled
	state, _ := b.client.HGet(ctx, jobKey(jobID), "state").Result()
	if state != "cancelled" {
		t.Errorf("state = %q, want %q", state, "cancelled")
	}

	// Verify removed from available
	_, err = b.client.ZScore(ctx, queueAvailableKey(queue), jobID).Result()
	if err == nil {
		t.Error("job should be removed from available set")
	}
}

func TestLuaCancel_FromActive(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	jobID := "cancel-active-1"
	queue := "cancel-active-q"

	b.client.HSet(ctx, jobKey(jobID), map[string]interface{}{
		"state": "active",
		"queue": queue,
	})
	b.client.SAdd(ctx, queueActiveKey(queue), jobID)
	b.client.Set(ctx, visibilityKey(jobID), "999999999", 0)

	result, err := cancelScript.Run(ctx, b.client, nil,
		jobID, time.Now().UTC().Format(time.RFC3339),
	).Result()
	if err != nil {
		t.Fatalf("cancel script: %v", err)
	}

	status, _ := parseLuaResult(result)
	if status != luaStatusOK {
		t.Fatalf("expected status %d, got %d", luaStatusOK, status)
	}

	// Verify removed from active set
	isMember, _ := b.client.SIsMember(ctx, queueActiveKey(queue), jobID).Result()
	if isMember {
		t.Error("job should be removed from active set")
	}

	// Verify visibility key deleted
	exists, _ := b.client.Exists(ctx, visibilityKey(jobID)).Result()
	if exists != 0 {
		t.Error("visibility key should be deleted")
	}
}

func TestLuaCancel_NotFound(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	result, err := cancelScript.Run(ctx, b.client, nil,
		"nonexistent-cancel-id", time.Now().UTC().Format(time.RFC3339),
	).Result()
	if err != nil {
		t.Fatalf("cancel script: %v", err)
	}

	status, _ := parseLuaResult(result)
	if status != luaStatusNotFound {
		t.Errorf("expected status %d, got %d", luaStatusNotFound, status)
	}
}

func TestLuaCancel_TerminalStateConflict(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	for _, terminalState := range []string{"completed", "discarded", "cancelled"} {
		t.Run(terminalState, func(t *testing.T) {
			jobID := "cancel-terminal-" + terminalState
			b.client.HSet(ctx, jobKey(jobID), map[string]interface{}{
				"state": terminalState,
				"queue": "cancel-terminal-q",
			})

			result, err := cancelScript.Run(ctx, b.client, nil,
				jobID, time.Now().UTC().Format(time.RFC3339),
			).Result()
			if err != nil {
				t.Fatalf("cancel script: %v", err)
			}

			status, data := parseLuaResult(result)
			if status != luaStatusConflict {
				t.Errorf("expected status %d, got %d", luaStatusConflict, status)
			}
			if len(data) < 1 || data[0].(string) != terminalState {
				t.Errorf("expected state %q in conflict data", terminalState)
			}
		})
	}
}

func TestLuaCancel_RemovesFromScheduledAndRetry(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	jobID := "cancel-sched-1"
	queue := "cancel-sched-q"

	b.client.HSet(ctx, jobKey(jobID), map[string]interface{}{
		"state": "scheduled",
		"queue": queue,
	})
	b.client.ZAdd(ctx, scheduledKey(), redisclient.Z{Score: float64(time.Now().UnixMilli()), Member: jobID})
	b.client.ZAdd(ctx, retryKey(), redisclient.Z{Score: float64(time.Now().UnixMilli()), Member: jobID})

	result, err := cancelScript.Run(ctx, b.client, nil,
		jobID, time.Now().UTC().Format(time.RFC3339),
	).Result()
	if err != nil {
		t.Fatalf("cancel script: %v", err)
	}

	status, _ := parseLuaResult(result)
	if status != luaStatusOK {
		t.Fatalf("expected status %d, got %d", luaStatusOK, status)
	}

	// Verify removed from scheduled and retry
	_, err = b.client.ZScore(ctx, scheduledKey(), jobID).Result()
	if err == nil {
		t.Error("job should be removed from scheduled set")
	}
	_, err = b.client.ZScore(ctx, retryKey(), jobID).Result()
	if err == nil {
		t.Error("job should be removed from retry set")
	}
}

// --- unique_check.lua ---

func TestLuaUniqueCheck_Proceed(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	result, err := uniqueCheckScript.Run(ctx, b.client, nil,
		"fp-unique-1",  // ARGV[1] fingerprint
		"new-job-1",    // ARGV[2] new_job_id
		"3600",         // ARGV[3] ttl_seconds
		"reject",       // ARGV[4] conflict_policy
		"",             // ARGV[5] states_json
	).Result()
	if err != nil {
		t.Fatalf("unique_check script: %v", err)
	}

	status, data := parseLuaResult(result)
	if status != luaStatusOK {
		t.Fatalf("expected status %d, got %d", luaStatusOK, status)
	}
	if len(data) < 1 || data[0].(string) != "proceed" {
		t.Errorf("expected action 'proceed', got %v", data)
	}

	// Verify unique key set
	val, _ := b.client.Get(ctx, "ojs:unique:fp-unique-1").Result()
	if val != "new-job-1" {
		t.Errorf("unique key = %q, want %q", val, "new-job-1")
	}
}

func TestLuaUniqueCheck_RejectDuplicate(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	fingerprint := "fp-dup-1"
	existingJobID := "existing-job-1"

	// Setup: existing unique key and active job
	b.client.Set(ctx, "ojs:unique:"+fingerprint, existingJobID, 0)
	b.client.HSet(ctx, jobKey(existingJobID), map[string]interface{}{
		"state": "active",
	})

	result, err := uniqueCheckScript.Run(ctx, b.client, nil,
		fingerprint, "new-job-2", "3600", "reject", "",
	).Result()
	if err != nil {
		t.Fatalf("unique_check script: %v", err)
	}

	status, data := parseLuaResult(result)
	if status != luaStatusOK {
		t.Fatalf("expected status %d, got %d", luaStatusOK, status)
	}
	if len(data) < 1 || data[0].(string) != "reject" {
		t.Errorf("expected action 'reject', got %v", data)
	}
	if len(data) < 2 || data[1].(string) != existingJobID {
		t.Errorf("expected existing_id %q in result", existingJobID)
	}
}

func TestLuaUniqueCheck_IgnoreDuplicate(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	fingerprint := "fp-ignore-1"
	existingJobID := "existing-ignore-1"

	b.client.Set(ctx, "ojs:unique:"+fingerprint, existingJobID, 0)
	b.client.HSet(ctx, jobKey(existingJobID), map[string]interface{}{
		"state": "available",
	})

	result, err := uniqueCheckScript.Run(ctx, b.client, nil,
		fingerprint, "new-job-3", "3600", "ignore", "",
	).Result()
	if err != nil {
		t.Fatalf("unique_check script: %v", err)
	}

	_, data := parseLuaResult(result)
	if len(data) < 1 || data[0].(string) != "ignore" {
		t.Errorf("expected action 'ignore', got %v", data)
	}
	if len(data) < 2 || data[1].(string) != existingJobID {
		t.Errorf("expected existing_id %q in result", existingJobID)
	}
}

func TestLuaUniqueCheck_ReplaceDuplicate(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	fingerprint := "fp-replace-1"
	existingJobID := "existing-replace-1"

	b.client.Set(ctx, "ojs:unique:"+fingerprint, existingJobID, 0)
	b.client.HSet(ctx, jobKey(existingJobID), map[string]interface{}{
		"state": "active",
	})

	result, err := uniqueCheckScript.Run(ctx, b.client, nil,
		fingerprint, "new-job-4", "3600", "replace", "",
	).Result()
	if err != nil {
		t.Fatalf("unique_check script: %v", err)
	}

	_, data := parseLuaResult(result)
	if len(data) < 1 || data[0].(string) != "replace" {
		t.Errorf("expected action 'replace', got %v", data)
	}

	// Verify unique key updated to new job
	val, _ := b.client.Get(ctx, "ojs:unique:"+fingerprint).Result()
	if val != "new-job-4" {
		t.Errorf("unique key = %q, want %q", val, "new-job-4")
	}
}

func TestLuaUniqueCheck_ProceedWhenExistingCompleted(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	fingerprint := "fp-completed-1"
	existingJobID := "existing-completed-1"

	b.client.Set(ctx, "ojs:unique:"+fingerprint, existingJobID, 0)
	b.client.HSet(ctx, jobKey(existingJobID), map[string]interface{}{
		"state": "completed",
	})

	result, err := uniqueCheckScript.Run(ctx, b.client, nil,
		fingerprint, "new-job-5", "3600", "reject", "",
	).Result()
	if err != nil {
		t.Fatalf("unique_check script: %v", err)
	}

	_, data := parseLuaResult(result)
	if len(data) < 1 || data[0].(string) != "proceed" {
		t.Errorf("expected action 'proceed' (terminal state not relevant), got %v", data)
	}
}

func TestLuaUniqueCheck_CustomStatesFilter(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	fingerprint := "fp-states-1"
	existingJobID := "existing-states-1"

	b.client.Set(ctx, "ojs:unique:"+fingerprint, existingJobID, 0)
	b.client.HSet(ctx, jobKey(existingJobID), map[string]interface{}{
		"state": "available",
	})

	// Only consider "active" as relevant — available is not in the filter
	result, err := uniqueCheckScript.Run(ctx, b.client, nil,
		fingerprint, "new-job-6", "3600", "reject", `["active"]`,
	).Result()
	if err != nil {
		t.Fatalf("unique_check script: %v", err)
	}

	_, data := parseLuaResult(result)
	if len(data) < 1 || data[0].(string) != "proceed" {
		t.Errorf("expected 'proceed' (available not in custom states), got %v", data)
	}
}

func TestLuaUniqueCheck_ProceedWhenJobDeleted(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	fingerprint := "fp-deleted-1"

	// Unique key exists but job hash does not
	b.client.Set(ctx, "ojs:unique:"+fingerprint, "deleted-job-1", 0)

	result, err := uniqueCheckScript.Run(ctx, b.client, nil,
		fingerprint, "new-job-7", "3600", "reject", "",
	).Result()
	if err != nil {
		t.Fatalf("unique_check script: %v", err)
	}

	_, data := parseLuaResult(result)
	if len(data) < 1 || data[0].(string) != "proceed" {
		t.Errorf("expected 'proceed' (orphan unique key), got %v", data)
	}
}

// --- advance_workflow.lua ---

func TestLuaAdvanceWorkflow_GroupCompleted(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	wfID := "wf-group-1"
	wfKey := workflowKey(wfID)
	nowFmt := time.Now().UTC().Format(time.RFC3339)

	b.client.HSet(ctx, wfKey, map[string]interface{}{
		"type":      "group",
		"state":     "running",
		"total":     "2",
		"completed": "1",
		"failed":    "0",
	})

	result, err := advanceWorkflowScript.Run(ctx, b.client, nil,
		wfID, "0", "1", `{"result":"ok"}`, nowFmt,
	).Result()
	if err != nil {
		t.Fatalf("advance_workflow script: %v", err)
	}

	status, data := parseLuaResult(result)
	if status != luaStatusOK {
		t.Fatalf("expected status %d, got %d", luaStatusOK, status)
	}
	if len(data) < 1 || data[0].(string) != "completed" {
		t.Errorf("expected action 'completed', got %v", data)
	}

	// Verify workflow state
	state, _ := b.client.HGet(ctx, wfKey, "state").Result()
	if state != "completed" {
		t.Errorf("workflow state = %q, want %q", state, "completed")
	}
}

func TestLuaAdvanceWorkflow_GroupInProgress(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	wfID := "wf-group-ip-1"
	wfKey := workflowKey(wfID)
	nowFmt := time.Now().UTC().Format(time.RFC3339)

	b.client.HSet(ctx, wfKey, map[string]interface{}{
		"type":      "group",
		"state":     "running",
		"total":     "3",
		"completed": "0",
		"failed":    "0",
	})

	result, err := advanceWorkflowScript.Run(ctx, b.client, nil,
		wfID, "0", "0", "", nowFmt,
	).Result()
	if err != nil {
		t.Fatalf("advance_workflow script: %v", err)
	}

	_, data := parseLuaResult(result)
	if len(data) < 1 || data[0].(string) != "in_progress" {
		t.Errorf("expected action 'in_progress', got %v", data)
	}

	// Verify workflow still running
	state, _ := b.client.HGet(ctx, wfKey, "state").Result()
	if state != "running" {
		t.Errorf("workflow state = %q, want %q", state, "running")
	}
}

func TestLuaAdvanceWorkflow_GroupFailed(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	wfID := "wf-group-fail-1"
	wfKey := workflowKey(wfID)
	nowFmt := time.Now().UTC().Format(time.RFC3339)

	b.client.HSet(ctx, wfKey, map[string]interface{}{
		"type":      "group",
		"state":     "running",
		"total":     "2",
		"completed": "0",
		"failed":    "1",
	})

	// Second job also fails
	result, err := advanceWorkflowScript.Run(ctx, b.client, nil,
		wfID, "1", "1", "", nowFmt,
	).Result()
	if err != nil {
		t.Fatalf("advance_workflow script: %v", err)
	}

	_, data := parseLuaResult(result)
	if len(data) < 1 || data[0].(string) != "failed" {
		t.Errorf("expected action 'failed', got %v", data)
	}

	state, _ := b.client.HGet(ctx, wfKey, "state").Result()
	if state != "failed" {
		t.Errorf("workflow state = %q, want %q", state, "failed")
	}
}

func TestLuaAdvanceWorkflow_ChainNext(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	wfID := "wf-chain-1"
	wfKey := workflowKey(wfID)
	nowFmt := time.Now().UTC().Format(time.RFC3339)

	b.client.HSet(ctx, wfKey, map[string]interface{}{
		"type":      "chain",
		"state":     "running",
		"total":     "3",
		"completed": "0",
		"failed":    "0",
	})

	result, err := advanceWorkflowScript.Run(ctx, b.client, nil,
		wfID, "0", "0", `{"step_result":"data"}`, nowFmt,
	).Result()
	if err != nil {
		t.Fatalf("advance_workflow script: %v", err)
	}

	_, data := parseLuaResult(result)
	if len(data) < 1 || data[0].(string) != "chain_next" {
		t.Errorf("expected action 'chain_next', got %v", data)
	}
	if len(data) < 2 || data[1].(string) != "1" {
		t.Errorf("expected next_step_idx '1', got %v", data)
	}

	// Verify result stored
	res, _ := b.client.HGet(ctx, wfKey+":results", "0").Result()
	if res != `{"step_result":"data"}` {
		t.Errorf("stored result = %q, want %q", res, `{"step_result":"data"}`)
	}
}

func TestLuaAdvanceWorkflow_ChainFailed(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	wfID := "wf-chain-fail-1"
	wfKey := workflowKey(wfID)
	nowFmt := time.Now().UTC().Format(time.RFC3339)

	b.client.HSet(ctx, wfKey, map[string]interface{}{
		"type":      "chain",
		"state":     "running",
		"total":     "3",
		"completed": "0",
		"failed":    "0",
	})

	result, err := advanceWorkflowScript.Run(ctx, b.client, nil,
		wfID, "1", "0", "", nowFmt,
	).Result()
	if err != nil {
		t.Fatalf("advance_workflow script: %v", err)
	}

	_, data := parseLuaResult(result)
	if len(data) < 1 || data[0].(string) != "chain_failed" {
		t.Errorf("expected action 'chain_failed', got %v", data)
	}

	state, _ := b.client.HGet(ctx, wfKey, "state").Result()
	if state != "failed" {
		t.Errorf("workflow state = %q, want %q", state, "failed")
	}
}

func TestLuaAdvanceWorkflow_ChainCompleted(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	wfID := "wf-chain-done-1"
	wfKey := workflowKey(wfID)
	nowFmt := time.Now().UTC().Format(time.RFC3339)

	b.client.HSet(ctx, wfKey, map[string]interface{}{
		"type":      "chain",
		"state":     "running",
		"total":     "2",
		"completed": "1",
		"failed":    "0",
	})

	result, err := advanceWorkflowScript.Run(ctx, b.client, nil,
		wfID, "0", "1", "", nowFmt,
	).Result()
	if err != nil {
		t.Fatalf("advance_workflow script: %v", err)
	}

	_, data := parseLuaResult(result)
	if len(data) < 1 || data[0].(string) != "completed" {
		t.Errorf("expected action 'completed', got %v", data)
	}

	state, _ := b.client.HGet(ctx, wfKey, "state").Result()
	if state != "completed" {
		t.Errorf("workflow state = %q, want %q", state, "completed")
	}
}

func TestLuaAdvanceWorkflow_BatchCompleted(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	wfID := "wf-batch-done-1"
	wfKey := workflowKey(wfID)
	nowFmt := time.Now().UTC().Format(time.RFC3339)

	b.client.HSet(ctx, wfKey, map[string]interface{}{
		"type":      "batch",
		"state":     "running",
		"total":     "2",
		"completed": "1",
		"failed":    "0",
	})

	result, err := advanceWorkflowScript.Run(ctx, b.client, nil,
		wfID, "0", "1", "", nowFmt,
	).Result()
	if err != nil {
		t.Fatalf("advance_workflow script: %v", err)
	}

	_, data := parseLuaResult(result)
	if len(data) < 1 || data[0].(string) != "batch_completed" {
		t.Errorf("expected action 'batch_completed', got %v", data)
	}
	if len(data) < 2 || data[1].(string) != "0" {
		t.Errorf("expected failed_count '0', got %v", data)
	}
}

func TestLuaAdvanceWorkflow_BatchFailed(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	wfID := "wf-batch-fail-1"
	wfKey := workflowKey(wfID)
	nowFmt := time.Now().UTC().Format(time.RFC3339)

	b.client.HSet(ctx, wfKey, map[string]interface{}{
		"type":      "batch",
		"state":     "running",
		"total":     "2",
		"completed": "0",
		"failed":    "1",
	})

	result, err := advanceWorkflowScript.Run(ctx, b.client, nil,
		wfID, "1", "1", "", nowFmt,
	).Result()
	if err != nil {
		t.Fatalf("advance_workflow script: %v", err)
	}

	_, data := parseLuaResult(result)
	if len(data) < 1 || data[0].(string) != "batch_failed" {
		t.Errorf("expected action 'batch_failed', got %v", data)
	}
	if len(data) < 2 || data[1].(string) != "2" {
		t.Errorf("expected failed_count '2', got %v", data)
	}
}

func TestLuaAdvanceWorkflow_NotFound(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	result, err := advanceWorkflowScript.Run(ctx, b.client, nil,
		"nonexistent-wf", "0", "0", "", time.Now().UTC().Format(time.RFC3339),
	).Result()
	if err != nil {
		t.Fatalf("advance_workflow script: %v", err)
	}

	status, _ := parseLuaResult(result)
	if status != luaStatusNotFound {
		t.Errorf("expected status %d, got %d", luaStatusNotFound, status)
	}
}

func TestLuaAdvanceWorkflow_NotRunningState(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	wfID := "wf-notrunning-1"
	wfKey := workflowKey(wfID)

	b.client.HSet(ctx, wfKey, map[string]interface{}{
		"type":      "group",
		"state":     "completed",
		"total":     "2",
		"completed": "2",
		"failed":    "0",
	})

	result, err := advanceWorkflowScript.Run(ctx, b.client, nil,
		wfID, "0", "0", "", time.Now().UTC().Format(time.RFC3339),
	).Result()
	if err != nil {
		t.Fatalf("advance_workflow script: %v", err)
	}

	_, data := parseLuaResult(result)
	if len(data) < 1 || data[0].(string) != "in_progress" {
		t.Errorf("expected action 'in_progress' for non-running wf, got %v", data)
	}
}

// --- promote.lua ---

func TestLuaPromote_ScheduledJobs(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	nowMs := time.Now().UnixMilli()
	nowFmt := time.Now().UTC().Format(time.RFC3339)
	sourceKey := scheduledKey()

	// Setup: two due jobs and one future job
	for i, id := range []string{"promo-1", "promo-2"} {
		b.client.HSet(ctx, jobKey(id), map[string]interface{}{
			"state":    "scheduled",
			"queue":    "promo-q",
			"priority": fmt.Sprintf("%d", i),
		})
		b.client.ZAdd(ctx, sourceKey, redisclient.Z{Score: float64(nowMs-1000), Member: id})
	}
	// Future job (not due yet)
	b.client.HSet(ctx, jobKey("promo-future"), map[string]interface{}{
		"state":    "scheduled",
		"queue":    "promo-q",
		"priority": "0",
	})
	b.client.ZAdd(ctx, sourceKey, redisclient.Z{Score: float64(nowMs+60000), Member: "promo-future"})

	result, err := promoteScript.Run(ctx, b.client, []string{sourceKey},
		fmt.Sprintf("%d", nowMs), nowFmt,
	).Result()
	if err != nil {
		t.Fatalf("promote script: %v", err)
	}

	status, data := parseLuaResult(result)
	if status != luaStatusOK {
		t.Fatalf("expected status %d, got %d", luaStatusOK, status)
	}
	count := data[0].(int64)
	if count != 2 {
		t.Errorf("promoted count = %d, want 2", count)
	}

	// Verify promoted jobs are in available queue
	for _, id := range []string{"promo-1", "promo-2"} {
		state, _ := b.client.HGet(ctx, jobKey(id), "state").Result()
		if state != "available" {
			t.Errorf("job %s state = %q, want %q", id, state, "available")
		}
		_, err := b.client.ZScore(ctx, queueAvailableKey("promo-q"), id).Result()
		if err != nil {
			t.Errorf("job %s not found in available set", id)
		}
	}

	// Verify future job still in scheduled
	_, err = b.client.ZScore(ctx, sourceKey, "promo-future").Result()
	if err != nil {
		t.Error("future job should still be in scheduled set")
	}
}

func TestLuaPromote_OrphanEntry(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	nowMs := time.Now().UnixMilli()
	sourceKey := scheduledKey()

	// Orphan: entry in sorted set but no job hash
	b.client.ZAdd(ctx, sourceKey, redisclient.Z{Score: float64(nowMs-1000), Member: "orphan-1"})

	result, err := promoteScript.Run(ctx, b.client, []string{sourceKey},
		fmt.Sprintf("%d", nowMs), time.Now().UTC().Format(time.RFC3339),
	).Result()
	if err != nil {
		t.Fatalf("promote script: %v", err)
	}

	_, data := parseLuaResult(result)
	count := data[0].(int64)
	if count != 0 {
		t.Errorf("promoted count = %d, want 0 (orphan removed but not counted)", count)
	}

	// Verify orphan removed from source set
	_, err = b.client.ZScore(ctx, sourceKey, "orphan-1").Result()
	if err == nil {
		t.Error("orphan should be removed from source set")
	}
}

func TestLuaPromote_EmptySet(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	nowMs := time.Now().UnixMilli()
	sourceKey := scheduledKey()

	result, err := promoteScript.Run(ctx, b.client, []string{sourceKey},
		fmt.Sprintf("%d", nowMs), time.Now().UTC().Format(time.RFC3339),
	).Result()
	if err != nil {
		t.Fatalf("promote script: %v", err)
	}

	_, data := parseLuaResult(result)
	count := data[0].(int64)
	if count != 0 {
		t.Errorf("promoted count = %d, want 0", count)
	}
}

// --- requeue_stalled.lua ---

func TestLuaRequeueStalled_Success(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	jobID := "stalled-job-1"
	queue := "stalled-q"
	nowMs := time.Now().UnixMilli()
	nowFmt := time.Now().UTC().Format(time.RFC3339)

	// Setup: active job with expired visibility
	b.client.HSet(ctx, jobKey(jobID), map[string]interface{}{
		"state":    "active",
		"queue":    queue,
		"priority": "0",
	})
	b.client.SAdd(ctx, queueActiveKey(queue), jobID)
	// Set visibility deadline in the past
	b.client.Set(ctx, visibilityKey(jobID), fmt.Sprintf("%d", nowMs-5000), 0)

	result, err := requeueStalledScript.Run(ctx, b.client, nil,
		jobID, queue, nowFmt, fmt.Sprintf("%d", nowMs),
	).Result()
	if err != nil {
		t.Fatalf("requeue_stalled script: %v", err)
	}

	status, _ := parseLuaResult(result)
	if status != 0 {
		t.Fatalf("expected status 0 (requeued), got %d", status)
	}

	// Verify state = available
	state, _ := b.client.HGet(ctx, jobKey(jobID), "state").Result()
	if state != "available" {
		t.Errorf("state = %q, want %q", state, "available")
	}

	// Verify removed from active set
	isMember, _ := b.client.SIsMember(ctx, queueActiveKey(queue), jobID).Result()
	if isMember {
		t.Error("job should be removed from active set")
	}

	// Verify added to available queue
	_, err = b.client.ZScore(ctx, queueAvailableKey(queue), jobID).Result()
	if err != nil {
		t.Error("job should be in available set")
	}

	// Verify visibility key deleted
	exists, _ := b.client.Exists(ctx, visibilityKey(jobID)).Result()
	if exists != 0 {
		t.Error("visibility key should be deleted")
	}
}

func TestLuaRequeueStalled_NotStalled(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	jobID := "notstalled-1"
	queue := "notstalled-q"
	nowMs := time.Now().UnixMilli()

	b.client.HSet(ctx, jobKey(jobID), map[string]interface{}{
		"state": "active",
		"queue": queue,
	})
	b.client.SAdd(ctx, queueActiveKey(queue), jobID)
	// Visibility deadline in the future
	b.client.Set(ctx, visibilityKey(jobID), fmt.Sprintf("%d", nowMs+30000), 0)

	result, err := requeueStalledScript.Run(ctx, b.client, nil,
		jobID, queue, time.Now().UTC().Format(time.RFC3339), fmt.Sprintf("%d", nowMs),
	).Result()
	if err != nil {
		t.Fatalf("requeue_stalled script: %v", err)
	}

	status, _ := parseLuaResult(result)
	if status != 1 { // 1 = not stalled
		t.Errorf("expected status 1 (not stalled), got %d", status)
	}

	// Verify still active
	state, _ := b.client.HGet(ctx, jobKey(jobID), "state").Result()
	if state != "active" {
		t.Errorf("state = %q, want %q", state, "active")
	}
}

func TestLuaRequeueStalled_NoVisibilityKey(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	result, err := requeueStalledScript.Run(ctx, b.client, nil,
		"no-vis-job", "no-vis-q", time.Now().UTC().Format(time.RFC3339), "0",
	).Result()
	if err != nil {
		t.Fatalf("requeue_stalled script: %v", err)
	}

	status, _ := parseLuaResult(result)
	if status != 1 { // 1 = no visibility key
		t.Errorf("expected status 1, got %d", status)
	}
}

func TestLuaRequeueStalled_NotActiveState(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	jobID := "stalled-notactive-1"
	nowMs := time.Now().UnixMilli()

	b.client.HSet(ctx, jobKey(jobID), map[string]interface{}{
		"state": "completed",
		"queue": "stalled-na-q",
	})
	// Visibility expired but state is not active
	b.client.Set(ctx, visibilityKey(jobID), fmt.Sprintf("%d", nowMs-5000), 0)

	result, err := requeueStalledScript.Run(ctx, b.client, nil,
		jobID, "stalled-na-q", time.Now().UTC().Format(time.RFC3339), fmt.Sprintf("%d", nowMs),
	).Result()
	if err != nil {
		t.Fatalf("requeue_stalled script: %v", err)
	}

	status, _ := parseLuaResult(result)
	if status != 2 { // 2 = not active
		t.Errorf("expected status 2 (not active), got %d", status)
	}
}

// --- retry_dead_letter.lua ---

func TestLuaRetryDeadLetter_Success(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	jobID := "dl-retry-1"
	queue := "dl-retry-q"
	nowMs := time.Now().UnixMilli()
	enqueuedAt := time.Now().UTC().Format(time.RFC3339)

	// Setup: discarded job in dead letter queue
	b.client.HSet(ctx, jobKey(jobID), map[string]interface{}{
		"state":         "discarded",
		"queue":         queue,
		"priority":      "3",
		"attempt":       "5",
		"error":         `{"message":"fatal"}`,
		"error_history": `[{"message":"fatal"}]`,
		"completed_at":  "2025-01-01T00:00:00Z",
		"retry_delay_ms": "5000",
	})
	b.client.ZAdd(ctx, deadKey(), redisclient.Z{Score: float64(nowMs-1000), Member: jobID})

	result, err := retryDeadLetterScript.Run(ctx, b.client, nil,
		jobID, enqueuedAt, fmt.Sprintf("%d", nowMs),
	).Result()
	if err != nil {
		t.Fatalf("retry_dead_letter script: %v", err)
	}

	status, _ := parseLuaResult(result)
	if status != 0 {
		t.Fatalf("expected status 0, got %d", status)
	}

	// Verify state = available
	state, _ := b.client.HGet(ctx, jobKey(jobID), "state").Result()
	if state != "available" {
		t.Errorf("state = %q, want %q", state, "available")
	}

	// Verify attempt reset to 0
	attempt, _ := b.client.HGet(ctx, jobKey(jobID), "attempt").Result()
	if attempt != "0" {
		t.Errorf("attempt = %q, want %q", attempt, "0")
	}

	// Verify error-related fields cleared
	errorExists, _ := b.client.HExists(ctx, jobKey(jobID), "error").Result()
	if errorExists {
		t.Error("error field should be cleared")
	}
	historyExists, _ := b.client.HExists(ctx, jobKey(jobID), "error_history").Result()
	if historyExists {
		t.Error("error_history field should be cleared")
	}
	completedAtExists, _ := b.client.HExists(ctx, jobKey(jobID), "completed_at").Result()
	if completedAtExists {
		t.Error("completed_at field should be cleared")
	}

	// Verify removed from dead letter queue
	_, err = b.client.ZScore(ctx, deadKey(), jobID).Result()
	if err == nil {
		t.Error("job should be removed from dead letter queue")
	}

	// Verify added to available queue
	_, err = b.client.ZScore(ctx, queueAvailableKey(queue), jobID).Result()
	if err != nil {
		t.Error("job should be in available set")
	}
}

func TestLuaRetryDeadLetter_NotInDLQ(t *testing.T) {
	b := testBackend(t)
	ctx := context.Background()

	result, err := retryDeadLetterScript.Run(ctx, b.client, nil,
		"not-in-dlq", time.Now().UTC().Format(time.RFC3339), "0",
	).Result()
	if err != nil {
		t.Fatalf("retry_dead_letter script: %v", err)
	}

	status, _ := parseLuaResult(result)
	if status != 1 { // 1 = not found in DLQ
		t.Errorf("expected status 1, got %d", status)
	}
}

