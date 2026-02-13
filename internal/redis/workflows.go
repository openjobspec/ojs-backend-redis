package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/openjobspec/ojs-backend-redis/internal/core"
)

// CreateWorkflow creates and starts a workflow.
func (b *RedisBackend) CreateWorkflow(ctx context.Context, req *core.WorkflowRequest) (*core.Workflow, error) {
	now := time.Now()
	wfID := core.NewUUIDv7()

	// Determine job list (chain uses Steps, group/batch uses Jobs)
	jobs := req.Jobs
	if req.Type == "chain" {
		jobs = req.Steps
	}

	total := len(jobs)

	// Build the workflow response
	wf := &core.Workflow{
		ID:        wfID,
		Name:      req.Name,
		Type:      req.Type,
		State:     "running",
		CreatedAt: core.FormatTime(now),
	}

	if req.Type == "chain" {
		wf.StepsTotal = &total
		zero := 0
		wf.StepsCompleted = &zero
	} else {
		wf.JobsTotal = &total
		zero := 0
		wf.JobsCompleted = &zero
	}

	// Store workflow metadata in Redis hash
	wfHash := map[string]any{
		"id":         wfID,
		"type":       req.Type,
		"name":       req.Name,
		"state":      "running",
		"total":      strconv.Itoa(total),
		"completed":  "0",
		"failed":     "0",
		"created_at": core.FormatTime(now),
	}
	if req.Callbacks != nil {
		cbJSON, _ := json.Marshal(req.Callbacks)
		wfHash["callbacks"] = string(cbJSON)
	}

	// Store workflow job definitions for chain (needed to enqueue later steps)
	jobDefs, _ := json.Marshal(jobs)
	wfHash["job_defs"] = string(jobDefs)

	if err := b.client.HSet(ctx, workflowKey(wfID), wfHash).Err(); err != nil {
		return nil, fmt.Errorf("store workflow: %w", err)
	}

	if req.Type == "chain" {
		// Chain: only enqueue the first step
		job := workflowJobToJob(jobs[0], wfID, 0)
		created, err := b.Push(ctx, job)
		if err != nil {
			return nil, err
		}
		if err := b.client.RPush(ctx, workflowKey(wfID)+":jobs", created.ID).Err(); err != nil {
			log.Printf("[workflow] error storing job ID for workflow %s: %v", wfID, err)
		}
	} else {
		// Group/Batch: enqueue all jobs immediately
		for i, step := range jobs {
			job := workflowJobToJob(step, wfID, i)
			created, err := b.Push(ctx, job)
			if err != nil {
				return nil, err
			}
			if err := b.client.RPush(ctx, workflowKey(wfID)+":jobs", created.ID).Err(); err != nil {
				log.Printf("[workflow] error storing job ID for workflow %s: %v", wfID, err)
			}
		}
	}

	return wf, nil
}

// GetWorkflow retrieves a workflow by ID.
func (b *RedisBackend) GetWorkflow(ctx context.Context, id string) (*core.Workflow, error) {
	data, err := b.client.HGetAll(ctx, workflowKey(id)).Result()
	if err != nil || len(data) == 0 {
		return nil, core.NewNotFoundError("Workflow", id)
	}

	wf := &core.Workflow{
		ID:        data["id"],
		Name:      data["name"],
		Type:      data["type"],
		State:     data["state"],
		CreatedAt: data["created_at"],
	}
	if v, ok := data["completed_at"]; ok && v != "" {
		wf.CompletedAt = v
	}

	total, _ := strconv.Atoi(data["total"])
	completed, _ := strconv.Atoi(data["completed"])

	if wf.Type == "chain" {
		wf.StepsTotal = &total
		wf.StepsCompleted = &completed
	} else {
		wf.JobsTotal = &total
		wf.JobsCompleted = &completed
	}

	return wf, nil
}

// CancelWorkflow cancels a workflow and its active/pending jobs.
func (b *RedisBackend) CancelWorkflow(ctx context.Context, id string) (*core.Workflow, error) {
	wf, err := b.GetWorkflow(ctx, id)
	if err != nil {
		return nil, err
	}

	if wf.State == "completed" || wf.State == "failed" || wf.State == "cancelled" {
		return nil, core.NewConflictError(
			fmt.Sprintf("Cannot cancel workflow in state '%s'.", wf.State),
			nil,
		)
	}

	// Cancel all jobs belonging to this workflow
	jobIDs, _ := b.client.LRange(ctx, workflowKey(id)+":jobs", 0, -1).Result()
	for _, jobID := range jobIDs {
		jobState, _ := b.client.HGet(ctx, jobKey(jobID), "state").Result()
		if jobState != "" && !core.IsTerminalState(jobState) {
			if _, err := b.Cancel(ctx, jobID); err != nil {
				log.Printf("[workflow] error cancelling job %s in workflow %s: %v", jobID, id, err)
			}
		}
	}

	wf.State = "cancelled"
	wf.CompletedAt = core.NowFormatted()

	if err := b.client.HSet(ctx, workflowKey(id), map[string]any{
		"state":        "cancelled",
		"completed_at": wf.CompletedAt,
	}).Err(); err != nil {
		log.Printf("[workflow] error updating cancelled workflow %s: %v", id, err)
	}

	return wf, nil
}

// AdvanceWorkflow is called after ACK or NACK to update workflow state.
// Uses an atomic Lua script to prevent race conditions when multiple jobs
// complete simultaneously.
func (b *RedisBackend) AdvanceWorkflow(ctx context.Context, workflowID string, jobID string, result json.RawMessage, failed bool) error {
	// Get the job's workflow step index
	stepStr, _ := b.client.HGet(ctx, jobKey(jobID), "workflow_step").Result()

	failedFlag := "0"
	if failed {
		failedFlag = "1"
	}

	resultStr := ""
	if len(result) > 0 {
		resultStr = string(result)
	}

	res, err := advanceWorkflowScript.Run(ctx, b.client, nil,
		workflowID,
		failedFlag,
		stepStr,
		resultStr,
		core.NowFormatted(),
	).Result()
	if err != nil {
		return fmt.Errorf("advance workflow: %w", err)
	}

	status, data := parseLuaResult(res)
	if status == luaStatusNotFound || len(data) == 0 {
		return nil
	}

	action, _ := data[0].(string)
	switch action {
	case "chain_next":
		nextStepIdx := 0
		if len(data) > 1 {
			nextStepStr, _ := data[1].(string)
			nextStepIdx, _ = strconv.Atoi(nextStepStr)
		}
		wfData, _ := b.client.HGetAll(ctx, workflowKey(workflowID)).Result()
		if len(wfData) > 0 {
			return b.enqueueChainStep(ctx, workflowID, wfData, nextStepIdx)
		}
	case "batch_completed":
		wfData, _ := b.client.HGetAll(ctx, workflowKey(workflowID)).Result()
		if len(wfData) > 0 {
			b.fireBatchCallbacks(ctx, workflowID, wfData, false)
		}
	case "batch_failed":
		wfData, _ := b.client.HGetAll(ctx, workflowKey(workflowID)).Result()
		if len(wfData) > 0 {
			b.fireBatchCallbacks(ctx, workflowID, wfData, true)
		}
	// "in_progress", "chain_failed", "completed", "failed": nothing more to do
	}

	return nil
}

// enqueueChainStep enqueues the next step in a chain workflow.
func (b *RedisBackend) enqueueChainStep(ctx context.Context, workflowID string, wfData map[string]string, stepIdx int) error {
	// Load job definitions
	var jobDefs []core.WorkflowJobRequest
	if err := json.Unmarshal([]byte(wfData["job_defs"]), &jobDefs); err != nil {
		return err
	}

	if stepIdx >= len(jobDefs) {
		return nil
	}

	// Collect parent results from previous steps
	var parentResults []json.RawMessage
	resultsData, _ := b.client.HGetAll(ctx, workflowKey(workflowID)+":results").Result()
	for i := 0; i < stepIdx; i++ {
		if r, ok := resultsData[strconv.Itoa(i)]; ok {
			parentResults = append(parentResults, json.RawMessage(r))
		}
	}

	job := workflowJobToJob(jobDefs[stepIdx], workflowID, stepIdx)
	job.ParentResults = parentResults

	created, err := b.Push(ctx, job)
	if err != nil {
		return err
	}

	if err := b.client.RPush(ctx, workflowKey(workflowID)+":jobs", created.ID).Err(); err != nil {
		log.Printf("[workflow] error storing chain step job ID for workflow %s: %v", workflowID, err)
	}
	return nil
}

// fireBatchCallbacks fires callback jobs based on batch outcome.
func (b *RedisBackend) fireBatchCallbacks(ctx context.Context, workflowID string, wfData map[string]string, hasFailure bool) {
	cbStr, ok := wfData["callbacks"]
	if !ok || cbStr == "" {
		return
	}

	var callbacks core.WorkflowCallbacks
	if err := json.Unmarshal([]byte(cbStr), &callbacks); err != nil {
		log.Printf("[workflow] error parsing callbacks for workflow %s: %v", workflowID, err)
		return
	}

	// on_complete always fires
	if callbacks.OnComplete != nil {
		b.fireCallback(ctx, callbacks.OnComplete)
	}

	// on_success fires only when all jobs succeeded
	if !hasFailure && callbacks.OnSuccess != nil {
		b.fireCallback(ctx, callbacks.OnSuccess)
	}

	// on_failure fires when any job failed
	if hasFailure && callbacks.OnFailure != nil {
		b.fireCallback(ctx, callbacks.OnFailure)
	}
}

// fireCallback creates a job from a workflow callback definition.
func (b *RedisBackend) fireCallback(ctx context.Context, cb *core.WorkflowCallback) {
	queue := "default"
	if cb.Options != nil && cb.Options.Queue != "" {
		queue = cb.Options.Queue
	}
	if _, err := b.Push(ctx, &core.Job{
		Type:  cb.Type,
		Args:  cb.Args,
		Queue: queue,
	}); err != nil {
		log.Printf("[workflow] error firing callback job %s: %v", cb.Type, err)
	}
}

// advanceWorkflow advances a workflow when a job completes or fails.
func (b *RedisBackend) advanceWorkflow(ctx context.Context, jobID, state string, result []byte) error {
	// Check if this job belongs to a workflow
	wfID, _ := b.client.HGet(ctx, jobKey(jobID), "workflow_id").Result()
	if wfID == "" {
		return nil
	}

	failed := state == core.StateDiscarded || state == core.StateCancelled
	return b.AdvanceWorkflow(ctx, wfID, jobID, json.RawMessage(result), failed)
}
