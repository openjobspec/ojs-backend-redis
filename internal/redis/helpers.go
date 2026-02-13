package redis

import (
	"crypto/sha256"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/openjobspec/ojs-backend-redis/internal/core"
)

// Constants for magic numbers used across the backend.
const (
	// DefaultVisibilityTimeoutMs is the default visibility timeout in milliseconds.
	DefaultVisibilityTimeoutMs = 30000
	// CronVisibilityTimeoutMs is the visibility timeout for cron-fired jobs (10 min).
	CronVisibilityTimeoutMs = 600000
	// PriorityScoreMultiplier separates priority bands in the sorted set score.
	PriorityScoreMultiplier = 1e15
	// CronLockTTL is the TTL for distributed cron leader election locks.
	CronLockTTL = 60 * time.Second
	// DefaultListLimit is the default page size for list endpoints.
	DefaultListLimit = 50
)

// computeScore creates a composite score for priority ordering.
// Higher priority jobs get lower scores (fetched first).
// Same priority jobs are FIFO by enqueue time.
// Priority range: -100 to 100.
func computeScore(priority *int, enqueueTime time.Time) float64 {
	p := 0
	if priority != nil {
		p = *priority
	}
	return float64(100-p)*PriorityScoreMultiplier + float64(enqueueTime.UnixMilli())
}

// computeScoreFromHash extracts priority from a Redis hash and computes the score.
// This eliminates the duplicated parse-priority-and-compute-score pattern.
func computeScoreFromHash(data map[string]string, now time.Time) float64 {
	if v, ok := data["priority"]; ok && v != "" {
		p, _ := strconv.Atoi(v)
		return computeScore(&p, now)
	}
	return computeScore(nil, now)
}

// computeFingerprint generates a SHA256 fingerprint for unique job deduplication.
func computeFingerprint(job *core.Job) string {
	h := sha256.New()
	keys := job.Unique.Keys
	if len(keys) == 0 {
		keys = []string{"type", "args"}
	}
	sort.Strings(keys)
	for _, key := range keys {
		switch key {
		case "type":
			h.Write([]byte("type:"))
			h.Write([]byte(job.Type))
		case "args":
			h.Write([]byte("args:"))
			if job.Args != nil {
				h.Write(job.Args)
			}
		case "queue":
			h.Write([]byte("queue:"))
			h.Write([]byte(job.Queue))
		}
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

// regexCache caches compiled regular expressions to avoid recompilation per call.
var regexCache sync.Map

// matchesPattern checks if s matches the given pattern (regex or exact match).
func matchesPattern(s, pattern string) bool {
	fullPattern := "^" + pattern + "$"

	// Check cache first
	if cached, ok := regexCache.Load(fullPattern); ok {
		if re, ok := cached.(*regexp.Regexp); ok {
			return re.MatchString(s)
		}
		// Cached nil means the pattern was invalid — fall through to exact match
		return s == pattern
	}

	// Compile and cache
	re, err := regexp.Compile(fullPattern)
	if err != nil {
		regexCache.Store(fullPattern, (*regexp.Regexp)(nil))
		return s == pattern
	}
	regexCache.Store(fullPattern, re)
	return re.MatchString(s)
}

// workflowJobToJob builds a core.Job from a WorkflowJobRequest with retry policy resolution.
func workflowJobToJob(step core.WorkflowJobRequest, workflowID string, stepIdx int) *core.Job {
	queue := "default"
	if step.Options != nil && step.Options.Queue != "" {
		queue = step.Options.Queue
	}

	job := &core.Job{
		Type:         step.Type,
		Args:         step.Args,
		Queue:        queue,
		WorkflowID:   workflowID,
		WorkflowStep: stepIdx,
	}

	if step.Options != nil && step.Options.RetryPolicy != nil {
		job.Retry = step.Options.RetryPolicy
		job.MaxAttempts = &step.Options.RetryPolicy.MaxAttempts
	} else if step.Options != nil && step.Options.Retry != nil {
		job.Retry = step.Options.Retry
		job.MaxAttempts = &step.Options.Retry.MaxAttempts
	}

	return job
}

