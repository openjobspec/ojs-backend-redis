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
	// DefaultVisibilityTimeoutMs re-exports the core default for package-internal use.
	DefaultVisibilityTimeoutMs = core.DefaultVisibilityTimeoutMs
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

// regexCacheMaxSize is the maximum number of entries in the regex cache.
// When exceeded, the oldest half of entries are evicted.
const regexCacheMaxSize = 256

// regexCacheEntry holds a compiled regex (nil if the pattern was invalid).
type regexCacheEntry struct {
	re       *regexp.Regexp
	valid    bool
	accessID uint64
}

// regexCacheMu protects the regexCache map.
var regexCacheMu sync.Mutex

// regexCache maps full anchored patterns to their compiled regex.
var regexCache = make(map[string]*regexCacheEntry, regexCacheMaxSize)

// regexCacheAccessCounter is a monotonically increasing counter for LRU eviction.
var regexCacheAccessCounter uint64

// matchesPattern checks if s matches the given pattern (regex or exact match).
func matchesPattern(s, pattern string) bool {
	fullPattern := "^" + pattern + "$"

	regexCacheMu.Lock()
	entry, ok := regexCache[fullPattern]
	if ok {
		regexCacheAccessCounter++
		entry.accessID = regexCacheAccessCounter
		regexCacheMu.Unlock()
		if entry.valid {
			return entry.re.MatchString(s)
		}
		return s == pattern
	}

	// Compile and cache
	re, err := regexp.Compile(fullPattern)
	regexCacheAccessCounter++
	newEntry := &regexCacheEntry{
		re:       re,
		valid:    err == nil,
		accessID: regexCacheAccessCounter,
	}
	regexCache[fullPattern] = newEntry

	// Evict oldest half if cache is full
	if len(regexCache) > regexCacheMaxSize {
		evictRegexCache()
	}
	regexCacheMu.Unlock()

	if err != nil {
		return s == pattern
	}
	return re.MatchString(s)
}

// evictRegexCache removes the oldest half of entries by accessID.
// Must be called with regexCacheMu held.
func evictRegexCache() {
	// Find the median accessID
	ids := make([]uint64, 0, len(regexCache))
	for _, e := range regexCache {
		ids = append(ids, e.accessID)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	median := ids[len(ids)/2]

	for k, e := range regexCache {
		if e.accessID < median {
			delete(regexCache, k)
		}
	}
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

