package redis

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"testing"
	"time"

	"github.com/openjobspec/ojs-backend-redis/internal/core"
)

// intPtr is a helper to create an *int from a literal.
func intPtr(v int) *int { return &v }

// --- computeScore tests ---

func TestComputeScore(t *testing.T) {
	refTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	refTimeMs := float64(refTime.UnixMilli())

	tests := []struct {
		name     string
		priority *int
		time     time.Time
		want     float64
	}{
		{
			name:     "nil priority defaults to 0",
			priority: nil,
			time:     refTime,
			want:     float64(100)*PriorityScoreMultiplier + refTimeMs,
		},
		{
			name:     "explicit priority 0 same as nil",
			priority: intPtr(0),
			time:     refTime,
			want:     float64(100)*PriorityScoreMultiplier + refTimeMs,
		},
		{
			name:     "priority 1 produces lower score than priority 0",
			priority: intPtr(1),
			time:     refTime,
			want:     float64(99)*PriorityScoreMultiplier + refTimeMs,
		},
		{
			name:     "priority 100 produces lowest possible score band",
			priority: intPtr(100),
			time:     refTime,
			want:     float64(0)*PriorityScoreMultiplier + refTimeMs,
		},
		{
			name:     "priority -100 produces highest possible score band",
			priority: intPtr(-100),
			time:     refTime,
			want:     float64(200)*PriorityScoreMultiplier + refTimeMs,
		},
		{
			name:     "priority 50",
			priority: intPtr(50),
			time:     refTime,
			want:     float64(50)*PriorityScoreMultiplier + refTimeMs,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := computeScore(tt.priority, tt.time)
			if got != tt.want {
				t.Errorf("computeScore() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestComputeScoreOrdering(t *testing.T) {
	refTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	t.Run("higher priority yields lower score (fetched first)", func(t *testing.T) {
		scoreLow := computeScore(intPtr(1), refTime)
		scoreHigh := computeScore(intPtr(10), refTime)
		if scoreHigh >= scoreLow {
			t.Errorf("priority 10 score (%v) should be < priority 1 score (%v)", scoreHigh, scoreLow)
		}
	})

	t.Run("same priority FIFO by enqueue time", func(t *testing.T) {
		t1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
		t2 := time.Date(2025, 1, 1, 0, 0, 1, 0, time.UTC) // 1 second later
		scoreEarlier := computeScore(intPtr(5), t1)
		scoreLater := computeScore(intPtr(5), t2)
		if scoreEarlier >= scoreLater {
			t.Errorf("earlier enqueue score (%v) should be < later enqueue score (%v)", scoreEarlier, scoreLater)
		}
	})

	t.Run("priority band separation is large enough to prevent time overlap", func(t *testing.T) {
		// Even with a very late time at lower priority, a higher priority job at an earlier time
		// should still have a lower score.
		earlyTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
		lateTime := time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
		highPriorityLateTime := computeScore(intPtr(10), lateTime)
		lowPriorityEarlyTime := computeScore(intPtr(9), earlyTime)
		if highPriorityLateTime >= lowPriorityEarlyTime {
			t.Errorf("higher priority (10) even with late time (%v) should score lower than priority 9 early time (%v)",
				highPriorityLateTime, lowPriorityEarlyTime)
		}
	})
}

// --- computeScoreFromHash tests ---

func TestComputeScoreFromHash(t *testing.T) {
	now := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	nowMs := float64(now.UnixMilli())

	tests := []struct {
		name string
		data map[string]string
		want float64
	}{
		{
			name: "priority present in hash",
			data: map[string]string{"priority": "5"},
			want: float64(95)*PriorityScoreMultiplier + nowMs,
		},
		{
			name: "priority zero in hash",
			data: map[string]string{"priority": "0"},
			want: float64(100)*PriorityScoreMultiplier + nowMs,
		},
		{
			name: "negative priority in hash",
			data: map[string]string{"priority": "-50"},
			want: float64(150)*PriorityScoreMultiplier + nowMs,
		},
		{
			name: "priority key missing from hash",
			data: map[string]string{"queue": "default"},
			want: float64(100)*PriorityScoreMultiplier + nowMs,
		},
		{
			name: "priority key is empty string",
			data: map[string]string{"priority": ""},
			want: float64(100)*PriorityScoreMultiplier + nowMs,
		},
		{
			name: "empty hash",
			data: map[string]string{},
			want: float64(100)*PriorityScoreMultiplier + nowMs,
		},
		{
			name: "non-numeric priority falls back to 0 via Atoi",
			data: map[string]string{"priority": "abc"},
			want: float64(100)*PriorityScoreMultiplier + nowMs,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := computeScoreFromHash(tt.data, now)
			if got != tt.want {
				t.Errorf("computeScoreFromHash() = %v, want %v", got, tt.want)
			}
		})
	}
}

// --- computeFingerprint tests ---

// expectedFingerprint computes the expected SHA256 fingerprint given sorted keys and the job fields.
func expectedFingerprint(job *core.Job, keys []string) string {
	h := sha256.New()
	sorted := make([]string, len(keys))
	copy(sorted, keys)
	sort.Strings(sorted)
	for _, key := range sorted {
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

func TestComputeFingerprint(t *testing.T) {
	tests := []struct {
		name       string
		job        *core.Job
		wantKeys   []string // keys used to compute expected fingerprint
		useDefault bool     // if true, wantKeys = {"type", "args"}
	}{
		{
			name: "default keys when Unique.Keys is empty",
			job: &core.Job{
				Type:   "email.send",
				Args:   json.RawMessage(`["hello"]`),
				Unique: &core.UniquePolicy{Keys: []string{}},
			},
			useDefault: true,
		},
		{
			name: "default keys when Unique.Keys is nil",
			job: &core.Job{
				Type:   "email.send",
				Args:   json.RawMessage(`["hello"]`),
				Unique: &core.UniquePolicy{Keys: nil},
			},
			useDefault: true,
		},
		{
			name: "custom keys type and queue",
			job: &core.Job{
				Type:   "email.send",
				Args:   json.RawMessage(`["hello"]`),
				Queue:  "critical",
				Unique: &core.UniquePolicy{Keys: []string{"type", "queue"}},
			},
			wantKeys: []string{"type", "queue"},
		},
		{
			name: "custom keys queue only",
			job: &core.Job{
				Type:   "email.send",
				Args:   json.RawMessage(`["hello"]`),
				Queue:  "low",
				Unique: &core.UniquePolicy{Keys: []string{"queue"}},
			},
			wantKeys: []string{"queue"},
		},
		{
			name: "nil args",
			job: &core.Job{
				Type:   "email.send",
				Unique: &core.UniquePolicy{Keys: []string{}},
			},
			useDefault: true,
		},
		{
			name: "all three keys",
			job: &core.Job{
				Type:   "report.generate",
				Args:   json.RawMessage(`{"id":1}`),
				Queue:  "reports",
				Unique: &core.UniquePolicy{Keys: []string{"args", "type", "queue"}},
			},
			wantKeys: []string{"args", "type", "queue"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := computeFingerprint(tt.job)
			keys := tt.wantKeys
			if tt.useDefault {
				keys = []string{"type", "args"}
			}
			want := expectedFingerprint(tt.job, keys)
			if got != want {
				t.Errorf("computeFingerprint() = %s, want %s", got, want)
			}
		})
	}
}

func TestComputeFingerprintDeterminism(t *testing.T) {
	job := &core.Job{
		Type:   "email.send",
		Args:   json.RawMessage(`[1,2,3]`),
		Unique: &core.UniquePolicy{Keys: []string{"type", "args"}},
	}
	first := computeFingerprint(job)
	for i := 0; i < 100; i++ {
		got := computeFingerprint(job)
		if got != first {
			t.Fatalf("fingerprint not deterministic: iteration %d got %s, want %s", i, got, first)
		}
	}
}

func TestComputeFingerprintSameTypeArgsProduceSameFingerprint(t *testing.T) {
	job1 := &core.Job{
		Type:   "email.send",
		Args:   json.RawMessage(`["user@example.com"]`),
		Unique: &core.UniquePolicy{Keys: []string{}},
	}
	job2 := &core.Job{
		Type:   "email.send",
		Args:   json.RawMessage(`["user@example.com"]`),
		Queue:  "different-queue",
		Unique: &core.UniquePolicy{Keys: []string{}},
	}
	fp1 := computeFingerprint(job1)
	fp2 := computeFingerprint(job2)
	if fp1 != fp2 {
		t.Errorf("same type+args with default keys should produce same fingerprint: %s != %s", fp1, fp2)
	}
}

func TestComputeFingerprintDifferentTypeProducesDifferentFingerprint(t *testing.T) {
	job1 := &core.Job{
		Type:   "email.send",
		Args:   json.RawMessage(`["hello"]`),
		Unique: &core.UniquePolicy{Keys: []string{}},
	}
	job2 := &core.Job{
		Type:   "sms.send",
		Args:   json.RawMessage(`["hello"]`),
		Unique: &core.UniquePolicy{Keys: []string{}},
	}
	fp1 := computeFingerprint(job1)
	fp2 := computeFingerprint(job2)
	if fp1 == fp2 {
		t.Errorf("different type should produce different fingerprint, both got: %s", fp1)
	}
}

func TestComputeFingerprintKeysSorted(t *testing.T) {
	// Keys should be sorted before hashing, so order in the slice should not matter.
	job1 := &core.Job{
		Type:   "report.generate",
		Args:   json.RawMessage(`{"x":1}`),
		Queue:  "reports",
		Unique: &core.UniquePolicy{Keys: []string{"queue", "type", "args"}},
	}
	job2 := &core.Job{
		Type:   "report.generate",
		Args:   json.RawMessage(`{"x":1}`),
		Queue:  "reports",
		Unique: &core.UniquePolicy{Keys: []string{"args", "queue", "type"}},
	}
	job3 := &core.Job{
		Type:   "report.generate",
		Args:   json.RawMessage(`{"x":1}`),
		Queue:  "reports",
		Unique: &core.UniquePolicy{Keys: []string{"type", "args", "queue"}},
	}
	fp1 := computeFingerprint(job1)
	fp2 := computeFingerprint(job2)
	fp3 := computeFingerprint(job3)
	if fp1 != fp2 || fp2 != fp3 {
		t.Errorf("key order should not affect fingerprint: %s, %s, %s", fp1, fp2, fp3)
	}
}

func TestComputeFingerprintIsValidSHA256Hex(t *testing.T) {
	job := &core.Job{
		Type:   "test",
		Args:   json.RawMessage(`[]`),
		Unique: &core.UniquePolicy{Keys: []string{}},
	}
	fp := computeFingerprint(job)
	// SHA256 hex digest is 64 characters
	if len(fp) != 64 {
		t.Errorf("fingerprint length = %d, want 64", len(fp))
	}
	for _, c := range fp {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			t.Errorf("fingerprint contains non-hex character: %c", c)
		}
	}
}

// --- matchesPattern tests ---

func TestMatchesPattern(t *testing.T) {
	tests := []struct {
		name    string
		s       string
		pattern string
		want    bool
	}{
		{
			name:    "exact match",
			s:       "foo",
			pattern: "foo",
			want:    true,
		},
		{
			name:    "exact match failure",
			s:       "bar",
			pattern: "foo",
			want:    false,
		},
		{
			name:    "regex wildcard matches",
			s:       "foobar",
			pattern: "foo.*",
			want:    true,
		},
		{
			name:    "regex wildcard does not match",
			s:       "bazbar",
			pattern: "foo.*",
			want:    false,
		},
		{
			name:    "regex character class",
			s:       "test123",
			pattern: "test[0-9]+",
			want:    true,
		},
		{
			name:    "regex character class no match",
			s:       "testabc",
			pattern: "test[0-9]+",
			want:    false,
		},
		{
			name:    "regex alternation",
			s:       "cat",
			pattern: "cat|dog",
			want:    true,
		},
		{
			name:    "regex alternation second option",
			s:       "dog",
			pattern: "cat|dog",
			want:    true,
		},
		{
			name:    "regex alternation no match",
			s:       "bird",
			pattern: "cat|dog",
			want:    false,
		},
		{
			name:    "pattern must match entire string not partial",
			s:       "foobar",
			pattern: "foo",
			want:    false,
		},
		{
			name:    "empty pattern matches empty string",
			s:       "",
			pattern: "",
			want:    true,
		},
		{
			name:    "empty pattern does not match non-empty string",
			s:       "foo",
			pattern: "",
			want:    false,
		},
		{
			name:    "dot star matches anything",
			s:       "anything-at-all",
			pattern: ".*",
			want:    true,
		},
		{
			name:    "invalid regex falls back to exact match - match",
			s:       "[invalid",
			pattern: "[invalid",
			want:    true,
		},
		{
			name:    "invalid regex falls back to exact match - no match",
			s:       "something",
			pattern: "[broken",
			want:    false,
		},
		{
			name:    "queue name pattern with dot",
			s:       "emails.critical",
			pattern: "emails\\..*",
			want:    true,
		},
		{
			name:    "queue name pattern with dot no match",
			s:       "emailsXcritical",
			pattern: "emails\\..*",
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := matchesPattern(tt.s, tt.pattern)
			if got != tt.want {
				t.Errorf("matchesPattern(%q, %q) = %v, want %v", tt.s, tt.pattern, got, tt.want)
			}
		})
	}
}

func TestMatchesPatternCaching(t *testing.T) {
	// Call twice with the same pattern to exercise the cache path.
	pattern := "cached_test_[0-9]+"
	s := "cached_test_42"

	result1 := matchesPattern(s, pattern)
	result2 := matchesPattern(s, pattern)

	if result1 != result2 {
		t.Errorf("cached result differs: first=%v, second=%v", result1, result2)
	}
	if !result1 {
		t.Errorf("expected match for %q against %q", s, pattern)
	}
}

func TestMatchesPatternCacheBounded(t *testing.T) {
	// Fill the cache past the max size to trigger eviction.
	// Use unique patterns that won't collide with other tests.
	for i := 0; i < regexCacheMaxSize+50; i++ {
		pattern := fmt.Sprintf("bounded_test_%d_[a-z]+", i)
		matchesPattern(fmt.Sprintf("bounded_test_%d_abc", i), pattern)
	}

	regexCacheMu.Lock()
	size := len(regexCache)
	regexCacheMu.Unlock()

	if size > regexCacheMaxSize {
		t.Errorf("cache size %d exceeds max %d after eviction", size, regexCacheMaxSize)
	}
}

func TestMatchesPatternEvictionPreservesRecent(t *testing.T) {
	// Access a pattern, then fill the cache past max. The recently accessed
	// pattern should survive eviction and still return correct results.
	recentPattern := "eviction_recent_[0-9]+"
	matchesPattern("eviction_recent_99", recentPattern)

	// Fill with other patterns to trigger eviction
	for i := 0; i < regexCacheMaxSize+10; i++ {
		matchesPattern(fmt.Sprintf("evict_fill_%d_x", i), fmt.Sprintf("evict_fill_%d_.*", i))
	}

	// Re-access the recent pattern — should still work correctly
	if !matchesPattern("eviction_recent_123", recentPattern) {
		t.Error("expected recent pattern to still work after eviction")
	}
}

// --- workflowJobToJob tests ---

func TestWorkflowJobToJob(t *testing.T) {
	tests := []struct {
		name       string
		step       core.WorkflowJobRequest
		workflowID string
		stepIdx    int
		wantQueue  string
		wantType   string
		wantArgs   json.RawMessage
		wantRetry  *core.RetryPolicy
		wantMaxAtt *int
	}{
		{
			name: "default queue when options is nil",
			step: core.WorkflowJobRequest{
				Type: "email.send",
				Args: json.RawMessage(`["hello"]`),
			},
			workflowID: "wf-1",
			stepIdx:    0,
			wantQueue:  "default",
			wantType:   "email.send",
			wantArgs:   json.RawMessage(`["hello"]`),
			wantRetry:  nil,
			wantMaxAtt: nil,
		},
		{
			name: "default queue when options.Queue is empty",
			step: core.WorkflowJobRequest{
				Type:    "email.send",
				Args:    json.RawMessage(`["hello"]`),
				Options: &core.EnqueueOptions{},
			},
			workflowID: "wf-2",
			stepIdx:    1,
			wantQueue:  "default",
			wantType:   "email.send",
			wantArgs:   json.RawMessage(`["hello"]`),
			wantRetry:  nil,
			wantMaxAtt: nil,
		},
		{
			name: "custom queue from options",
			step: core.WorkflowJobRequest{
				Type: "report.generate",
				Args: json.RawMessage(`{"id":42}`),
				Options: &core.EnqueueOptions{
					Queue: "critical",
				},
			},
			workflowID: "wf-3",
			stepIdx:    2,
			wantQueue:  "critical",
			wantType:   "report.generate",
			wantArgs:   json.RawMessage(`{"id":42}`),
			wantRetry:  nil,
			wantMaxAtt: nil,
		},
		{
			name: "retry policy from Options.RetryPolicy",
			step: core.WorkflowJobRequest{
				Type: "email.send",
				Args: json.RawMessage(`[]`),
				Options: &core.EnqueueOptions{
					RetryPolicy: &core.RetryPolicy{
						MaxAttempts: 5,
					},
				},
			},
			workflowID: "wf-4",
			stepIdx:    0,
			wantQueue:  "default",
			wantType:   "email.send",
			wantArgs:   json.RawMessage(`[]`),
			wantRetry:  &core.RetryPolicy{MaxAttempts: 5},
			wantMaxAtt: intPtr(5),
		},
		{
			name: "retry policy from Options.Retry (fallback)",
			step: core.WorkflowJobRequest{
				Type: "email.send",
				Args: json.RawMessage(`[]`),
				Options: &core.EnqueueOptions{
					Retry: &core.RetryPolicy{
						MaxAttempts: 3,
					},
				},
			},
			workflowID: "wf-5",
			stepIdx:    1,
			wantQueue:  "default",
			wantType:   "email.send",
			wantArgs:   json.RawMessage(`[]`),
			wantRetry:  &core.RetryPolicy{MaxAttempts: 3},
			wantMaxAtt: intPtr(3),
		},
		{
			name: "RetryPolicy takes precedence over Retry",
			step: core.WorkflowJobRequest{
				Type: "email.send",
				Args: json.RawMessage(`[]`),
				Options: &core.EnqueueOptions{
					RetryPolicy: &core.RetryPolicy{
						MaxAttempts: 10,
					},
					Retry: &core.RetryPolicy{
						MaxAttempts: 2,
					},
				},
			},
			workflowID: "wf-6",
			stepIdx:    0,
			wantQueue:  "default",
			wantType:   "email.send",
			wantArgs:   json.RawMessage(`[]`),
			wantRetry:  &core.RetryPolicy{MaxAttempts: 10},
			wantMaxAtt: intPtr(10),
		},
		{
			name: "nil args are preserved",
			step: core.WorkflowJobRequest{
				Type: "cleanup",
			},
			workflowID: "wf-7",
			stepIdx:    0,
			wantQueue:  "default",
			wantType:   "cleanup",
			wantArgs:   nil,
			wantRetry:  nil,
			wantMaxAtt: nil,
		},
		{
			name: "workflowID and stepIdx are set correctly",
			step: core.WorkflowJobRequest{
				Type: "step.task",
				Args: json.RawMessage(`{}`),
			},
			workflowID: "workflow-abc-123",
			stepIdx:    7,
			wantQueue:  "default",
			wantType:   "step.task",
			wantArgs:   json.RawMessage(`{}`),
			wantRetry:  nil,
			wantMaxAtt: nil,
		},
		{
			name: "retry policy with full options",
			step: core.WorkflowJobRequest{
				Type: "heavy.task",
				Args: json.RawMessage(`[1]`),
				Options: &core.EnqueueOptions{
					Queue: "heavy",
					RetryPolicy: &core.RetryPolicy{
						MaxAttempts:        8,
						InitialInterval:    "5s",
						BackoffCoefficient: 2.0,
						MaxInterval:        "1m",
					},
				},
			},
			workflowID: "wf-8",
			stepIdx:    3,
			wantQueue:  "heavy",
			wantType:   "heavy.task",
			wantArgs:   json.RawMessage(`[1]`),
			wantRetry: &core.RetryPolicy{
				MaxAttempts:        8,
				InitialInterval:    "5s",
				BackoffCoefficient: 2.0,
				MaxInterval:        "1m",
			},
			wantMaxAtt: intPtr(8),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := workflowJobToJob(tt.step, tt.workflowID, tt.stepIdx)

			if got.Type != tt.wantType {
				t.Errorf("Type = %q, want %q", got.Type, tt.wantType)
			}
			if got.Queue != tt.wantQueue {
				t.Errorf("Queue = %q, want %q", got.Queue, tt.wantQueue)
			}
			if got.WorkflowID != tt.workflowID {
				t.Errorf("WorkflowID = %q, want %q", got.WorkflowID, tt.workflowID)
			}
			if got.WorkflowStep != tt.stepIdx {
				t.Errorf("WorkflowStep = %d, want %d", got.WorkflowStep, tt.stepIdx)
			}

			// Compare Args
			if tt.wantArgs == nil {
				if got.Args != nil {
					t.Errorf("Args = %s, want nil", string(got.Args))
				}
			} else {
				if got.Args == nil {
					t.Errorf("Args = nil, want %s", string(tt.wantArgs))
				} else if string(got.Args) != string(tt.wantArgs) {
					t.Errorf("Args = %s, want %s", string(got.Args), string(tt.wantArgs))
				}
			}

			// Compare Retry
			if tt.wantRetry == nil {
				if got.Retry != nil {
					t.Errorf("Retry = %+v, want nil", got.Retry)
				}
			} else {
				if got.Retry == nil {
					t.Errorf("Retry = nil, want %+v", tt.wantRetry)
				} else {
					if got.Retry.MaxAttempts != tt.wantRetry.MaxAttempts {
						t.Errorf("Retry.MaxAttempts = %d, want %d", got.Retry.MaxAttempts, tt.wantRetry.MaxAttempts)
					}
					if got.Retry.InitialInterval != tt.wantRetry.InitialInterval {
						t.Errorf("Retry.InitialInterval = %q, want %q", got.Retry.InitialInterval, tt.wantRetry.InitialInterval)
					}
					if got.Retry.BackoffCoefficient != tt.wantRetry.BackoffCoefficient {
						t.Errorf("Retry.BackoffCoefficient = %f, want %f", got.Retry.BackoffCoefficient, tt.wantRetry.BackoffCoefficient)
					}
					if got.Retry.MaxInterval != tt.wantRetry.MaxInterval {
						t.Errorf("Retry.MaxInterval = %q, want %q", got.Retry.MaxInterval, tt.wantRetry.MaxInterval)
					}
				}
			}

			// Compare MaxAttempts
			if tt.wantMaxAtt == nil {
				if got.MaxAttempts != nil {
					t.Errorf("MaxAttempts = %d, want nil", *got.MaxAttempts)
				}
			} else {
				if got.MaxAttempts == nil {
					t.Errorf("MaxAttempts = nil, want %d", *tt.wantMaxAtt)
				} else if *got.MaxAttempts != *tt.wantMaxAtt {
					t.Errorf("MaxAttempts = %d, want %d", *got.MaxAttempts, *tt.wantMaxAtt)
				}
			}
		})
	}
}

// --- Constants tests ---

func TestConstants(t *testing.T) {
	t.Run("DefaultVisibilityTimeoutMs", func(t *testing.T) {
		if DefaultVisibilityTimeoutMs != 30000 {
			t.Errorf("DefaultVisibilityTimeoutMs = %d, want 30000", DefaultVisibilityTimeoutMs)
		}
	})

	t.Run("PriorityScoreMultiplier", func(t *testing.T) {
		if math.Abs(PriorityScoreMultiplier-1e15) > 0 {
			t.Errorf("PriorityScoreMultiplier = %v, want 1e15", PriorityScoreMultiplier)
		}
	})
}
