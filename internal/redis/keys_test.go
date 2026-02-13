package redis

import (
	"strings"
	"testing"
)

// TestKeyPrefix verifies the constant used by every key builder.
func TestKeyPrefix(t *testing.T) {
	if keyPrefix != "ojs:" {
		t.Fatalf("expected keyPrefix %q, got %q", "ojs:", keyPrefix)
	}
}

// ---------------------------------------------------------------------------
// Parameterised key builders — each tested with multiple representative inputs
// ---------------------------------------------------------------------------

func TestJobKey(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "uuid-style id", input: "0191b3a0-7e8a-7b3a-8f3e-1234567890ab", want: "ojs:job:0191b3a0-7e8a-7b3a-8f3e-1234567890ab"},
		{name: "simple id", input: "123", want: "ojs:job:123"},
		{name: "empty id", input: "", want: "ojs:job:"},
		{name: "id with special chars", input: "job-test_1.v2", want: "ojs:job:job-test_1.v2"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := jobKey(tt.input)
			if got != tt.want {
				t.Errorf("jobKey(%q) = %q, want %q", tt.input, got, tt.want)
			}
			if !strings.HasPrefix(got, keyPrefix) {
				t.Errorf("jobKey(%q) missing prefix %q", tt.input, keyPrefix)
			}
		})
	}
}

func TestQueueAvailableKey(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "default queue", input: "default", want: "ojs:queue:default:available"},
		{name: "critical queue", input: "critical", want: "ojs:queue:critical:available"},
		{name: "my-queue", input: "my-queue", want: "ojs:queue:my-queue:available"},
		{name: "empty queue name", input: "", want: "ojs:queue::available"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := queueAvailableKey(tt.input)
			if got != tt.want {
				t.Errorf("queueAvailableKey(%q) = %q, want %q", tt.input, got, tt.want)
			}
			if !strings.HasPrefix(got, keyPrefix) {
				t.Errorf("queueAvailableKey(%q) missing prefix %q", tt.input, keyPrefix)
			}
		})
	}
}

func TestQueueActiveKey(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "default queue", input: "default", want: "ojs:queue:default:active"},
		{name: "critical queue", input: "critical", want: "ojs:queue:critical:active"},
		{name: "my-queue", input: "my-queue", want: "ojs:queue:my-queue:active"},
		{name: "empty queue name", input: "", want: "ojs:queue::active"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := queueActiveKey(tt.input)
			if got != tt.want {
				t.Errorf("queueActiveKey(%q) = %q, want %q", tt.input, got, tt.want)
			}
			if !strings.HasPrefix(got, keyPrefix) {
				t.Errorf("queueActiveKey(%q) missing prefix %q", tt.input, keyPrefix)
			}
		})
	}
}

func TestQueuePausedKey(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "default queue", input: "default", want: "ojs:queue:default:paused"},
		{name: "critical queue", input: "critical", want: "ojs:queue:critical:paused"},
		{name: "my-queue", input: "my-queue", want: "ojs:queue:my-queue:paused"},
		{name: "empty queue name", input: "", want: "ojs:queue::paused"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := queuePausedKey(tt.input)
			if got != tt.want {
				t.Errorf("queuePausedKey(%q) = %q, want %q", tt.input, got, tt.want)
			}
			if !strings.HasPrefix(got, keyPrefix) {
				t.Errorf("queuePausedKey(%q) missing prefix %q", tt.input, keyPrefix)
			}
		})
	}
}

func TestQueueCompletedKey(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "default queue", input: "default", want: "ojs:queue:default:completed"},
		{name: "critical queue", input: "critical", want: "ojs:queue:critical:completed"},
		{name: "my-queue", input: "my-queue", want: "ojs:queue:my-queue:completed"},
		{name: "empty queue name", input: "", want: "ojs:queue::completed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := queueCompletedKey(tt.input)
			if got != tt.want {
				t.Errorf("queueCompletedKey(%q) = %q, want %q", tt.input, got, tt.want)
			}
			if !strings.HasPrefix(got, keyPrefix) {
				t.Errorf("queueCompletedKey(%q) missing prefix %q", tt.input, keyPrefix)
			}
		})
	}
}

func TestQueueRateLimitKey(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "default queue", input: "default", want: "ojs:queue:default:ratelimit"},
		{name: "critical queue", input: "critical", want: "ojs:queue:critical:ratelimit"},
		{name: "my-queue", input: "my-queue", want: "ojs:queue:my-queue:ratelimit"},
		{name: "empty queue name", input: "", want: "ojs:queue::ratelimit"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := queueRateLimitKey(tt.input)
			if got != tt.want {
				t.Errorf("queueRateLimitKey(%q) = %q, want %q", tt.input, got, tt.want)
			}
			if !strings.HasPrefix(got, keyPrefix) {
				t.Errorf("queueRateLimitKey(%q) missing prefix %q", tt.input, keyPrefix)
			}
		})
	}
}

func TestQueueRateLimitLastKey(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "default queue", input: "default", want: "ojs:queue:default:ratelimit:last"},
		{name: "critical queue", input: "critical", want: "ojs:queue:critical:ratelimit:last"},
		{name: "my-queue", input: "my-queue", want: "ojs:queue:my-queue:ratelimit:last"},
		{name: "empty queue name", input: "", want: "ojs:queue::ratelimit:last"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := queueRateLimitLastKey(tt.input)
			if got != tt.want {
				t.Errorf("queueRateLimitLastKey(%q) = %q, want %q", tt.input, got, tt.want)
			}
			if !strings.HasPrefix(got, keyPrefix) {
				t.Errorf("queueRateLimitLastKey(%q) missing prefix %q", tt.input, keyPrefix)
			}
		})
	}
}

func TestUniqueKey(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "sha256 fingerprint", input: "abc123def456", want: "ojs:unique:abc123def456"},
		{name: "descriptive fingerprint", input: "email-send-user42", want: "ojs:unique:email-send-user42"},
		{name: "empty fingerprint", input: "", want: "ojs:unique:"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := uniqueKey(tt.input)
			if got != tt.want {
				t.Errorf("uniqueKey(%q) = %q, want %q", tt.input, got, tt.want)
			}
			if !strings.HasPrefix(got, keyPrefix) {
				t.Errorf("uniqueKey(%q) missing prefix %q", tt.input, keyPrefix)
			}
		})
	}
}

func TestCronKey(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "daily-report", input: "daily-report", want: "ojs:cron:daily-report"},
		{name: "cleanup", input: "cleanup", want: "ojs:cron:cleanup"},
		{name: "my-queue", input: "my-queue", want: "ojs:cron:my-queue"},
		{name: "empty name", input: "", want: "ojs:cron:"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := cronKey(tt.input)
			if got != tt.want {
				t.Errorf("cronKey(%q) = %q, want %q", tt.input, got, tt.want)
			}
			if !strings.HasPrefix(got, keyPrefix) {
				t.Errorf("cronKey(%q) missing prefix %q", tt.input, keyPrefix)
			}
		})
	}
}

func TestCronInstanceKey(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "daily-report", input: "daily-report", want: "ojs:cron:daily-report:instance"},
		{name: "cleanup", input: "cleanup", want: "ojs:cron:cleanup:instance"},
		{name: "my-queue", input: "my-queue", want: "ojs:cron:my-queue:instance"},
		{name: "empty name", input: "", want: "ojs:cron::instance"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := cronInstanceKey(tt.input)
			if got != tt.want {
				t.Errorf("cronInstanceKey(%q) = %q, want %q", tt.input, got, tt.want)
			}
			if !strings.HasPrefix(got, keyPrefix) {
				t.Errorf("cronInstanceKey(%q) missing prefix %q", tt.input, keyPrefix)
			}
		})
	}
}

func TestWorkflowKey(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "uuid-style id", input: "0191b3a0-aaaa-bbbb-cccc-dddddddddddd", want: "ojs:workflow:0191b3a0-aaaa-bbbb-cccc-dddddddddddd"},
		{name: "simple id", input: "wf-1", want: "ojs:workflow:wf-1"},
		{name: "empty id", input: "", want: "ojs:workflow:"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := workflowKey(tt.input)
			if got != tt.want {
				t.Errorf("workflowKey(%q) = %q, want %q", tt.input, got, tt.want)
			}
			if !strings.HasPrefix(got, keyPrefix) {
				t.Errorf("workflowKey(%q) missing prefix %q", tt.input, keyPrefix)
			}
		})
	}
}

func TestWorkerKey(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "uuid-style id", input: "0191b3a0-1111-2222-3333-444444444444", want: "ojs:worker:0191b3a0-1111-2222-3333-444444444444"},
		{name: "hostname-style id", input: "worker-node-3", want: "ojs:worker:worker-node-3"},
		{name: "empty id", input: "", want: "ojs:worker:"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := workerKey(tt.input)
			if got != tt.want {
				t.Errorf("workerKey(%q) = %q, want %q", tt.input, got, tt.want)
			}
			if !strings.HasPrefix(got, keyPrefix) {
				t.Errorf("workerKey(%q) missing prefix %q", tt.input, keyPrefix)
			}
		})
	}
}

func TestVisibilityKey(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "uuid-style job id", input: "0191b3a0-7e8a-7b3a-8f3e-1234567890ab", want: "ojs:visibility:0191b3a0-7e8a-7b3a-8f3e-1234567890ab"},
		{name: "simple job id", input: "job-99", want: "ojs:visibility:job-99"},
		{name: "empty job id", input: "", want: "ojs:visibility:"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := visibilityKey(tt.input)
			if got != tt.want {
				t.Errorf("visibilityKey(%q) = %q, want %q", tt.input, got, tt.want)
			}
			if !strings.HasPrefix(got, keyPrefix) {
				t.Errorf("visibilityKey(%q) missing prefix %q", tt.input, keyPrefix)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Constant key builders (no parameters) — verify exact output
// ---------------------------------------------------------------------------

func TestConstantKeys(t *testing.T) {
	tests := []struct {
		name string
		fn   func() string
		want string
	}{
		{name: "queuesKey", fn: queuesKey, want: "ojs:queues"},
		{name: "scheduledKey", fn: scheduledKey, want: "ojs:scheduled"},
		{name: "retryKey", fn: retryKey, want: "ojs:retry"},
		{name: "deadKey", fn: deadKey, want: "ojs:dead"},
		{name: "cronNamesKey", fn: cronNamesKey, want: "ojs:cron:names"},
		{name: "workersKey", fn: workersKey, want: "ojs:workers"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.fn()
			if got != tt.want {
				t.Errorf("%s() = %q, want %q", tt.name, got, tt.want)
			}
			if !strings.HasPrefix(got, keyPrefix) {
				t.Errorf("%s() missing prefix %q", tt.name, keyPrefix)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Cross-cutting: verify no two distinct builders produce the same key
// ---------------------------------------------------------------------------

func TestKeyUniqueness(t *testing.T) {
	// Use a representative input to generate all keys and ensure no collisions.
	input := "default"
	keys := map[string]string{
		"jobKey":                jobKey(input),
		"queueAvailableKey":    queueAvailableKey(input),
		"queueActiveKey":       queueActiveKey(input),
		"queuePausedKey":       queuePausedKey(input),
		"queueCompletedKey":    queueCompletedKey(input),
		"queueRateLimitKey":    queueRateLimitKey(input),
		"queueRateLimitLastKey": queueRateLimitLastKey(input),
		"uniqueKey":            uniqueKey(input),
		"cronKey":              cronKey(input),
		"cronInstanceKey":      cronInstanceKey(input),
		"workflowKey":          workflowKey(input),
		"workerKey":            workerKey(input),
		"visibilityKey":        visibilityKey(input),
		"queuesKey":            queuesKey(),
		"scheduledKey":         scheduledKey(),
		"retryKey":             retryKey(),
		"deadKey":              deadKey(),
		"cronNamesKey":         cronNamesKey(),
		"workersKey":           workersKey(),
	}

	seen := make(map[string]string) // value -> builder name
	for builderName, key := range keys {
		if prev, exists := seen[key]; exists {
			t.Errorf("collision: %s and %s both produce %q", prev, builderName, key)
		}
		seen[key] = builderName
	}
}
