package core

import (
	"testing"
	"time"
)

func TestCalculateBackoff_NilPolicy(t *testing.T) {
	// nil policy should use defaults: exponential, 1s initial, coefficient 2.0, jitter enabled.
	// We cannot check exact values because jitter is enabled by default, but we can
	// verify the result falls within the expected jitter range.
	tests := []struct {
		name    string
		attempt int
		baseMin time.Duration // 0.5 * base
		baseMax time.Duration // 1.5 * base
	}{
		{
			name:    "attempt 1 uses default exponential 1s*2^0=1s with jitter",
			attempt: 1,
			baseMin: 500 * time.Millisecond,  // 0.5 * 1s
			baseMax: 1500 * time.Millisecond,  // 1.5 * 1s
		},
		{
			name:    "attempt 2 uses default exponential 1s*2^1=2s with jitter",
			attempt: 2,
			baseMin: 1 * time.Second,          // 0.5 * 2s
			baseMax: 3 * time.Second,           // 1.5 * 2s
		},
		{
			name:    "attempt 3 uses default exponential 1s*2^2=4s with jitter",
			attempt: 3,
			baseMin: 2 * time.Second,          // 0.5 * 4s
			baseMax: 6 * time.Second,           // 1.5 * 4s
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i := 0; i < 100; i++ {
				got := CalculateBackoff(nil, tt.attempt)
				if got < tt.baseMin || got > tt.baseMax {
					t.Fatalf("CalculateBackoff(nil, %d) = %v, want in [%v, %v]",
						tt.attempt, got, tt.baseMin, tt.baseMax)
				}
			}
		})
	}
}

func TestCalculateBackoff_Exponential(t *testing.T) {
	tests := []struct {
		name    string
		policy  *RetryPolicy
		attempt int
		want    time.Duration
	}{
		{
			name: "attempt 1 coefficient 2.0 gives 1s",
			policy: &RetryPolicy{
				InitialInterval:    "PT1S",
				BackoffCoefficient: 2.0,
				BackoffType:        "exponential",
			},
			attempt: 1,
			want:    1 * time.Second, // 1s * 2^0
		},
		{
			name: "attempt 2 coefficient 2.0 gives 2s",
			policy: &RetryPolicy{
				InitialInterval:    "PT1S",
				BackoffCoefficient: 2.0,
				BackoffType:        "exponential",
			},
			attempt: 2,
			want:    2 * time.Second, // 1s * 2^1
		},
		{
			name: "attempt 3 coefficient 2.0 gives 4s",
			policy: &RetryPolicy{
				InitialInterval:    "PT1S",
				BackoffCoefficient: 2.0,
				BackoffType:        "exponential",
			},
			attempt: 3,
			want:    4 * time.Second, // 1s * 2^2
		},
		{
			name: "attempt 4 coefficient 2.0 gives 8s",
			policy: &RetryPolicy{
				InitialInterval:    "PT1S",
				BackoffCoefficient: 2.0,
				BackoffType:        "exponential",
			},
			attempt: 4,
			want:    8 * time.Second, // 1s * 2^3
		},
		{
			name: "attempt 1 coefficient 3.0 gives 1s",
			policy: &RetryPolicy{
				InitialInterval:    "PT1S",
				BackoffCoefficient: 3.0,
				BackoffType:        "exponential",
			},
			attempt: 1,
			want:    1 * time.Second, // 1s * 3^0
		},
		{
			name: "attempt 2 coefficient 3.0 gives 3s",
			policy: &RetryPolicy{
				InitialInterval:    "PT1S",
				BackoffCoefficient: 3.0,
				BackoffType:        "exponential",
			},
			attempt: 2,
			want:    3 * time.Second, // 1s * 3^1
		},
		{
			name: "attempt 3 coefficient 3.0 gives 9s",
			policy: &RetryPolicy{
				InitialInterval:    "PT1S",
				BackoffCoefficient: 3.0,
				BackoffType:        "exponential",
			},
			attempt: 3,
			want:    9 * time.Second, // 1s * 3^2
		},
		{
			name: "attempt 5 coefficient 2.0 gives 16s",
			policy: &RetryPolicy{
				InitialInterval:    "PT1S",
				BackoffCoefficient: 2.0,
				BackoffType:        "exponential",
			},
			attempt: 5,
			want:    16 * time.Second, // 1s * 2^4
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CalculateBackoff(tt.policy, tt.attempt)
			if got != tt.want {
				t.Errorf("CalculateBackoff(%+v, %d) = %v, want %v",
					tt.policy, tt.attempt, got, tt.want)
			}
		})
	}
}

func TestCalculateBackoff_Linear(t *testing.T) {
	tests := []struct {
		name    string
		policy  *RetryPolicy
		attempt int
		want    time.Duration
	}{
		{
			name: "attempt 1 gives 1s",
			policy: &RetryPolicy{
				InitialInterval: "PT1S",
				BackoffType:     "linear",
			},
			attempt: 1,
			want:    1 * time.Second, // 1s * 1
		},
		{
			name: "attempt 2 gives 2s",
			policy: &RetryPolicy{
				InitialInterval: "PT1S",
				BackoffType:     "linear",
			},
			attempt: 2,
			want:    2 * time.Second, // 1s * 2
		},
		{
			name: "attempt 3 gives 3s",
			policy: &RetryPolicy{
				InitialInterval: "PT1S",
				BackoffType:     "linear",
			},
			attempt: 3,
			want:    3 * time.Second, // 1s * 3
		},
		{
			name: "attempt 5 gives 5s",
			policy: &RetryPolicy{
				InitialInterval: "PT1S",
				BackoffType:     "linear",
			},
			attempt: 5,
			want:    5 * time.Second, // 1s * 5
		},
		{
			name: "attempt 3 with 2s initial gives 6s",
			policy: &RetryPolicy{
				InitialInterval: "PT2S",
				BackoffType:     "linear",
			},
			attempt: 3,
			want:    6 * time.Second, // 2s * 3
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CalculateBackoff(tt.policy, tt.attempt)
			if got != tt.want {
				t.Errorf("CalculateBackoff(%+v, %d) = %v, want %v",
					tt.policy, tt.attempt, got, tt.want)
			}
		})
	}
}

func TestCalculateBackoff_Constant(t *testing.T) {
	tests := []struct {
		name    string
		policy  *RetryPolicy
		attempt int
		want    time.Duration
	}{
		{
			name: "attempt 1 gives 1s",
			policy: &RetryPolicy{
				InitialInterval: "PT1S",
				BackoffType:     "constant",
			},
			attempt: 1,
			want:    1 * time.Second,
		},
		{
			name: "attempt 2 gives 1s",
			policy: &RetryPolicy{
				InitialInterval: "PT1S",
				BackoffType:     "constant",
			},
			attempt: 2,
			want:    1 * time.Second,
		},
		{
			name: "attempt 5 gives 1s",
			policy: &RetryPolicy{
				InitialInterval: "PT1S",
				BackoffType:     "constant",
			},
			attempt: 5,
			want:    1 * time.Second,
		},
		{
			name: "attempt 10 gives 1s",
			policy: &RetryPolicy{
				InitialInterval: "PT1S",
				BackoffType:     "constant",
			},
			attempt: 10,
			want:    1 * time.Second,
		},
		{
			name: "attempt 3 with 5s initial gives 5s",
			policy: &RetryPolicy{
				InitialInterval: "PT5S",
				BackoffType:     "constant",
			},
			attempt: 3,
			want:    5 * time.Second,
		},
		{
			name: "attempt 100 with 10s initial gives 10s",
			policy: &RetryPolicy{
				InitialInterval: "PT10S",
				BackoffType:     "constant",
			},
			attempt: 100,
			want:    10 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CalculateBackoff(tt.policy, tt.attempt)
			if got != tt.want {
				t.Errorf("CalculateBackoff(%+v, %d) = %v, want %v",
					tt.policy, tt.attempt, got, tt.want)
			}
		})
	}
}

func TestCalculateBackoff_MaxIntervalCap(t *testing.T) {
	tests := []struct {
		name    string
		policy  *RetryPolicy
		attempt int
		want    time.Duration
	}{
		{
			name: "exponential below cap is not capped",
			policy: &RetryPolicy{
				InitialInterval:    "PT1S",
				BackoffCoefficient: 2.0,
				BackoffType:        "exponential",
				MaxInterval:        "PT10S",
			},
			attempt: 2,
			want:    2 * time.Second, // 1s * 2^1 = 2s, under 10s cap
		},
		{
			name: "exponential at cap is capped",
			policy: &RetryPolicy{
				InitialInterval:    "PT1S",
				BackoffCoefficient: 2.0,
				BackoffType:        "exponential",
				MaxInterval:        "PT4S",
			},
			attempt: 3,
			want:    4 * time.Second, // 1s * 2^2 = 4s, equals 4s cap
		},
		{
			name: "exponential exceeding cap is capped",
			policy: &RetryPolicy{
				InitialInterval:    "PT1S",
				BackoffCoefficient: 2.0,
				BackoffType:        "exponential",
				MaxInterval:        "PT5S",
			},
			attempt: 5,
			want:    5 * time.Second, // 1s * 2^4 = 16s, capped to 5s
		},
		{
			name: "exponential large attempt capped to max interval",
			policy: &RetryPolicy{
				InitialInterval:    "PT1S",
				BackoffCoefficient: 2.0,
				BackoffType:        "exponential",
				MaxInterval:        "PT1M",
			},
			attempt: 10,
			want:    1 * time.Minute, // 1s * 2^9 = 512s, capped to 60s
		},
		{
			name: "linear exceeding cap is capped",
			policy: &RetryPolicy{
				InitialInterval: "PT5S",
				BackoffType:     "linear",
				MaxInterval:     "PT10S",
			},
			attempt: 5,
			want:    10 * time.Second, // 5s * 5 = 25s, capped to 10s
		},
		{
			name: "constant with max interval stays at initial",
			policy: &RetryPolicy{
				InitialInterval: "PT3S",
				BackoffType:     "constant",
				MaxInterval:     "PT10S",
			},
			attempt: 5,
			want:    3 * time.Second, // constant is always 3s, under 10s cap
		},
		{
			name: "max interval of 5 minutes matches default",
			policy: &RetryPolicy{
				InitialInterval:    "PT1S",
				BackoffCoefficient: 2.0,
				BackoffType:        "exponential",
				MaxInterval:        "PT5M",
			},
			attempt: 20,
			want:    5 * time.Minute, // 1s * 2^19 is huge, capped to 5m
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CalculateBackoff(tt.policy, tt.attempt)
			if got != tt.want {
				t.Errorf("CalculateBackoff(%+v, %d) = %v, want %v",
					tt.policy, tt.attempt, got, tt.want)
			}
		})
	}
}

func TestCalculateBackoff_Jitter(t *testing.T) {
	// With jitter enabled, the result should be in [0.5 * base, 1.5 * base).
	// We run many iterations to verify the range is covered and no value falls outside.
	t.Run("jitter range for exponential", func(t *testing.T) {
		policy := &RetryPolicy{
			InitialInterval:    "PT1S",
			BackoffCoefficient: 2.0,
			BackoffType:        "exponential",
			Jitter:             true,
		}
		attempt := 3
		base := 4 * time.Second // 1s * 2^2
		minExpected := time.Duration(float64(base) * 0.5)
		maxExpected := time.Duration(float64(base) * 1.5)

		var sawLow, sawHigh bool
		iterations := 10000

		for i := 0; i < iterations; i++ {
			got := CalculateBackoff(policy, attempt)
			if got < minExpected || got > maxExpected {
				t.Fatalf("iteration %d: CalculateBackoff = %v, want in [%v, %v]",
					i, got, minExpected, maxExpected)
			}
			midpoint := time.Duration(float64(base) * 1.0)
			if got < midpoint {
				sawLow = true
			}
			if got > midpoint {
				sawHigh = true
			}
		}

		if !sawLow {
			t.Error("jitter never produced a value below the base; expected spread in [0.5x, 1.5x)")
		}
		if !sawHigh {
			t.Error("jitter never produced a value above the base; expected spread in [0.5x, 1.5x)")
		}
	})

	t.Run("jitter range for linear", func(t *testing.T) {
		policy := &RetryPolicy{
			InitialInterval: "PT2S",
			BackoffType:     "linear",
			Jitter:          true,
		}
		attempt := 3
		base := 6 * time.Second // 2s * 3
		minExpected := time.Duration(float64(base) * 0.5)
		maxExpected := time.Duration(float64(base) * 1.5)

		for i := 0; i < 1000; i++ {
			got := CalculateBackoff(policy, attempt)
			if got < minExpected || got > maxExpected {
				t.Fatalf("iteration %d: CalculateBackoff = %v, want in [%v, %v]",
					i, got, minExpected, maxExpected)
			}
		}
	})

	t.Run("jitter range for constant", func(t *testing.T) {
		policy := &RetryPolicy{
			InitialInterval: "PT5S",
			BackoffType:     "constant",
			Jitter:          true,
		}
		base := 5 * time.Second
		minExpected := time.Duration(float64(base) * 0.5)
		maxExpected := time.Duration(float64(base) * 1.5)

		for i := 0; i < 1000; i++ {
			got := CalculateBackoff(policy, 1)
			if got < minExpected || got > maxExpected {
				t.Fatalf("iteration %d: CalculateBackoff = %v, want in [%v, %v]",
					i, got, minExpected, maxExpected)
			}
		}
	})

	t.Run("jitter produces different values", func(t *testing.T) {
		policy := &RetryPolicy{
			InitialInterval:    "PT1S",
			BackoffCoefficient: 2.0,
			BackoffType:        "exponential",
			Jitter:             true,
		}
		seen := make(map[time.Duration]bool)
		for i := 0; i < 100; i++ {
			got := CalculateBackoff(policy, 2)
			seen[got] = true
		}
		if len(seen) < 2 {
			t.Errorf("jitter produced only %d unique values in 100 iterations; expected variation", len(seen))
		}
	})
}

func TestCalculateBackoff_NoJitter(t *testing.T) {
	// Without jitter, the result should be deterministic and exactly equal to the computed value.
	tests := []struct {
		name    string
		policy  *RetryPolicy
		attempt int
		want    time.Duration
	}{
		{
			name: "exponential no jitter attempt 1",
			policy: &RetryPolicy{
				InitialInterval:    "PT1S",
				BackoffCoefficient: 2.0,
				BackoffType:        "exponential",
				Jitter:             false,
			},
			attempt: 1,
			want:    1 * time.Second,
		},
		{
			name: "exponential no jitter attempt 3",
			policy: &RetryPolicy{
				InitialInterval:    "PT1S",
				BackoffCoefficient: 2.0,
				BackoffType:        "exponential",
				Jitter:             false,
			},
			attempt: 3,
			want:    4 * time.Second,
		},
		{
			name: "linear no jitter attempt 4",
			policy: &RetryPolicy{
				InitialInterval: "PT1S",
				BackoffType:     "linear",
				Jitter:          false,
			},
			attempt: 4,
			want:    4 * time.Second,
		},
		{
			name: "constant no jitter attempt 7",
			policy: &RetryPolicy{
				InitialInterval: "PT3S",
				BackoffType:     "constant",
				Jitter:          false,
			},
			attempt: 7,
			want:    3 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Run multiple times to confirm determinism.
			for i := 0; i < 50; i++ {
				got := CalculateBackoff(tt.policy, tt.attempt)
				if got != tt.want {
					t.Fatalf("iteration %d: CalculateBackoff(%+v, %d) = %v, want %v",
						i, tt.policy, tt.attempt, got, tt.want)
				}
			}
		})
	}
}

func TestCalculateBackoff_CustomInitialInterval(t *testing.T) {
	tests := []struct {
		name    string
		policy  *RetryPolicy
		attempt int
		want    time.Duration
	}{
		{
			name: "5s initial exponential attempt 1",
			policy: &RetryPolicy{
				InitialInterval:    "PT5S",
				BackoffCoefficient: 2.0,
				BackoffType:        "exponential",
			},
			attempt: 1,
			want:    5 * time.Second, // 5s * 2^0
		},
		{
			name: "5s initial exponential attempt 2",
			policy: &RetryPolicy{
				InitialInterval:    "PT5S",
				BackoffCoefficient: 2.0,
				BackoffType:        "exponential",
			},
			attempt: 2,
			want:    10 * time.Second, // 5s * 2^1
		},
		{
			name: "5s initial exponential attempt 3",
			policy: &RetryPolicy{
				InitialInterval:    "PT5S",
				BackoffCoefficient: 2.0,
				BackoffType:        "exponential",
			},
			attempt: 3,
			want:    20 * time.Second, // 5s * 2^2
		},
		{
			name: "30s initial linear attempt 3",
			policy: &RetryPolicy{
				InitialInterval: "PT30S",
				BackoffType:     "linear",
			},
			attempt: 3,
			want:    90 * time.Second, // 30s * 3
		},
		{
			name: "1m initial constant attempt 5",
			policy: &RetryPolicy{
				InitialInterval: "PT1M",
				BackoffType:     "constant",
			},
			attempt: 5,
			want:    1 * time.Minute, // always 1m
		},
		{
			name: "10s initial exponential attempt 4",
			policy: &RetryPolicy{
				InitialInterval:    "PT10S",
				BackoffCoefficient: 2.0,
				BackoffType:        "exponential",
			},
			attempt: 4,
			want:    80 * time.Second, // 10s * 2^3
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CalculateBackoff(tt.policy, tt.attempt)
			if got != tt.want {
				t.Errorf("CalculateBackoff(%+v, %d) = %v, want %v",
					tt.policy, tt.attempt, got, tt.want)
			}
		})
	}
}

func TestCalculateBackoff_BackoffTypePrecedence(t *testing.T) {
	// BackoffType takes precedence over BackoffStrategy when both are set.
	tests := []struct {
		name    string
		policy  *RetryPolicy
		attempt int
		want    time.Duration
	}{
		{
			name: "BackoffType exponential overrides BackoffStrategy linear",
			policy: &RetryPolicy{
				InitialInterval:    "PT1S",
				BackoffCoefficient: 2.0,
				BackoffType:        "exponential",
				BackoffStrategy:    "linear",
			},
			attempt: 3,
			want:    4 * time.Second, // exponential: 1s * 2^2 = 4s (not linear: 1s * 3 = 3s)
		},
		{
			name: "BackoffType linear overrides BackoffStrategy exponential",
			policy: &RetryPolicy{
				InitialInterval:    "PT1S",
				BackoffCoefficient: 2.0,
				BackoffType:        "linear",
				BackoffStrategy:    "exponential",
			},
			attempt: 3,
			want:    3 * time.Second, // linear: 1s * 3 = 3s (not exponential: 1s * 2^2 = 4s)
		},
		{
			name: "BackoffType constant overrides BackoffStrategy exponential",
			policy: &RetryPolicy{
				InitialInterval:    "PT1S",
				BackoffCoefficient: 2.0,
				BackoffType:        "constant",
				BackoffStrategy:    "exponential",
			},
			attempt: 5,
			want:    1 * time.Second, // constant: always 1s (not exponential: 1s * 2^4 = 16s)
		},
		{
			name: "BackoffStrategy used when BackoffType is empty",
			policy: &RetryPolicy{
				InitialInterval:    "PT1S",
				BackoffCoefficient: 2.0,
				BackoffStrategy:    "linear",
			},
			attempt: 3,
			want:    3 * time.Second, // linear via strategy: 1s * 3 = 3s
		},
		{
			name: "BackoffStrategy constant used when BackoffType is empty",
			policy: &RetryPolicy{
				InitialInterval: "PT1S",
				BackoffStrategy: "constant",
			},
			attempt: 5,
			want:    1 * time.Second, // constant via strategy
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CalculateBackoff(tt.policy, tt.attempt)
			if got != tt.want {
				t.Errorf("CalculateBackoff(%+v, %d) = %v, want %v",
					tt.policy, tt.attempt, got, tt.want)
			}
		})
	}
}

func TestCalculateBackoff_DefaultBackoffType(t *testing.T) {
	// When neither BackoffType nor BackoffStrategy is set, default is exponential.
	policy := &RetryPolicy{
		InitialInterval:    "PT1S",
		BackoffCoefficient: 2.0,
	}

	tests := []struct {
		name    string
		attempt int
		want    time.Duration
	}{
		{"attempt 1", 1, 1 * time.Second},
		{"attempt 2", 2, 2 * time.Second},
		{"attempt 3", 3, 4 * time.Second},
		{"attempt 4", 4, 8 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CalculateBackoff(policy, tt.attempt)
			if got != tt.want {
				t.Errorf("CalculateBackoff(%+v, %d) = %v, want %v",
					policy, tt.attempt, got, tt.want)
			}
		})
	}
}

func TestCalculateBackoff_DefaultCoefficient(t *testing.T) {
	// When BackoffCoefficient is zero (or unset), the default 2.0 is used.
	tests := []struct {
		name    string
		policy  *RetryPolicy
		attempt int
		want    time.Duration
	}{
		{
			name: "zero coefficient defaults to 2.0 attempt 2",
			policy: &RetryPolicy{
				InitialInterval:    "PT1S",
				BackoffCoefficient: 0,
				BackoffType:        "exponential",
			},
			attempt: 2,
			want:    2 * time.Second, // 1s * 2^1
		},
		{
			name: "zero coefficient defaults to 2.0 attempt 3",
			policy: &RetryPolicy{
				InitialInterval:    "PT1S",
				BackoffCoefficient: 0,
				BackoffType:        "exponential",
			},
			attempt: 3,
			want:    4 * time.Second, // 1s * 2^2
		},
		{
			name: "negative coefficient defaults to 2.0",
			policy: &RetryPolicy{
				InitialInterval:    "PT1S",
				BackoffCoefficient: -1.0,
				BackoffType:        "exponential",
			},
			attempt: 2,
			want:    2 * time.Second, // 1s * 2^1
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CalculateBackoff(tt.policy, tt.attempt)
			if got != tt.want {
				t.Errorf("CalculateBackoff(%+v, %d) = %v, want %v",
					tt.policy, tt.attempt, got, tt.want)
			}
		})
	}
}

func TestCalculateBackoff_DefaultInitialInterval(t *testing.T) {
	// When InitialInterval is empty, the default 1s is used.
	policy := &RetryPolicy{
		BackoffCoefficient: 2.0,
		BackoffType:        "exponential",
	}

	tests := []struct {
		name    string
		attempt int
		want    time.Duration
	}{
		{"attempt 1 defaults to 1s", 1, 1 * time.Second},
		{"attempt 2 defaults to 2s", 2, 2 * time.Second},
		{"attempt 3 defaults to 4s", 3, 4 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CalculateBackoff(policy, tt.attempt)
			if got != tt.want {
				t.Errorf("CalculateBackoff(%+v, %d) = %v, want %v",
					policy, tt.attempt, got, tt.want)
			}
		})
	}
}

func TestCalculateBackoff_JitterWithMaxInterval(t *testing.T) {
	// Jitter is applied after the max interval cap.
	// So the capped value becomes the base for jitter: result in [0.5 * cap, 1.5 * cap).
	policy := &RetryPolicy{
		InitialInterval:    "PT1S",
		BackoffCoefficient: 2.0,
		BackoffType:        "exponential",
		MaxInterval:        "PT5S",
		Jitter:             true,
	}
	attempt := 10 // 1s * 2^9 = 512s, capped to 5s, then jitter applied

	cappedBase := 5 * time.Second
	minExpected := time.Duration(float64(cappedBase) * 0.5)
	maxExpected := time.Duration(float64(cappedBase) * 1.5)

	for i := 0; i < 1000; i++ {
		got := CalculateBackoff(policy, attempt)
		if got < minExpected || got > maxExpected {
			t.Fatalf("iteration %d: CalculateBackoff = %v, want in [%v, %v]",
				i, got, minExpected, maxExpected)
		}
	}
}

func TestCalculateBackoff_EmptyMaxInterval(t *testing.T) {
	// When MaxInterval is empty, no cap is applied.
	policy := &RetryPolicy{
		InitialInterval:    "PT1S",
		BackoffCoefficient: 2.0,
		BackoffType:        "exponential",
	}

	got := CalculateBackoff(policy, 10)
	want := 512 * time.Second // 1s * 2^9
	if got != want {
		t.Errorf("CalculateBackoff with no max interval = %v, want %v", got, want)
	}
}

func TestCalculateBackoff_InvalidInitialIntervalFallsBackToDefault(t *testing.T) {
	// If InitialInterval cannot be parsed, it falls back to 1s default.
	policy := &RetryPolicy{
		InitialInterval:    "invalid",
		BackoffCoefficient: 2.0,
		BackoffType:        "exponential",
	}

	got := CalculateBackoff(policy, 1)
	want := 1 * time.Second // falls back to 1s
	if got != want {
		t.Errorf("CalculateBackoff with invalid initial interval = %v, want %v (default 1s)", got, want)
	}
}

func TestCalculateBackoff_FractionalCoefficient(t *testing.T) {
	tests := []struct {
		name    string
		policy  *RetryPolicy
		attempt int
		want    time.Duration
	}{
		{
			name: "coefficient 1.5 attempt 1",
			policy: &RetryPolicy{
				InitialInterval:    "PT2S",
				BackoffCoefficient: 1.5,
				BackoffType:        "exponential",
			},
			attempt: 1,
			want:    2 * time.Second, // 2s * 1.5^0 = 2s
		},
		{
			name: "coefficient 1.5 attempt 2",
			policy: &RetryPolicy{
				InitialInterval:    "PT2S",
				BackoffCoefficient: 1.5,
				BackoffType:        "exponential",
			},
			attempt: 2,
			want:    3 * time.Second, // 2s * 1.5^1 = 3s
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CalculateBackoff(tt.policy, tt.attempt)
			if got != tt.want {
				t.Errorf("CalculateBackoff(%+v, %d) = %v, want %v",
					tt.policy, tt.attempt, got, tt.want)
			}
		})
	}
}
