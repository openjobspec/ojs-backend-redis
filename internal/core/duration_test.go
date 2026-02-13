package core

import (
	"testing"
	"time"
)

func TestParseISO8601Duration_Valid(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  time.Duration
	}{
		{
			name:  "seconds only",
			input: "PT1S",
			want:  1 * time.Second,
		},
		{
			name:  "minutes only",
			input: "PT5M",
			want:  5 * time.Minute,
		},
		{
			name:  "hours only",
			input: "PT1H",
			want:  1 * time.Hour,
		},
		{
			name:  "hours and minutes",
			input: "PT1H30M",
			want:  1*time.Hour + 30*time.Minute,
		},
		{
			name:  "hours minutes and seconds",
			input: "PT1H30M45S",
			want:  1*time.Hour + 30*time.Minute + 45*time.Second,
		},
		{
			name:  "fractional seconds",
			input: "PT0.5S",
			want:  500 * time.Millisecond,
		},
		{
			name:  "fractional seconds with precision",
			input: "PT1.25S",
			want:  1*time.Second + 250*time.Millisecond,
		},
		{
			name:  "minutes and seconds",
			input: "PT10M30S",
			want:  10*time.Minute + 30*time.Second,
		},
		{
			name:  "hours and seconds",
			input: "PT2H15S",
			want:  2*time.Hour + 15*time.Second,
		},
		{
			name:  "large hours",
			input: "PT24H",
			want:  24 * time.Hour,
		},
		{
			name:  "large minutes",
			input: "PT120M",
			want:  120 * time.Minute,
		},
		{
			name:  "large seconds",
			input: "PT3600S",
			want:  3600 * time.Second,
		},
		{
			name:  "fractional seconds three decimals",
			input: "PT0.001S",
			want:  1 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseISO8601Duration(tt.input)
			if err != nil {
				t.Fatalf("ParseISO8601Duration(%q) returned unexpected error: %v", tt.input, err)
			}
			if got != tt.want {
				t.Errorf("ParseISO8601Duration(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestParseISO8601Duration_Invalid(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "empty string",
			input: "",
		},
		{
			name:  "day duration not supported",
			input: "P1D",
		},
		{
			name:  "missing PT prefix",
			input: "1S",
		},
		{
			name:  "non-numeric components",
			input: "PTXYZ",
		},
		{
			name:  "bare PT with no components",
			input: "PT",
		},
		{
			name:  "zero seconds is zero duration",
			input: "PT0S",
		},
		{
			name:  "plain text",
			input: "hello",
		},
		{
			name:  "negative duration",
			input: "PT-5S",
		},
		{
			name:  "wrong component order",
			input: "PT5S1M",
		},
		{
			name:  "date components not supported",
			input: "P1Y2M3D",
		},
		{
			name:  "mixed date and time",
			input: "P1DT1H",
		},
		{
			name:  "lowercase prefix",
			input: "pt1s",
		},
		{
			name:  "trailing whitespace",
			input: "PT1S ",
		},
		{
			name:  "leading whitespace",
			input: " PT1S",
		},
		{
			name:  "zero hours is zero duration",
			input: "PT0H",
		},
		{
			name:  "zero minutes is zero duration",
			input: "PT0M",
		},
		{
			name:  "all zero components is zero duration",
			input: "PT0H0M0S",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseISO8601Duration(tt.input)
			if err == nil {
				t.Errorf("ParseISO8601Duration(%q) = %v, want error", tt.input, got)
			}
		})
	}
}

func TestFormatISO8601Duration(t *testing.T) {
	tests := []struct {
		name  string
		input time.Duration
		want  string
	}{
		{
			name:  "zero duration",
			input: 0,
			want:  "PT0S",
		},
		{
			name:  "one second",
			input: 1 * time.Second,
			want:  "PT1S",
		},
		{
			name:  "five minutes",
			input: 5 * time.Minute,
			want:  "PT5M",
		},
		{
			name:  "one hour",
			input: 1 * time.Hour,
			want:  "PT1H",
		},
		{
			name:  "one hour thirty minutes",
			input: 1*time.Hour + 30*time.Minute,
			want:  "PT1H30M",
		},
		{
			name:  "hours minutes and seconds",
			input: 1*time.Hour + 30*time.Minute + 45*time.Second,
			want:  "PT1H30M45S",
		},
		{
			name:  "minutes and seconds",
			input: 10*time.Minute + 30*time.Second,
			want:  "PT10M30S",
		},
		{
			name:  "hours and seconds no minutes",
			input: 2*time.Hour + 15*time.Second,
			want:  "PT2H15S",
		},
		{
			name:  "fractional seconds",
			input: 500 * time.Millisecond,
			want:  "PT0.500S",
		},
		{
			name:  "seconds with milliseconds",
			input: 1*time.Second + 500*time.Millisecond,
			want:  "PT1.500S",
		},
		{
			name:  "24 hours",
			input: 24 * time.Hour,
			want:  "PT24H",
		},
		{
			name:  "120 minutes",
			input: 120 * time.Minute,
			want:  "PT2H",
		},
		{
			name:  "90 seconds",
			input: 90 * time.Second,
			want:  "PT1M30S",
		},
		{
			name:  "3661 seconds as combined",
			input: 1*time.Hour + 1*time.Minute + 1*time.Second,
			want:  "PT1H1M1S",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FormatISO8601Duration(tt.input)
			if got != tt.want {
				t.Errorf("FormatISO8601Duration(%v) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestRoundtrip_ParseThenFormat(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{name: "seconds", input: "PT1S"},
		{name: "minutes", input: "PT5M"},
		{name: "hours", input: "PT1H"},
		{name: "hours and minutes", input: "PT1H30M"},
		{name: "hours minutes seconds", input: "PT1H30M45S"},
		{name: "minutes and seconds", input: "PT10M30S"},
		{name: "hours and seconds", input: "PT2H15S"},
		{name: "large values", input: "PT24H"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, err := ParseISO8601Duration(tt.input)
			if err != nil {
				t.Fatalf("ParseISO8601Duration(%q) returned unexpected error: %v", tt.input, err)
			}
			got := FormatISO8601Duration(d)
			if got != tt.input {
				t.Errorf("roundtrip failed: ParseISO8601Duration(%q) → %v → FormatISO8601Duration → %q", tt.input, d, got)
			}
		})
	}
}

func TestRoundtrip_FormatThenParse(t *testing.T) {
	tests := []struct {
		name  string
		input time.Duration
	}{
		{name: "one second", input: 1 * time.Second},
		{name: "five minutes", input: 5 * time.Minute},
		{name: "one hour", input: 1 * time.Hour},
		{name: "combined", input: 2*time.Hour + 15*time.Minute + 30*time.Second},
		{name: "minutes and seconds", input: 7*time.Minute + 45*time.Second},
		{name: "hours and seconds", input: 3*time.Hour + 10*time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := FormatISO8601Duration(tt.input)
			got, err := ParseISO8601Duration(s)
			if err != nil {
				t.Fatalf("ParseISO8601Duration(%q) returned unexpected error: %v", s, err)
			}
			if got != tt.input {
				t.Errorf("roundtrip failed: FormatISO8601Duration(%v) → %q → ParseISO8601Duration → %v", tt.input, s, got)
			}
		})
	}
}
