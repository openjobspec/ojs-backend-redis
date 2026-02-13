package core

import (
	"testing"
)

func TestNewUUIDv7(t *testing.T) {
	t.Run("returns valid UUIDv7 format", func(t *testing.T) {
		id := NewUUIDv7()
		if !IsValidUUIDv7(id) {
			t.Errorf("NewUUIDv7() = %q, want valid UUIDv7 format", id)
		}
	})

	t.Run("returns valid UUID", func(t *testing.T) {
		id := NewUUIDv7()
		if !IsValidUUID(id) {
			t.Errorf("NewUUIDv7() = %q, want valid UUID", id)
		}
	})

	t.Run("version nibble is 7", func(t *testing.T) {
		id := NewUUIDv7()
		// UUID format: xxxxxxxx-xxxx-Vxxx-xxxx-xxxxxxxxxxxx
		// The version nibble V is at index 14
		if len(id) < 15 {
			t.Fatalf("NewUUIDv7() = %q, too short", id)
		}
		if id[14] != '7' {
			t.Errorf("NewUUIDv7() = %q, version nibble = %c, want '7'", id, id[14])
		}
	})

	t.Run("variant bits are correct", func(t *testing.T) {
		id := NewUUIDv7()
		// UUID format: xxxxxxxx-xxxx-xxxx-Vxxx-xxxxxxxxxxxx
		// The variant nibble is at index 19 and must be one of 8, 9, a, b
		if len(id) < 20 {
			t.Fatalf("NewUUIDv7() = %q, too short", id)
		}
		variant := id[19]
		if variant != '8' && variant != '9' && variant != 'a' && variant != 'b' {
			t.Errorf("NewUUIDv7() = %q, variant nibble = %c, want one of [8, 9, a, b]", id, variant)
		}
	})

	t.Run("successive calls produce unique values", func(t *testing.T) {
		seen := make(map[string]bool)
		count := 100
		for i := 0; i < count; i++ {
			id := NewUUIDv7()
			if seen[id] {
				t.Errorf("NewUUIDv7() produced duplicate value %q on call %d", id, i)
			}
			seen[id] = true
		}
		if len(seen) != count {
			t.Errorf("NewUUIDv7() produced %d unique values out of %d calls", len(seen), count)
		}
	})

	t.Run("successive calls are monotonically ordered", func(t *testing.T) {
		prev := NewUUIDv7()
		for i := 0; i < 50; i++ {
			curr := NewUUIDv7()
			if curr <= prev {
				t.Errorf("NewUUIDv7() not monotonically ordered: %q >= %q on iteration %d", prev, curr, i)
			}
			prev = curr
		}
	})
}

func TestIsValidUUIDv7(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{
			name:  "valid UUIDv7 from NewUUIDv7",
			input: NewUUIDv7(),
			want:  true,
		},
		{
			name:  "valid UUIDv7 literal with variant 8",
			input: "01906e5a-4d2b-7f1c-8a3e-5b6c7d8e9f0a",
			want:  true,
		},
		{
			name:  "valid UUIDv7 literal with variant 9",
			input: "01906e5a-4d2b-7f1c-9a3e-5b6c7d8e9f0a",
			want:  true,
		},
		{
			name:  "valid UUIDv7 literal with variant a",
			input: "01906e5a-4d2b-7f1c-aa3e-5b6c7d8e9f0a",
			want:  true,
		},
		{
			name:  "valid UUIDv7 literal with variant b",
			input: "01906e5a-4d2b-7f1c-ba3e-5b6c7d8e9f0a",
			want:  true,
		},
		{
			name:  "UUIDv4 returns false",
			input: "550e8400-e29b-41d4-a716-446655440000",
			want:  false,
		},
		{
			name:  "UUIDv1 returns false",
			input: "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
			want:  false,
		},
		{
			name:  "empty string returns false",
			input: "",
			want:  false,
		},
		{
			name:  "malformed UUID missing dashes",
			input: "01906e5a4d2b7f1c8a3e5b6c7d8e9f0a",
			want:  false,
		},
		{
			name:  "malformed UUID too short",
			input: "01906e5a-4d2b-7f1c-8a3e",
			want:  false,
		},
		{
			name:  "malformed UUID too long",
			input: "01906e5a-4d2b-7f1c-8a3e-5b6c7d8e9f0a0",
			want:  false,
		},
		{
			name:  "random string returns false",
			input: "not-a-uuid-at-all",
			want:  false,
		},
		{
			name:  "uppercase UUIDv7 returns false",
			input: "01906E5A-4D2B-7F1C-8A3E-5B6C7D8E9F0A",
			want:  false,
		},
		{
			name:  "invalid variant nibble c",
			input: "01906e5a-4d2b-7f1c-ca3e-5b6c7d8e9f0a",
			want:  false,
		},
		{
			name:  "invalid variant nibble 0",
			input: "01906e5a-4d2b-7f1c-0a3e-5b6c7d8e9f0a",
			want:  false,
		},
		{
			name:  "nil UUID returns false",
			input: "00000000-0000-0000-0000-000000000000",
			want:  false,
		},
		{
			name:  "contains non-hex characters",
			input: "01906e5a-4d2b-7f1c-8a3e-5b6c7d8e9gzz",
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsValidUUIDv7(tt.input)
			if got != tt.want {
				t.Errorf("IsValidUUIDv7(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestIsValidUUID(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{
			name:  "valid UUIDv4",
			input: "550e8400-e29b-41d4-a716-446655440000",
			want:  true,
		},
		{
			name:  "valid UUIDv7 from NewUUIDv7",
			input: NewUUIDv7(),
			want:  true,
		},
		{
			name:  "valid UUIDv7 literal",
			input: "01906e5a-4d2b-7f1c-8a3e-5b6c7d8e9f0a",
			want:  true,
		},
		{
			name:  "valid UUIDv1",
			input: "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
			want:  true,
		},
		{
			name:  "uppercase UUID is accepted",
			input: "550E8400-E29B-41D4-A716-446655440000",
			want:  true,
		},
		{
			name:  "mixed case UUID is accepted",
			input: "550e8400-E29B-41d4-A716-446655440000",
			want:  true,
		},
		{
			name:  "nil UUID is valid",
			input: "00000000-0000-0000-0000-000000000000",
			want:  true,
		},
		{
			name:  "empty string returns false",
			input: "",
			want:  false,
		},
		{
			name:  "not-a-uuid returns false",
			input: "not-a-uuid",
			want:  false,
		},
		{
			name:  "UUID without dashes is accepted by uuid.Parse",
			input: "550e8400e29b41d4a716446655440000",
			want:  true,
		},
		{
			name:  "too short returns false",
			input: "550e8400-e29b-41d4-a716",
			want:  false,
		},
		{
			name:  "too long returns false",
			input: "550e8400-e29b-41d4-a716-4466554400001",
			want:  false,
		},
		{
			name:  "contains non-hex characters",
			input: "550e8400-e29b-41d4-a716-44665544zzzz",
			want:  false,
		},
		{
			name:  "whitespace only returns false",
			input: "   ",
			want:  false,
		},
		{
			name:  "UUID with leading space returns false",
			input: " 550e8400-e29b-41d4-a716-446655440000",
			want:  false,
		},
		{
			name:  "UUID with trailing space returns false",
			input: "550e8400-e29b-41d4-a716-446655440000 ",
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsValidUUID(tt.input)
			if got != tt.want {
				t.Errorf("IsValidUUID(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}
