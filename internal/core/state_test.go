package core

import "testing"

func TestIsValidTransition(t *testing.T) {
	t.Run("valid transitions", func(t *testing.T) {
		tests := []struct {
			name string
			from string
			to   string
		}{
			// available transitions
			{"available to active", StateAvailable, StateActive},
			{"available to cancelled", StateAvailable, StateCancelled},

			// scheduled transitions
			{"scheduled to available", StateScheduled, StateAvailable},
			{"scheduled to cancelled", StateScheduled, StateCancelled},

			// pending transitions
			{"pending to available", StatePending, StateAvailable},
			{"pending to cancelled", StatePending, StateCancelled},

			// active transitions
			{"active to completed", StateActive, StateCompleted},
			{"active to retryable", StateActive, StateRetryable},
			{"active to discarded", StateActive, StateDiscarded},
			{"active to cancelled", StateActive, StateCancelled},

			// retryable transitions
			{"retryable to available", StateRetryable, StateAvailable},
			{"retryable to cancelled", StateRetryable, StateCancelled},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if !IsValidTransition(tt.from, tt.to) {
					t.Errorf("IsValidTransition(%q, %q) = false, want true", tt.from, tt.to)
				}
			})
		}
	})

	t.Run("invalid transitions", func(t *testing.T) {
		tests := []struct {
			name string
			from string
			to   string
		}{
			// available cannot go to non-allowed states
			{"available to completed", StateAvailable, StateCompleted},
			{"available to retryable", StateAvailable, StateRetryable},
			{"available to discarded", StateAvailable, StateDiscarded},
			{"available to scheduled", StateAvailable, StateScheduled},
			{"available to pending", StateAvailable, StatePending},
			{"available to available", StateAvailable, StateAvailable},

			// scheduled cannot go to non-allowed states
			{"scheduled to active", StateScheduled, StateActive},
			{"scheduled to completed", StateScheduled, StateCompleted},
			{"scheduled to retryable", StateScheduled, StateRetryable},
			{"scheduled to discarded", StateScheduled, StateDiscarded},
			{"scheduled to pending", StateScheduled, StatePending},
			{"scheduled to scheduled", StateScheduled, StateScheduled},

			// pending cannot go to non-allowed states
			{"pending to active", StatePending, StateActive},
			{"pending to completed", StatePending, StateCompleted},
			{"pending to retryable", StatePending, StateRetryable},
			{"pending to discarded", StatePending, StateDiscarded},
			{"pending to scheduled", StatePending, StateScheduled},
			{"pending to pending", StatePending, StatePending},

			// active cannot go to non-allowed states
			{"active to available", StateActive, StateAvailable},
			{"active to scheduled", StateActive, StateScheduled},
			{"active to pending", StateActive, StatePending},
			{"active to active", StateActive, StateActive},

			// retryable cannot go to non-allowed states
			{"retryable to active", StateRetryable, StateActive},
			{"retryable to completed", StateRetryable, StateCompleted},
			{"retryable to discarded", StateRetryable, StateDiscarded},
			{"retryable to scheduled", StateRetryable, StateScheduled},
			{"retryable to pending", StateRetryable, StatePending},
			{"retryable to retryable", StateRetryable, StateRetryable},

			// terminal states have no outgoing transitions
			{"completed to available", StateCompleted, StateAvailable},
			{"completed to scheduled", StateCompleted, StateScheduled},
			{"completed to pending", StateCompleted, StatePending},
			{"completed to active", StateCompleted, StateActive},
			{"completed to retryable", StateCompleted, StateRetryable},
			{"completed to cancelled", StateCompleted, StateCancelled},
			{"completed to discarded", StateCompleted, StateDiscarded},
			{"completed to completed", StateCompleted, StateCompleted},

			{"cancelled to available", StateCancelled, StateAvailable},
			{"cancelled to scheduled", StateCancelled, StateScheduled},
			{"cancelled to pending", StateCancelled, StatePending},
			{"cancelled to active", StateCancelled, StateActive},
			{"cancelled to retryable", StateCancelled, StateRetryable},
			{"cancelled to completed", StateCancelled, StateCompleted},
			{"cancelled to discarded", StateCancelled, StateDiscarded},
			{"cancelled to cancelled", StateCancelled, StateCancelled},

			{"discarded to available", StateDiscarded, StateAvailable},
			{"discarded to scheduled", StateDiscarded, StateScheduled},
			{"discarded to pending", StateDiscarded, StatePending},
			{"discarded to active", StateDiscarded, StateActive},
			{"discarded to retryable", StateDiscarded, StateRetryable},
			{"discarded to completed", StateDiscarded, StateCompleted},
			{"discarded to cancelled", StateDiscarded, StateCancelled},
			{"discarded to discarded", StateDiscarded, StateDiscarded},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if IsValidTransition(tt.from, tt.to) {
					t.Errorf("IsValidTransition(%q, %q) = true, want false", tt.from, tt.to)
				}
			})
		}
	})

	t.Run("unknown states", func(t *testing.T) {
		tests := []struct {
			name string
			from string
			to   string
		}{
			{"unknown from state", "nonexistent", StateActive},
			{"unknown to state", StateAvailable, "nonexistent"},
			{"both unknown", "foo", "bar"},
			{"empty from", "", StateActive},
			{"empty to", StateAvailable, ""},
			{"both empty", "", ""},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if IsValidTransition(tt.from, tt.to) {
					t.Errorf("IsValidTransition(%q, %q) = true, want false", tt.from, tt.to)
				}
			})
		}
	})
}

func TestIsTerminalState(t *testing.T) {
	t.Run("terminal states return true", func(t *testing.T) {
		tests := []struct {
			name  string
			state string
		}{
			{"completed", StateCompleted},
			{"cancelled", StateCancelled},
			{"discarded", StateDiscarded},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if !IsTerminalState(tt.state) {
					t.Errorf("IsTerminalState(%q) = false, want true", tt.state)
				}
			})
		}
	})

	t.Run("non-terminal states return false", func(t *testing.T) {
		tests := []struct {
			name  string
			state string
		}{
			{"available", StateAvailable},
			{"scheduled", StateScheduled},
			{"pending", StatePending},
			{"active", StateActive},
			{"retryable", StateRetryable},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if IsTerminalState(tt.state) {
					t.Errorf("IsTerminalState(%q) = true, want false", tt.state)
				}
			})
		}
	})

	t.Run("unknown states return false", func(t *testing.T) {
		tests := []struct {
			name  string
			state string
		}{
			{"unknown state", "nonexistent"},
			{"empty string", ""},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if IsTerminalState(tt.state) {
					t.Errorf("IsTerminalState(%q) = true, want false", tt.state)
				}
			})
		}
	})
}

func TestIsCancellableState(t *testing.T) {
	t.Run("cancellable states return true", func(t *testing.T) {
		tests := []struct {
			name  string
			state string
		}{
			{"available", StateAvailable},
			{"scheduled", StateScheduled},
			{"pending", StatePending},
			{"active", StateActive},
			{"retryable", StateRetryable},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if !IsCancellableState(tt.state) {
					t.Errorf("IsCancellableState(%q) = false, want true", tt.state)
				}
			})
		}
	})

	t.Run("non-cancellable states return false", func(t *testing.T) {
		tests := []struct {
			name  string
			state string
		}{
			{"completed", StateCompleted},
			{"cancelled", StateCancelled},
			{"discarded", StateDiscarded},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if IsCancellableState(tt.state) {
					t.Errorf("IsCancellableState(%q) = true, want false", tt.state)
				}
			})
		}
	})

	t.Run("unknown states return false", func(t *testing.T) {
		tests := []struct {
			name  string
			state string
		}{
			{"unknown state", "nonexistent"},
			{"empty string", ""},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if IsCancellableState(tt.state) {
					t.Errorf("IsCancellableState(%q) = true, want false", tt.state)
				}
			})
		}
	})
}
