package scheduler

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mockBackend records how many times each method was called.
type mockBackend struct {
	promoteScheduled atomic.Int64
	promoteRetries   atomic.Int64
	requeueStalled   atomic.Int64
	fireCron         atomic.Int64
	err              error // optional error to return
}

func (m *mockBackend) PromoteScheduled(ctx context.Context) error {
	m.promoteScheduled.Add(1)
	return m.err
}
func (m *mockBackend) PromoteRetries(ctx context.Context) error {
	m.promoteRetries.Add(1)
	return m.err
}
func (m *mockBackend) RequeueStalled(ctx context.Context) error {
	m.requeueStalled.Add(1)
	return m.err
}
func (m *mockBackend) FireCronJobs(ctx context.Context) error {
	m.fireCron.Add(1)
	return m.err
}

func TestScheduler_StartStop(t *testing.T) {
	m := &mockBackend{}
	s := New(m, DefaultConfig())

	s.Start()
	// Allow some ticks to fire
	time.Sleep(350 * time.Millisecond)
	s.Stop()

	// retry-promoter runs at 200ms so should have fired at least once
	if m.promoteRetries.Load() == 0 {
		t.Error("expected PromoteRetries to be called at least once")
	}
	// stalled-reaper runs at 500ms, should have fired at least once within 350ms
	// (may or may not depending on timing); skip asserting exact count
}

func TestScheduler_StopIdempotent(t *testing.T) {
	m := &mockBackend{}
	s := New(m, DefaultConfig())
	s.Start()

	// Calling Stop multiple times should not panic
	s.Stop()
	s.Stop()
	s.Stop()
}

func TestScheduler_AllLoopsRun(t *testing.T) {
	m := &mockBackend{}
	s := New(m, DefaultConfig())
	s.Start()

	// Wait long enough for all loops to fire at least once
	// Slowest loop is cron at 10s, but we don't want a 10s test.
	// Instead verify the faster loops and skip cron timing assertion.
	time.Sleep(1200 * time.Millisecond)
	s.Stop()

	if m.promoteScheduled.Load() == 0 {
		t.Error("expected PromoteScheduled to be called")
	}
	if m.promoteRetries.Load() == 0 {
		t.Error("expected PromoteRetries to be called")
	}
	if m.requeueStalled.Load() == 0 {
		t.Error("expected RequeueStalled to be called")
	}
	// Cron fires at 10s intervals; we don't wait that long in unit tests
}

func TestScheduler_StopHaltsExecution(t *testing.T) {
	m := &mockBackend{}
	s := New(m, DefaultConfig())
	s.Start()

	// Let some ticks fire
	time.Sleep(300 * time.Millisecond)
	s.Stop()

	// Record counts after stop
	countAfterStop := m.promoteRetries.Load()

	// Wait and verify no more calls
	time.Sleep(500 * time.Millisecond)
	countAfterWait := m.promoteRetries.Load()

	if countAfterWait != countAfterStop {
		t.Errorf("expected no more calls after Stop, but got %d → %d", countAfterStop, countAfterWait)
	}
}

func TestScheduler_ErrorsDoNotStopLoop(t *testing.T) {
	m := &mockBackend{err: context.DeadlineExceeded}
	s := New(m, DefaultConfig())
	s.Start()

	// Even with errors, the loop should keep running
	time.Sleep(500 * time.Millisecond)
	s.Stop()

	if m.promoteRetries.Load() < 2 {
		t.Errorf("expected multiple calls despite errors, got %d", m.promoteRetries.Load())
	}
}

func TestScheduler_ConcurrentStartStop(t *testing.T) {
	m := &mockBackend{}
	s := New(m, DefaultConfig())

	var wg sync.WaitGroup
	// Start, then concurrently call Stop from multiple goroutines
	s.Start()
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.Stop()
		}()
	}
	wg.Wait()
}
