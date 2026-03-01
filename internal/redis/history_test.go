package redis

import (
	"context"
	"testing"

	"github.com/openjobspec/ojs-backend-redis/internal/core"
)

func TestHistoryRecordAndRetrieve(t *testing.T) {
	// This test verifies the history recording integration:
	// 1. Record a history event
	// 2. Retrieve it via GetJobHistory
	// 3. Verify the event matches

	b := newTestBackend(t)
	ctx := context.Background()
	jobID := core.NewUUIDv7()

	// Record an event
	event := core.NewHistoryEvent(jobID, core.HistoryEventJobCreated,
		core.ClientActor("test"), map[string]any{"queue": "default", "type": "test.job"})

	if err := b.RecordEvent(ctx, event); err != nil {
		t.Fatalf("RecordEvent: %v", err)
	}

	// Retrieve history
	page, err := b.GetJobHistory(ctx, jobID, 50, "")
	if err != nil {
		t.Fatalf("GetJobHistory: %v", err)
	}

	if page.Total != 1 {
		t.Errorf("expected 1 event, got %d", page.Total)
	}
	if len(page.Events) != 1 {
		t.Fatalf("expected 1 event in page, got %d", len(page.Events))
	}
	if page.Events[0].EventType != core.HistoryEventJobCreated {
		t.Errorf("expected event type %s, got %s", core.HistoryEventJobCreated, page.Events[0].EventType)
	}
	if page.Events[0].JobID != jobID {
		t.Errorf("expected job ID %s, got %s", jobID, page.Events[0].JobID)
	}
}

func TestHistoryMultipleEventsAndPagination(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()
	jobID := core.NewUUIDv7()

	// Record 3 events
	for _, eventType := range []string{core.HistoryEventJobCreated, core.HistoryEventStateChanged, core.HistoryEventAttemptCompleted} {
		event := core.NewHistoryEvent(jobID, eventType, core.SystemActor(), nil)
		if err := b.RecordEvent(ctx, event); err != nil {
			t.Fatalf("RecordEvent(%s): %v", eventType, err)
		}
	}

	// Retrieve with limit=2
	page, err := b.GetJobHistory(ctx, jobID, 2, "")
	if err != nil {
		t.Fatalf("GetJobHistory: %v", err)
	}
	if len(page.Events) != 2 {
		t.Errorf("expected 2 events, got %d", len(page.Events))
	}
	if page.NextCursor == "" {
		t.Error("expected non-empty next cursor for pagination")
	}
	if page.Total != 3 {
		t.Errorf("expected total=3, got %d", page.Total)
	}
}

func TestHistoryPurge(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()
	jobID := core.NewUUIDv7()

	event := core.NewHistoryEvent(jobID, core.HistoryEventJobCreated, core.SystemActor(), nil)
	b.RecordEvent(ctx, event)

	// Purge
	if err := b.PurgeJobHistory(ctx, jobID); err != nil {
		t.Fatalf("PurgeJobHistory: %v", err)
	}

	// Verify empty
	page, _ := b.GetJobHistory(ctx, jobID, 50, "")
	if page.Total != 0 {
		t.Errorf("expected 0 events after purge, got %d", page.Total)
	}
}
