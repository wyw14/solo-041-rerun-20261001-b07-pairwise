package sensor

import (
	"context"
	"testing"
	"time"

	"github.com/wyw14/cry-97/internal/journal"
	"github.com/wyw14/cry-97/internal/model"
)

func newTestReceiver(t *testing.T) (*Receiver, *journal.FileStore, *journal.CursorStore) {
	t.Helper()
	dataDir := t.TempDir()
	store, err := journal.NewFileStore(dataDir + "/journal")
	if err != nil {
		t.Fatalf("file store: %v", err)
	}
	cursor, err := journal.NewCursorStore(dataDir + "/cursors.json")
	if err != nil {
		t.Fatalf("cursor store: %v", err)
	}
	receiver, err := NewReceiver(store, cursor)
	if err != nil {
		t.Fatalf("receiver: %v", err)
	}
	return receiver, store, cursor
}

func makeSample(kind model.SampleKind, seq uint64) model.Sample {
	return model.NewSample("line-1", "basin-1", kind, seq, 1.2, "m", time.Date(2026, 9, 30, 8, 0, 0, 0, time.UTC))
}

func TestReceive_AdvancesCursorAndStoresEvent(t *testing.T) {
	receiver, store, cursor := newTestReceiver(t)
	ctx := context.Background()

	receipt, err := receiver.Receive(ctx, makeSample(model.SampleWaterLevel, 1), time.Now())
	if err != nil {
		t.Fatalf("receive seq 1: %v", err)
	}
	if !receipt.Stored || receipt.Cursor != 1 {
		t.Fatalf("seq 1 should be stored, got %+v", receipt)
	}
	if cursor.Current("line-1") != 1 {
		t.Fatalf("cursor = %d, want 1", cursor.Current("line-1"))
	}
	events, err := store.Events(ctx, "line-1")
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	if len(events) != 1 || events[0].Kind != model.EventSampleStored {
		t.Fatalf("events = %+v, want one sample.stored event", events)
	}
}

func TestReceive_SameSequenceDifferentKindIsStale(t *testing.T) {
	receiver, store, cursor := newTestReceiver(t)
	ctx := context.Background()

	if _, err := receiver.Receive(ctx, makeSample(model.SampleWaterLevel, 1), time.Now()); err != nil {
		t.Fatalf("receive water seq 1: %v", err)
	}

	// Same sequence, different sample kind must not slip through as a new sample.
	receipt, err := receiver.Receive(ctx, makeSample(model.SampleSludgeBlanket, 1), time.Now())
	if err != nil {
		t.Fatalf("stale sample must be acknowledged, got error: %v", err)
	}
	if receipt.Stored {
		t.Fatalf("same-sequence different-kind sample must not be stored")
	}
	if cursor.Current("line-1") != 1 {
		t.Fatalf("cursor advanced to %d, want 1", cursor.Current("line-1"))
	}
	events, err := store.Events(ctx, "line-1")
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("stale sample appended %d events, want still 1", len(events))
	}
}

func TestReceive_ExactDuplicateIsStale(t *testing.T) {
	receiver, _, _ := newTestReceiver(t)
	ctx := context.Background()

	if _, err := receiver.Receive(ctx, makeSample(model.SampleWaterLevel, 1), time.Now()); err != nil {
		t.Fatalf("receive first: %v", err)
	}
	receipt, err := receiver.Receive(ctx, makeSample(model.SampleWaterLevel, 1), time.Now())
	if err != nil {
		t.Fatalf("duplicate must be acknowledged, got: %v", err)
	}
	if receipt.Stored {
		t.Fatalf("exact duplicate must not be stored")
	}
}

func TestReceive_SequenceGapRejected(t *testing.T) {
	receiver, store, _ := newTestReceiver(t)
	ctx := context.Background()

	if _, err := receiver.Receive(ctx, makeSample(model.SampleWaterLevel, 1), time.Now()); err != nil {
		t.Fatalf("receive seq 1: %v", err)
	}
	if _, err := receiver.Receive(ctx, makeSample(model.SampleWaterLevel, 3), time.Now()); err == nil {
		t.Fatal("seq 3 with cursor 1 must be rejected as a gap")
	}
	events, _ := store.Events(ctx, "line-1")
	if len(events) != 1 {
		t.Fatalf("gap sample appended events, total = %d, want 1", len(events))
	}
}

func TestReceive_NextSequenceDifferentKindStored(t *testing.T) {
	receiver, store, cursor := newTestReceiver(t)
	ctx := context.Background()

	if _, err := receiver.Receive(ctx, makeSample(model.SampleWaterLevel, 1), time.Now()); err != nil {
		t.Fatalf("receive seq 1: %v", err)
	}
	// The legitimate next sequence may carry a new kind and must be accepted.
	receipt, err := receiver.Receive(ctx, makeSample(model.SampleSludgeBlanket, 2), time.Now())
	if err != nil {
		t.Fatalf("receive seq 2: %v", err)
	}
	if !receipt.Stored || receipt.Cursor != 2 {
		t.Fatalf("seq 2 should be stored, got %+v", receipt)
	}
	if cursor.Current("line-1") != 2 {
		t.Fatalf("cursor = %d, want 2", cursor.Current("line-1"))
	}
	events, _ := store.Events(ctx, "line-1")
	if len(events) != 2 {
		t.Fatalf("events = %d, want 2", len(events))
	}
}

func TestReceive_InvalidSampleRejected(t *testing.T) {
	receiver, _, _ := newTestReceiver(t)

	missingKind := makeSample(model.SampleWaterLevel, 1)
	missingKind.Kind = ""
	if _, err := receiver.Receive(context.Background(), missingKind, time.Now()); err == nil {
		t.Fatal("sample without kind must be rejected")
	}
}
