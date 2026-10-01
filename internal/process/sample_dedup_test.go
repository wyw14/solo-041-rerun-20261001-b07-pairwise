package process

import (
	"context"
	"testing"
	"time"

	"github.com/wyw14/cry-97/internal/model"
)

func newTestPlant(t *testing.T, dir string) *Plant {
	t.Helper()
	base := time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC)
	clock := func() time.Time { return base }
	plant, err := NewPlant(dir, []model.LineID{"line-1"}, clock)
	if err != nil {
		t.Fatalf("new plant: %v", err)
	}
	return plant
}

func storedSampleEvents(t *testing.T, p *Plant, lineID model.LineID) int {
	t.Helper()
	events, err := p.store.Events(context.Background(), lineID)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	count := 0
	for _, event := range events {
		if event.Kind == model.EventSampleStored {
			count++
		}
	}
	return count
}

// TestReceiveSamplePerKindDedup reproduces the reported defect: after a
// water-quality sample at sequence 1, a sludge-blanket sample with the same
// sequence is an independent, legitimate stream. It must be stored, advance
// its own cursor, be published to subscribers and still be able to raise the
// high-blanket alarm. Re-delivering the same (kind, sequence) must be a
// no-op with no realtime side effect.
func TestReceiveSamplePerKindDedup(t *testing.T) {
	ctx := context.Background()
	line := model.LineID("line-1")
	p := newTestPlant(t, t.TempDir())

	// Water-quality sample, sequence 1 (low COD, unrelated to the blanket).
	water := model.NewSample(line, "basin-line-1", model.SampleCOD, 1, 20, "mg/L", p.now())
	if err := p.ReceiveSample(ctx, water); err != nil {
		t.Fatalf("receive water seq1: %v", err)
	}
	if got := storedSampleEvents(t, p, line); got != 1 {
		t.Fatalf("events after water seq1 = %d, want 1", got)
	}

	// Count realtime publications from here on.
	published := 0
	if err := p.sampler.Subscribe(func(model.Sample) { published++ }); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// Sludge-blanket sample with the SAME sequence but a different kind. This
	// is a fresh sample for its own stream and must be fully accepted.
	blanket := model.NewSample(line, "settler-line-1", model.SampleSludgeBlanket, 1, 3.0, "m", p.now())
	if err := p.ReceiveSample(ctx, blanket); err != nil {
		t.Fatalf("receive blanket seq1: %v", err)
	}
	if published != 1 {
		t.Fatalf("legitimate blanket seq1 published %d times, want 1", published)
	}
	if got := storedSampleEvents(t, p, line); got != 2 {
		t.Fatalf("events after blanket seq1 = %d, want 2", got)
	}

	// The legitimate new sample must still trigger the high-blanket alarm
	// (0 m -> 3.0 m crosses the 2.8 m threshold).
	active := p.Alarms()
	if len(active) != 1 || active[0].Code != "SETTLING_BLANKET_HIGH" {
		t.Fatalf("alarms after high blanket = %+v, want one SETTLING_BLANKET_HIGH", active)
	}

	// Re-deliver the same (kind, sequence) with a value that would clear the
	// high state if it leaked into the sampler. It is expired: nothing moves.
	duplicate := model.NewSample(line, "settler-line-1", model.SampleSludgeBlanket, 1, 1.0, "m", p.now())
	if err := p.ReceiveSample(ctx, duplicate); err != nil {
		t.Fatalf("re-delivery must be a successful no-op, got error: %v", err)
	}
	if published != 1 {
		t.Fatalf("duplicate seq1 published %d times, want still 1", published)
	}
	if got := storedSampleEvents(t, p, line); got != 2 {
		t.Fatalf("events after duplicate = %d, want still 2", got)
	}
	if level := p.settling[line].Snapshot().BlanketLevel; level != 3.0 {
		t.Fatalf("duplicate mutated live blanket level to %.2f, want 3.00", level)
	}
	if n := len(p.Alarms()); n != 1 {
		t.Fatalf("duplicate produced %d alarms, want still 1", n)
	}

	// Any older sequence for the same kind is likewise dead.
	stale := model.NewSample(line, "settler-line-1", model.SampleSludgeBlanket, 1, 0.5, "m", p.now())
	if err := p.ReceiveSample(ctx, stale); err != nil {
		t.Fatalf("stale seq must be a no-op, got: %v", err)
	}
	if published != 1 {
		t.Fatalf("stale seq published, count = %d, want 1", published)
	}

	// A genuine next sequence for the same kind is accepted and published.
	next := model.NewSample(line, "settler-line-1", model.SampleSludgeBlanket, 2, 3.5, "m", p.now())
	if err := p.ReceiveSample(ctx, next); err != nil {
		t.Fatalf("receive blanket seq2: %v", err)
	}
	if published != 2 {
		t.Fatalf("blanket seq2 published %d times, want 2", published)
	}
	if got := storedSampleEvents(t, p, line); got != 3 {
		t.Fatalf("events after seq2 = %d, want 3", got)
	}

	// A sequence that skips ahead is a gap and must be rejected, not published.
	gap := model.NewSample(line, "settler-line-1", model.SampleSludgeBlanket, 9, 3.6, "m", p.now())
	if err := p.ReceiveSample(ctx, gap); err == nil {
		t.Fatal("gap sequence must return an error")
	}
	if published != 2 {
		t.Fatalf("gap sequence published, count = %d, want 2", published)
	}

	// Status must expose independent per-kind cursors alongside the samples.
	status, err := p.Status(line)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.SampleCursors[model.SampleCOD] != 1 || status.SampleCursors[model.SampleSludgeBlanket] != 2 {
		t.Fatalf("per-kind cursors = %v, want cod=1 sludge_blanket=2", status.SampleCursors)
	}
	if len(status.Samples) != 2 {
		t.Fatalf("live samples = %d, want one per kind (2)", len(status.Samples))
	}
}

// TestReceiveSampleCursorPersistence ensures the per-kind dedup decision
// survives a restart from the durable cursor file, and that the other kind at
// the same sequence is not mistakenly treated as stale afterwards.
func TestReceiveSampleCursorPersistence(t *testing.T) {
	ctx := context.Background()
	line := model.LineID("line-1")
	dir := t.TempDir()

	first := newTestPlant(t, dir)
	water := model.NewSample(line, "basin-line-1", model.SampleCOD, 1, 20, "mg/L", first.now())
	if err := first.ReceiveSample(ctx, water); err != nil {
		t.Fatalf("receive water seq1: %v", err)
	}

	restarted := newTestPlant(t, dir)
	published := 0
	if err := restarted.sampler.Subscribe(func(model.Sample) { published++ }); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// Same (kind, sequence) after restart is still deduped: no republish.
	replay := model.NewSample(line, "basin-line-1", model.SampleCOD, 1, 21, "mg/L", restarted.now())
	if err := restarted.ReceiveSample(ctx, replay); err != nil {
		t.Fatalf("restarted duplicate must be no-op, got: %v", err)
	}
	if published != 0 {
		t.Fatalf("restarted duplicate published %d times, want 0", published)
	}
	if got := storedSampleEvents(t, restarted, line); got != 1 {
		t.Fatalf("events after restart duplicate = %d, want 1", got)
	}

	// A different kind at sequence 1 remains a legitimate new sample.
	blanket := model.NewSample(line, "settler-line-1", model.SampleSludgeBlanket, 1, 3.0, "m", restarted.now())
	if err := restarted.ReceiveSample(ctx, blanket); err != nil {
		t.Fatalf("restarted blanket seq1: %v", err)
	}
	if published != 1 {
		t.Fatalf("restarted blanket seq1 published %d times, want 1", published)
	}
	if len(restarted.Alarms()) != 1 {
		t.Fatalf("restarted legitimate blanket sample should raise one alarm")
	}
}
