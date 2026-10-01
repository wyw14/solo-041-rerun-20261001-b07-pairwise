package process

import (
	"context"
	"testing"
	"time"

	"github.com/wyw14/cry-97/internal/model"
)

func newTestPlant(t *testing.T) *Plant {
	t.Helper()
	clock := func() time.Time { return time.Date(2026, 9, 30, 8, 0, 0, 0, time.UTC) }
	plant, err := NewPlant(t.TempDir(), []model.LineID{"line-1"}, clock)
	if err != nil {
		t.Fatalf("new plant: %v", err)
	}
	return plant
}

func sample(kind model.SampleKind, seq uint64, value float64) model.Sample {
	return model.NewSample("line-1", "settler-line-1", kind, seq, value, "m",
		time.Date(2026, 9, 30, 8, 0, 0, 0, time.UTC))
}

func activeAlarmCount(plant *Plant) int {
	total := 0
	for _, a := range plant.Alarms() {
		if a.LineID == "line-1" && a.Active() {
			total++
		}
	}
	return total
}

func TestReceiveSample_StaleSequenceHasNoRealtimeSideEffects(t *testing.T) {
	plant := newTestPlant(t)
	ctx := context.Background()

	// Sequence 1: a water-quality sample is accepted normally.
	receipt, err := plant.ReceiveSample(ctx, sample(model.SampleWaterLevel, 1, 1.0))
	if err != nil {
		t.Fatalf("receive water seq 1: %v", err)
	}
	if !receipt.Stored {
		t.Fatal("water seq 1 should be stored")
	}

	// Same sequence, different kind (sludge blanket), high enough to alarm.
	// It is stale: cursor already covers seq 1, so it must be acknowledged
	// without entering the real-time sampler or raising the blanket alarm.
	stale, err := plant.ReceiveSample(ctx, sample(model.SampleSludgeBlanket, 1, 3.5))
	if err != nil {
		t.Fatalf("stale sample must be acknowledged without error, got: %v", err)
	}
	if stale.Stored {
		t.Fatal("same-sequence different-kind sample must not be stored")
	}
	if got := activeAlarmCount(plant); got != 0 {
		t.Fatalf("stale sample raised %d alarms, want 0", got)
	}
	if plant.receiver.Current("line-1") != 1 {
		t.Fatalf("cursor moved to %d, want 1", plant.receiver.Current("line-1"))
	}
	status, err := plant.Status("line-1")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if len(status.Samples) != 1 || status.Samples[0].Kind != model.SampleWaterLevel {
		t.Fatalf("sampler snapshot = %+v, want only the water sample", status.Samples)
	}
	if status.Settling.High {
		t.Fatal("stale blanket sample must not update settling state")
	}
}

func TestReceiveSample_LegitimateNewSampleTriggersAlarm(t *testing.T) {
	plant := newTestPlant(t)
	ctx := context.Background()

	if _, err := plant.ReceiveSample(ctx, sample(model.SampleWaterLevel, 1, 1.0)); err != nil {
		t.Fatalf("receive water seq 1: %v", err)
	}
	if _, err := plant.ReceiveSample(ctx, sample(model.SampleSludgeBlanket, 1, 3.5)); err != nil {
		t.Fatalf("stale sludge seq 1: %v", err)
	}
	if activeAlarmCount(plant) != 0 {
		t.Fatal("stale seq 1 must not raise an alarm")
	}

	// Legitimate next sequence carrying a high blanket level must still alarm.
	receipt, err := plant.ReceiveSample(ctx, sample(model.SampleSludgeBlanket, 2, 3.5))
	if err != nil {
		t.Fatalf("receive sludge seq 2: %v", err)
	}
	if !receipt.Stored || receipt.Cursor != 2 {
		t.Fatalf("seq 2 should be stored, got %+v", receipt)
	}
	alarms := plant.Alarms()
	if len(alarms) != 1 || alarms[0].Code != "SETTLING_BLANKET_HIGH" {
		t.Fatalf("alarms = %+v, want one SETTLING_BLANKET_HIGH", alarms)
	}
	status, _ := plant.Status("line-1")
	if !status.Settling.High {
		t.Fatal("legitimate high blanket sample must mark settling state high")
	}
	if len(status.Samples) != 2 {
		t.Fatalf("sampler snapshot = %d samples, want 2 (one per kind)", len(status.Samples))
	}

	// Replaying the same sequence (even a different value) is a no-op: no new
	// event, no cursor movement, no second alarm.
	duplicate, err := plant.ReceiveSample(ctx, sample(model.SampleSludgeBlanket, 2, 0.5))
	if err != nil {
		t.Fatalf("duplicate seq 2: %v", err)
	}
	if duplicate.Stored {
		t.Fatal("duplicate seq 2 must not be stored")
	}
	if activeAlarmCount(plant) != 1 {
		t.Fatalf("duplicate changed alarms, active = %d, want 1", activeAlarmCount(plant))
	}
	if plant.receiver.Current("line-1") != 2 {
		t.Fatalf("cursor = %d, want 2", plant.receiver.Current("line-1"))
	}
}

func TestReceiveSample_SequenceGapRejected(t *testing.T) {
	plant := newTestPlant(t)
	ctx := context.Background()

	if _, err := plant.ReceiveSample(ctx, sample(model.SampleWaterLevel, 1, 1.0)); err != nil {
		t.Fatalf("receive seq 1: %v", err)
	}
	if _, err := plant.ReceiveSample(ctx, sample(model.SampleSludgeBlanket, 3, 3.5)); err == nil {
		t.Fatal("seq 3 with cursor 1 must be rejected as a gap")
	}
	if activeAlarmCount(plant) != 0 {
		t.Fatal("gapped sample must not trigger an alarm")
	}
}
