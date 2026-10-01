package sensor

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/wyw14/cry-97/internal/journal"
	"github.com/wyw14/cry-97/internal/model"
)

// Cursor tracks the durable high-water mark of every per-kind sample sequence.
type Cursor interface {
	Current(model.LineID, model.SampleKind) uint64
	Commit(model.LineID, model.SampleKind, uint64) error
}

type Receiver struct {
	journal journal.Appender
	cursor  Cursor
}

func NewReceiver(appender journal.Appender, cursor Cursor) (*Receiver, error) {
	if appender == nil || cursor == nil {
		return nil, errors.New("sample receiver requires journal and cursor")
	}
	return &Receiver{journal: appender, cursor: cursor}, nil
}

// Receive stores a sample. It returns accepted=true only when the sample was
// appended and its cursor advanced. A stale or duplicate sequence for the
// sample's kind returns accepted=false with no error and no side effect: it is
// neither appended nor committed, and callers must not publish it to live
// subscribers. A sequence that would skip a number is a gap and is rejected.
//
// Sequences are scoped to (line, sample kind): a water-quality sample and a
// sludge-blanket sample on the same line are independent streams and may both
// legitimately arrive at sequence 1.
func (r *Receiver) Receive(ctx context.Context, sample model.Sample, now time.Time) (accepted bool, err error) {
	if !sample.Finite() || sample.Sequence == 0 || sample.LineID == "" || sample.Kind == "" || sample.ObservedAt.Equal(time.Time{}) {
		return false, errors.New("sample is invalid")
	}
	current := r.cursor.Current(sample.LineID, sample.Kind)
	if sample.Sequence <= current {
		// Already persisted: an expired/duplicate sequence. No event, no cursor
		// movement, and no realtime side effect.
		return false, nil
	}
	if sample.Sequence != current+1 {
		return false, fmt.Errorf("sample sequence gap: current=%d received=%d", current, sample.Sequence)
	}
	event, err := model.NewEvent(sample.LineID, 1, model.EventSampleStored, sample, now)
	if err != nil {
		return false, err
	}
	if _, err := r.journal.Append(ctx, event); err != nil {
		return false, fmt.Errorf("store sample %d failed: %w", sample.Sequence, err)
	}
	if err := r.cursor.Commit(sample.LineID, sample.Kind, sample.Sequence); err != nil {
		return false, fmt.Errorf("commit sample cursor: %w", err)
	}
	return true, nil
}

// Cursors returns the durable per-kind sample cursors for a line.
func (r *Receiver) Cursors(lineID model.LineID) map[model.SampleKind]uint64 {
	if provider, ok := r.cursor.(interface {
		LineCursors(model.LineID) map[model.SampleKind]uint64
	}); ok {
		return provider.LineCursors(lineID)
	}
	return nil
}
