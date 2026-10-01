package sensor

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/wyw14/cry-97/internal/journal"
	"github.com/wyw14/cry-97/internal/model"
)

type Cursor interface {
	Current(model.LineID) uint64
	Commit(model.LineID, uint64) error
}

// Receipt describes how a submitted sample was handled. Stored is true only for
// a sample that was durably journaled and advanced the line cursor; callers may
// publish such a sample to real-time subscribers. A stale or duplicate sample
// is acknowledged idempotently with Stored false and must have no side effects.
type Receipt struct {
	Sample model.Sample
	Stored bool
	Cursor uint64
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

func (r *Receiver) Receive(ctx context.Context, sample model.Sample, now time.Time) (Receipt, error) {
	if !sample.Finite() || sample.Sequence == 0 || sample.LineID == "" ||
		sample.Kind == "" || sample.ObservedAt.Equal(time.Time{}) {
		return Receipt{Sample: sample}, errors.New("sample is invalid")
	}
	current := r.cursor.Current(sample.LineID)
	if sample.Sequence <= current {
		// Already covered by the durable cursor: idempotent acknowledgement,
		// regardless of sample kind. Never journal or publish it again.
		return Receipt{Sample: sample, Stored: false, Cursor: current}, nil
	}
	if sample.Sequence != current+1 {
		return Receipt{Sample: sample, Cursor: current},
			fmt.Errorf("sample sequence gap: current=%d received=%d", current, sample.Sequence)
	}
	event, err := model.NewEvent(sample.LineID, 1, model.EventSampleStored, sample, now)
	if err != nil {
		return Receipt{Sample: sample, Cursor: current}, err
	}
	if _, err := r.journal.Append(ctx, event); err != nil {
		return Receipt{Sample: sample, Cursor: current},
			fmt.Errorf("store sample %d failed: %w", sample.Sequence, err)
	}
	if err := r.cursor.Commit(sample.LineID, sample.Sequence); err != nil {
		return Receipt{Sample: sample, Cursor: current}, fmt.Errorf("commit sample cursor: %w", err)
	}
	return Receipt{Sample: sample, Stored: true, Cursor: sample.Sequence}, nil
}

func (r *Receiver) Current(lineID model.LineID) uint64 {
	return r.cursor.Current(lineID)
}
