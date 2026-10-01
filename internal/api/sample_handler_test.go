package api

import (
	"bytes"
	"encoding/json"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/fstest"
	"time"

	"github.com/wyw14/cry-97/internal/model"
	"github.com/wyw14/cry-97/internal/process"
)

type sampleResult struct {
	Sample    model.Sample `json:"sample"`
	Stored    bool         `json:"stored"`
	Duplicate bool         `json:"duplicate"`
	Cursor    uint64       `json:"cursor"`
	Error     string       `json:"error"`
}

func newSampleServer(t *testing.T) (*Server, *process.Plant) {
	t.Helper()
	clock := func() time.Time { return time.Date(2026, 9, 30, 8, 0, 0, 0, time.UTC) }
	plant, err := process.NewPlant(t.TempDir(), []model.LineID{"line-1"}, clock)
	if err != nil {
		t.Fatalf("new plant: %v", err)
	}
	var web fs.FS = fstest.MapFS{}
	server, err := NewServer(plant, web)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	return server, plant
}

func postSample(t *testing.T, server *Server, kind string, seq uint64, value float64) (int, sampleResult) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{
		"line_id": "line-1", "basin_id": "settler-line-1", "kind": kind,
		"sequence": seq, "value": value, "unit": "m",
	})
	req := httptest.NewRequest(http.MethodPost, "/api/samples", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)
	var result sampleResult
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode response %q: %v", rec.Body.String(), err)
	}
	return rec.Code, result
}

func TestReceiveSampleEndpoint_FreshStored(t *testing.T) {
	server, _ := newSampleServer(t)

	status, result := postSample(t, server, string(model.SampleWaterLevel), 1, 1.0)
	if status != http.StatusAccepted {
		t.Fatalf("fresh sample status = %d, want 202", status)
	}
	if !result.Stored || result.Duplicate || result.Cursor != 1 {
		t.Fatalf("fresh sample result = %+v", result)
	}
}

func TestReceiveSampleEndpoint_StaleSequenceSucceedsButMarkedDuplicate(t *testing.T) {
	server, plant := newSampleServer(t)

	if _, first := postSample(t, server, string(model.SampleWaterLevel), 1, 1.0); !first.Stored {
		t.Fatalf("first sample not stored: %+v", first)
	}

	// Same sequence, different kind: request still succeeds (idempotent), but
	// is reported as a duplicate and causes no real-time alarm.
	status, result := postSample(t, server, string(model.SampleSludgeBlanket), 1, 3.5)
	if status != http.StatusOK {
		t.Fatalf("stale sample status = %d, want 200", status)
	}
	if result.Stored || !result.Duplicate || result.Cursor != 1 {
		t.Fatalf("stale sample result = %+v, want duplicate at cursor 1", result)
	}
	for _, a := range plant.Alarms() {
		if a.Active() {
			t.Fatalf("stale sample raised alarm: %+v", a)
		}
	}
}

func TestReceiveSampleEndpoint_FreshBlanketAlarmsThenNextStored(t *testing.T) {
	server, plant := newSampleServer(t)

	postSample(t, server, string(model.SampleWaterLevel), 1, 1.0)
	postSample(t, server, string(model.SampleSludgeBlanket), 1, 3.5) // stale, no alarm

	status, result := postSample(t, server, string(model.SampleSludgeBlanket), 2, 3.5)
	if status != http.StatusAccepted || !result.Stored || result.Cursor != 2 {
		t.Fatalf("legitimate seq 2 = status %d result %+v", status, result)
	}
	found := false
	for _, a := range plant.Alarms() {
		if a.Code == "SETTLING_BLANKET_HIGH" && a.Active() {
			found = true
		}
	}
	if !found {
		t.Fatal("legitimate high blanket sample must raise SETTLING_BLANKET_HIGH")
	}
}

func TestReceiveSampleEndpoint_SequenceGapConflict(t *testing.T) {
	server, _ := newSampleServer(t)

	postSample(t, server, string(model.SampleWaterLevel), 1, 1.0)
	status, result := postSample(t, server, string(model.SampleSludgeBlanket), 5, 3.5)
	if status != http.StatusConflict {
		t.Fatalf("gap status = %d, want 409", status)
	}
	if result.Error == "" {
		t.Fatal("gap response must carry an error message")
	}
}
