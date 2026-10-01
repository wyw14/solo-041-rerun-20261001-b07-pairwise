package api

import (
	"io"
	"net/http"
	"time"

	"github.com/wyw14/cry-97/internal/model"
)

type sampleRequest struct {
	LineID     string  `json:"line_id"`
	BasinID    string  `json:"basin_id"`
	Kind       string  `json:"kind"`
	Sequence   uint64  `json:"sequence"`
	Value      float64 `json:"value"`
	Unit       string  `json:"unit"`
	ObservedAt string  `json:"observed_at"`
}

type sampleResponse struct {
	Sample    model.Sample `json:"sample"`
	Stored    bool         `json:"stored"`
	Duplicate bool         `json:"duplicate"`
	Cursor    uint64       `json:"cursor"`
}

func (s *Server) receiveSample(w http.ResponseWriter, r *http.Request) {
	var input sampleRequest
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	lineID, err := model.ParseLineID(input.LineID)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	kind, err := model.ParseSampleKind(input.Kind)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	observedAt := s.plant.Clock()
	if input.ObservedAt != "" {
		observedAt, err = time.Parse(time.RFC3339, input.ObservedAt)
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
	}
	sample := model.NewSample(lineID, input.BasinID, kind, input.Sequence, input.Value, input.Unit, observedAt)
	receipt, err := s.plant.ReceiveSample(r.Context(), sample)
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	// A stale/duplicate sequence is acknowledged idempotently: the request
	// still succeeds, but nothing was stored or pushed to real-time consumers.
	response := sampleResponse{Sample: receipt.Sample, Stored: receipt.Stored, Cursor: receipt.Cursor}
	if receipt.Stored {
		writeJSON(w, http.StatusAccepted, response)
		return
	}
	response.Duplicate = true
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) labResult(w http.ResponseWriter, r *http.Request) {
	payload, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	outcome, err := s.plant.SubmitLabPayload(r.Context(), payload)
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, err)
		return
	}
	writeJSON(w, http.StatusAccepted, outcome)
}
