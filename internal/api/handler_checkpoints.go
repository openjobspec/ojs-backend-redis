package api

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/openjobspec/ojs-go-backend-common/core"
)

// CheckpointHandler handles checkpoint HTTP endpoints.
type CheckpointHandler struct {
	checkpoints core.CheckpointManager
	backend     core.Backend
}

// NewCheckpointHandler creates a new CheckpointHandler.
func NewCheckpointHandler(checkpoints core.CheckpointManager, backend core.Backend) *CheckpointHandler {
	return &CheckpointHandler{checkpoints: checkpoints, backend: backend}
}

// Save handles POST /ojs/v1/jobs/{id}/checkpoint
func (h *CheckpointHandler) Save(w http.ResponseWriter, r *http.Request) {
	jobID := chi.URLParam(r, "id")

	// Validate job exists and is active
	job, err := h.backend.Info(r.Context(), jobID)
	if err != nil {
		HandleError(w, err)
		return
	}
	if job.State != "active" {
		WriteError(w, http.StatusConflict, core.NewConflictError(
			"Checkpoints can only be saved for active jobs.",
			map[string]any{"job_id": jobID, "state": job.State},
		))
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		WriteError(w, http.StatusBadRequest, core.NewInvalidRequestError("Failed to read request body.", nil))
		return
	}

	// Enforce 1MB size limit
	if len(body) > 1048576 {
		WriteError(w, http.StatusRequestEntityTooLarge, core.NewInvalidRequestError(
			"Checkpoint state exceeds 1MB size limit.",
			map[string]any{"size_bytes": len(body), "max_bytes": 1048576},
		))
		return
	}

	var req core.SaveCheckpointRequest
	if err := json.Unmarshal(body, &req); err != nil {
		WriteError(w, http.StatusBadRequest, core.NewInvalidRequestError("Invalid JSON in request body.", nil))
		return
	}

	if len(req.State) == 0 {
		WriteError(w, http.StatusBadRequest, core.NewInvalidRequestError("The 'state' field is required.", nil))
		return
	}

	if err := h.checkpoints.SaveCheckpoint(r.Context(), jobID, req.State); err != nil {
		HandleError(w, err)
		return
	}

	// Retrieve the saved checkpoint to return sequence and timestamp
	cp, err := h.checkpoints.GetCheckpoint(r.Context(), jobID)
	if err != nil {
		HandleError(w, err)
		return
	}

	WriteJSON(w, http.StatusOK, map[string]any{"checkpoint": map[string]any{
		"job_id":     cp.JobID,
		"sequence":   cp.Sequence,
		"created_at": cp.CreatedAt,
	}})
}

// Get handles GET /ojs/v1/jobs/{id}/checkpoint
func (h *CheckpointHandler) Get(w http.ResponseWriter, r *http.Request) {
	jobID := chi.URLParam(r, "id")

	cp, err := h.checkpoints.GetCheckpoint(r.Context(), jobID)
	if err != nil {
		HandleError(w, err)
		return
	}

	WriteJSON(w, http.StatusOK, map[string]any{"checkpoint": cp})
}

// Delete handles DELETE /ojs/v1/jobs/{id}/checkpoint
func (h *CheckpointHandler) Delete(w http.ResponseWriter, r *http.Request) {
	jobID := chi.URLParam(r, "id")

	err := h.checkpoints.DeleteCheckpoint(r.Context(), jobID)
	if err != nil {
		HandleError(w, err)
		return
	}

	WriteJSON(w, http.StatusOK, map[string]any{"deleted": true, "job_id": jobID})
}
