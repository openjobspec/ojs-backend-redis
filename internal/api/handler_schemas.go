package api

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/openjobspec/ojs-go-backend-common/core"
)

// SchemaHandler handles schema registry HTTP endpoints.
type SchemaHandler struct {
	registry core.SchemaRegistry
}

// NewSchemaHandler creates a new SchemaHandler.
func NewSchemaHandler(registry core.SchemaRegistry) *SchemaHandler {
	return &SchemaHandler{registry: registry}
}

// Register handles POST /ojs/v1/schemas
func (h *SchemaHandler) Register(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		WriteError(w, http.StatusBadRequest, core.NewInvalidRequestError("Failed to read request body.", nil))
		return
	}

	var req core.RegisterSchemaRequest
	if err := json.Unmarshal(body, &req); err != nil {
		WriteError(w, http.StatusBadRequest, core.NewInvalidRequestError("Invalid JSON in request body.", nil))
		return
	}

	if req.JobType == "" {
		WriteError(w, http.StatusBadRequest, core.NewInvalidRequestError("The 'job_type' field is required.", nil))
		return
	}
	if req.Version == "" {
		WriteError(w, http.StatusBadRequest, core.NewInvalidRequestError("The 'version' field is required.", nil))
		return
	}
	if len(req.Schema) == 0 {
		WriteError(w, http.StatusBadRequest, core.NewInvalidRequestError("The 'schema' field is required.", nil))
		return
	}

	// Validate that schema is valid JSON
	if !json.Valid(req.Schema) {
		WriteError(w, http.StatusBadRequest, core.NewInvalidRequestError("The 'schema' field must be valid JSON.", nil))
		return
	}

	schema, err := h.registry.RegisterSchema(r.Context(), req.JobType, req.Version, req.Schema)
	if err != nil {
		HandleError(w, err)
		return
	}

	WriteJSON(w, http.StatusCreated, map[string]any{"schema": schema})
}

// Get handles GET /ojs/v1/schemas/{type}
func (h *SchemaHandler) Get(w http.ResponseWriter, r *http.Request) {
	jobType := chi.URLParam(r, "type")

	schema, err := h.registry.GetSchema(r.Context(), jobType)
	if err != nil {
		HandleError(w, err)
		return
	}

	WriteJSON(w, http.StatusOK, map[string]any{"schema": schema})
}

// ListVersions handles GET /ojs/v1/schemas/{type}/versions
func (h *SchemaHandler) ListVersions(w http.ResponseWriter, r *http.Request) {
	jobType := chi.URLParam(r, "type")

	versions, err := h.registry.ListVersions(r.Context(), jobType)
	if err != nil {
		HandleError(w, err)
		return
	}

	if versions == nil {
		versions = []*core.SchemaVersion{}
	}

	WriteJSON(w, http.StatusOK, map[string]any{"versions": versions})
}

// Delete handles DELETE /ojs/v1/schemas/{type}/{version}
func (h *SchemaHandler) Delete(w http.ResponseWriter, r *http.Request) {
	jobType := chi.URLParam(r, "type")
	version := chi.URLParam(r, "version")

	err := h.registry.DeleteSchema(r.Context(), jobType, version)
	if err != nil {
		HandleError(w, err)
		return
	}

	WriteJSON(w, http.StatusOK, map[string]any{"deleted": true, "job_type": jobType, "version": version})
}
