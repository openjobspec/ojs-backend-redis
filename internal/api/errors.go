package api

import (
	"net/http"

	commonapi "github.com/openjobspec/ojs-go-backend-common/api"
	"github.com/openjobspec/ojs-go-backend-common/core"
	"github.com/openjobspec/ojs-go-backend-common/httputil"
)

// ErrorResponse wraps an OJS error for JSON serialization.
type ErrorResponse = httputil.ErrorResponse

// WriteError writes an OJS-formatted error response.
func WriteError(w http.ResponseWriter, status int, err *core.OJSError) {
	commonapi.WriteError(w, status, err)
}

// WriteJSON writes a JSON response with the given status code.
func WriteJSON(w http.ResponseWriter, status int, data any) {
	commonapi.WriteJSON(w, status, data)
}

// WriteOJSError maps an OJSError to the appropriate HTTP status code and writes it.
func WriteOJSError(w http.ResponseWriter, err *core.OJSError) {
	commonapi.WriteOJSError(w, err)
}

// HandleError dispatches an error as an HTTP response.
func HandleError(w http.ResponseWriter, err error) {
	commonapi.HandleError(w, err)
}
