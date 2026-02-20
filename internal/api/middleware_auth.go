package api

import (
	"net/http"

	commonapi "github.com/openjobspec/ojs-go-backend-common/api"
)

// KeyAuth returns a middleware that validates Bearer token authentication.
func KeyAuth(apiKey string, skipPaths ...string) func(http.Handler) http.Handler {
	return commonapi.KeyAuth(apiKey, skipPaths...)
}
