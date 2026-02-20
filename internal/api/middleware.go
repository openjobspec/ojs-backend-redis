package api

import (
	"net/http"

	commonapi "github.com/openjobspec/ojs-go-backend-common/api"
)

// OJSHeaders middleware adds required OJS response headers.
func OJSHeaders(next http.Handler) http.Handler {
	return commonapi.OJSHeaders(next)
}

// ValidateContentType middleware validates the Content-Type header for POST requests.
func ValidateContentType(next http.Handler) http.Handler {
	return commonapi.ValidateContentType(next)
}

// LimitRequestBody middleware limits the size of request bodies to prevent DoS.
func LimitRequestBody(next http.Handler) http.Handler {
	return commonapi.LimitRequestBody(next)
}
