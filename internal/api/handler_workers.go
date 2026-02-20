package api

import (
	commonapi "github.com/openjobspec/ojs-go-backend-common/api"
	"github.com/openjobspec/ojs-go-backend-common/core"
)

// WorkerHandler handles worker-related HTTP endpoints.
// Delegates to the shared commonapi.WorkerHandler with Redis metrics.
type WorkerHandler = commonapi.WorkerHandler

// NewWorkerHandler creates a new WorkerHandler with Redis Prometheus metrics.
func NewWorkerHandler(backend core.Backend) *WorkerHandler {
	h := commonapi.NewWorkerHandler(backend)
	h.SetMetrics(&redisJobMetrics{})
	return h
}
