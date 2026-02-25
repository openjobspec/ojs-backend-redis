package api

import (
	commonapi "github.com/openjobspec/ojs-go-backend-common/api"
	"github.com/openjobspec/ojs-go-backend-common/core"

	"github.com/openjobspec/ojs-backend-redis/internal/metrics"
)

// JobHandler handles job-related HTTP endpoints.
// Delegates to the shared commonapi.JobHandler with Redis metrics.
type JobHandler = commonapi.JobHandler

// NewJobHandler creates a new JobHandler with Redis Prometheus metrics wired in.
func NewJobHandler(backend core.Backend) *JobHandler {
	h := commonapi.NewJobHandler(backend)
	h.SetMetrics(&redisJobMetrics{})
	return h
}

// RequestToJob converts an EnqueueRequest into a Job for backend Push.
var RequestToJob = commonapi.RequestToJob

// redisJobMetrics adapts Redis Prometheus metrics to the shared MetricsCollector interface.
type redisJobMetrics struct{}

func (m *redisJobMetrics) JobEnqueued(queue, jobType string) {
	metrics.JobsEnqueued.WithLabelValues(queue, jobType).Inc()
}
func (m *redisJobMetrics) JobFetched(queue string) {
	metrics.JobsFetched.WithLabelValues(queue).Inc()
}
func (m *redisJobMetrics) JobCompleted(queue, jobType string) {
	metrics.JobsCompleted.WithLabelValues(queue, jobType).Inc()
}
func (m *redisJobMetrics) JobFailed(queue, jobType string) {
	metrics.JobsFailed.WithLabelValues(queue, jobType).Inc()
}
func (m *redisJobMetrics) JobCancelled(queue, jobType string) {
	metrics.JobsCancelled.WithLabelValues(queue, jobType).Inc()
}
func (m *redisJobMetrics) ActiveJobInc(queue string) {
	metrics.ActiveJobs.WithLabelValues(queue).Inc()
	metrics.JobsActive.Inc()
}
func (m *redisJobMetrics) ActiveJobDec(queue string) {
	metrics.ActiveJobs.WithLabelValues(queue).Dec()
	metrics.JobsActive.Dec()
}
func (m *redisJobMetrics) FetchDuration(seconds float64) {
	metrics.FetchDuration.Observe(seconds)
}

