package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestJobsEnqueuedCounter(t *testing.T) {
	before := testutil.ToFloat64(JobsEnqueued.WithLabelValues("default", "email.send"))
	JobsEnqueued.WithLabelValues("default", "email.send").Inc()
	after := testutil.ToFloat64(JobsEnqueued.WithLabelValues("default", "email.send"))

	if after != before+1 {
		t.Errorf("JobsEnqueued delta = %f, want +1", after-before)
	}
}

func TestJobsCompletedCounter(t *testing.T) {
	before := testutil.ToFloat64(JobsCompleted.WithLabelValues("default", "email.send"))
	JobsCompleted.WithLabelValues("default", "email.send").Inc()
	after := testutil.ToFloat64(JobsCompleted.WithLabelValues("default", "email.send"))

	if after != before+1 {
		t.Errorf("JobsCompleted delta = %f, want +1", after-before)
	}
}

func TestJobsFailedCounter(t *testing.T) {
	before := testutil.ToFloat64(JobsFailed.WithLabelValues("critical", "payment.process"))
	JobsFailed.WithLabelValues("critical", "payment.process").Inc()
	after := testutil.ToFloat64(JobsFailed.WithLabelValues("critical", "payment.process"))

	if after != before+1 {
		t.Errorf("JobsFailed delta = %f, want +1", after-before)
	}
}

func TestJobsCancelledCounter(t *testing.T) {
	before := testutil.ToFloat64(JobsCancelled.WithLabelValues("default", "report.generate"))
	JobsCancelled.WithLabelValues("default", "report.generate").Inc()
	after := testutil.ToFloat64(JobsCancelled.WithLabelValues("default", "report.generate"))

	if after != before+1 {
		t.Errorf("JobsCancelled delta = %f, want +1", after-before)
	}
}

func TestJobDurationHistogram(t *testing.T) {
	JobDuration.WithLabelValues("default", "email.send").Observe(0.5)
	JobDuration.WithLabelValues("default", "email.send").Observe(1.2)

	// Verify the histogram collects without panicking; we use the vec directly.
	count := testutil.CollectAndCount(JobDuration)
	if count == 0 {
		t.Error("JobDuration should have at least one metric")
	}
}

func TestJobWaitTimeHistogram(t *testing.T) {
	JobWaitTime.WithLabelValues("default", "email.send").Observe(0.1)

	count := testutil.CollectAndCount(JobWaitTime)
	if count == 0 {
		t.Error("JobWaitTime should have at least one metric")
	}
}

func TestQueueDepthGauge(t *testing.T) {
	QueueDepth.WithLabelValues("default").Set(42)
	val := testutil.ToFloat64(QueueDepth.WithLabelValues("default"))
	if val != 42 {
		t.Errorf("QueueDepth = %f, want 42", val)
	}
}

func TestWorkersActiveGauge(t *testing.T) {
	WorkersActive.WithLabelValues("default").Set(5)
	val := testutil.ToFloat64(WorkersActive.WithLabelValues("default"))
	if val != 5 {
		t.Errorf("WorkersActive = %f, want 5", val)
	}
}

func TestJobsActiveGauge(t *testing.T) {
	JobsActive.Set(0)
	JobsActive.Inc()
	JobsActive.Inc()
	val := testutil.ToFloat64(JobsActive)
	if val != 2 {
		t.Errorf("JobsActive = %f, want 2", val)
	}
}

func TestServerInfoInit(t *testing.T) {
	Init("1.0.0", "redis")
	val := testutil.ToFloat64(ServerInfo.WithLabelValues("1.0.0", "redis"))
	if val != 1 {
		t.Errorf("ServerInfo = %f, want 1", val)
	}
}

func TestHTTPRequestsTotal(t *testing.T) {
	before := testutil.ToFloat64(HTTPRequestsTotal.WithLabelValues("GET", "/test", "200"))
	HTTPRequestsTotal.WithLabelValues("GET", "/test", "200").Inc()
	after := testutil.ToFloat64(HTTPRequestsTotal.WithLabelValues("GET", "/test", "200"))

	if after != before+1 {
		t.Errorf("HTTPRequestsTotal delta = %f, want +1", after-before)
	}
}

func TestHTTPRequestDuration(t *testing.T) {
	HTTPRequestDuration.WithLabelValues("POST", "/ojs/v1/jobs", "201").Observe(0.05)

	count := testutil.CollectAndCount(HTTPRequestDuration)
	if count == 0 {
		t.Error("HTTPRequestDuration should have at least one metric")
	}
}
