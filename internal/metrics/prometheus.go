package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	JobsEnqueuedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "taskflow_jobs_enqueued_total",
			Help: "Total number of jobs enqueued",
		},
		[]string{"type", "priority"},
	)

	JobsCompletedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "taskflow_jobs_completed_total",
			Help: "Total number of jobs completed",
		},
		[]string{"type"},
	)

	JobsFailedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "taskflow_jobs_failed_total",
			Help: "Total number of jobs failed",
		},
		[]string{"type"},
	)

	JobsDeadTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "taskflow_jobs_dead_total",
			Help: "Total number of jobs moved to dead-letter queue",
		},
		[]string{"type"},
	)

	QueueDepth = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "taskflow_queue_depth",
			Help: "Current depth of job queues",
		},
		[]string{"queue"},
	)

	CircuitBreakerState = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "taskflow_circuit_breaker_state",
			Help: "Circuit breaker state (0=closed, 1=open)",
		},
		[]string{"type"},
	)

	ProcessingDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "taskflow_worker_processing_duration_seconds",
			Help:    "Time spent processing jobs",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"type"},
	)
)

func init() {
	prometheus.MustRegister(JobsEnqueuedTotal)
	prometheus.MustRegister(JobsCompletedTotal)
	prometheus.MustRegister(JobsFailedTotal)
	prometheus.MustRegister(JobsDeadTotal)
	prometheus.MustRegister(QueueDepth)
	prometheus.MustRegister(CircuitBreakerState)
	prometheus.MustRegister(ProcessingDuration)
}

func Handler() http.Handler {
	return promhttp.Handler()
}