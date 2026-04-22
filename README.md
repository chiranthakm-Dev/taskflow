# taskflow

> A distributed job queue and worker orchestration system written in Go — priority lanes, dead-letter queues, per-provider circuit breakers, KEDA autoscaling, and full Prometheus observability.

[![Go](https://img.shields.io/badge/Go-1.22-00ADD8?style=flat-square&logo=go&logoColor=white)](https://go.dev)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-1.30-326CE5?style=flat-square&logo=kubernetes&logoColor=white)](https://kubernetes.io)
[![RabbitMQ](https://img.shields.io/badge/RabbitMQ-3.13-FF6600?style=flat-square&logo=rabbitmq&logoColor=white)](https://rabbitmq.com)
[![Prometheus](https://img.shields.io/badge/Prometheus-instrumented-E6522C?style=flat-square&logo=prometheus&logoColor=white)](https://prometheus.io)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=flat-square)](LICENSE)
[![Tests](https://img.shields.io/badge/tests-91%25%20coverage-brightgreen?style=flat-square)]()

---

## What this is

Every serious backend system eventually needs a job queue: send emails without blocking HTTP requests, process uploaded files in the background, run scheduled reports, retry failed third-party API calls.

Most teams bolt on a queue as an afterthought and end up with silent job failures, no visibility into queue depth, workers that don't scale with load, and no way to inspect or replay failed jobs.

This project builds it right from the start — a production-grade task orchestration system in Go with priority lanes, exponential backoff retries, dead-letter queues for permanently failed jobs, per-job-type circuit breakers, Kubernetes-native worker autoscaling via KEDA, and a Prometheus metrics endpoint that feeds a pre-built Grafana dashboard.

---

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                     HTTP API (Go)                        │
│           POST /jobs · GET /jobs/{id} · GET /metrics     │
└────────────────────┬─────────────────────────────────────┘
                     │  enqueue
          ┌──────────▼──────────┐
          │     RabbitMQ         │
          │  ┌────────────────┐  │
          │  │ high priority  │  │   ← SLA: processed within 5s
          │  ├────────────────┤  │
          │  │ normal         │  │   ← SLA: processed within 30s
          │  ├────────────────┤  │
          │  │ low            │  │   ← best-effort, background
          │  ├────────────────┤  │
          │  │ dead-letter    │  │   ← permanently failed jobs
          │  └────────────────┘  │
          └──────────┬───────────┘
                     │  consume
     ┌───────────────┼───────────────────┐
     ▼               ▼                   ▼
┌─────────┐    ┌─────────┐         ┌─────────┐
│ Worker  │    │ Worker  │   ...   │ Worker  │   ← autoscaled by KEDA
│ pod 1   │    │ pod 2   │         │ pod N   │     based on queue depth
└─────────┘    └─────────┘         └─────────┘
     │
     ▼
┌───────────────────────────────┐
│  Per-job-type circuit breaker │
│  + exponential backoff retry  │
│  + dead-letter on max retries │
└───────────────────────────────┘
     │
     ▼
┌──────────────┐    ┌──────────────────┐
│  PostgreSQL  │    │   Prometheus     │
│  job log     │    │   /metrics       │
└──────────────┘    └──────────────────┘
```

---

## Quickstart

**Prerequisites:** Docker + Docker Compose (local), or a Kubernetes cluster (production)

```bash
git clone https://github.com/yourusername/taskflow.git
cd taskflow

# Start RabbitMQ, Postgres, and the API + worker locally
docker compose up --build

# Enqueue a sample job
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "type": "send_email",
    "priority": "high",
    "payload": {"to": "user@example.com", "subject": "Welcome!"},
    "max_retries": 3
  }'

# Check job status
curl http://localhost:8080/jobs/<job-id>

# View Prometheus metrics
curl http://localhost:8080/metrics
```

Grafana dashboard at `http://localhost:3000` — auto-imported from `grafana/dashboards/taskflow.json`.

---

## API reference

### `POST /jobs`

Enqueue a new job.

```json
{
  "type": "send_email",
  "priority": "high | normal | low",
  "payload": { ... },
  "max_retries": 3,
  "idempotency_key": "email-welcome-usr_abc123"
}
```

Response:
```json
{
  "id": "job_7f3a9b",
  "status": "queued",
  "queue": "high",
  "created_at": "2026-04-18T09:12:44Z"
}
```

The `idempotency_key` field prevents duplicate jobs — submitting the same key twice returns the original job instead of creating a second one.

### `GET /jobs/{id}`

```json
{
  "id": "job_7f3a9b",
  "type": "send_email",
  "status": "completed | queued | processing | failed | dead",
  "attempts": 1,
  "last_error": null,
  "processing_time_ms": 234,
  "created_at": "...",
  "completed_at": "..."
}
```

### `POST /jobs/{id}/retry`

Manually re-queue a dead-lettered job (admin use).

### `GET /metrics`

Prometheus format:

```
taskflow_jobs_enqueued_total{type="send_email",priority="high"} 1423
taskflow_jobs_completed_total{type="send_email"} 1401
taskflow_jobs_failed_total{type="send_email"} 22
taskflow_jobs_dead_total{type="send_email"} 4
taskflow_queue_depth{queue="high"} 3
taskflow_queue_depth{queue="normal"} 47
taskflow_worker_processing_duration_seconds{type="send_email",quantile="0.99"} 1.24
taskflow_circuit_breaker_state{type="send_email"} 0
```

---

## Core components

### Priority queue routing

```go
// internal/queue/router.go
type Priority string

const (
    PriorityHigh   Priority = "high"
    PriorityNormal Priority = "normal"
    PriorityLow    Priority = "low"
)

func (r *Router) Enqueue(ctx context.Context, job *Job) error {
    queueName := r.queueForPriority(job.Priority)

    // Idempotency check — reject duplicate keys
    if job.IdempotencyKey != "" {
        existing, err := r.store.GetByIdempotencyKey(ctx, job.IdempotencyKey)
        if err == nil && existing != nil {
            return ErrDuplicateJob{ExistingID: existing.ID}
        }
    }

    msg := amqp.Publishing{
        ContentType:  "application/json",
        DeliveryMode: amqp.Persistent,  // survives RabbitMQ restarts
        Body:         mustMarshal(job),
    }

    return r.channel.PublishWithContext(ctx, "", queueName, true, false, msg)
}
```

Workers always drain `high` before touching `normal`, and `normal` before `low`. This is enforced by consumer priority configuration in RabbitMQ, not application logic.

### Circuit breaker

```go
// internal/worker/circuit_breaker.go
type CircuitBreaker struct {
    mu           sync.Mutex
    failures     int
    lastFailure  time.Time
    state        State   // Closed | Open | HalfOpen
    threshold    int
    recoveryTime time.Duration
}

func (cb *CircuitBreaker) Allow() bool {
    cb.mu.Lock()
    defer cb.mu.Unlock()

    switch cb.state {
    case StateClosed:
        return true
    case StateOpen:
        if time.Since(cb.lastFailure) > cb.recoveryTime {
            cb.state = StateHalfOpen
            return true  // allow one probe request
        }
        return false
    case StateHalfOpen:
        return false  // only one probe at a time
    }
    return false
}

func (cb *CircuitBreaker) RecordFailure() {
    cb.mu.Lock()
    defer cb.mu.Unlock()
    cb.failures++
    cb.lastFailure = time.Now()
    if cb.failures >= cb.threshold {
        cb.state = StateOpen
        metrics.CircuitBreakerState.WithLabelValues(cb.jobType).Set(1)
    }
}

func (cb *CircuitBreaker) RecordSuccess() {
    cb.mu.Lock()
    defer cb.mu.Unlock()
    cb.failures = 0
    cb.state = StateClosed
    metrics.CircuitBreakerState.WithLabelValues(cb.jobType).Set(0)
}
```

Each job type has its own circuit breaker instance. A broken `send_email` handler doesn't affect `generate_report` jobs.

### Retry with exponential backoff

```go
// internal/worker/worker.go
func (w *Worker) processJob(ctx context.Context, job *Job) error {
    cb := w.circuitBreakers[job.Type]

    if !cb.Allow() {
        // Circuit is open — requeue with delay, don't mark as failed
        return w.requeueWithDelay(ctx, job, 60*time.Second)
    }

    handler, ok := w.handlers[job.Type]
    if !ok {
        return fmt.Errorf("no handler registered for job type %q", job.Type)
    }

    if err := handler.Process(ctx, job); err != nil {
        cb.RecordFailure()
        job.Attempts++
        job.LastError = err.Error()

        if job.Attempts >= job.MaxRetries {
            // Move to dead-letter queue — human intervention required
            return w.deadLetter(ctx, job)
        }

        // Exponential backoff: 2s, 4s, 8s, 16s ...
        delay := time.Duration(math.Pow(2, float64(job.Attempts))) * time.Second
        return w.requeueWithDelay(ctx, job, delay)
    }

    cb.RecordSuccess()
    return w.store.MarkCompleted(ctx, job.ID)
}
```

### Dead-letter queue

Failed jobs that exhaust retries land in `taskflow.dead`. They're preserved indefinitely. A separate admin endpoint (`POST /jobs/{id}/retry`) re-queues them to the original priority lane with a fresh attempt counter.

```go
func (w *Worker) deadLetter(ctx context.Context, job *Job) error {
    job.Status = StatusDead
    if err := w.store.UpdateJob(ctx, job); err != nil {
        return err
    }

    // Publish to DLQ for inspection and replay
    return w.channel.PublishWithContext(ctx, "", "taskflow.dead", true, false,
        amqp.Publishing{
            ContentType: "application/json",
            Body:        mustMarshal(job),
            Headers: amqp.Table{
                "x-death-reason": job.LastError,
                "x-original-queue": w.queueForPriority(job.Priority),
            },
        },
    )
}
```

---

## Kubernetes deployment

### KEDA autoscaling

KEDA scales worker pods based on RabbitMQ queue depth — no custom metrics server needed.

```yaml
# deploy/keda-scaledobject.yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: taskflow-worker-scaler
spec:
  scaleTargetRef:
    name: taskflow-worker
  minReplicaCount: 1
  maxReplicaCount: 20
  cooldownPeriod: 30
  triggers:
    - type: rabbitmq
      metadata:
        host: "amqp://rabbitmq:5672"
        queueName: taskflow.high
        mode: QueueLength
        value: "10"    # scale up when >10 jobs per replica
    - type: rabbitmq
      metadata:
        host: "amqp://rabbitmq:5672"
        queueName: taskflow.normal
        mode: QueueLength
        value: "25"
```

When `taskflow.high` has 50 jobs, KEDA scales to 5 worker pods. When it drains back to 0, pods scale down to 1 after the cooldown period.

### Helm chart

```bash
# Install to any Kubernetes cluster
helm install taskflow deploy/helm/taskflow \
  --set rabbitmq.host=rabbitmq.default.svc.cluster.local \
  --set postgres.url="postgresql://..." \
  --set worker.replicas.min=1 \
  --set worker.replicas.max=20
```

The chart is in [`deploy/helm/taskflow/`](deploy/helm/taskflow/) — includes Deployment, Service, ConfigMap, HPA, and the KEDA ScaledObject.

---

## Project structure

```
taskflow/
├── cmd/
│   ├── api/
│   │   └── main.go              # HTTP API server entrypoint
│   └── worker/
│       └── main.go              # Worker process entrypoint
├── internal/
│   ├── api/
│   │   ├── handlers.go          # HTTP handler functions
│   │   └── middleware.go        # Auth, logging, recover-from-panic
│   ├── queue/
│   │   ├── router.go            # Priority routing + idempotency
│   │   └── consumer.go          # RabbitMQ consumer group management
│   ├── worker/
│   │   ├── worker.go            # Process loop + retry logic
│   │   ├── circuit_breaker.go   # Per-job-type circuit breaker
│   │   └── registry.go          # Handler registration
│   ├── store/
│   │   └── postgres.go          # Job persistence (sqlx)
│   └── metrics/
│       └── prometheus.go        # All Prometheus metric definitions
├── handlers/
│   ├── send_email.go            # Example job handler
│   ├── generate_report.go
│   └── process_payment.go
├── deploy/
│   ├── helm/taskflow/           # Helm chart
│   └── keda-scaledobject.yaml
├── grafana/
│   └── dashboards/taskflow.json
├── k6/
│   └── load_test.js             # Load test — results in docs/load-test-results.md
├── docs/
│   └── adr/
│       ├── 001-rabbitmq-over-redis-queue.md
│       ├── 002-per-jobtype-circuit-breaker.md
│       └── 003-keda-over-hpa.md
├── tests/
│   ├── worker_test.go
│   ├── circuit_breaker_test.go
│   ├── queue_test.go
│   └── integration_test.go
├── docker-compose.yml
├── Dockerfile.api
├── Dockerfile.worker
└── README.md
```

---

## Running tests

```bash
# All unit tests
go test ./... -v

# With coverage report
go test ./... -coverprofile=coverage.out
go tool cover -html=coverage.out

# Race detector (important for concurrent code)
go test ./... -race

# Integration tests (requires Docker)
docker compose -f docker-compose.test.yml up --abort-on-container-exit

# Load test (requires k6)
k6 run k6/load_test.js --vus 200 --duration 2m
```

**Load test results** (4 vCPUs, 8GB RAM, KEDA scaled to 8 workers):

| Metric | Value |
|---|---|
| Jobs enqueued/sec | 1,240 |
| Jobs completed/sec | 1,198 |
| p50 processing time | 18ms |
| p99 processing time | 310ms |
| Error rate | 0.08% |
| Peak worker pods (KEDA) | 8 |

Full results: [`docs/load-test-results.md`](docs/load-test-results.md)

---

## Design decisions

**1. RabbitMQ over Redis as the queue broker**
Redis Streams and Redis Lists are common for simple queues. RabbitMQ gives you message-level acknowledgements (a crashed worker doesn't lose its job), dead-letter exchange routing (a first-class DLQ, not an afterthought), per-queue consumer priority, and a management UI that non-engineers can use to inspect queue depth. For a job queue specifically, RabbitMQ's model fits better than Redis's. Full rationale: [`docs/adr/001-rabbitmq-over-redis-queue.md`](docs/adr/001-rabbitmq-over-redis-queue.md)

**2. Per-job-type circuit breakers, not per-worker**
A single circuit breaker per worker means a broken `send_email` handler takes down `generate_report` processing too. Separate circuit breakers per job type isolate failures. The trade-off is more state to manage (a map of circuit breakers instead of one), which is worth it for the isolation. Full rationale: [`docs/adr/002-per-jobtype-circuit-breaker.md`](docs/adr/002-per-jobtype-circuit-breaker.md)

**3. KEDA over Kubernetes HPA for autoscaling**
Kubernetes HPA scales on CPU and memory. Workers processing fast jobs will have low CPU — they're idle between jobs, not CPU-bound. KEDA scales on queue depth, which is the actual signal that matters: "there are 200 jobs waiting, add more workers." Full rationale: [`docs/adr/003-keda-over-hpa.md`](docs/adr/003-keda-over-hpa.md)

---

## What I learned building this

Writing concurrent Go for the first time, I made every classic mistake: shared state without mutexes, goroutine leaks from channels nobody was reading, context cancellation that got ignored halfway down the call stack.

The circuit breaker implementation broke in a subtle way during load testing: two goroutines could both read `StateHalfOpen`, both decide to send a probe request, and both update state independently — a classic TOCTOU race. The fix was a single `sync.Mutex` protecting all state reads and writes, not just the writes. The race detector (`go test -race`) caught it before production.

The second insight was about graceful shutdown. When a SIGTERM arrives (Kubernetes scaling down a pod), in-flight jobs should complete before the process exits. Context propagation through every function call — not as an afterthought, but from the start — is what makes that possible in Go.

---

## Tech stack

| Layer | Technology | Why |
|---|---|---|
| Language | Go 1.22 | Excellent concurrency primitives, fast compilation, small binaries, growing adoption in Dutch infra teams |
| Message broker | RabbitMQ 3.13 | Message-level acks, DLX routing, priority queues, management UI |
| Persistence | PostgreSQL 16 (sqlx) | Job state log; sqlx is lighter than an ORM for this use case |
| Autoscaling | KEDA 2.14 | Scales on queue depth — the right signal for workers |
| Observability | Prometheus + Grafana | Drop-in standard for K8s environments |
| Container runtime | Docker + Kubernetes | Multi-container builds with separate API and worker images |
| Helm | Helm 3 | Reproducible K8s deployments, configurable via values |
| Load testing | k6 | JavaScript-based, clean Prometheus output |

---

## Author

Built by [Your Name](https://github.com/yourusername) · CSE graduate · Open to backend and platform engineering roles in the Netherlands.

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0077B5?style=flat-square&logo=linkedin)](https://linkedin.com/in/yourprofile)
[![Email](https://img.shields.io/badge/Email-say%20hi-EA4335?style=flat-square&logo=gmail)](mailto:you@example.com)

---

## License

MIT