package queue

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/chiranthakm-Dev/taskflow/internal"
	"github.com/chiranthakm-Dev/taskflow/internal/metrics"
	"github.com/chiranthakm-Dev/taskflow/internal/store"
	"github.com/google/uuid"
	"github.com/streadway/amqp"
)

type Router struct {
	channel *amqp.Channel
	store   *store.Store
}

func NewRouter(conn *amqp.Connection, store *store.Store) (*Router, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	// Declare queues
	for _, q := range []string{"taskflow.high", "taskflow.normal", "taskflow.low", "taskflow.dead"} {
		_, err := ch.QueueDeclare(q, true, false, false, false, nil)
		if err != nil {
			return nil, err
		}
	}

	return &Router{channel: ch, store: store}, nil
}

func (r *Router) Enqueue(ctx context.Context, job *internal.Job) error {
	// Idempotency check
	if job.IdempotencyKey != "" {
		existing, err := r.store.GetByIdempotencyKey(ctx, job.IdempotencyKey)
		if err != nil {
			return err
		}
		if existing != nil {
			return fmt.Errorf("duplicate job")
		}
	}

	// Generate ID if not set
	if job.ID == "" {
		job.ID = uuid.New().String()
	}

	queueName := r.queueForPriority(job.Priority)

	msg := amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         mustMarshal(job),
	}

	err := r.channel.Publish("", queueName, true, false, msg)
	if err != nil {
		return err
	}

	// Record metrics
	metrics.JobsEnqueuedTotal.WithLabelValues(job.Type, string(job.Priority)).Inc()

	return nil
}

func (r *Router) queueForPriority(p internal.Priority) string {
	switch p {
	case internal.PriorityHigh:
		return "taskflow.high"
	case internal.PriorityNormal:
		return "taskflow.normal"
	case internal.PriorityLow:
		return "taskflow.low"
	default:
		return "taskflow.normal"
	}
}

func mustMarshal(v interface{}) []byte {
	b, _ := json.Marshal(v)
	return b
}