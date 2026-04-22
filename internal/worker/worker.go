package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/chiranthakm-Dev/taskflow/internal"
	"github.com/chiranthakm-Dev/taskflow/internal/store"
	"github.com/streadway/amqp"
)

type Handler interface {
	Process(ctx context.Context, job *internal.Job) error
}

type Worker struct {
	channel         *amqp.Channel
	store           *store.Store
	handlers        map[string]Handler
	circuitBreakers map[string]*CircuitBreaker
}

func NewWorker(conn *amqp.Connection, store *store.Store) (*Worker, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	return &Worker{
		channel:         ch,
		store:           store,
		handlers:        make(map[string]Handler),
		circuitBreakers: make(map[string]*CircuitBreaker),
	}, nil
}

func (w *Worker) RegisterHandler(jobType string, handler Handler) {
	w.handlers[jobType] = handler
	w.circuitBreakers[jobType] = NewCircuitBreaker(jobType, 5, 60*time.Second)
}

func (w *Worker) Start() {
	// Consume from all queues
	queues := []string{"taskflow.high", "taskflow.normal", "taskflow.low"}
	for _, q := range queues {
		go w.consumeQueue(q)
	}
}

func (w *Worker) consumeQueue(queueName string) {
	msgs, err := w.channel.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		log.Printf("Failed to consume %s: %v", queueName, err)
		return
	}

	for d := range msgs {
		var job internal.Job
		if err := json.Unmarshal(d.Body, &job); err != nil {
			log.Printf("Failed to unmarshal job: %v", err)
			d.Nack(false, false)
			continue
		}

		if err := w.processJob(context.Background(), &job); err != nil {
			log.Printf("Failed to process job %s: %v", job.ID, err)
			d.Nack(false, true) // requeue
		} else {
			d.Ack(false)
		}
	}
}

func (w *Worker) processJob(ctx context.Context, job *internal.Job) error {
	cb := w.circuitBreakers[job.Type]
	if cb == nil {
		cb = NewCircuitBreaker(job.Type, 5, 60*time.Second)
		w.circuitBreakers[job.Type] = cb
	}

	if !cb.Allow() {
		return w.requeueWithDelay(ctx, job, 60*time.Second)
	}

	handler, ok := w.handlers[job.Type]
	if !ok {
		return fmt.Errorf("no handler for job type %s", job.Type)
	}

	start := time.Now()
	err := handler.Process(ctx, job)
	duration := time.Since(start)

	if err != nil {
		cb.RecordFailure()
		job.Attempts++
		job.LastError = err.Error()

		if job.Attempts >= job.MaxRetries {
			return w.deadLetter(ctx, job)
		}

		delay := time.Duration(math.Pow(2, float64(job.Attempts))) * time.Second
		return w.requeueWithDelay(ctx, job, delay)
	}

	cb.RecordSuccess()
	job.Status = internal.StatusCompleted
	job.CompletedAt = &start
	job.ProcessingTimeMs = duration.Milliseconds()
	return w.store.UpdateJob(ctx, job)
}

func (w *Worker) requeueWithDelay(ctx context.Context, job *internal.Job, delay time.Duration) error {
	time.Sleep(delay)
	queueName := w.queueForPriority(job.Priority)
	body, _ := json.Marshal(job)
	return w.channel.Publish("", queueName, true, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        body,
	})
}

func (w *Worker) deadLetter(ctx context.Context, job *internal.Job) error {
	job.Status = internal.StatusDead
	if err := w.store.UpdateJob(ctx, job); err != nil {
		return err
	}

	body, _ := json.Marshal(job)
	return w.channel.Publish("", "taskflow.dead", true, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        body,
		Headers: amqp.Table{
			"x-death-reason": job.LastError,
		},
	})
}

func (w *Worker) queueForPriority(p internal.Priority) string {
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