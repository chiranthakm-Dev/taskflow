package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/chiranthakm-Dev/taskflow/internal"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type Store struct {
	db *sqlx.DB
}

func NewStore(dsn string) (*Store, error) {
	db, err := sqlx.Connect("postgres", dsn)
	if err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) CreateJob(ctx context.Context, job *internal.Job) error {
	payload, _ := json.Marshal(job.Payload)
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO jobs (id, type, priority, payload, max_retries, status, idempotency_key, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		job.ID, job.Type, job.Priority, payload, job.MaxRetries, job.Status, job.IdempotencyKey, job.CreatedAt)
	return err
}

func (s *Store) GetJob(ctx context.Context, id string) (*internal.Job, error) {
	var job internal.Job
	var payloadBytes []byte
	err := s.db.QueryRowContext(ctx, `
		SELECT id, type, priority, payload, max_retries, attempts, last_error, status, idempotency_key, created_at, completed_at, processing_time_ms
		FROM jobs WHERE id = $1`, id).Scan(
		&job.ID, &job.Type, &job.Priority, &payloadBytes, &job.MaxRetries, &job.Attempts, &job.LastError, &job.Status, &job.IdempotencyKey, &job.CreatedAt, &job.CompletedAt, &job.ProcessingTimeMs)
	if err != nil {
		return nil, err
	}
	json.Unmarshal(payloadBytes, &job.Payload)
	return &job, nil
}

func (s *Store) UpdateJob(ctx context.Context, job *internal.Job) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE jobs SET attempts = $1, last_error = $2, status = $3, completed_at = $4, processing_time_ms = $5
		WHERE id = $6`,
		job.Attempts, job.LastError, job.Status, job.CompletedAt, job.ProcessingTimeMs, job.ID)
	return err
}

func (s *Store) GetByIdempotencyKey(ctx context.Context, key string) (*internal.Job, error) {
	var job internal.Job
	var payloadBytes []byte
	err := s.db.QueryRowContext(ctx, `
		SELECT id, type, priority, payload, max_retries, attempts, last_error, status, idempotency_key, created_at, completed_at, processing_time_ms
		FROM jobs WHERE idempotency_key = $1`, key).Scan(
		&job.ID, &job.Type, &job.Priority, &payloadBytes, &job.MaxRetries, &job.Attempts, &job.LastError, &job.Status, &job.IdempotencyKey, &job.CreatedAt, &job.CompletedAt, &job.ProcessingTimeMs)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	json.Unmarshal(payloadBytes, &job.Payload)
	return &job, nil
}

func (s *Store) MarkCompleted(ctx context.Context, id string) error {
	now := time.Now()
	_, err := s.db.ExecContext(ctx, `
		UPDATE jobs SET status = $1, completed_at = $2
		WHERE id = $3`, internal.StatusCompleted, now, id)
	return err
}