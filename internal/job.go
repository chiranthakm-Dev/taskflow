package internal

import (
	"time"
)

type JobStatus string

const (
	StatusQueued   JobStatus = "queued"
	StatusProcessing JobStatus = "processing"
	StatusCompleted JobStatus = "completed"
	StatusFailed   JobStatus = "failed"
	StatusDead     JobStatus = "dead"
)

type Priority string

const (
	PriorityHigh   Priority = "high"
	PriorityNormal Priority = "normal"
	PriorityLow    Priority = "low"
)

type Job struct {
	ID             string                 `json:"id" db:"id"`
	Type           string                 `json:"type" db:"type"`
	Priority       Priority               `json:"priority" db:"priority"`
	Payload        map[string]interface{} `json:"payload" db:"payload"`
	MaxRetries     int                    `json:"max_retries" db:"max_retries"`
	Attempts       int                    `json:"attempts" db:"attempts"`
	LastError      string                 `json:"last_error,omitempty" db:"last_error"`
	Status         JobStatus              `json:"status" db:"status"`
	IdempotencyKey string                 `json:"idempotency_key,omitempty" db:"idempotency_key"`
	CreatedAt      time.Time              `json:"created_at" db:"created_at"`
	CompletedAt    *time.Time             `json:"completed_at,omitempty" db:"completed_at"`
	ProcessingTimeMs int64                `json:"processing_time_ms,omitempty" db:"processing_time_ms"`
}