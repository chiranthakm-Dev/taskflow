package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/chiranthakm-Dev/taskflow/internal"
	"github.com/chiranthakm-Dev/taskflow/internal/queue"
	"github.com/chiranthakm-Dev/taskflow/internal/store"
	"github.com/google/uuid"
)

type Handler struct {
	store  *store.Store
	router *queue.Router
}

func NewHandler(store *store.Store, router *queue.Router) *Handler {
	return &Handler{store: store, router: router}
}

func (h *Handler) HandleJobs(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.handleEnqueueJob(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (h *Handler) handleEnqueueJob(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Type           string                 `json:"type"`
		Priority       string                 `json:"priority"`
		Payload        map[string]interface{} `json:"payload"`
		MaxRetries     int                    `json:"max_retries"`
		IdempotencyKey string                 `json:"idempotency_key"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	job := &internal.Job{
		ID:             uuid.New().String(),
		Type:           req.Type,
		Priority:       internal.Priority(req.Priority),
		Payload:        req.Payload,
		MaxRetries:     req.MaxRetries,
		Status:         internal.StatusQueued,
		IdempotencyKey: req.IdempotencyKey,
		CreatedAt:      time.Now(),
	}

	if err := h.store.CreateJob(r.Context(), job); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := h.router.Enqueue(r.Context(), job); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"id":        job.ID,
		"status":    job.Status,
		"queue":     h.queueName(job.Priority),
		"created_at": job.CreatedAt,
	})
}

func (h *Handler) HandleJobByID(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/jobs/")
	if id == "" {
		http.NotFound(w, r)
		return
	}

	switch r.Method {
	case http.MethodGet:
		h.handleGetJob(w, r, id)
	case http.MethodPost:
		if strings.HasSuffix(r.URL.Path, "/retry") {
			h.handleRetryJob(w, r, strings.TrimSuffix(id, "/retry"))
		} else {
			http.Error(w, "Not found", http.StatusNotFound)
		}
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (h *Handler) handleGetJob(w http.ResponseWriter, r *http.Request, id string) {
	job, err := h.store.GetJob(r.Context(), id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if job == nil {
		http.NotFound(w, r)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(job)
}

func (h *Handler) handleRetryJob(w http.ResponseWriter, r *http.Request, id string) {
	job, err := h.store.GetJob(r.Context(), id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if job == nil || job.Status != internal.StatusDead {
		http.Error(w, "Job not found or not dead", http.StatusBadRequest)
		return
	}

	job.Status = internal.StatusQueued
	job.Attempts = 0
	job.LastError = ""
	job.CompletedAt = nil
	job.ProcessingTimeMs = 0

	if err := h.store.UpdateJob(r.Context(), job); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := h.router.Enqueue(r.Context(), job); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *Handler) HandleMetrics(w http.ResponseWriter, r *http.Request) {
	// TODO: Implement Prometheus metrics
	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte("# TODO: Prometheus metrics\n"))
}

func (h *Handler) queueName(p internal.Priority) string {
	switch p {
	case internal.PriorityHigh:
		return "high"
	case internal.PriorityNormal:
		return "normal"
	case internal.PriorityLow:
		return "low"
	default:
		return "normal"
	}
}