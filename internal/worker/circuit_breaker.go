package worker

import (
	"sync"
	"time"

	"github.com/chiranthakm-Dev/taskflow/internal/metrics"
)

type State int

const (
	StateClosed State = iota
	StateOpen
	StateHalfOpen
)

type CircuitBreaker struct {
	mu           sync.Mutex
	failures     int
	lastFailure  time.Time
	state        State
	threshold    int
	recoveryTime time.Duration
	jobType      string
}

func NewCircuitBreaker(jobType string, threshold int, recoveryTime time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		jobType:      jobType,
		threshold:    threshold,
		recoveryTime: recoveryTime,
		state:        StateClosed,
	}
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
			return true
		}
		return false
	case StateHalfOpen:
		return false
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