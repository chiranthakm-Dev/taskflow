package worker

import (
	"testing"
	"time"
)

func TestCircuitBreaker(t *testing.T) {
	cb := NewCircuitBreaker("test", 3, time.Minute)

	// Initially closed
	if !cb.Allow() {
		t.Error("Circuit breaker should allow initially")
	}

	// Record failures
	cb.RecordFailure()
	if cb.state != StateClosed {
		t.Error("Should still be closed after 1 failure")
	}

	cb.RecordFailure()
	cb.RecordFailure()

	// Should be open now
	if cb.Allow() {
		t.Error("Circuit breaker should not allow after threshold failures")
	}

	// Record success should close it
	cb.RecordSuccess()
	if !cb.Allow() {
		t.Error("Circuit breaker should allow after success")
	}
}

func TestCircuitBreakerRecovery(t *testing.T) {
	cb := NewCircuitBreaker("test", 2, 100*time.Millisecond)

	cb.RecordFailure()
	cb.RecordFailure()

	if cb.Allow() {
		t.Error("Should be open")
	}

	// Wait for recovery time
	time.Sleep(150 * time.Millisecond)

	if !cb.Allow() {
		t.Error("Should allow after recovery time")
	}
}