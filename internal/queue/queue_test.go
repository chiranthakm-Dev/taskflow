package queue

import (
	"testing"

	"github.com/chiranthakm-Dev/taskflow/internal"
)

func TestQueueForPriority(t *testing.T) {
	r := &Router{}

	tests := []struct {
		priority internal.Priority
		expected string
	}{
		{internal.PriorityHigh, "taskflow.high"},
		{internal.PriorityNormal, "taskflow.normal"},
		{internal.PriorityLow, "taskflow.low"},
	}

	for _, test := range tests {
		result := r.queueForPriority(test.priority)
		if result != test.expected {
			t.Errorf("Expected %s, got %s", test.expected, result)
		}
	}
}