package handlers

import (
	"context"
	"fmt"

	"github.com/chiranthakm-Dev/taskflow/internal"
)

type ProcessPaymentHandler struct{}

func (h *ProcessPaymentHandler) Process(ctx context.Context, job *internal.Job) error {
	amount, ok := job.Payload["amount"].(float64)
	if !ok {
		return fmt.Errorf("missing 'amount' in payload")
	}

	userID, ok := job.Payload["user_id"].(string)
	if !ok {
		return fmt.Errorf("missing 'user_id' in payload")
	}

	// Simulate payment processing
	fmt.Printf("Processing payment of $%.2f for user %s\n", amount, userID)

	// Simulate potential failure (for testing circuit breaker)
	// Uncomment to test: if rand.Float32() < 0.3 { return fmt.Errorf("payment gateway timeout") }

	fmt.Printf("Payment processed successfully\n")

	return nil
}