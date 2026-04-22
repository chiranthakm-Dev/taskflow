package handlers

import (
	"context"
	"fmt"

	"github.com/chiranthakm-Dev/taskflow/internal"
)

type SendEmailHandler struct{}

func (h *SendEmailHandler) Process(ctx context.Context, job *internal.Job) error {
	to, ok := job.Payload["to"].(string)
	if !ok {
		return fmt.Errorf("missing 'to' in payload")
	}
	subject, ok := job.Payload["subject"].(string)
	if !ok {
		return fmt.Errorf("missing 'subject' in payload")
	}

	// Simulate sending email
	fmt.Printf("Sending email to %s with subject %s\n", to, subject)

	// In real implementation, integrate with email service
	return nil
}