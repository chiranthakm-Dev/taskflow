package handlers

import (
	"context"
	"fmt"
	"time"

	"github.com/chiranthakm-Dev/taskflow/internal"
)

type GenerateReportHandler struct{}

func (h *GenerateReportHandler) Process(ctx context.Context, job *internal.Job) error {
	reportType, ok := job.Payload["report_type"].(string)
	if !ok {
		return fmt.Errorf("missing 'report_type' in payload")
	}

	// Simulate report generation
	fmt.Printf("Generating %s report...\n", reportType)
	time.Sleep(100 * time.Millisecond) // Simulate processing time
	fmt.Printf("Report %s generated successfully\n", reportType)

	return nil
}