package partialbatch

import (
	"context"
	"fmt"

	"github.com/aws/aws-lambda-go/events"
)

// SQS processes each SQSMessage in a SQSEvent
type SQS struct {
	// Invoked to process each events.SQSMessage
	RecordFunc func(context.Context, events.SQSMessage) error
	// Recover from panic when processing an individual record
	Recover bool
	// If the underlying queue is a FIFO queue processing is halted at the first error
	FIFO bool
	// Invoked when an error occurs processing a single record
	ErrorFunc func(context.Context, events.SQSMessage, error)
}

// handler invokes Handler recovering from any panic's and returning them if Recover is true
func (bp *SQS) handler(ctx context.Context, record events.SQSMessage) (err error) {
	if bp.Recover {
		defer func() {
			if rerr := recover(); rerr != nil {
				switch rerr := rerr.(type) {
				case error:
					err = fmt.Errorf("panic: %w", rerr)
				default:
					err = fmt.Errorf("panic: %v", rerr)
				}
			}
		}()
	}
	return bp.RecordFunc(ctx, record)
}

// Handler processes the events in the SQSEvent using RecordFunc and the other configured options
func (bp *SQS) Handler(ctx context.Context, evt events.SQSEvent) (resp events.SQSEventResponse, _ error) {
	for idx, record := range evt.Records {
		if err := bp.handler(ctx, record); err != nil {
			resp.BatchItemFailures = append(resp.BatchItemFailures, events.SQSBatchItemFailure{
				ItemIdentifier: record.MessageId,
			})
			if bp.ErrorFunc != nil {
				bp.ErrorFunc(ctx, record, err)
			}
			if bp.FIFO {
				for _, record := range evt.Records[idx+1:] {
					resp.BatchItemFailures = append(resp.BatchItemFailures, events.SQSBatchItemFailure{
						ItemIdentifier: record.MessageId,
					})
				}
				return
			}
		}
	}
	return
}
