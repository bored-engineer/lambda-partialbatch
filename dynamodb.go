package partialbatch

import (
	"context"
	"fmt"

	"github.com/aws/aws-lambda-go/events"
)

// DynamoDB processes each DynamoDBEventRecord in a DynamoDBEvent
type DynamoDB struct {
	// Invoked to process each events.DynamoDBEventRecord
	RecordFunc func(context.Context, events.DynamoDBEventRecord) error
	// Recover from panic when processing an individual record
	Recover bool
	// Invoked when an error occurs processing a single record
	ErrorFunc func(context.Context, events.DynamoDBEventRecord, error)
}

// handler invokes Handler recovering from any panic's and returning them if Recover is true
func (bp *DynamoDB) handler(ctx context.Context, record events.DynamoDBEventRecord) (err error) {
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

// Handler processes the events in the DynamoDBEvent using RecordFunc and the other configured options
func (bp *DynamoDB) Handler(ctx context.Context, evt events.DynamoDBEvent) (resp events.DynamoDBEventResponse, _ error) {
	for _, record := range evt.Records {
		if err := bp.handler(ctx, record); err != nil {
			resp.BatchItemFailures = append(resp.BatchItemFailures, events.DynamoDBBatchItemFailure{
				ItemIdentifier: record.EventID,
			})
			if bp.ErrorFunc != nil {
				bp.ErrorFunc(ctx, record, err)
			}
		}
	}
	return
}

// DynamoDBTimeWindow processes each DynamoDBEventRecord in a DynamoDBTimeWindowEvent
type DynamoDBTimeWindow struct {
	// Invoked to process each events.DynamoDBEventRecord
	RecordFunc func(context.Context, events.TimeWindowProperties, events.DynamoDBEventRecord) error
	// Recover from panic when processing an individual record
	Recover bool
	// Invoked when an error occurs processing a single record
	ErrorFunc func(context.Context, events.TimeWindowProperties, events.DynamoDBEventRecord, error)
}

// handler invokes Handler recovering from any panic's and returning them if Recover is true
func (bp *DynamoDBTimeWindow) handler(ctx context.Context, props events.TimeWindowProperties, record events.DynamoDBEventRecord) (err error) {
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
	return bp.RecordFunc(ctx, props, record)
}

// Handler processes the events in the DynamoDBEvent using RecordFunc and the other configured options
func (bp *DynamoDBTimeWindow) Handler(ctx context.Context, evt events.DynamoDBTimeWindowEvent) (resp events.DynamoDBTimeWindowEventResponse, _ error) {
	for _, record := range evt.Records {
		if err := bp.handler(ctx, evt.TimeWindowProperties, record); err != nil {
			resp.BatchItemFailures = append(resp.BatchItemFailures, events.DynamoDBBatchItemFailure{
				ItemIdentifier: record.EventID,
			})
			if bp.ErrorFunc != nil {
				bp.ErrorFunc(ctx, evt.TimeWindowProperties, record, err)
			}
		}
	}
	resp.State = evt.State
	return
}
