package partialbatch

import (
	"context"
	"fmt"

	"github.com/aws/aws-lambda-go/events"
)

// Kinesis processes each KinesisEventRecord in a KinesisEvent
type Kinesis struct {
	// Invoked to process each events.KinesisEventRecord
	RecordFunc func(context.Context, events.KinesisEventRecord) error
	// Recover from panic when processing an individual record
	Recover bool
	// Invoked when an error occurs processing a single record
	ErrorFunc func(context.Context, events.KinesisEventRecord, error)
}

// handler invokes Handler recovering from any panic's and returning them if Recover is true
func (bp *Kinesis) handler(ctx context.Context, record events.KinesisEventRecord) (err error) {
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

// Handler processes the events in the KinesisEvent using RecordFunc and the other configured options
func (bp *Kinesis) Handler(ctx context.Context, evt events.KinesisEvent) (resp events.KinesisEventResponse, _ error) {
	for _, record := range evt.Records {
		if err := bp.handler(ctx, record); err != nil {
			resp.BatchItemFailures = append(resp.BatchItemFailures, events.KinesisBatchItemFailure{
				ItemIdentifier: record.EventID,
			})
			if bp.ErrorFunc != nil {
				bp.ErrorFunc(ctx, record, err)
			}
		}
	}
	return
}

// KinesisTimeWindow processes each KinesisEventRecord in a KinesisTimeWindowEvent
type KinesisTimeWindow struct {
	// Invoked to process each events.KinesisEventRecord
	RecordFunc func(context.Context, events.TimeWindowProperties, events.KinesisEventRecord) error
	// Recover from panic when processing an individual record
	Recover bool
	// Invoked when an error occurs processing a single record
	ErrorFunc func(context.Context, events.TimeWindowProperties, events.KinesisEventRecord, error)
}

// handler invokes Handler recovering from any panic's and returning them if Recover is true
func (bp *KinesisTimeWindow) handler(ctx context.Context, props events.TimeWindowProperties, record events.KinesisEventRecord) (err error) {
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

// Handler processes the events in the KinesisTimeWindowEvent using RecordFunc and the other configured options
func (bp *KinesisTimeWindow) Handler(ctx context.Context, evt events.KinesisTimeWindowEvent) (resp events.KinesisTimeWindowEventResponse, _ error) {
	for _, record := range evt.Records {
		if err := bp.handler(ctx, evt.TimeWindowProperties, record); err != nil {
			resp.BatchItemFailures = append(resp.BatchItemFailures, events.KinesisBatchItemFailure{
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
