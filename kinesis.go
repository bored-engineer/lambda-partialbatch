package partialbatch

import (
	"context"

	"github.com/aws/aws-lambda-go/events"
)

// KinesisEventHandler processes a single events.KinesisEventRecord
type KinesisEventHandler interface {
	func(context.Context, events.KinesisEventRecord) error |
		func(context.Context, events.KinesisEventRecord) |
		func(events.KinesisEventRecord) error |
		func(events.KinesisEventRecord)
}

// DynamoDBProcessor can be passed directly to lamdba.Handle
type KinesisProcessor func(context.Context, events.KinesisEvent) (events.KinesisEventResponse, error)

// Kinesis processes each DynamoDBEventRecord in a DynamoDBEvent by invoking the provided DynamoDBHandler
func Kinesis[THandler KinesisEventHandler](handler THandler, opts ...Option) KinesisProcessor {
	o := applyOptions(opts...)
	handlerFunc := convertHandler[events.KinesisEventRecord](handler)
	if o.recovery {
		handlerFunc = wrapRecovery(handlerFunc)
	}
	return func(ctx context.Context, evt events.KinesisEvent) (resp events.KinesisEventResponse, _ error) {
		for idx, record := range evt.Records {
			if err := handlerFunc(ctx, record); err != nil {
				resp.BatchItemFailures = append(resp.BatchItemFailures, events.KinesisBatchItemFailure{
					ItemIdentifier: record.EventID,
				})
				if o.errFunc != nil {
					o.errFunc(ctx, record.EventID, err)
				}
				if o.halt {
					for _, record := range evt.Records[idx+1:] {
						resp.BatchItemFailures = append(resp.BatchItemFailures, events.KinesisBatchItemFailure{
							ItemIdentifier: record.EventID,
						})
					}
					return
				}
			}
		}
		return
	}
}
