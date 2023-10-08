package partialbatch

import (
	"context"

	"github.com/aws/aws-lambda-go/events"
)

// DynamoDBHandler processes a single events.DynamoDBEventRecord
type DynamoDBHandler interface {
	func(context.Context, events.DynamoDBEventRecord) error |
		func(context.Context, events.DynamoDBEventRecord) |
		func(events.DynamoDBEventRecord) error |
		func(events.DynamoDBEventRecord)
}

// DynamoDBProcessor can be passed directly to lamdba.Handle
type DynamoDBProcessor func(context.Context, events.DynamoDBEvent) (events.DynamoDBEventResponse, error)

// DynamoDB processes each DynamoDBEventRecord in a DynamoDBEvent by invoking the provided DynamoDBHandler
func DynamoDB[THandler DynamoDBHandler](handler THandler, opts ...Option) DynamoDBProcessor {
	o := applyOptions(opts...)
	handlerFunc := convertHandler[events.DynamoDBEventRecord](handler)
	if o.recovery {
		handlerFunc = wrapRecovery(handlerFunc)
	}
	return func(ctx context.Context, evt events.DynamoDBEvent) (resp events.DynamoDBEventResponse, _ error) {
		for idx, record := range evt.Records {
			if err := handlerFunc(ctx, record); err != nil {
				resp.BatchItemFailures = append(resp.BatchItemFailures, events.DynamoDBBatchItemFailure{
					ItemIdentifier: record.EventID,
				})
				if o.errFunc != nil {
					o.errFunc(ctx, record.EventID, err)
				}
				if o.halt {
					for _, record := range evt.Records[idx+1:] {
						resp.BatchItemFailures = append(resp.BatchItemFailures, events.DynamoDBBatchItemFailure{
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
