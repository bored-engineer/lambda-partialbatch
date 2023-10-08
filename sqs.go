package partialbatch

import (
	"context"

	"github.com/aws/aws-lambda-go/events"
)

// SQSHandler processes a single events.SQSMessage
type SQSHandler interface {
	func(context.Context, events.SQSMessage) error |
		func(context.Context, events.SQSMessage) |
		func(events.SQSMessage) error |
		func(events.SQSMessage)
}

// SQSProcessor can be passed directly to lamdba.Handle
type SQSProcessor func(context.Context, events.SQSEvent) (events.SQSEventResponse, error)

// SQS processes each SQSMessage in a SQSEvent by invoking the provided SQSHandler
func SQS[THandler SQSHandler](handler THandler, opts ...Option) SQSProcessor {
	o := applyOptions(opts...)
	handlerFunc := convertHandler[events.SQSMessage](handler)
	if o.recovery {
		handlerFunc = wrapRecovery(handlerFunc)
	}
	return func(ctx context.Context, evt events.SQSEvent) (resp events.SQSEventResponse, _ error) {
		for idx, record := range evt.Records {
			if err := handlerFunc(ctx, record); err != nil {
				resp.BatchItemFailures = append(resp.BatchItemFailures, events.SQSBatchItemFailure{
					ItemIdentifier: record.MessageId,
				})
				if o.errFunc != nil {
					o.errFunc(ctx, record.MessageId, err)
				}
				if o.halt {
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
}
