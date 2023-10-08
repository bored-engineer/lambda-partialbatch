# lambda-partialbatch [![Go Reference](https://pkg.go.dev/badge/github.com/bored-engineer/lambda-partialbatch.svg)](https://pkg.go.dev/github.com/bored-engineer/lambda-partialbatch)
Support for [AWS Lambda's Partial Batch Response](https://aws.amazon.com/about-aws/whats-new/2021/11/aws-lambda-partial-batch-response-sqs-event-source/) for DynamoDB, Kinesis and SQS in Golang

## Example
```go
package main

import (
	"context"
	"log"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	partialbatch "github.com/bored-engineer/lambda-partialbatch"
)

func handler(ctx context.Context, msg events.SQSMessage) error {
	// TODO: Some processing that could fail
	return nil
}

func main() {
	// Process batches of SQS messages
	lambda.Start(partialbatch.SQS(
		handler,
		// Recover from any panics, treating as a failed record
		partialbatch.WithRecovery(true),
		// For FIFO queues, halt after the first failure
		partialbatch.WithHalt(true),
		// Log any errors for debugging/alerting
		partialbatch.WithErrorCallback(func(_ context.Context, itemIdentifier string, err error) {
			log.Printf("failed to process SQS message %q: %s", itemIdentifier, err)
		}),
	))
}
```
