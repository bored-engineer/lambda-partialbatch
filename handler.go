package partialbatch

import (
	"context"
	"fmt"
)

// convertHandler converts a given Handler function into the full extended type regardless of the input
func convertHandler[
	TIn any,
	THandler func(TIn) | func(TIn) error | func(context.Context, TIn) | func(context.Context, TIn) error,
](
	handler THandler,
) func(context.Context, TIn) error {
	switch handler := any(handler).(type) {
	case func(context.Context, TIn) error:
		return func(ctx context.Context, record TIn) error {
			return handler(ctx, record)
		}
	case func(context.Context, TIn):
		return func(ctx context.Context, record TIn) error {
			handler(ctx, record)
			return nil
		}
	case func(TIn) error:
		return func(ctx context.Context, record TIn) error {
			return handler(record)
		}
	case func(TIn):
		return func(ctx context.Context, record TIn) error {
			handler(record)
			return nil
		}
	default:
		panic(fmt.Errorf("unknown Handler type: %T", handler))
	}
}

// wrapRecovery catches any panics and returns them instead
func wrapRecovery[
	TIn any,
	THandler func(context.Context, TIn) error,
](
	handler THandler,
) func(context.Context, TIn) error {
	return func(ctx context.Context, record TIn) (err error) {
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
		return handler(ctx, record)
	}
}
