package partialbatch

import "context"

type ErrorFunc func(ctx context.Context, itemIdentifier string, err error)

// options are shared by all events processors
type options struct {
	recovery bool
	halt     bool
	errFunc  ErrorFunc
}

// Option modifies the behavior of the event processing
type Option func(*options)

// WithRecovery captures any panics treating them as a returned error
func WithRecovery(v bool) Option {
	return func(o *options) {
		o.recovery = v
	}
}

// WithHalt changes the behavior to halt when an error occurs instead of continuing
func WithHalt(v bool) Option {
	return func(o *options) {
		o.halt = v
	}
}

// WithErrorCallback is invoked (if non-nil) when an error occurs processing an item
// Typically this is used to log the error for debugging/alerting
func WithErrorCallback(cb ErrorFunc) Option {
	return func(o *options) {
		o.errFunc = cb
	}
}

// applyOptions aggregates the functions
func applyOptions(opts ...Option) options {
	var o options
	for _, optFunc := range opts {
		optFunc(&o)
	}
	return o
}
