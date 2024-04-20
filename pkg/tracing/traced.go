package tracing

import "go.opentelemetry.io/otel/propagation"

// Traced is an interface that allows setting and receiving
// contextual information.
type Traced interface {
	propagation.TextMapCarrier
	Topic() string
	Offset() int64
	Partition() int
}
