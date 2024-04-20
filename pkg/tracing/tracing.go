package tracing

import (
	"context"
	"fmt"
	"strconv"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"
)

// TraceSending
func TraceSending(ctx context.Context, tracer trace.Tracer, carrier Traced) (context.Context, trace.Span) {
	// Create a span.
	attrs := []attribute.KeyValue{
		semconv.MessagingSystemKafka,
		semconv.MessagingDestinationPublishName(carrier.Topic()),
		semconv.MessagingOperationPublish,
	}
	opts := []trace.SpanStartOption{
		trace.WithAttributes(attrs...),
		trace.WithSpanKind(trace.SpanKindProducer),
	}
	ctx, span := tracer.Start(ctx, fmt.Sprintf("%s publish", carrier.Topic()), opts...)

	// Inject current span context, so consumers can use it to propagate span.
	otel.GetTextMapPropagator().Inject(ctx, carrier)

	return ctx, span
}

// TraceReceiving takes attributes from the carried message and sets the
// trace with the relevant attributes.
func TraceReceiving(ctx context.Context, tracer trace.Tracer, carrier Traced) (context.Context, trace.Span) {
	parentSpanContext := otel.GetTextMapPropagator().Extract(ctx, carrier)

	// Create a span.
	attrs := []attribute.KeyValue{
		semconv.MessagingSystemKafka,
		semconv.MessagingDestinationPublishName(carrier.Topic()),
		semconv.MessagingOperationReceive,
		semconv.MessagingMessageID(strconv.FormatInt(carrier.Offset(), 10)),
		semconv.MessagingKafkaDestinationPartition(int(carrier.Partition())),
		semconv.MessagingKafkaMessageOffset(int(carrier.Offset())),
	}
	opts := []trace.SpanStartOption{
		trace.WithAttributes(attrs...),
		trace.WithSpanKind(trace.SpanKindConsumer),
	}
	newCtx, span := tracer.Start(parentSpanContext, fmt.Sprintf("%s receive", carrier.Topic()), opts...)

	// Inject current span context, so consumers can use it to propagate span.
	otel.GetTextMapPropagator().Inject(newCtx, carrier)
	return newCtx, span
}
