package tracing_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/nachogiljaldo/otel-messaging/pkg/propagation/segmentio/kafkago"
	"github.com/nachogiljaldo/otel-messaging/pkg/tracing"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

var defaultTracer = otel.Tracer("test")

func Test_TraceSending(t *testing.T) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
	))

	tp := trace.NewTracerProvider()
	// set the global tracer provider
	otel.SetTracerProvider(tp)
	msg := kafka.Message{
		Topic: "topic",
		Key:   []byte("k"),
		Value: []byte("v"),
		Headers: []kafka.Header{
			{
				Key:   "header-key",
				Value: []byte("header-value"),
			},
			{
				Key:   "other-header-key",
				Value: []byte("other-header-value"),
			},
		},
	}
	ctx := context.Background()
	ctx, span := defaultTracer.Start(ctx, "something")
	defer span.End()
	carrier := kafkago.NewMessageCarrier(&msg)
	ctx, span = tracing.TraceReceiving(ctx, defaultTracer, carrier)
	defer span.End()
	readWriteSpan := span.(trace.ReadWriteSpan)
	attrs := []attribute.KeyValue{
		semconv.MessagingSystemKafka,
		semconv.MessagingDestinationPublishName(carrier.Topic()),
		semconv.MessagingOperationReceive,
		semconv.MessagingMessageID(strconv.FormatInt(carrier.Offset(), 10)),
		semconv.MessagingKafkaDestinationPartition(int(carrier.Partition())),
		semconv.MessagingKafkaMessageOffset(int(carrier.Offset())),
	}
	assert.Equal(t, attrs, readWriteSpan.Attributes())
	assert.NotNil(t, ctx)
	assert.NotNil(t, span)
}

func Test_Receive(t *testing.T) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
	))
	tp := trace.NewTracerProvider()
	// set the global tracer provider
	otel.SetTracerProvider(tp)
	msg := kafka.Message{
		Topic: "topic",
		Key:   []byte("k"),
		Value: []byte("v"),
		Headers: []kafka.Header{
			{
				Key:   "header-key",
				Value: []byte("header-value"),
			},
			{
				Key:   "other-header-key",
				Value: []byte("other-header-value"),
			},
		},
	}
	ctx := context.Background()
	ctx, span := defaultTracer.Start(ctx, "something")
	defer span.End()
	carrier := kafkago.NewMessageCarrier(&msg)
	ctx, span = tracing.TraceSending(ctx, defaultTracer, carrier)
	defer span.End()
	readWriteSpan := span.(trace.ReadWriteSpan)
	attrs := []attribute.KeyValue{
		semconv.MessagingSystemKafka,
		semconv.MessagingDestinationPublishName(carrier.Topic()),
		semconv.MessagingOperationPublish,
	}
	assert.Equal(t, attrs, readWriteSpan.Attributes())
	assert.NotNil(t, ctx)
	assert.NotNil(t, span)
}
