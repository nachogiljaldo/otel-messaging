package kafkago

import (
	"context"
	"time"

	"github.com/nachogiljaldo/otel-messaging/pkg/tracing"
	kafka "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// Message holds the message
type Message struct {
	kafka.Message
	Ctx context.Context
}

// Reader is a clone of the interface implemented by kafka-go's Reader.
type Reader interface {
	Stats() kafka.ReaderStats
	SetOffsetAt(ctx context.Context, t time.Time) error
	SetOffset(offset int64) error
	Lag() int64
	Offset() int64
	ReadLag(ctx context.Context) (lag int64, err error)
	CommitMessages(ctx context.Context, msgs ...Message) error
	FetchMessage(ctx context.Context) (Message, error)
	ReadMessage(ctx context.Context) (Message, error)
	Close() error
	Config() kafka.ReaderConfig
	WithTracer(trace.Tracer) Reader
}

var defaultTracer = otel.Tracer("github.com/nachogiljaldo/otel-messaging/pkg/segmentio/kafkago")

type otelReader struct {
	reader *kafka.Reader
	tracer trace.Tracer
}

func (r otelReader) Stats() kafka.ReaderStats {
	return r.reader.Stats()
}

func (r otelReader) SetOffsetAt(ctx context.Context, t time.Time) error {
	return r.reader.SetOffsetAt(ctx, t)
}

func (r otelReader) SetOffset(offset int64) error {
	return r.reader.SetOffset(offset)
}

func (r otelReader) Lag() int64 {
	return r.reader.Lag()
}

func (r otelReader) Offset() int64 {
	return r.reader.Offset()
}

func (r otelReader) ReadLag(ctx context.Context) (lag int64, err error) {
	return r.reader.ReadLag(ctx)
}

func (r otelReader) CommitMessages(ctx context.Context, msgs ...Message) error {
	unwrappedMsgs := make([]kafka.Message, 0, len(msgs))
	for _, msg := range msgs {
		unwrappedMsgs = append(unwrappedMsgs, msg.Message)
	}
	return r.reader.CommitMessages(ctx, unwrappedMsgs...)
}

func (r otelReader) FetchMessage(ctx context.Context) (Message, error) {
	msg, err := r.reader.FetchMessage(ctx)
	ctx, span := tracing.TraceReceiving(ctx, r.tracer, NewMessageCarrier(&msg))
	defer span.End()
	if err != nil {
		return Message{}, nil
	}
	return Message{
		Message: msg,
		Ctx:     ctx,
	}, nil
}

func (r otelReader) ReadMessage(ctx context.Context) (Message, error) {
	msg, err := r.reader.ReadMessage(ctx)
	ctx, span := tracing.TraceReceiving(ctx, r.tracer, NewMessageCarrier(&msg))
	defer span.End()
	if err != nil {
		return Message{}, nil
	}
	return Message{
		Message: msg,
		Ctx:     ctx,
	}, nil
}

func (r otelReader) Close() error {
	return r.reader.Close()
}

func (r otelReader) Config() kafka.ReaderConfig {
	return r.reader.Config()
}

func (r otelReader) WithTracer(tracer trace.Tracer) Reader {
	result := r
	r.tracer = tracer
	return result
}

func WrapReader(reader *kafka.Reader) Reader {
	return otelReader{
		reader: reader,
		tracer: defaultTracer,
	}
}
