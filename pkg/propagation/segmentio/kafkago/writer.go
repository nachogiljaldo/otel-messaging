package kafkago

import (
	"context"

	kafka "github.com/segmentio/kafka-go"
)

// Writer is a clone of the interface implemented by kafka-go's Writer.
type Writer interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

type otelWriter struct {
	writer *kafka.Writer
}

func (r otelWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	return r.writer.WriteMessages(ctx, msgs...)
}

func (r otelWriter) Close() error {
	return r.writer.Close()
}

func WrapWriter(writer *kafka.Writer) Writer {
	return otelWriter{
		writer: writer,
	}
}
