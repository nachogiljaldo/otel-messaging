package kafkago

import (
	"github.com/nachogiljaldo/otel-messaging/pkg/tracing"
	kafka "github.com/segmentio/kafka-go"
)

type segmentIoMessageCarrier struct {
	message *kafka.Message
}

// NewMessageCarrier produces a new message carrier from a segmentio/kafka-go message.
func NewMessageCarrier(message *kafka.Message) tracing.Traced {
	return segmentIoMessageCarrier{
		message: message,
	}
}

func (m segmentIoMessageCarrier) Get(key string) string {
	for _, header := range m.message.Headers {
		if header.Key == key {
			return string(header.Value)
		}
	}
	return ""
}

// Topic returns the topic of the message.
func (m segmentIoMessageCarrier) Topic() string {
	return m.message.Topic
}

// Offset returns the offset of the message.
func (m segmentIoMessageCarrier) Offset() int64 {
	return m.message.Offset
}

// Partition returns the partition the message belongs to. Only relevant when receiving.
func (m segmentIoMessageCarrier) Partition() int {
	return m.message.Partition
}

// Set stores the key-value pair.
func (m segmentIoMessageCarrier) Set(key string, value string) {
	for _, header := range m.message.Headers {
		if header.Key == key {
			header.Value = []byte(value)
			return
		}
	}
	m.message.Headers = append(m.message.Headers, kafka.Header{
		Key:   key,
		Value: []byte(value),
	})
}

// Keys lists the keys stored in this carrier.
func (m segmentIoMessageCarrier) Keys() []string {
	keys := make([]string, 0, len(m.message.Headers))
	for _, header := range m.message.Headers {
		keys = append(keys, header.Key)
	}
	return keys
}
