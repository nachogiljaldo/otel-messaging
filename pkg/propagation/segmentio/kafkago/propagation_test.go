package kafkago

import (
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

func Test_MessageCarrier(t *testing.T) {
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

	carrier := NewMessageCarrier(&msg)

	t.Run("keys are retrieved correctly", func(t *testing.T) {
		assert.Equal(t, carrier.Keys(), []string{"header-key", "other-header-key"})
	})

	t.Run("getting value", func(t *testing.T) {
		assert.Equal(t, carrier.Get("header-key"), "header-value")
	})

	t.Run("setting value", func(t *testing.T) {
		carrier.Set("new-header-key", "new-header-value")
		assert.Equal(t, carrier.Get("new-header-key"), "new-header-value")
	})

	t.Run("proxied methods", func(t *testing.T) {
		assert.Equal(t, msg.Offset, carrier.Offset())
		assert.Equal(t, msg.Partition, carrier.Partition())
		assert.Equal(t, msg.Topic, carrier.Topic())
	})
}
