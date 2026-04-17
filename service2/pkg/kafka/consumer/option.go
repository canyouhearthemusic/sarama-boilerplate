package consumer

import "github.com/IBM/sarama"

// Option configures a TopicConsumer.
type Option func(*TopicConsumer)

// WithSetup registers a callback invoked when partitions are assigned.
func WithSetup(fn func(sarama.ConsumerGroupSession) error) Option {
	return func(c *TopicConsumer) { c.onSetup = fn }
}

// WithCleanup registers a callback invoked when partitions are revoked.
func WithCleanup(fn func(sarama.ConsumerGroupSession) error) Option {
	return func(c *TopicConsumer) { c.onCleanup = fn }
}
