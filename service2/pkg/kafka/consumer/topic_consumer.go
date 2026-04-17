package consumer

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

// TopicConsumer manages a single topic with its own consumer group.
type TopicConsumer struct {
	topic   string
	group   string
	brokers []string
	config  *sarama.Config

	handler      Handler
	batchHandler BatchHandler
	batchSize    int
	batchTimeout time.Duration

	onSetup   func(sarama.ConsumerGroupSession) error
	onCleanup func(sarama.ConsumerGroupSession) error
}

// NewTopicConsumer creates a per-message consumer for a single topic.
func NewTopicConsumer(
	topic, group string,
	brokers []string,
	cfg *sarama.Config,
	handler Handler,
	opts ...Option,
) *TopicConsumer {
	c := &TopicConsumer{
		topic:   topic,
		group:   group,
		brokers: brokers,
		config:  cfg,
		handler: handler,
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

// NewBatchTopicConsumer creates a batch consumer for a single topic.
func NewBatchTopicConsumer(
	topic, group string,
	brokers []string,
	cfg *sarama.Config,
	batchHandler BatchHandler,
	batchSize int,
	batchTimeout time.Duration,
	opts ...Option,
) *TopicConsumer {
	c := &TopicConsumer{
		topic:        topic,
		group:        group,
		brokers:      brokers,
		config:       cfg,
		batchHandler: batchHandler,
		batchSize:    batchSize,
		batchTimeout: batchTimeout,
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

func (c *TopicConsumer) Topic() string { return c.topic }

// Run blocks until ctx is cancelled. Handles rebalances automatically.
func (c *TopicConsumer) Run(ctx context.Context) error {
	cgroup, err := sarama.NewConsumerGroup(c.brokers, c.group, c.config)
	if err != nil {
		return fmt.Errorf("create consumer group %s: %w", c.group, err)
	}
	defer cgroup.Close()

	go func() {
		for err := range cgroup.Errors() {
			log.Printf("[%s] consumer group error: %v", c.topic, err)
		}
	}()

	log.Printf("[%s] started consumer group %q", c.topic, c.group)

	for {
		if err := cgroup.Consume(ctx, []string{c.topic}, c); err != nil {
			log.Printf("[%s] consume error: %v", c.topic, err)
		}

		if ctx.Err() != nil {
			log.Printf("[%s] context cancelled, stopping", c.topic)
			return nil
		}
	}
}

// --- sarama.ConsumerGroupHandler implementation ---

func (c *TopicConsumer) Setup(session sarama.ConsumerGroupSession) error {
	if c.onSetup != nil {
		return c.onSetup(session)
	}

	return nil
}

func (c *TopicConsumer) Cleanup(session sarama.ConsumerGroupSession) error {
	if c.onCleanup != nil {
		return c.onCleanup(session)
	}

	return nil
}

func (c *TopicConsumer) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	if c.batchHandler != nil {
		return c.consumeClaimBatch(session, claim)
	}

	return c.consumeClaimSingle(session, claim)
}

func (c *TopicConsumer) consumeClaimSingle(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			if err := c.handler.Handle(session.Context(), msg); err != nil {
				log.Printf("[%s] unhandled error on offset %d: %v",
					c.topic, msg.Offset, err)
				continue
			}

			session.MarkMessage(msg, "")

		case <-session.Context().Done():
			return nil
		}
	}
}

func (c *TopicConsumer) consumeClaimBatch(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	batch := make([]*sarama.ConsumerMessage, 0, c.batchSize)
	ticker := time.NewTicker(c.batchTimeout)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := c.batchHandler.HandleBatch(session.Context(), batch); err != nil {
			log.Printf("[%s] unhandled batch error (%d msgs): %v",
				c.topic, len(batch), err)
		} else {
			session.MarkMessage(batch[len(batch)-1], "")
		}

		batch = batch[:0]
	}

	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				flush()
				return nil
			}
			batch = append(batch, msg)
			if len(batch) >= c.batchSize {
				flush()
			}

		case <-ticker.C:
			flush()

		case <-session.Context().Done():
			flush()
			return nil
		}
	}
}
