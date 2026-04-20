package consumer

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

type retryHandler struct {
	next        Handler
	maxAttempts int
	backoff     time.Duration
}

func (h *retryHandler) Handle(ctx context.Context, msg *sarama.ConsumerMessage) error {
	var err error
	for attempt := 1; attempt <= h.maxAttempts; attempt++ {
		if err = h.next.Handle(ctx, msg); err == nil {
			return nil
		}

		if attempt == h.maxAttempts {
			break
		}

		select {
		case <-time.After(h.backoff * time.Duration(attempt)):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return err
}

type dlqHandler struct {
	next     Handler
	producer sarama.SyncProducer
	dlqTopic string
}

func (h *dlqHandler) Handle(ctx context.Context, msg *sarama.ConsumerMessage) error {
	err := h.next.Handle(ctx, msg)
	if err == nil {
		return nil
	}
	if dlqErr := sendToDLQ(h.producer, h.dlqTopic, msg, err); dlqErr != nil {
		return fmt.Errorf("handler: %w; dlq send: %v", err, dlqErr)
	}
	log.Printf("[dlq] sent to %s (topic=%s, offset=%d): %v",
		h.dlqTopic, msg.Topic, msg.Offset, err)

	return nil
}

// --- Batch middleware ---

type batchRetryHandler struct {
	next        BatchHandler
	maxAttempts int
	backoff     time.Duration
}

func (h *batchRetryHandler) HandleBatch(ctx context.Context, msgs []*sarama.ConsumerMessage) error {
	var err error
	for attempt := 1; attempt <= h.maxAttempts; attempt++ {
		if err = h.next.HandleBatch(ctx, msgs); err == nil {
			return nil
		}
		if attempt == h.maxAttempts {
			break
		}
		select {
		case <-time.After(h.backoff * time.Duration(attempt)):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return err
}

type batchDLQHandler struct {
	next     BatchHandler
	producer sarama.SyncProducer
	dlqTopic string
}

func (h *batchDLQHandler) HandleBatch(ctx context.Context, msgs []*sarama.ConsumerMessage) error {
	err := h.next.HandleBatch(ctx, msgs)
	if err == nil {
		return nil
	}
	for _, msg := range msgs {
		if dlqErr := sendToDLQ(h.producer, h.dlqTopic, msg, err); dlqErr != nil {
			return fmt.Errorf("handler: %w; dlq send: %v", err, dlqErr)
		}
	}
	log.Printf("[dlq] sent %d msgs to %s: %v", len(msgs), h.dlqTopic, err)

	return nil
}

// --- shared helpers ---

func sendToDLQ(producer sarama.SyncProducer, dlqTopic string, msg *sarama.ConsumerMessage, handlerErr error) error {
	headers := make([]sarama.RecordHeader, 0, len(msg.Headers)+3)
	for _, h := range msg.Headers {
		headers = append(headers, *h)
	}
	headers = append(headers,
		sarama.RecordHeader{
			Key: []byte("dlq.original-topic"), Value: []byte(msg.Topic)},
		sarama.RecordHeader{
			Key: []byte("dlq.error"), Value: []byte(handlerErr.Error())},
		sarama.RecordHeader{
			Key:   []byte("dlq.original-partition"),
			Value: []byte(fmt.Sprintf("%d", msg.Partition))},
	)

	dlqMsg := &sarama.ProducerMessage{
		Topic:   dlqTopic,
		Key:     sarama.ByteEncoder(msg.Key),
		Value:   sarama.ByteEncoder(msg.Value),
		Headers: headers,
	}

	_, _, err := producer.SendMessage(dlqMsg)
	return err
}
