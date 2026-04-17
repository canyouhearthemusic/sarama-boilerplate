package consumer

import (
	"time"

	"github.com/IBM/sarama"
)

// Middleware wraps a per-message handler with cross-cutting behavior.
type Middleware func(Handler) Handler

// BatchMiddleware wraps a batch handler with cross-cutting behavior.
type BatchMiddleware func(BatchHandler) BatchHandler

// WithRecovery catches panics and converts them to errors.
func WithRecovery() Middleware {
	return func(next Handler) Handler {
		return &recoveryHandler{next}
	}
}

// WithRetry retries the handler with linear backoff.
func WithRetry(maxAttempts int, backoff time.Duration) Middleware {
	return func(next Handler) Handler {
		return &retryHandler{next: next, maxAttempts: maxAttempts, backoff: backoff}
	}
}

// WithDLQ sends failed messages to a dead-letter topic.
// Returns nil after successful DLQ send so the offset is committed.
func WithDLQ(producer sarama.SyncProducer, dlqTopic string) Middleware {
	return func(next Handler) Handler {
		return &dlqHandler{next: next, producer: producer, dlqTopic: dlqTopic}
	}
}

// WithBatchDLQ sends all messages in a failed batch to the dead-letter topic.
func WithBatchDLQ(producer sarama.SyncProducer, dlqTopic string) BatchMiddleware {
	return func(next BatchHandler) BatchHandler {
		return &batchDLQHandler{next: next, producer: producer, dlqTopic: dlqTopic}
	}
}

// WithBatchRetry retries the batch handler with linear backoff.
func WithBatchRetry(maxAttempts int, backoff time.Duration) BatchMiddleware {
	return func(next BatchHandler) BatchHandler {
		return &batchRetryHandler{next: next, maxAttempts: maxAttempts, backoff: backoff}
	}
}

// WithBatchRecovery catches panics in batch handlers.
func WithBatchRecovery() BatchMiddleware {
	return func(next BatchHandler) BatchHandler {
		return &batchRecoveryHandler{next}
	}
}
