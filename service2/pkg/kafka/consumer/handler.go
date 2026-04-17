package consumer

import (
	"context"

	"github.com/IBM/sarama"
)

// Handler processes a single Kafka message.
type Handler interface {
	Handle(ctx context.Context, msg *sarama.ConsumerMessage) error
}

// BatchHandler processes a batch of Kafka messages at once.
type BatchHandler interface {
	HandleBatch(ctx context.Context, msgs []*sarama.ConsumerMessage) error
}

// Chain applies middlewares to a handler. First middleware is innermost.
// Chain(h, A, B, C) produces C(B(A(h))).
func Chain(h Handler, mw ...Middleware) Handler {
	for i := 0; i < len(mw); i++ {
		h = mw[i](h)
	}

	return h
}

// ChainBatch applies middlewares to a batch handler. First middleware is innermost.
// ChainBatch(h, A, B, C) produces C(B(A(h))).
func ChainBatch(h BatchHandler, mw ...BatchMiddleware) BatchHandler {
	for i := 0; i < len(mw); i++ {
		h = mw[i](h)
	}

	return h
}
