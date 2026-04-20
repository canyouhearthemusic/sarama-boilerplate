package main

import (
	"context"
	"database/sql"
	"kafka/service2/pkg/kafka/consumer"
	"log"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	_ "github.com/jackc/pgx/v5/stdlib"

	"kafka/service2/handlers"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	brokers := []string{"localhost:9095"}

	db, err := sql.Open("pgx",
		"postgres://user:secret@0.0.0.0:5432/service_2?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	dlqProducer := mustProducer(brokers)
	defer dlqProducer.Close()

	cfg := sarama.NewConfig()
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumers := []*consumer.TopicConsumer{
		consumer.NewTopicConsumer("user_waved", "svc2-user-waved", brokers, cfg,
			consumer.Chain(
				handlers.NewUserWavedHandler(db),
				consumer.WithRetry(3, 500*time.Millisecond),
				consumer.WithDLQ(dlqProducer, "user_waved_dlq"),
			),
		),
	}

	var wg sync.WaitGroup
	for _, c := range consumers {
		wg.Add(1)

		go func(c *consumer.TopicConsumer) {
			defer wg.Done()
			if err := c.Run(ctx); err != nil {
				log.Printf("[%s] stopped with error: %v", c.Topic(), err)
			}
		}(c)
	}

	wg.Wait()

	log.Println("all consumers stopped")
}

func mustProducer(brokers []string) sarama.SyncProducer {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true

	p, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		log.Fatal(err)
	}

	return p
}
