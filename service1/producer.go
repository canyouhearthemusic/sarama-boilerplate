package main

import (
	"log"

	"github.com/IBM/sarama"
)

func newProducer() sarama.SyncProducer {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true

	log.Println("Kafka config initialized")

	producer, err := sarama.NewSyncProducer([]string{"0.0.0.0:9095"}, cfg)
	if err != nil {
		log.Fatalf("Kafka error: %s", err)
	}
	log.Println("Kafka producer created")

	return producer
}
