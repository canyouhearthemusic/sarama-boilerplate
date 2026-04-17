package main

import (
	"context"
	"fmt"

	"github.com/IBM/sarama"
)

type greetingService struct {
	kafkaProducer sarama.SyncProducer
}

func newGreetingService(kafkaProducer sarama.SyncProducer) *greetingService {
	return &greetingService{kafkaProducer: kafkaProducer}
}

func (s *greetingService) Greetings(ctx context.Context, name string) error {
	text := fmt.Sprintf("Hi, %s!", name)

	msg := &sarama.ProducerMessage{
		Topic: "user_waved",
		Key:   sarama.StringEncoder(name),
		Value: sarama.StringEncoder(text),
	}

	_, _, err := s.kafkaProducer.SendMessage(msg)
	if err != nil {
		return err
	}

	return nil
}
