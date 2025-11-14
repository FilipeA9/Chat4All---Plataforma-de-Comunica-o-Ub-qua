package kafka

import (
	"context"
	"fmt"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Producer struct {
	producer *ckafka.Producer
}

func NewProducer(brokers []string) (*Producer, error) {
	cfg := &ckafka.ConfigMap{
		"bootstrap.servers": brokersAsString(brokers),
	}
	producer, err := ckafka.NewProducer(cfg)
	if err != nil {
		return nil, fmt.Errorf("create kafka producer: %w", err)
	}
	return &Producer{producer: producer}, nil
}

func (p *Producer) Close() {
	if p.producer != nil {
		p.producer.Close()
	}
}

func (p *Producer) Produce(ctx context.Context, topic string, key, value []byte) error {
	deliveryChan := make(chan ckafka.Event, 1)
	message := &ckafka.Message{
		TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: ckafka.PartitionAny},
		Key:            key,
		Value:          value,
	}

	if err := p.producer.Produce(message, deliveryChan); err != nil {
		return fmt.Errorf("produce message: %w", err)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case e := <-deliveryChan:
		switch ev := e.(type) {
		case *ckafka.Message:
			if ev.TopicPartition.Error != nil {
				return fmt.Errorf("delivery failed: %w", ev.TopicPartition.Error)
			}
		default:
			return fmt.Errorf("unexpected delivery event: %T", e)
		}
	case <-time.After(10 * time.Second):
		return fmt.Errorf("delivery timeout")
	}

	return nil
}

func brokersAsString(brokers []string) string {
	if len(brokers) == 0 {
		return ""
	}
	result := brokers[0]
	for i := 1; i < len(brokers); i++ {
		result += "," + brokers[i]
	}
	return result
}
