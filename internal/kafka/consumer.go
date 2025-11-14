package kafka

import (
	"context"
	"fmt"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Consumer struct {
	consumer *ckafka.Consumer
}

func NewConsumer(brokers []string, groupID string, topics []string) (*Consumer, error) {
	cfg := &ckafka.ConfigMap{
		"bootstrap.servers": brokersAsString(brokers),
		"group.id":          groupID,
		"auto.offset.reset": "earliest",
	}
	consumer, err := ckafka.NewConsumer(cfg)
	if err != nil {
		return nil, fmt.Errorf("create kafka consumer: %w", err)
	}
	if err := consumer.SubscribeTopics(topics, nil); err != nil {
		consumer.Close()
		return nil, fmt.Errorf("subscribe topics: %w", err)
	}
	return &Consumer{consumer: consumer}, nil
}

func (c *Consumer) Close() {
	if c.consumer != nil {
		c.consumer.Close()
	}
}

func (c *Consumer) ReadMessage(ctx context.Context, timeout time.Duration) (*ckafka.Message, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			msg, err := c.consumer.ReadMessage(timeout)
			if err != nil {
				if ckErr, ok := err.(ckafka.Error); ok && ckErr.Code() == ckafka.ErrTimedOut {
					continue
				}
				return nil, fmt.Errorf("read message: %w", err)
			}
			return msg, nil
		}
	}
}
