package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com/chat4all/chat4all-v2/internal/kafka"
	"github.com/chat4all/chat4all-v2/internal/models"
	"github.com/chat4all/chat4all-v2/internal/storage"
)

type Service struct {
	consumer *kafka.Consumer
	store    *storage.Store
	logger   *slog.Logger
}

func NewService(consumer *kafka.Consumer, store *storage.Store, logger *slog.Logger) *Service {
	return &Service{consumer: consumer, store: store, logger: logger}
}

func (s *Service) Run(ctx context.Context) error {
	s.logger.Info("worker started")
	for {
		msg, err := s.consumer.ReadMessage(ctx, time.Second)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				s.logger.Info("worker stopping", slog.String("reason", err.Error()))
				return nil
			}
			s.logger.Error("failed to read message", slog.Any("error", err))
			continue
		}
		if msg == nil {
			continue
		}

		if err := s.processMessage(ctx, msg); err != nil {
			s.logger.Error("failed to process message", slog.Any("error", err))
		}
	}
}

func (s *Service) processMessage(ctx context.Context, msg *ckafka.Message) error {
	topic := ""
	if msg.TopicPartition.Topic != nil {
		topic = *msg.TopicPartition.Topic
	}
	s.logger.Info("message consumed", slog.String("topic", topic), slog.Int("partition", int(msg.TopicPartition.Partition)), slog.Int64("offset", int64(msg.TopicPartition.Offset)))

	var message models.Message
	if err := json.Unmarshal(msg.Value, &message); err != nil {
		return err
	}

	s.logger.Info("saving message", slog.String("conversation_id", message.ConversationID), slog.String("message_id", message.MessageID))

	message.CreatedAt = message.CreatedAt.UTC()
	if err := s.store.CreateMessage(ctx, message); err != nil {
		return err
	}

	s.logger.Info("message persisted", slog.String("conversation_id", message.ConversationID), slog.String("message_id", message.MessageID))

	time.Sleep(200 * time.Millisecond)

	s.logger.Info("updating message status", slog.String("conversation_id", message.ConversationID), slog.String("message_id", message.MessageID), slog.String("status", string(models.MessageStatusDelivered)))

	if err := s.store.UpdateMessageStatus(ctx, message.ConversationID, message.MessageID, models.MessageStatusDelivered); err != nil {
		return err
	}

	s.logger.Info("message delivered", slog.String("conversation_id", message.ConversationID), slog.String("message_id", message.MessageID))

	return nil
}
