package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/chat4all/chat4all-v2/internal/models"
)

type Store struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() {
	if s.pool != nil {
		s.pool.Close()
	}
}

func (s *Store) CreateMessage(ctx context.Context, msg models.Message) error {
	if s.pool == nil {
		return errors.New("storage pool is nil")
	}
	if err := s.ensureConversation(ctx, msg.ConversationID); err != nil {
		return err
	}
	const query = `INSERT INTO messages (conversation_id, message_id, from_user, payload, status, metadata, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)`
	metadataJSON := mapToJSONB(msg.Metadata)
	_, err := s.pool.Exec(ctx, query,
		msg.ConversationID,
		msg.MessageID,
		msg.FromUser,
		msg.Payload,
		string(msg.Status),
		metadataJSON,
		msg.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert message: %w", err)
	}
	return nil
}

func (s *Store) UpdateMessageStatus(ctx context.Context, conversationID, messageID string, status models.MessageStatus) error {
	const query = `UPDATE messages SET status=$1, updated_at=NOW() WHERE conversation_id=$2 AND message_id=$3`
	_, err := s.pool.Exec(ctx, query, string(status), conversationID, messageID)
	if err != nil {
		return fmt.Errorf("update message status: %w", err)
	}
	return nil
}

func (s *Store) ListMessages(ctx context.Context, conversationID string, limit int) ([]models.Message, error) {
	const query = `SELECT conversation_id, message_id, from_user, payload, status, metadata, created_at FROM messages
WHERE conversation_id=$1 ORDER BY created_at ASC LIMIT $2`
	rows, err := s.pool.Query(ctx, query, conversationID, int32(limit))
	if err != nil {
		return nil, fmt.Errorf("query messages: %w", err)
	}
	defer rows.Close()

	var messages []models.Message
	for rows.Next() {
		var (
			msg         models.Message
			metadataRaw any
		)
		if err := rows.Scan(
			&msg.ConversationID,
			&msg.MessageID,
			&msg.FromUser,
			&msg.Payload,
			&msg.Status,
			&metadataRaw,
			&msg.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan message: %w", err)
		}
		metadata, err := normalizeMetadata(metadataRaw)
		if err != nil {
			return nil, err
		}
		msg.Metadata = metadata
		messages = append(messages, msg)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate messages: %w", err)
	}
	return messages, nil
}

func mapToJSONB(metadata map[string]any) any {
	if metadata == nil {
		return map[string]any{}
	}
	return metadata
}

func (s *Store) ensureConversation(ctx context.Context, conversationID string) error {
	const query = `INSERT INTO conversations (id) VALUES ($1) ON CONFLICT (id) DO NOTHING`
	if _, err := s.pool.Exec(ctx, query, conversationID); err != nil {
		return fmt.Errorf("ensure conversation: %w", err)
	}
	return nil
}

func normalizeMetadata(value any) (map[string]any, error) {
	switch v := value.(type) {
	case nil:
		return map[string]any{}, nil
	case map[string]any:
		return v, nil
	case []byte:
		if len(v) == 0 {
			return map[string]any{}, nil
		}
		var metadata map[string]any
		if err := json.Unmarshal(v, &metadata); err != nil {
			return nil, fmt.Errorf("decode metadata: %w", err)
		}
		return metadata, nil
	default:
		bytes, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("marshal metadata: %w", err)
		}
		var metadata map[string]any
		if err := json.Unmarshal(bytes, &metadata); err != nil {
			return nil, fmt.Errorf("decode metadata: %w", err)
		}
		return metadata, nil
	}
}
