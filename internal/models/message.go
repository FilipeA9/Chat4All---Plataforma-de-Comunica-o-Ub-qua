package models

import "time"

type MessageStatus string

const (
	MessageStatusSent      MessageStatus = "SENT"
	MessageStatusDelivered MessageStatus = "DELIVERED"
)

type Message struct {
	ConversationID string         `json:"conversation_id"`
	MessageID      string         `json:"message_id"`
	FromUser       string         `json:"from_user"`
	Payload        string         `json:"payload"`
	Status         MessageStatus  `json:"status"`
	Metadata       map[string]any `json:"metadata"`
	CreatedAt      time.Time      `json:"created_at"`
}

type CreateMessageRequest struct {
	ConversationID string         `json:"conversation_id"`
	FromUser       string         `json:"from_user"`
	Payload        string         `json:"payload"`
	Metadata       map[string]any `json:"metadata"`
}
