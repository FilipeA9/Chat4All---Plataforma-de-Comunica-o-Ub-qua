package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"

	"github.com/chat4all/chat4all-v2/internal/auth"
	"github.com/chat4all/chat4all-v2/internal/cfg"
	"github.com/chat4all/chat4all-v2/internal/kafka"
	"github.com/chat4all/chat4all-v2/internal/models"
	"github.com/chat4all/chat4all-v2/internal/storage"
)

type Server struct {
	cfg      *cfg.Config
	router   *mux.Router
	producer *kafka.Producer
	store    *storage.Store
	logger   *slog.Logger
}

func NewServer(cfg *cfg.Config, producer *kafka.Producer, store *storage.Store, logger *slog.Logger) *Server {
	s := &Server{
		cfg:      cfg,
		router:   mux.NewRouter(),
		producer: producer,
		store:    store,
		logger:   logger,
	}

	apiRouter := s.router.PathPrefix("/v1").Subrouter()
	authMiddleware := auth.JWTMiddleware(cfg.JWTSecret)
	apiRouter.Use(authMiddleware)

	apiRouter.HandleFunc("/messages", s.handleCreateMessage).Methods(http.MethodPost)
	apiRouter.HandleFunc("/conversations/{id}/messages", s.handleListMessages).Methods(http.MethodGet)

	return s
}

func (s *Server) Router() http.Handler {
	return s.router
}

func (s *Server) handleCreateMessage(w http.ResponseWriter, r *http.Request) {
	var req models.CreateMessageRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := validateCreateMessageRequest(req); err != nil {
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	messageID := uuid.NewString()
	metadata := req.Metadata
	if metadata == nil {
		metadata = map[string]any{}
	}

	msg := models.Message{
		ConversationID: req.ConversationID,
		MessageID:      messageID,
		FromUser:       req.FromUser,
		Payload:        req.Payload,
		Status:         models.MessageStatusSent,
		Metadata:       metadata,
		CreatedAt:      time.Now().UTC(),
	}

	s.logger.Info("message received", slog.String("conversation_id", msg.ConversationID), slog.String("message_id", msg.MessageID))

	payload, err := json.Marshal(msg)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "failed to serialize message")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	if err := s.producer.Produce(ctx, s.cfg.KafkaTopic, []byte(msg.ConversationID), payload); err != nil {
		s.logger.Error("failed to produce message", slog.String("conversation_id", msg.ConversationID), slog.String("message_id", msg.MessageID), slog.Any("error", err))
		s.writeError(w, http.StatusInternalServerError, "failed to publish message")
		return
	}

	s.logger.Info("message published to kafka", slog.String("conversation_id", msg.ConversationID), slog.String("message_id", msg.MessageID))

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"message_id":      msg.MessageID,
		"conversation_id": msg.ConversationID,
		"status":          "accepted",
	})
}

func (s *Server) handleListMessages(w http.ResponseWriter, r *http.Request) {
	conversationID := mux.Vars(r)["id"]
	if conversationID == "" {
		s.writeError(w, http.StatusBadRequest, "conversation_id is required")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	messages, err := s.store.ListMessages(ctx, conversationID, 100)
	if err != nil {
		s.logger.Error("failed to list messages", slog.String("conversation_id", conversationID), slog.Any("error", err))
		s.writeError(w, http.StatusInternalServerError, "failed to fetch messages")
		return
	}

	s.logger.Info("messages retrieved", slog.String("conversation_id", conversationID), slog.Int("count", len(messages)))

	s.writeJSON(w, http.StatusOK, map[string]any{
		"conversation_id": conversationID,
		"messages":        messages,
	})
}

func (s *Server) writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func (s *Server) writeError(w http.ResponseWriter, status int, message string) {
	s.writeJSON(w, status, map[string]any{"error": message})
}

func validateCreateMessageRequest(req models.CreateMessageRequest) error {
	if req.ConversationID == "" {
		return errors.New("conversation_id is required")
	}
	if _, err := uuid.Parse(req.ConversationID); err != nil {
		return errors.New("conversation_id must be a valid UUID")
	}
	if req.FromUser == "" {
		return errors.New("from_user is required")
	}
	if req.Payload == "" {
		return errors.New("payload is required")
	}
	return nil
}
