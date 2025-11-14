package cfg

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

type Config struct {
	HTTPPort         int
	JWTSecret        string
	KafkaBrokers     []string
	KafkaTopic       string
	KafkaGroupID     string
	PostgresURL      string
	ShutdownTimeoutS int
}

func Load() (*Config, error) {
	_ = godotenv.Load()

	httpPort := getEnvAsInt("HTTP_PORT", 8080)
	jwtSecret := getEnv("JWT_SECRET", "chat4all-static-secret")
	brokers := getEnvAsSlice("KAFKA_BROKERS", []string{"kafka:9092"}, ",")
	topic := getEnv("KAFKA_TOPIC", "messages")
	groupID := getEnv("KAFKA_GROUP_ID", "chat4all-worker")
	postgresURL := getEnv("POSTGRES_URL", "postgres://chat4all:chat4all@postgres:5432/chat4all?sslmode=disable")
	shutdownTimeout := getEnvAsInt("SHUTDOWN_TIMEOUT", 15)

	return &Config{
		HTTPPort:         httpPort,
		JWTSecret:        jwtSecret,
		KafkaBrokers:     brokers,
		KafkaTopic:       topic,
		KafkaGroupID:     groupID,
		PostgresURL:      postgresURL,
		ShutdownTimeoutS: shutdownTimeout,
	}, nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvAsInt(key string, defaultValue int) int {
	if valueStr := os.Getenv(key); valueStr != "" {
		if value, err := strconv.Atoi(valueStr); err == nil {
			return value
		}
	}
	return defaultValue
}

func getEnvAsSlice(key string, defaultValue []string, sep string) []string {
	valueStr := os.Getenv(key)
	if valueStr == "" {
		return defaultValue
	}
	parts := strings.Split(valueStr, sep)
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	if len(result) == 0 {
		return defaultValue
	}
	return result
}

func (c *Config) HTTPAddr() string {
	return fmt.Sprintf(":%d", c.HTTPPort)
}
