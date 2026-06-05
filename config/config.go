package config

import (
	"fmt"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	DbURL                      string
	KafkaBrokers               []string
	KafkaConsumerMockRequester string
	KafkaConsumerIngester      string
	KafkaConsumerDispatcher    string
	KafkaConsumerSender        string
	KafkaConsumerWriter        string
	TopicEmailRequests         string
	TopicEmailExecute          string
	TopicEmailResults          string

	SMTPHost string
	SMTPPort string
	SMTPUser string
	SMTPPass string

	DispatcherInterval string
	MaxRetries         string
}

func LoadConfig() (*Config, error) {
	// Load .env file (ignoring error gracefully if file is missing in prod)
	_ = godotenv.Load()

	dbURL := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s",
		os.Getenv("DB_USER"),
		os.Getenv("DB_PASSWORD"),
		os.Getenv("DB_HOST"),
		os.Getenv("DB_PORT"),
		os.Getenv("DB_NAME"),
		os.Getenv("DB_SSLMODE"),
	)

	return &Config{
		DbURL:                      dbURL,
		KafkaBrokers:               []string{os.Getenv("KAFKA_BROKERS")},
		KafkaConsumerMockRequester: os.Getenv("KAFKA_CONSUMER_MOCK_REQUESTER"),
		KafkaConsumerIngester:      os.Getenv("KAFKA_CONSUMER_INGESTER"),
		KafkaConsumerDispatcher:    os.Getenv("KAFKA_CONSUMER_DISPATCHER"),
		KafkaConsumerSender:        os.Getenv("KAFKA_CONSUMER_SENDER"),
		KafkaConsumerWriter:        os.Getenv("KAFKA_CONSUMER_RESULT_WRITER"),

		TopicEmailRequests: os.Getenv("TOPIC_EMAIL_REQUESTS"),
		TopicEmailExecute:  os.Getenv("TOPIC_EMAIL_EXECUTE"),
		TopicEmailResults:  os.Getenv("TOPIC_EMAIL_RESULTS"),

		SMTPHost: os.Getenv("SMTP_HOST"),
		SMTPPort: os.Getenv("SMTP_PORT"),
		SMTPUser: os.Getenv("SMTP_USER"),
		SMTPPass: os.Getenv("SMTP_PASS"),

		DispatcherInterval: os.Getenv("DISPATCHER_INTERVAL"),
		MaxRetries:         os.Getenv("MAX_RETRIES"),
	}, nil
}
