package config

import (
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	DbURL        string
	KafkaBrokers []string
	// add more...
}

func LoadConfig() *Config {
	// Load .env file
	_ = godotenv.Load() 

	return &Config{
		DbURL: os.Getenv("DATABASE_URL"), // Or construct from parts
		KafkaBrokers: []string{os.Getenv("KAFKA_BROKERS")},
	}
}