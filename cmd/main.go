package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Eri-stay/practice-kafka/cmd/dispatcher"
	"github.com/Eri-stay/practice-kafka/cmd/ingester"
	"github.com/Eri-stay/practice-kafka/cmd/mock_email_requester"
	"github.com/Eri-stay/practice-kafka/cmd/result_recorder"
	"github.com/Eri-stay/practice-kafka/cmd/sender"
	"github.com/Eri-stay/practice-kafka/config"
	"github.com/urfave/cli/v2"
)

func main() {
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Printf("Failed to load config: %v", err)
	}

	app := &cli.App{
		Name:                 "email",
		Usage:                "Microservice for managing and sending emails",
		EnableBashCompletion: true,
		Commands: []*cli.Command{
			mock_email_requester.Command(cfg),
			ingester.Command(cfg),
			dispatcher.Command(cfg),
			sender.Command(cfg),
			result_recorder.Command(cfg),
		},
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := app.RunContext(ctx, os.Args); err != nil {
		log.Printf("Failed to run a program: ", err)
	}
}
