package main

import (
	"log"
	"os"

	"github.com/Eri-stay/practice-kafka/cmd/dispatcher"
	"github.com/Eri-stay/practice-kafka/cmd/ingester"
	"github.com/Eri-stay/practice-kafka/cmd/mock_email_requester"
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
			// sender.Command(cfg),
			// result_handler.Command(cfg),
		},
	}
	if err := app.Run(os.Args); err != nil {
		log.Printf("Failed to run a program: ", err)
	}
}
