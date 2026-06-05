package dispatcher

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Eri-stay/practice-kafka/db"
	"github.com/Eri-stay/practice-kafka/entities"
)

type Producer interface {
	ExecutionRequestEvent(event entities.Email) error
}

type dispatcher struct {
	emailsDB     db.Emails
	channel      chan entities.Email
	context      context.Context
	timeInterval int
}

func (d *dispatcher) RetrieveEmailsToSend() error {
	ticker_p := time.NewTicker(2 * time.Second)
	ticker_f := time.NewTicker(10 * time.Second)
	defer ticker_f.Stop()
	defer ticker_p.Stop()

	for {
		select {
		case <-d.context.Done():
			close(d.channel)
			return nil

		case <-ticker_p.C:
			emails, err := d.emailsDB.RetrievePending(d.context, EmailLimitPending)
			if err != nil {
				log.Printf("Failed to fetch pending emails: %w", err)
				return fmt.Errorf("fetch pending emails: %w", err)
			}

			for _, e := range emails {
				d.channel <- e
			}

		case <-ticker_f.C:
			emails, err := d.emailsDB.RetrieveTempFailed(d.context, d.timeInterval, EmailLimitFailed)
			if err != nil {
				return fmt.Errorf("fetch fauled emails: %w", err)
			}

			for _, e := range emails {
				d.channel <- e
			}
		}
	}
}
