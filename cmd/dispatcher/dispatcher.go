package dispatcher

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/Eri-stay/practice-kafka/config"
	"github.com/Eri-stay/practice-kafka/db"
	"github.com/Eri-stay/practice-kafka/entities"
	"github.com/Eri-stay/practice-kafka/messenger"
)

type Dispatcher struct {
	EmailsDB db.Emails
	Channel  chan entities.Email
	Context  context.Context
	Produser messenger.Producer
	Config   *config.Config
}

func (d *Dispatcher) RetrieveEmailsToSend() error {
	ticker_p := time.NewTicker(2 * time.Second)
	ticker_f := time.NewTicker(10 * time.Second)
	defer ticker_f.Stop()
	defer ticker_p.Stop()

	for {
		select {
		case <-d.Context.Done():
			close(d.Channel)
			return nil

		case <-ticker_p.C:
			emails, err := d.EmailsDB.RetrievePending(d.Context, EmailLimitPending)
			if err != nil {
				log.Printf("Failed to fetch pending emails: %w", err)
				return fmt.Errorf("fetch pending emails: %w", err)
			}

			for _, e := range emails {
				d.Channel <- e
			}

		case <-ticker_f.C:
			// emails, err := d.EmailsDB.RetrieveFailed(d.Context, EmailLimitFailed)
			// if err != nil {
			// return fmt.Errorf("fetch fauled emails: %w", err)
			// }

			// for _, e := range emails {
			// 	d.Ch <- e
			// }
		}
	}
}

func (d *Dispatcher) DispatchToSender(email entities.Email) error {
	bytes, err := json.Marshal(email)
	if err != nil {
		return fmt.Errorf("marshal email: %w", err)
	}

	if err := d.Produser.Produce(d.Config.TopicEmailExecute, bytes); err != nil {
		return fmt.Errorf("produse an execute request: %w", err)
	}

	return nil
}
