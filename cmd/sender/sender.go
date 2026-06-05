package sender

import (
	"context"
	"fmt"
	"time"

	"github.com/wneessen/go-mail"

	"github.com/Eri-stay/practice-kafka/config"
	"github.com/Eri-stay/practice-kafka/entities"
	"github.com/Eri-stay/practice-kafka/messenger"
	wherr "github.com/Eri-stay/practice-kafka/pkg/errors"
)

const (
	ErrInvalidParameter = wherr.Error("invalid parameter")
	ErrDeliveryFailed   = wherr.Error("smtp delivery failed")
)

type Sender struct {
	Cfg      *config.Config
	Producer messenger.Producer
	Client   *mail.Client
}

func (s *Sender) SendEmail(ctx context.Context, email entities.Email) error {
	m := mail.NewMsg()

	if err := m.To(email.Recipient); err != nil {
		return fmt.Errorf("%w: recipient: %s", ErrInvalidParameter, err.Error())
	}
	if err := m.From(s.Cfg.SMTPUser); err != nil {
		return fmt.Errorf("%w: sender: %s", ErrInvalidParameter, err.Error())
	}
	m.Subject(email.Subject)
	m.SetBodyString(mail.TypeTextPlain, email.Body)

	if err := s.Client.DialAndSend(m); err != nil {
		// retry later
		return fmt.Errorf("%w: %s", ErrDeliveryFailed, err.Error())
	}

	// success
	return nil
}

func (s *Sender) ProduceResult(emailId int, status entities.Status, errorMsg string) error {
	res := entities.Result{
		EmailId:    emailId,
		Status:     string(status),
		ErrorMsg:   errorMsg,
		Created_at: &time.Time{},
	}

	if err := s.Producer.ExecutionResultEvent(res); err != nil {
		return fmt.Errorf("produce execution result: %w", err)
	}

	return nil
}
