package sender

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/wneessen/go-mail"

	"github.com/Eri-stay/practice-kafka/config"
	"github.com/Eri-stay/practice-kafka/entities"
)

type Handler struct {
	Cfg *config.Config
}

func (h Handler) SendEmail(ctx context.Context, message []byte) error {
	var email entities.Email
	if err := json.Unmarshal(message, &email); err != nil {
		return fmt.Errorf("unmarshal email: %w", err)
	}

	m := mail.NewMsg()
	if err := m.To(email.Recipient); err != nil {
		return fmt.Errorf("set To address: %w", err)
	}
	if err := m.From(h.Cfg.SMTPUser); err != nil {
		return fmt.Errorf("set From address: %w", err)
	}
	m.Subject(email.Subject)
	m.SetBodyString(mail.TypeTextPlain, email.Body)

	port, err := strconv.Atoi(h.Cfg.SMTPPort)
	if err != nil {
		return fmt.Errorf("convert port to int: %w", err)
	}
	c, err := mail.NewClient(h.Cfg.SMTPHost,
		mail.WithPort(port),
		mail.WithSMTPAuth(mail.SMTPAuthPlain),
		mail.WithUsername(h.Cfg.SMTPUser),
		mail.WithPassword(h.Cfg.SMTPPass))
	if err != nil {
		return fmt.Errorf("create smtp client: %w", err)
	}

	if err := c.DialAndSend(m); err != nil {
		return fmt.Errorf("send mail: %w", err)
	}

	return nil
}
