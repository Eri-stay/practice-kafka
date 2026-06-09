package sender

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/wneessen/go-mail"

	"github.com/Eri-stay/practice-kafka/config"
	"github.com/Eri-stay/practice-kafka/entities"
	"github.com/Eri-stay/practice-kafka/messenger"
	wherr "github.com/Eri-stay/practice-kafka/pkg/errors"
	"github.com/Eri-stay/practice-kafka/pkg/metrics"
)

const (
	ErrInvalidParameter = wherr.Error("invalid parameter")
	ErrDeliveryFailed   = wherr.Error("smtp delivery failed")
)

type sender struct {
	cfg      *config.Config
	producer messenger.Producer
	client   *mail.Client
}

func (s *sender) SendEmail(ctx context.Context, email entities.Email) error {
	m := mail.NewMsg()

	if err := m.To(email.Recipient); err != nil {
		metrics.DeliveryAttempts.WithLabelValues("perm_fail").Inc()
		return fmt.Errorf("%w: recipient: %s", ErrInvalidParameter, err.Error())
	}
	if err := m.From(s.cfg.SMTPUser); err != nil {
		metrics.DeliveryAttempts.WithLabelValues("perm_fail").Inc()
		return fmt.Errorf("%w: sender: %s", ErrInvalidParameter, err.Error())
	}
	m.Subject(email.Subject)
	m.SetBodyString(mail.TypeTextPlain, email.Body)

	timer := prometheus.NewTimer(metrics.SMTPRequestDuration)
	err := s.client.DialAndSend(m)
	timer.ObserveDuration()

	if err != nil {
		// retry later
		metrics.DeliveryAttempts.WithLabelValues("temp_fail").Inc()
		return fmt.Errorf("%w: %s", ErrDeliveryFailed, err.Error())
	}

	// success
	metrics.DeliveryAttempts.WithLabelValues("success").Inc()
	return nil
}

func (s *sender) ProduceResult(emailId int, status entities.Status, errorMsg string) error {
	now := time.Now()
	res := entities.Result{
		EmailId:     emailId,
		Status:      string(status),
		ErrorMsg:    errorMsg,
		Executed_at: &now,
	}

	if err := s.producer.ExecutionResultEvent(res); err != nil {
		return fmt.Errorf("produce execution result: %w", err)
	}

	return nil
}
