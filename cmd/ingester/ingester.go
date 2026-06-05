package ingester

import (
	"context"
	"fmt"

	"github.com/Eri-stay/practice-kafka/db"
	"github.com/Eri-stay/practice-kafka/entities"
)

type Ingester struct {
	db *db.Emails
}

func (i *Ingester) SaveEmailRequest(ctx context.Context, req entities.Request) error {
	dbReq := entities.Request{
		Recipient:    req.Recipient,
		Subject:      req.Subject,
		Body:         req.Body,
		ScheduleTime: req.ScheduleTime,
	}

	if _, err := i.db.Add(ctx, &dbReq); err != nil {
		return fmt.Errorf("write to db: %w", err)
	}

	return nil
}
