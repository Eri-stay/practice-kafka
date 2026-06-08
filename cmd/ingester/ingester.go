package ingester

import (
	"context"
	"fmt"

	"github.com/Eri-stay/practice-kafka/db"
	"github.com/Eri-stay/practice-kafka/entities"
)

type ingester struct {
	db *db.Emails
}

func (i *ingester) SaveEmailRequest(ctx context.Context, req entities.Request) error {
	if _, err := i.db.Add(ctx, &req); err != nil {
		return fmt.Errorf("write to db: %w", err)
	}

	return nil
}
