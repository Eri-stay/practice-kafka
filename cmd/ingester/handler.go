package ingester

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Eri-stay/practice-kafka/db"
	"github.com/Eri-stay/practice-kafka/entities"
)

type Handler struct {
	db *db.Emails
}

func (h *Handler) SaveEmailRequest(ctx context.Context, message []byte) error {
	var req entities.Request
	if err := json.Unmarshal(message, &req); err != nil {
		return fmt.Errorf("unmarshal email request: %w", err)
	}

	if _, err := h.db.Add(ctx, &req); err != nil {
		return fmt.Errorf("save email request: %w", err)
	}

	return nil
}
