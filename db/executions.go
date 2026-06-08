package db

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/Eri-stay/practice-kafka/entities"
)

type Executions struct {
	DB *sql.DB
}

func (e *Executions) Insert(ctx context.Context, res entities.Result) error {
	query := `
		INSERT INTO executions (
			email_id, 
			status, 
			error_message, 
			executed_at)
		VALUES ($1, $2, $3, COALESCE($4, NOW()))
	`
	_, err := e.DB.ExecContext(ctx, query, res.EmailId, res.Status, res.ErrorMsg, res.Executed_at)
	if err != nil {
		return fmt.Errorf("insert execution record: %w", err)
	}
	return nil
}

func (e *Executions) CountByEmailID(ctx context.Context, emailId int) (int, error) {
	query := `
		SELECT COUNT(*) 
		FROM executions 
		WHERE email_id = $1
	`
	var count int
	err := e.DB.QueryRowContext(ctx, query, emailId).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count existing executions: %w", err)
	}
	return count, nil
}
