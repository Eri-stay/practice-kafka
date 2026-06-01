package db

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/Eri-stay/practice-kafka/entities"
	"github.com/lib/pq"
)

type Emails struct {
	DB *sql.DB
}

func (e *Emails) Add(ctx context.Context, req *entities.Request) (int, error) {
	query := `
	INSERT INTO emails (recipient, subject, body, schedule_time)
	VALUES ($1, $2, $3, $4)
	RETURNING id
	`
	var id int
	err := e.DB.QueryRowContext(ctx, query, req.Recipient, req.Subject, req.Body, req.ScheduleTime).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("save request: %w", err)
	}
	return id, nil
}

func (e *Emails) RetrievePending(ctx context.Context, count int) ([]entities.Email, error) {
	tx, err := e.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("open new transaction: %w", err)
	}
	defer tx.Rollback()
	query := `
	SELECT id, recipient, subject, body
	FROM emails
	WHERE status = 'pending' AND (schedule_time IS NULL OR schedule_time <= NOW())
	LIMIT $1
	FOR UPDATE SKIP LOCKED;
	`
	rows, err := tx.QueryContext(ctx, query, count)
	if err != nil {
		return nil, fmt.Errorf("list pending emails: %w", err)
	}
	defer rows.Close()

	var emails []entities.Email
	var ids []int

	for rows.Next() {
		var email entities.Email
		if err := rows.Scan(&email.Id, &email.Recipient, &email.Subject, &email.Body); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}
		emails = append(emails, email)
		ids = append(ids, email.Id)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	// if no emails - return
	if len(ids) == 0 {
		return emails, nil
	}

	query = `
	UPDATE emails
	SET  status = 'in_progress'
	WHERE id = ANY ($1)
	`
	_, err = tx.ExecContext(ctx, query, pq.Array(ids))
	if err != nil {
		return nil, fmt.Errorf("update status for pending emails: %w", err)
	}

	// end transaction
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit transaction: %w", err)
	}
	return emails, nil
}
