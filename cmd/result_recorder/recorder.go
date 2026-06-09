package result_recorder

import (
	"context"
	"fmt"

	"github.com/Eri-stay/practice-kafka/db"
	"github.com/Eri-stay/practice-kafka/entities"
	"github.com/Eri-stay/practice-kafka/pkg/metrics"
)

type recorder struct {
	dbEmails     *db.Emails
	dbExecutions *db.Executions
	maxRetries   int
}

func (r *recorder) SaveExecResult(ctx context.Context, res entities.Result) error {
	var emailStatus string

	switch res.Status {
	case string(entities.StatusSuccess):
		metrics.FinalEmailOutcome.WithLabelValues("sent").Inc()
		emailStatus = "sent"

	case string(entities.StatusPermFail):
		metrics.FinalEmailOutcome.WithLabelValues("totally_failed").Inc()
		emailStatus = "totally_failed"

	case string(entities.StatusTempFail):
		// find out emailStatus based on number of execution attempts
		count, err := r.dbExecutions.CountByEmailID(ctx, res.EmailId)
		if err != nil {
			return err
		}
		if count >= r.maxRetries {
			metrics.FinalEmailOutcome.WithLabelValues("totally_failed").Inc()
			emailStatus = "totally_failed"
		} else {
			emailStatus = "failed"
		}

	default:
		return fmt.Errorf("unknown result status: %s", res.Status)
	}

	// insert new execution
	if err := r.dbExecutions.Insert(ctx, res); err != nil {
		return err
	}

	// update email
	if emailStatus == "sent" {
		if err := r.dbEmails.MarkAsSent(ctx, res.EmailId, res.Executed_at); err != nil {
			return err
		}
	} else {
		if err := r.dbEmails.UpdateStatus(ctx, res.EmailId, emailStatus); err != nil {
			return err
		}
	}

	return nil
}
