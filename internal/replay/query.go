package replay

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/pentops/log.go/log"
)

type Row interface {
	Scan(dest ...any) error
}

type MessageQuery[T Message] interface {
	// SelectQuery returns the SQL query to select messages from the table.
	SelectQuery() string

	// DeleteQuery returns the SQL query to delete successfully published messages from the table.
	DeleteQuery([]T) (string, []interface{}, error)

	// ScanRow scans a row from the DB result into a message wrapper
	ScanRow(row Row) (T, error)
}

func doPage[T Message](ctx context.Context, tx pgx.Tx, sqs SQS, qq MessageQuery[T]) (bool, error) {
	var count int

	selectQuery := qq.SelectQuery()

	var sendError error

	batches := messageBatches[T]{}
	count = 0
	rows, err := tx.Query(ctx, selectQuery)
	if err != nil {
		return false, fmt.Errorf("error selecting outbox messages: %w", err)
	}

	if err := func() error {
		defer rows.Close()
		for rows.Next() {
			count++
			msg, err := qq.ScanRow(rows)
			if err != nil {
				return fmt.Errorf("error scanning outbox row: %w", err)
			}
			if err := batches.addRow(msg); err != nil {
				return err
			}
		}
		return rows.Err()
	}(); err != nil {
		return false, err
	}

	log.WithField(ctx, "count", count).Debug("got messages")

	for _, batch := range batches {
		successMessages, err := publishBatch(ctx, sqs, batch)
		if err != nil {
			log.WithError(ctx, err).Error("error publishing batch")
			sendError = err
			// NOTE: sendError is handled at the end, the transaction should still be
			// committed for any prior successful sends
			break
		}

		log.WithField(ctx, "successCount", len(successMessages)).Debug("published outbox messages")
		deleteQuery, args, err := qq.DeleteQuery(successMessages)
		if err != nil {
			return false, fmt.Errorf("error creating delete query: %w", err)
		}
		_, err = tx.Exec(ctx, deleteQuery, args...)
		if err != nil {
			return false, fmt.Errorf("error deleting outbox messages: %w", err)
		}
	}

	if sendError != nil {
		return true, fmt.Errorf("error sending batch of replay messages: %w", sendError)
	}

	return count > 0, nil
}
