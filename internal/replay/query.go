package replay

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/pentops/log.go/log"
	"github.com/pentops/sqrlx.go/sqrlx"
)

type Row interface {
	Scan(dest ...any) error
}

type Rows interface {
	Row
	Next() bool
	Close() error
	Err() error
}

type MessageQuery[T Message] interface {
	// SelectQuery returns the SQL query to select messages from the table.
	SelectQuery() string

	// DeleteQuery returns the SQL query to delete successfully published messages from the table.
	DeleteQuery([]T) (string, []interface{}, error)

	// ScanRow scans a row from the DB result into a message wrapper
	ScanRow(row Row) (T, error)
}

func WrapPublisher[T Message](sqs SQS, mq MessageQuery[T]) func(context.Context, Transaction) (bool, error) {
	return func(ctx context.Context, tx Transaction) (bool, error) {
		return DoPage(ctx, tx, sqs, mq)
	}
}

type Transaction interface {
	Query(ctx context.Context, query string, args ...any) (Rows, error)
	Exec(ctx context.Context, query string, args ...any) error
}

type pgxTx struct {
	wrapped pgx.Tx
}

func (tx *pgxTx) Query(ctx context.Context, query string, args ...any) (Rows, error) {
	rows, err := tx.wrapped.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("error querying messages: %w", err)
	}
	return pgxRows{rows}, nil
}

type pgxRows struct {
	pgx.Rows
}

func (r pgxRows) Close() error {
	r.Rows.Close()
	return nil
}

func (tx *pgxTx) Exec(ctx context.Context, query string, args ...any) error {
	_, err := tx.wrapped.Exec(ctx, query, args...)
	return err
}

type sqrlxTx struct {
	wrapped sqrlx.Transaction
}

func WrapSqrlx(tx sqrlx.Transaction) Transaction {
	return &sqrlxTx{wrapped: tx}
}

func (tx *sqrlxTx) Query(ctx context.Context, query string, args ...any) (Rows, error) {
	return tx.wrapped.QueryRaw(ctx, query, args...)
}

func (tx *sqrlxTx) Exec(ctx context.Context, query string, args ...any) error {
	_, err := tx.wrapped.ExecRaw(ctx, query, args...)
	return err
}

func DoPage[T Message](ctx context.Context, tx Transaction, sqs SQS, qq MessageQuery[T]) (bool, error) {
	var count int

	selectQuery := qq.SelectQuery()

	var sendError error

	batches := messageBatches[T]{}
	count = 0
	rows, err := tx.Query(ctx, selectQuery)
	if err != nil {
		log.WithError(ctx, err).Error("error selecting messages")
		return false, fmt.Errorf("error selecting messages: %w", err)
	}

	if err := func() error {
		defer rows.Close()
		for rows.Next() {
			count++
			msg, err := qq.ScanRow(rows)
			if err != nil {
				return fmt.Errorf("error scanning row: %w", err)
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

		log.WithField(ctx, "successCount", len(successMessages)).Debug("published messages")
		deleteQuery, args, err := qq.DeleteQuery(successMessages)
		if err != nil {
			return false, fmt.Errorf("error creating delete query: %w", err)
		}
		err = tx.Exec(ctx, deleteQuery, args...)
		if err != nil {
			log.WithError(ctx, err).Error("error deleting sent messages")
			return false, fmt.Errorf("error deleting messages: %w", err)
		}
	}

	if sendError != nil {
		return true, fmt.Errorf("error sending batch of replay messages: %w", sendError)
	}

	return count > 0, nil
}
