package replay

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pentops/log.go/log"
)

type Topic struct {
	Name string

	// Callback runs at the beginning of each connection, and on each
	// notification. Return 'true' to re-run the callback (e.g. for draining the
	// a queue)
	Callback func(context.Context, Transaction) (more bool, err error)
}

// Listener listens for Postgres notifications on a loop, and handles dropped
// connections.
type Listener struct {
	postgresDSN string
	conn        *pgx.Conn
	topics      []Topic
}

func NewListener(dsn string, topics []Topic) (*Listener, error) {
	return &Listener{
		postgresDSN: dsn,
		topics:      topics,
	}, nil
}

func (ll *Listener) connection(ctx context.Context) (*pgx.Conn, error) {
	if ll.conn != nil {
		return ll.conn, nil
	}

	dialContext, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	conn, err := pgx.Connect(dialContext, ll.postgresDSN)
	if err != nil {
		return nil, fmt.Errorf("connecting to PG: %w", err)
	}

	for {
		if err := conn.Ping(dialContext); err != nil {
			log.WithError(ctx, err).Warn("pinging Listener PG")
			time.Sleep(time.Second)
			continue
		}

		log.Info(ctx, "pinging Listener PG OK")
		break
	}

	for _, topic := range ll.topics {
		if !rePostgresCleanTopic.MatchString(topic.Name) {
			return nil, fmt.Errorf("invalid topic name: %s", topic.Name)
		}
		if _, err := conn.Exec(ctx, fmt.Sprintf("LISTEN %s", topic.Name)); err != nil {
			return nil, fmt.Errorf("error listening to topic %s: %w", topic.Name, err)
		}
	}

	return conn, nil
}

var rePostgresCleanTopic = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

func (ll *Listener) closeConnection(ctx context.Context) {
	if err := ll.conn.Close(ctx); err != nil {
		log.WithError(ctx, err).Error("error closing PG connection")
		// but ignore
	}
	ll.conn = nil
}

func (ll *Listener) waitForNotification(ctx context.Context) (*pgconn.Notification, error) {
	conn, err := ll.connection(ctx)
	if err != nil {
		return nil, err
	}
	noti, err := conn.WaitForNotification(ctx)
	if err == nil { // Short Happy Path
		return noti, nil
	}

	log.WithError(ctx, err).Warn("listener error, drop connection")
	ll.closeConnection(ctx)
	return ll.waitForNotification(ctx)
}

// Listen first calls each callback to handle any messages while the connection
// was down, then runs the callbacks on each notification.
// Any callback error will exit the listener loop.
func (ll *Listener) Listen(ctx context.Context) error {
	var err error

	conn, err := ll.connection(ctx)
	if err != nil {
		return fmt.Errorf("error getting connection: %w", err)
	}

	for _, topic := range ll.topics {
		err := ll.runCallback(ctx, topic, conn)
		if err != nil {
			return err
		}
	}

	for {
		log.Debug(ctx, "waiting for notification")

		noti, err := ll.waitForNotification(ctx)
		if err != nil {
			return err
		}

		found := false
		for _, topic := range ll.topics {
			if topic.Name == noti.Channel {
				err := ll.runCallback(ctx, topic, conn)
				if err != nil {
					return err
				}
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("unknown notification channel: %s", noti.Channel)
		}
	}
}

func (ll *Listener) runCallback(ctx context.Context, topic Topic, conn *pgx.Conn) error {
	log.WithField(ctx, "topic", topic.Name).Debug("running callback")
	hasMore := true
	var err error
	for hasMore {
		err = transact(ctx, conn, func(ctx context.Context, tx Transaction) error {
			hasMore, err = topic.Callback(ctx, tx)
			if err != nil {
				return fmt.Errorf("error running callback: %w", err)
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func transact(ctx context.Context, conn *pgx.Conn, callback func(context.Context, Transaction) error) error {
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.ReadCommitted,
	})
	if err != nil {
		return fmt.Errorf("error beginning transaction: %w", err)
	}

	wrapped := &pgxTx{wrapped: tx}
	err = callback(ctx, wrapped)
	if err != nil {
		rollbackErr := tx.Rollback(ctx)
		if rollbackErr != nil {
			log.WithError(ctx, rollbackErr).Error("error rolling back transaction")
		}
		return err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("error committing transaction: %w", err)
	}

	return nil
}
