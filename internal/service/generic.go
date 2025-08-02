package service

import (
	"context"
	"database/sql"
	"fmt"

	sq "github.com/elgris/sqrl"
	"github.com/lib/pq"
	"github.com/pentops/ges/internal/gen/o5/ges/v1/ges_tpb"
	"github.com/pentops/ges/internal/replay"
	"github.com/pentops/j5/lib/j5codec"
	"github.com/pentops/log.go/log"
	"github.com/pentops/o5-messaging/gen/o5/messaging/v1/messaging_pb"
	"github.com/pentops/sqrlx.go/sqrlx"
)

func storeGeneric(ctx context.Context, db sqrlx.Transactor, msg *messaging_pb.Message) error {

	messageData, err := j5codec.Global.ProtoToJSON(msg.ProtoReflect())
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	log.WithFields(ctx, map[string]any{
		"messageId":   msg.MessageId,
		"grpcService": msg.GrpcService,
		"grpcMethod":  msg.GrpcMethod,
	}).Info("Generic message")

	return db.Transact(ctx, &sqrlx.TxOptions{
		ReadOnly:  false,
		Retryable: true,
		Isolation: sql.LevelReadCommitted,
	}, func(ctx context.Context, tx sqrlx.Transaction) error {
		_, err := tx.Exec(ctx, sq.Insert("generic").
			Columns(
				"message_id",
				"timestamp",
				"grpc_service",
				"grpc_method",
				"data",
			).
			Values(
				msg.MessageId,
				msg.Timestamp.AsTime(),
				msg.GrpcService,
				msg.GrpcMethod,
				messageData,
			),
		)
		if err != nil {
			return fmt.Errorf("failed to insert generic message: %w", err)
		}

		return nil
	})
}

func queueGenericMessages(ctx context.Context, db sqrlx.Transactor, req *ges_tpb.GenericMessage) error {
	sel := sq.Select().
		Column("CONCAT(?::text, '/', grpc_service, '/', grpc_method, '/', message_id)", req.QueueUrl).
		Column("grpc_service").
		Column("grpc_method").
		Column("message_id").
		Column("?", req.QueueUrl).
		From("generic").
		Where("grpc_service = ?", req.GrpcService)

	if req.GrpcMethod != nil {
		sel.Where("grpc_method = ?", *req.GrpcMethod)
	}

	ins := sq.Insert("replay_generic").
		Columns(
			"replay_id",
			"grpc_service",
			"grpc_method",
			"message_id",
			"queue_url",
		).
		Select(sel)

	log.WithFields(ctx, map[string]any{
		"queueUrl":    req.QueueUrl,
		"grpcService": req.GrpcService,
		"grpcMethod":  req.GrpcMethod,
	}).Info("Queueing generic events")

	return db.Transact(ctx, &sqrlx.TxOptions{
		ReadOnly:  false,
		Retryable: true,
	}, func(ctx context.Context, tx sqrlx.Transaction) error {
		_, err := tx.Exec(ctx, ins)
		if err != nil {
			return err
		}
		_, err = tx.ExecRaw(ctx, "NOTIFY "+ReplayGenericPGChannel)
		if err != nil {
			return err
		}
		return nil

	})

}

type genericRow struct {
	id          string
	destination string
	message     *messaging_pb.Message
}

func (gr *genericRow) Message() (*messaging_pb.Message, error) {
	return gr.message, nil
}

func (gr *genericRow) Destination() string {
	return gr.destination
}

var _ replay.MessageQuery[*genericRow] = (*GenericReplay)(nil)

type GenericReplay struct{}

func (gr *GenericReplay) SelectQuery() string {
	return "SELECT " +
		"  replay_generic.replay_id, " +
		"  replay_generic.queue_url, " +
		"  generic.data " +
		"FROM replay_generic " +
		"INNER JOIN generic " +
		"  ON generic.grpc_service = replay_generic.grpc_service " +
		"  AND generic.grpc_method = replay_generic.grpc_method " +
		"  AND generic.message_id = replay_generic.message_id " +
		"LIMIT 10 FOR UPDATE SKIP LOCKED"
}

func (gr *GenericReplay) ScanRow(row replay.Row) (*genericRow, error) {
	var outboxRow genericRow
	var dataBytes []byte
	err := row.Scan(&outboxRow.id, &outboxRow.destination, &dataBytes)
	if err != nil {
		return nil, fmt.Errorf("error scanning generic row: %w", err)
	}
	outboxRow.message = &messaging_pb.Message{}
	err = j5codec.Global.JSONToProto(dataBytes, outboxRow.message.ProtoReflect())
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling generic data: %w", err)
	}
	return &outboxRow, nil
}

func (uq *GenericReplay) DeleteQuery(rows []*genericRow) (string, []any, error) {
	ids := make([]string, len(rows))
	for i, row := range rows {
		ids[i] = row.id
	}
	return "DELETE FROM replay_generic WHERE replay_id = ANY($1)", []any{
		pq.StringArray(ids),
	}, nil
}
