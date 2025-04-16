package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	sq "github.com/elgris/sqrl"
	"github.com/lib/pq"
	"github.com/pentops/ges/internal/gen/o5/ges/v1/ges_pb"
	"github.com/pentops/ges/internal/gen/o5/ges/v1/ges_tpb"
	"github.com/pentops/ges/internal/replay"
	"github.com/pentops/j5/j5types/any_j5t"
	"github.com/pentops/log.go/log"
	"github.com/pentops/o5-messaging/gen/o5/messaging/v1/messaging_pb"
	"github.com/pentops/sqrlx.go/sqrlx"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type upsertRow struct {
	id          string
	message     *ges_pb.Upsert
	destination string
}

func (ur *upsertRow) Message() (*messaging_pb.Message, error) {
	return &messaging_pb.Message{
		MessageId:   ur.id,
		GrpcService: ur.message.GrpcService,
		GrpcMethod:  ur.message.GrpcMethod,
		Timestamp:   timestamppb.Now(),
		Extension: &messaging_pb.Message_Upsert_{
			Upsert: &messaging_pb.Message_Upsert{
				EntityName: ur.message.EntityName,
			},
		},
		Body: &messaging_pb.Any{
			TypeUrl:  fmt.Sprintf("type.googleapis.com/%s", ur.message.Data.TypeName),
			Encoding: messaging_pb.WireEncoding_J5_JSON,
			Value:    ur.message.Data.J5Json,
		},
	}, nil

}

func (ur *upsertRow) Destination() string {
	return ur.destination
}

type UpsertReplay struct{}

var _ replay.MessageQuery[*upsertRow] = (*UpsertReplay)(nil)

func storeUpsert(ctx context.Context, db sqrlx.Transactor, msg *messaging_pb.Message) error {
	ext := msg.GetUpsert()

	shell := &UpsertShell{}

	err := json.Unmarshal(msg.Body.Value, &shell)
	if err != nil {
		return fmt.Errorf("failed to unmarshal body: %w", err)
	}

	upsert := &ges_pb.Upsert{
		EntityName:         ext.EntityName,
		EntityId:           shell.Metadata.EntityID,
		LastEventId:        shell.Metadata.EventID,
		GrpcMethod:         msg.GrpcMethod,
		GrpcService:        msg.GrpcService,
		LastEventTimestamp: timestamppb.New(shell.Metadata.Timestamp),
		Data: &any_j5t.Any{
			TypeName: strings.TrimPrefix(msg.Body.TypeUrl, googleTypePrefix),
			J5Json:   msg.Body.Value,
		},
	}

	upsertData, err := protojson.Marshal(upsert)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}
	log.WithFields(ctx, map[string]interface{}{
		"entityName":         upsert.EntityName,
		"entityId":           upsert.EntityId,
		"lastEventId":        upsert.LastEventId,
		"lastEventTimestamp": upsert.LastEventTimestamp.AsTime(),
	}).Info("Upsert")

	qq := sqrlx.
		Upsert("upsert").
		Key("grpc_service", upsert.GrpcService).
		Key("grpc_method", upsert.GrpcMethod).
		Key("entity_id", upsert.EntityId).
		Set("entity_name", upsert.EntityName).
		Set("last_event_id", upsert.LastEventId).
		Set("last_event_timestamp", upsert.LastEventTimestamp.AsTime()).
		Set("data", upsertData).
		Where("EXCLUDED.last_event_timestamp > upsert.last_event_timestamp")

	return db.Transact(ctx, &sqrlx.TxOptions{
		ReadOnly:  false,
		Retryable: true,
		Isolation: sql.LevelReadCommitted,
	}, func(ctx context.Context, tx sqrlx.Transaction) error {
		_, err := tx.Exec(ctx, qq)
		if err != nil {
			return fmt.Errorf("failed to insert event: %w", err)
		}

		return nil
	})
}

type UpsertShell struct {
	Metadata UpsertMetadata `json:"upsert"`
}

type UpsertMetadata struct {
	EntityID  string    `json:"entityId"`
	EventID   string    `json:"eventId"`
	Timestamp time.Time `json:"timestamp"`
}

func (uq *UpsertReplay) SelectQuery() string {
	return "SELECT " +
		"  replay_upsert.replay_id, " +
		"  replay_upsert.queue_url, " +
		"  upsert.data " +
		"FROM replay_upsert " +
		"INNER JOIN upsert " +
		"  ON upsert.grpc_service = replay_upsert.grpc_service " +
		"  AND upsert.grpc_method = replay_upsert.grpc_method " +
		"  AND upsert.entity_id = replay_upsert.entity_id " +
		"LIMIT 10 FOR UPDATE SKIP LOCKED"
}

func queueUpsertEvents(ctx context.Context, db sqrlx.Transactor, req *ges_tpb.UpsertsMessage) error {
	sel := sq.Select().
		Column("CONCAT(?::text, '/', grpc_service, '/', grpc_method, '/', entity_id)", req.QueueUrl).
		Column("grpc_service").
		Column("grpc_method").
		Column("entity_id").
		Column("?", req.QueueUrl).
		From("upsert").
		Where("grpc_service = ?", req.GrpcService).
		Where("grpc_method = ?", req.GrpcMethod)

	ins := sq.Insert("replay_upsert").
		Columns(
			"replay_id",
			"grpc_service",
			"grpc_method",
			"entity_id",
			"queue_url",
		).
		Select(sel)

	log.WithFields(ctx, map[string]interface{}{
		"queueUrl":    req.QueueUrl,
		"grpcService": req.GrpcService,
		"grpcMethod":  req.GrpcMethod,
	}).Info("Queueing replay upserts")

	return db.Transact(ctx, &sqrlx.TxOptions{
		ReadOnly:  false,
		Retryable: true,
	}, func(ctx context.Context, tx sqrlx.Transaction) error {
		_, err := tx.Exec(ctx, ins)
		if err != nil {
			return err
		}
		_, err = tx.ExecRaw(ctx, "NOTIFY "+ReplayUpsertPGChannel)
		if err != nil {
			return err
		}

		return nil
	})
}

func (uq *UpsertReplay) ScanRow(row replay.Row) (*upsertRow, error) {
	var outboxRow upsertRow
	var dataBytes []byte
	err := row.Scan(&outboxRow.id, &outboxRow.destination, &dataBytes)
	if err != nil {
		return nil, fmt.Errorf("error scanning outbox row: %w", err)
	}
	outboxRow.message = &ges_pb.Upsert{}
	err = protojson.Unmarshal(dataBytes, outboxRow.message)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling upsert data: %w", err)
	}
	return &outboxRow, nil
}

func (uq *UpsertReplay) DeleteQuery(rows []*upsertRow) (string, []interface{}, error) {
	ids := make([]string, len(rows))
	for i, row := range rows {
		ids[i] = row.id
	}
	return "DELETE FROM replay_upsert WHERE replay_id = ANY($1)", []interface{}{
		pq.StringArray(ids),
	}, nil
}
