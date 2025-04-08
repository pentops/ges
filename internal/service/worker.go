package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	sq "github.com/elgris/sqrl"
	"github.com/pentops/ges/internal/gen/o5/ges/v1/ges_pb"
	"github.com/pentops/j5/j5types/any_j5t"
	"github.com/pentops/log.go/log"
	"github.com/pentops/o5-messaging/gen/o5/messaging/v1/messaging_pb"
	"github.com/pentops/o5-messaging/gen/o5/messaging/v1/messaging_tpb"
	"github.com/pentops/sqrlx.go/sqrlx"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	googleTypePrefix = "type.googleapis.com/"
)

type EventWorker struct {
	db sqrlx.Transactor

	messaging_tpb.UnsafeGenericMessageTopicServer
}

var _ messaging_tpb.GenericMessageTopicServer = &EventWorker{}

func NewEventWorker(db sqrlx.Transactor) *EventWorker {
	return &EventWorker{
		db: db,
	}
}

func (ww *EventWorker) RegisterGRPC(server grpc.ServiceRegistrar) {
	messaging_tpb.RegisterGenericMessageTopicServer(server, ww)
}

func (ww *EventWorker) Generic(ctx context.Context, req *messaging_tpb.GenericMessage) (*emptypb.Empty, error) {
	err := ww.storeGeneric(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to store generic message: %w", err)
	}
	return &emptypb.Empty{}, nil
}

func (ww *EventWorker) storeGeneric(ctx context.Context, req *messaging_tpb.GenericMessage) error {
	if req.Message.Body.Encoding != messaging_pb.WireEncoding_J5_JSON {
		return fmt.Errorf("GES requires J5_JSON encoding, got %v", req.Message.Body.Encoding)
	}

	switch ext := req.Message.Extension.(type) {
	case *messaging_pb.Message_Event_:
		return ww.storeEvent(ctx, req.Message)

	case *messaging_pb.Message_Upsert_:
		return ww.storeUpsert(ctx, req.Message)

	default:
		return fmt.Errorf("unexpected message extension: %T", ext)
	}
}

func (ww *EventWorker) storeEvent(ctx context.Context, msg *messaging_pb.Message) error {

	event, err := parseEvent(msg)
	if err != nil {
		return fmt.Errorf("failed to parse event: %w", err)
	}

	eventData, err := protojson.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	log.WithFields(ctx, map[string]interface{}{
		"eventId":        event.Metadata.EventId,
		"eventTimestamp": event.Metadata.Timestamp.AsTime(),
		"entityName":     event.EntityName,
	}).Info("Event")

	return ww.db.Transact(ctx, &sqrlx.TxOptions{
		ReadOnly:  false,
		Retryable: true,
		Isolation: sql.LevelReadCommitted,
	}, func(ctx context.Context, tx sqrlx.Transaction) error {
		_, err := tx.Exec(ctx, sq.Insert("event").
			Columns(
				"id",
				"timestamp",
				"entity_name",
				"data",
			).
			Values(
				event.Metadata.EventId,
				event.Metadata.Timestamp.AsTime(),
				event.EntityName,
				eventData,
			),
		)
		if err != nil {
			return fmt.Errorf("failed to insert event: %w", err)
		}

		return nil
	})
}

func (ww *EventWorker) storeUpsert(ctx context.Context, msg *messaging_pb.Message) error {
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
		Key("entity_name", upsert.EntityName).
		Key("entity_id", upsert.EntityId).
		Set("last_event_id", upsert.LastEventId).
		Set("last_event_timestamp", upsert.LastEventTimestamp.AsTime()).
		Set("data", upsertData).
		Where("EXCLUDED.last_event_timestamp > upsert.last_event_timestamp")

	return ww.db.Transact(ctx, &sqrlx.TxOptions{
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
