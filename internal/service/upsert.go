package service

import (
	"fmt"

	"github.com/pentops/ges/internal/gen/o5/ges/v1/ges_pb"
	"github.com/pentops/ges/internal/replay"
	"github.com/pentops/o5-messaging/gen/o5/messaging/v1/messaging_pb"
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

type upsertQueryer struct{}

var _ replay.MessageQuery[*upsertRow] = (*upsertQueryer)(nil)

func (uq *upsertQueryer) SelectQuery() string {
	return "SELECT " +
		"replay_upsert.replay_id, " +
		"replay_upsert.queue_url, " +
		"upsert.data " +
		"FROM replay_upsert " +
		"LEFT JOIN upsert ON upsert.entity_name = replay_upsert.entity_name " +
		"AND upsert.entity_id = replay_upsert.entity_id " +
		"LIMIT 10 FOR UPDATE SKIP LOCKED"
}

func (uq *upsertQueryer) ScanRow(row replay.Row) (*upsertRow, error) {
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

func (uq *upsertQueryer) DeleteQuery(rows []*upsertRow) (string, []interface{}, error) {
	ids := make([]string, len(rows))
	for i, row := range rows {
		ids[i] = row.id
	}
	return "DELETE FROM replay_upsert WHERE replay_id = ANY($1)", []interface{}{ids}, nil
}
