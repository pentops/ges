package service

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/pentops/ges/internal/gen/o5/ges/v1/ges_pb"
	"github.com/pentops/ges/internal/replay"
	"github.com/pentops/j5/gen/j5/state/v1/psm_j5pb"
	"github.com/pentops/j5/j5types/any_j5t"
	"github.com/pentops/j5/lib/j5codec"
	"github.com/pentops/o5-messaging/gen/o5/messaging/v1/messaging_pb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func reconstructEvent(event *ges_pb.Event) (*messaging_pb.Message, error) {

	type ReconstructedEvent struct {
		Metadata json.RawMessage        `json:"metadata"`
		Keys     json.RawMessage        `json:"keys"`
		Event    map[string]interface{} `json:"event"`
		Data     json.RawMessage        `json:"data"`
		Status   string                 `json:"status"`
	}

	metadata, err := j5codec.Global.ProtoToJSON(event.Metadata.ProtoReflect())
	if err != nil {
		return nil, fmt.Errorf("error converting metadata to JSON: %w", err)
	}

	shell := &ReconstructedEvent{
		Metadata: json.RawMessage(metadata),
		Keys:     event.EntityKeys.J5Json,
		Data:     event.EntityState.J5Json,
		Status:   event.EntityStatus,
		Event: map[string]interface{}{
			event.EventType: json.RawMessage(event.EventData.J5Json),
			"!type":         event.EventType,
		},
	}

	// shell holds the j5-json structure generically as a native go json object
	bodyBytes, err := json.Marshal(shell)
	if err != nil {
		return nil, fmt.Errorf("error marshalling event shell: %w", err)
	}

	return &messaging_pb.Message{
		MessageId:   uuid.NewString(),
		GrpcService: event.GrpcService,
		GrpcMethod:  event.GrpcMethod,
		Timestamp:   timestamppb.Now(),
		Extension: &messaging_pb.Message_Event_{
			Event: &messaging_pb.Message_Event{
				EntityName: event.EntityName,
			},
		},
		Body: &messaging_pb.Any{
			TypeUrl:  fmt.Sprintf("type.googleapis.com/%s", event.BodyType),
			Encoding: messaging_pb.WireEncoding_J5_JSON,
			Value:    bodyBytes,
		},
	}, nil
}

func parseEvent(msg *messaging_pb.Message) (*ges_pb.Event, error) {

	type EventShell struct {
		Metadata json.RawMessage            `json:"metadata"`
		Keys     json.RawMessage            `json:"keys"`
		Event    map[string]json.RawMessage `json:"event"`
		Data     json.RawMessage            `json:"data"`
		Status   string                     `json:"status"`
	}

	ext := msg.GetEvent()
	if ext == nil {
		return nil, fmt.Errorf("message does not contain event extension")
	}

	shell := &EventShell{}

	err := json.Unmarshal(msg.Body.Value, &shell)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal body: %w", err)
	}

	metadata := &psm_j5pb.EventPublishMetadata{}

	err = j5codec.Global.JSONToProto(shell.Metadata, metadata.ProtoReflect())
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	event := &ges_pb.Event{
		Metadata:   metadata,
		EntityName: ext.EntityName,
		EntityState: &any_j5t.Any{
			TypeName: fmt.Sprintf("%sState", ext.EntityName),
			J5Json:   shell.Data,
		},
		EntityKeys: &any_j5t.Any{
			TypeName: fmt.Sprintf("%sKeys", ext.EntityName),
			J5Json:   shell.Keys,
		},
		EntityStatus: shell.Status,
		GrpcService:  msg.GrpcService,
		GrpcMethod:   msg.GrpcMethod,
		BodyType:     strings.TrimPrefix(msg.Body.TypeUrl, googleTypePrefix),
	}

	for k, v := range shell.Event {
		if k == "!type" {
			// !type should equal the other key, but not required.
			continue
		}
		if event.EventType != "" {
			return nil, fmt.Errorf("unexpected key in event oneof wrapper %s", k)
		}
		event.EventType = k
		event.EventData = &any_j5t.Any{
			TypeName: fmt.Sprintf("%sEventType", ext.EntityName),
			J5Json:   v,
		}
		break
	}

	return event, nil
}

type eventRow struct {
	id          string
	message     *ges_pb.Event
	destination string
}

func (er *eventRow) Message() (*messaging_pb.Message, error) {

	return reconstructEvent(er.message)
}

func wrapAny(val *any_j5t.Any) *JSONAny {
	return &JSONAny{
		TypeName: val.TypeName,
		J5Json:   val.J5Json,
	}
}

type JSONAny struct {
	TypeName string `json:"typeName"`
	J5Json   []byte `json:"j5json"`
}

func (er *eventRow) Destination() string {
	return er.destination
}

type eventQueryer struct{}

var _ replay.MessageQuery[*eventRow] = (*eventQueryer)(nil)

func (eq *eventQueryer) SelectQuery() string {
	return "SELECT " +
		"replay_event.replay_id, " +
		"replay_event.queue_url, " +
		"event.data " +
		"FROM replay_event " +
		"LEFT JOIN event ON event.id = replay_event.event_id " +
		"LIMIT 10 FOR UPDATE SKIP LOCKED"
}

func (eq *eventQueryer) ScanRow(row replay.Row) (*eventRow, error) {
	var outboxRow eventRow
	var dataBytes []byte
	err := row.Scan(&outboxRow.id, &outboxRow.destination, &dataBytes)
	if err != nil {
		return nil, fmt.Errorf("error scanning outbox row: %w", err)
	}
	outboxRow.message = &ges_pb.Event{}
	err = protojson.Unmarshal(dataBytes, outboxRow.message)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling event data: %w", err)
	}
	return &outboxRow, nil
}

func (eq *eventQueryer) DeleteQuery(rows []*eventRow) (string, []interface{}, error) {
	ids := make([]string, len(rows))
	for i, row := range rows {
		ids[i] = row.id
	}
	return "DELETE FROM replay_event WHERE replay_id = ANY($1)", []interface{}{ids}, nil
}
