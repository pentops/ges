package service

import (
	"context"
	"fmt"

	"github.com/pentops/o5-messaging/gen/o5/messaging/v1/messaging_pb"
	"github.com/pentops/o5-messaging/gen/o5/messaging/v1/messaging_tpb"
	"github.com/pentops/sqrlx.go/sqrlx"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
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
		return storeEvent(ctx, ww.db, req.Message)

	case *messaging_pb.Message_Upsert_:
		return storeUpsert(ctx, ww.db, req.Message)

	default:
		return fmt.Errorf("unexpected message extension: %T", ext)
	}
}
