package service

import (
	"context"

	"github.com/pentops/log.go/log"
	"github.com/pentops/o5-messaging/gen/o5/messaging/v1/messaging_pb"
	"github.com/pentops/o5-messaging/gen/o5/messaging/v1/messaging_tpb"
	"github.com/pentops/sqrlx.go/sqrlx"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
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
	if err := ww.store(ctx, req.Message); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (ww *EventWorker) store(ctx context.Context, msg *messaging_pb.Message) error {

	log.WithField(ctx, "message", msg).Info("Message")
	return nil
}
