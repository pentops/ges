package service

import (
	"context"

	"github.com/pentops/ges/internal/gen/o5/ges/v1/ges_spb"
	"github.com/pentops/ges/internal/gen/o5/ges/v1/ges_tpb"
	"github.com/pentops/o5-messaging/outbox"
	"github.com/pentops/sqrlx.go/sqrlx"
	"google.golang.org/grpc"
)

type CommandService struct {
	db sqrlx.Transactor

	ges_spb.UnsafeCommandServiceServer
}

func NewCommandService(db sqrlx.Transactor) (*CommandService, error) {
	return &CommandService{
		db: db,
	}, nil
}

func (s *CommandService) RegisterGRPC(server grpc.ServiceRegistrar) {
	ges_spb.RegisterCommandServiceServer(server, s)
}

func (s *CommandService) ReplayEvents(ctx context.Context, req *ges_spb.ReplayEventsRequest) (*ges_spb.ReplayEventsResponse, error) {
	msg := &ges_tpb.EventsMessage{
		QueueUrl:    req.QueueUrl,
		GrpcService: req.GrpcService,
		GrpcMethod:  req.GrpcService,
	}
	if err := s.db.Transact(ctx, &sqrlx.TxOptions{
		ReadOnly:  false,
		Retryable: true,
	}, func(ctx context.Context, tx sqrlx.Transaction) error {
		return outbox.Send(ctx, tx, msg)
	}); err != nil {
		return nil, err
	}
	return &ges_spb.ReplayEventsResponse{}, nil
}

func (s *CommandService) ReplayUpserts(ctx context.Context, req *ges_spb.ReplayUpsertsRequest) (*ges_spb.ReplayUpsertsResponse, error) {
	msg := &ges_tpb.UpsertsMessage{
		QueueUrl:    req.QueueUrl,
		GrpcService: req.GrpcService,
		GrpcMethod:  req.GrpcService,
	}
	if err := s.db.Transact(ctx, &sqrlx.TxOptions{
		ReadOnly:  false,
		Retryable: true,
	}, func(ctx context.Context, tx sqrlx.Transaction) error {
		return outbox.Send(ctx, tx, msg)
	}); err != nil {
		return nil, err
	}
	return &ges_spb.ReplayUpsertsResponse{}, nil
}
