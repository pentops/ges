package service

import (
	"context"

	"github.com/pentops/ges/internal/gen/o5/ges/v1/ges_tpb"
	"github.com/pentops/ges/internal/replay"
	"github.com/pentops/sqrlx.go/sqrlx"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	ReplayEventPGChannel  = "replay_event_notification"
	ReplayUpsertPGChannel = "replay_upsert_notification"
)

func ReplayListener(dsn string, sqs replay.SQS) (*replay.Listener, error) {
	return replay.NewListener(dsn, []replay.Topic{{
		Name:     ReplayEventPGChannel,
		Callback: replay.WrapPublisher(sqs, &EventReplay{}),
	}, {
		Name:     ReplayUpsertPGChannel,
		Callback: replay.WrapPublisher(sqs, &UpsertReplay{}),
	}})
}

type ReplayWorker struct {
	db sqrlx.Transactor

	ges_tpb.UnsafeReplayTopicServer
}

var _ ges_tpb.ReplayTopicServer = &ReplayWorker{}

func NewReplayWorker(db sqrlx.Transactor) *ReplayWorker {
	return &ReplayWorker{
		db: db,
	}
}

func (rr *ReplayWorker) RegisterGRPC(server grpc.ServiceRegistrar) {
	ges_tpb.RegisterReplayTopicServer(server, rr)
}

func (rr *ReplayWorker) Events(ctx context.Context, req *ges_tpb.EventsMessage) (*emptypb.Empty, error) {
	if err := queueReplayEvents(ctx, rr.db, req); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (rr *ReplayWorker) Upserts(ctx context.Context, req *ges_tpb.UpsertsMessage) (*emptypb.Empty, error) {
	if err := queueUpsertEvents(ctx, rr.db, req); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}
