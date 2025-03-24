package integration

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/pentops/flowtest"
	"github.com/pentops/ges/internal/gen/o5/ges/v1/ges_spb"
	"github.com/pentops/ges/internal/service"
	"github.com/pentops/j5/lib/j5codec"
	"github.com/pentops/log.go/log"
	"github.com/pentops/o5-messaging/gen/o5/messaging/v1/messaging_pb"
	"github.com/pentops/o5-messaging/gen/o5/messaging/v1/messaging_tpb"
	"github.com/pentops/o5-messaging/o5msg"
	"github.com/pentops/o5-messaging/outbox/outboxtest"
	"github.com/pentops/pgtest.go/pgtest"
	"github.com/pentops/sqrlx.go/sqrlx"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Universe struct {
	GenericTopic messaging_tpb.GenericMessageTopicClient

	Query ges_spb.QueryServiceClient

	DB       sqrlx.Transactor
	GRPCPair *flowtest.GRPCPair
	Outbox   *outboxtest.OutboxAsserter

	Codec *j5codec.Codec
}

func NewUniverse(ctx context.Context, t *testing.T) (*flowtest.Stepper[*testing.T], *Universe) {

	name := t.Name()
	stepper := flowtest.NewStepper[*testing.T](name)
	uu := &Universe{
		Codec: j5codec.NewCodec(),
	}
	stepper.Setup(func(ctx context.Context, t flowtest.Asserter) error {
		log.DefaultLogger = log.NewCallbackLogger(stepper.LevelLog)
		//log.DefaultLogger.SetLevel(slog.LevelDebug)
		setupUniverse(ctx, t, uu)
		return nil
	})

	stepper.PostStepHook(func(ctx context.Context, t flowtest.Asserter) error {
		return nil
	})

	return stepper, uu
}

func setupUniverse(ctx context.Context, t flowtest.Asserter, uu *Universe) {
	t.Helper()

	conn := pgtest.GetTestDB(t, pgtest.WithDir("../../ext/db"), pgtest.WithSchemaName("testing_oms"))

	db := sqrlx.NewPostgres(conn)
	uu.DB = db

	uu.Outbox = outboxtest.NewOutboxAsserter(t, conn)

	middleware := service.GRPCMiddleware()

	uu.GRPCPair = flowtest.NewGRPCPair(t, middleware...)

	appSet, err := service.NewApp(uu.DB)
	if err != nil {
		t.Fatalf("failed to build app: %v", err)
	}

	appSet.RegisterGRPC(uu.GRPCPair.Server)

	uu.GenericTopic = messaging_tpb.NewGenericMessageTopicClient(uu.GRPCPair.Client)
	uu.Query = ges_spb.NewQueryServiceClient(uu.GRPCPair.Client)

	uu.Run(ctx, t)
}

func (uu *Universe) Run(ctx context.Context, t flowtest.TB) {
	uu.GRPCPair.ServeUntilDone(t, ctx)
}

func (uu *Universe) HandleGeneric(ctx context.Context, t flowtest.TB, msg o5msg.Message) {

	bodyData, err := uu.Codec.EncodeAny(msg.ProtoReflect())
	if err != nil {
		t.Fatalf("failed to encode message: %v", err)
	}

	header := msg.O5MessageHeader()

	wrapper := &messaging_pb.Message{
		MessageId:   uuid.New().String(),
		Timestamp:   timestamppb.Now(),
		GrpcService: header.GrpcService,
		GrpcMethod:  header.GrpcMethod,
		Body: &messaging_pb.Any{
			TypeUrl:  fmt.Sprintf("type.googleapis.com/%s", msg.ProtoReflect().Descriptor().FullName()),
			Value:    bodyData.J5Json,
			Encoding: messaging_pb.WireEncoding_J5_JSON,
		},
		DelaySeconds:     0,
		DestinationTopic: header.DestinationTopic,
		Headers:          header.Headers,
		Extension:        header.Extension,
	}

	_, err = uu.GenericTopic.Generic(ctx, &messaging_tpb.GenericMessage{
		Message: wrapper,
	})
	if err != nil {
		t.Fatalf("failed to send message: %v", err)
	}
}
