package integration

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/pentops/flowtest"
	"github.com/pentops/ges/internal/gen/o5/ges/v1/ges_spb"
	"github.com/pentops/ges/internal/gen/o5/ges/v1/ges_tpb"
	"github.com/pentops/ges/internal/replay"
	"github.com/pentops/ges/internal/service"
	"github.com/pentops/j5/j5types/any_j5t"
	"github.com/pentops/j5/lib/j5codec"
	"github.com/pentops/log.go/log"
	"github.com/pentops/o5-messaging/gen/o5/messaging/v1/messaging_pb"
	"github.com/pentops/o5-messaging/gen/o5/messaging/v1/messaging_tpb"
	"github.com/pentops/o5-messaging/o5msg"
	"github.com/pentops/o5-messaging/outbox/outboxtest"
	"github.com/pentops/pgtest.go/pgtest"
	"github.com/pentops/sqrlx.go/sqrlx"
	"google.golang.org/protobuf/proto"
)

type Universe struct {
	GenericTopic messaging_tpb.GenericMessageTopicClient
	ReplayTopic  ges_tpb.ReplayTopicClient

	Query ges_spb.QueryServiceClient

	DB       sqrlx.Transactor
	GRPCPair *flowtest.GRPCPair
	Outbox   *outboxtest.OutboxAsserter
}

func NewUniverse(ctx context.Context, t *testing.T) (*flowtest.Stepper[*testing.T], *Universe) {

	name := t.Name()
	stepper := flowtest.NewStepper[*testing.T](name)
	uu := &Universe{}
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

	conn := pgtest.GetTestDB(t, pgtest.WithDir("../../ext/db"))

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
	uu.ReplayTopic = ges_tpb.NewReplayTopicClient(uu.GRPCPair.Client)
	uu.Query = ges_spb.NewQueryServiceClient(uu.GRPCPair.Client)

	uu.Run(ctx, t)
}

func (uu *Universe) Run(ctx context.Context, t flowtest.TB) {
	uu.GRPCPair.ServeUntilDone(t, ctx)
}

type mockSQS struct {
	sends []sqs.SendMessageBatchInput
}

func (m *mockSQS) SendMessageBatch(ctx context.Context, input *sqs.SendMessageBatchInput, opts ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
	m.sends = append(m.sends, *input)
	success := make([]types.SendMessageBatchResultEntry, len(input.Entries))
	for i := range input.Entries {
		success[i] = types.SendMessageBatchResultEntry{
			Id: input.Entries[i].Id,
		}
	}
	return &sqs.SendMessageBatchOutput{
		Failed:     []types.BatchResultErrorEntry{},
		Successful: success,
	}, nil
}

func (uu *Universe) CaptureReplayEvents(ctx context.Context, t flowtest.TB) []*messaging_pb.Message {
	t.Helper()
	return captureReplay(ctx, t, uu.DB, &service.EventReplay{})
}

func (uu *Universe) CaptureReplayUpserts(ctx context.Context, t flowtest.TB) []*messaging_pb.Message {
	t.Helper()
	return captureReplay(ctx, t, uu.DB, &service.UpsertReplay{})
}

func (uu *Universe) CaptureReplayGeneric(ctx context.Context, t flowtest.TB) []*messaging_pb.Message {
	t.Helper()
	return captureReplay(ctx, t, uu.DB, &service.GenericReplay{})
}

func captureReplay[T replay.Message](ctx context.Context, t flowtest.TB, db sqrlx.Transactor, query replay.MessageQuery[T]) []*messaging_pb.Message {
	t.Helper()

	capture := &mockSQS{}

	err := db.Transact(ctx, &sqrlx.TxOptions{
		ReadOnly:  false,
		Retryable: false,
	}, func(ctx context.Context, tx sqrlx.Transaction) error {
		wrapped := replay.WrapSqrlx(tx)
		_, err := replay.DoPage(ctx, wrapped, capture, query)
		return err
	})

	if err != nil {
		t.Fatalf("failed to capture replay events: %v", err)
	}

	var messages []*messaging_pb.Message
	for _, send := range capture.sends {
		for _, entry := range send.Entries {
			msg := &messaging_pb.Message{}
			err := j5codec.Global.JSONToProto([]byte(*entry.MessageBody), msg.ProtoReflect())
			if err != nil {
				t.Fatalf("failed to unmarshal message: %v", err)
			}
			messages = append(messages, msg)
		}
	}

	return messages
}

func (uu *Universe) HandleGeneric(ctx context.Context, t flowtest.TB, msg o5msg.Message) {

	bodyData, err := j5codec.Global.EncodeAny(msg.ProtoReflect())
	if err != nil {
		t.Fatalf("failed to encode message: %v", err)
	}

	wrapper := o5msg.MessageWrapper(msg)
	wrapper.Body = &messaging_pb.Any{
		TypeUrl:  fmt.Sprintf("type.googleapis.com/%s", msg.ProtoReflect().Descriptor().FullName()),
		Value:    bodyData.J5Json,
		Encoding: messaging_pb.WireEncoding_J5_JSON,
	}

	_, err = uu.GenericTopic.Generic(ctx, &messaging_tpb.GenericMessage{
		Message: wrapper,
	})
	if err != nil {
		t.Fatalf("failed to send message: %v", err)
	}
}

func (uu *Universe) DecodeAnyTo(t flowtest.TB, input *any_j5t.Any, output proto.Message) {
	t.Helper()
	if input == nil {
		t.Fatalf("input any is nil")
	}
	err := j5codec.Global.DecodeAnyTo(input, output)
	if err != nil {
		t.Fatalf("failed to decode any: %v", err)
	}
}
