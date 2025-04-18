package integration

import (
	"context"
	"testing"

	"github.com/pentops/flowtest"
	"github.com/pentops/ges/internal/gen/gestest/v1/gestest_pb"
	"github.com/pentops/ges/internal/gen/gestest/v1/gestest_tpb"
	"github.com/pentops/ges/internal/gen/o5/ges/v1/ges_spb"
	"github.com/pentops/ges/internal/gen/o5/ges/v1/ges_tpb"
	"github.com/pentops/j5/gen/j5/state/v1/psm_j5pb"
	"github.com/pentops/j5/lib/id62"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestEventCycle(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	flow, uu := NewUniverse(ctx, t)
	defer flow.RunSteps(t)

	var fooMsg *gestest_tpb.FooEventMessage
	flow.Step("Message Input", func(ctx context.Context, t flowtest.Asserter) {

		fooMsg = &gestest_tpb.FooEventMessage{
			Metadata: &psm_j5pb.EventPublishMetadata{
				EventId:   id62.NewString(),
				Timestamp: timestamppb.Now(),
				Sequence:  1,
			},
			Keys: &gestest_pb.FooKeys{
				FooId: id62.NewString(),
			},
			Event: &gestest_pb.FooEventType{
				Type: &gestest_pb.FooEventType_Create_{
					Create: &gestest_pb.FooEventType_Create{
						Name: "Foo",
					},
				},
			},
			Data: &gestest_pb.FooData{
				Name: "Foo",
			},
			Status: gestest_pb.FooStatus_ACTIVE,
		}

		uu.HandleGeneric(ctx, t, fooMsg)
	})

	flow.Step("List", func(ctx context.Context, t flowtest.Asserter) {
		res, err := uu.Query.EventsList(ctx, &ges_spb.EventsListRequest{})
		t.NoError(err)

		if len(res.Events) != 1 {
			t.Fatalf("expected 1 event, got %d", len(res.Events))
		}
		evt := res.Events[0]

		t.Log(evt)
		fooKeys := &gestest_pb.FooKeys{}
		uu.DecodeAnyTo(t, evt.EntityKeys, fooKeys)
		t.Equal(fooMsg.Keys.FooId, fooKeys.FooId)
		t.Equal("gestest.v1.Foo", evt.EntityName)

	})

	flow.Step("Replay", func(ctx context.Context, t flowtest.Asserter) {
		t.MustMessage(uu.ReplayTopic.Events(ctx, &ges_tpb.EventsMessage{
			QueueUrl: "https://sqs/test-queue",

			GrpcService: "gestest.v1.topic.FooPublishTopic",
			GrpcMethod:  "FooEvent",
		}))

		out := uu.CaptureReplayEvents(ctx, t)

		if len(out) != 1 {
			t.Fatalf("expected 1 event, got %d", len(out))
		}

		evt := out[0]

		t.Log(evt)
	})

	flow.Step("Empty Replay", func(ctx context.Context, t flowtest.Asserter) {
		out := uu.CaptureReplayEvents(ctx, t)
		if len(out) != 0 {
			t.Fatalf("expected event to be consumed, %d in queue", len(out))
		}
	})

}
