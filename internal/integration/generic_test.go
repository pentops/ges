package integration

import (
	"context"
	"testing"

	"github.com/pentops/flowtest"
	"github.com/pentops/ges/internal/gen/gestest/v1/gestest_tpb"
	"github.com/pentops/ges/internal/gen/o5/ges/v1/ges_spb"
	"github.com/pentops/ges/internal/gen/o5/ges/v1/ges_tpb"
	"github.com/pentops/j5/lib/id62"
	"github.com/pentops/o5-messaging/gen/o5/messaging/v1/messaging_pb"
)

func TestGenericCycle(t *testing.T) {

	ctx := t.Context()

	flow, uu := NewUniverse(ctx, t)
	defer flow.RunSteps(t)

	var fooMsg *gestest_tpb.FooMessage
	flow.Step("Message Input", func(ctx context.Context, t flowtest.Asserter) {

		fooMsg = &gestest_tpb.FooMessage{

			FooId: id62.NewString(),
			Name:  "Foo",
		}

		uu.HandleGeneric(ctx, t, fooMsg)
	})

	flow.Step("List", func(ctx context.Context, t flowtest.Asserter) {
		res, err := uu.Query.GenericList(ctx, &ges_spb.GenericListRequest{})
		t.NoError(err)

		if len(res.Events) != 1 {
			t.Fatalf("expected 1 event, got %d", len(res.Events))
		}
		evt := res.Events[0]
		t.Log(evt)

		if evt.Body.Encoding != messaging_pb.WireEncoding_J5_JSON {
			t.Fatalf("expected J5 JSON encoding, got %s", evt.Body.Encoding)
		}

	})

	flow.Step("Replay", func(ctx context.Context, t flowtest.Asserter) {
		t.MustMessage(uu.ReplayTopic.Generic(ctx, &ges_tpb.GenericMessage{
			QueueUrl: "https://sqs/test-queue",

			GrpcService: "gestest.v1.topic.GenericTopic",
		}))

		out := uu.CaptureReplayGeneric(ctx, t)

		if len(out) != 1 {
			t.Fatalf("expected 1 event, got %d", len(out))
		}

		evt := out[0]

		t.Log(evt)
	})

	flow.Step("Empty Replay", func(ctx context.Context, t flowtest.Asserter) {
		out := uu.CaptureReplayGeneric(ctx, t)
		if len(out) != 0 {
			t.Fatalf("expected event to be consumed, %d in queue", len(out))
		}
	})

}
