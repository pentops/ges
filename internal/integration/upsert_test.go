package integration

import (
	"context"
	"testing"
	"time"

	"github.com/pentops/flowtest"
	"github.com/pentops/flowtest/prototest"
	"github.com/pentops/ges/internal/gen/gestest/v1/gestest_tpb"
	"github.com/pentops/ges/internal/gen/o5/ges/v1/ges_spb"
	"github.com/pentops/j5/gen/j5/messaging/v1/messaging_j5pb"
	"github.com/pentops/j5/lib/id62"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestUpsertCycle(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	flow, uu := NewUniverse(ctx, t)
	defer flow.RunSteps(t)

	var event1 *gestest_tpb.FooSummaryMessage
	var event2 *gestest_tpb.FooSummaryMessage

	var t1 time.Time

	flow.Step("Event 1", func(ctx context.Context, t flowtest.Asserter) {
		t1 = time.Now()
		event1 = &gestest_tpb.FooSummaryMessage{
			Upsert: &messaging_j5pb.UpsertMetadata{
				EventId:   id62.NewString(),
				Timestamp: timestamppb.New(t1),
				EntityId:  id62.NewString(),
			},
			Name: "Evt1",
		}
		uu.HandleGeneric(ctx, t, event1)
	})

	flow.Step("Event 2", func(ctx context.Context, t flowtest.Asserter) {
		t2 := t1.Add(time.Second)
		event2 = &gestest_tpb.FooSummaryMessage{
			Upsert: &messaging_j5pb.UpsertMetadata{
				EventId:   id62.NewString(),
				Timestamp: timestamppb.New(t2),
				EntityId:  event1.Upsert.EntityId,
			},
			Name: "Evt2",
		}
		uu.HandleGeneric(ctx, t, event2)
	})

	flow.Step("Event Old", func(ctx context.Context, t flowtest.Asserter) {
		t3 := t1.Add(time.Second * -5) // Older!
		event3 := &gestest_tpb.FooSummaryMessage{
			Upsert: &messaging_j5pb.UpsertMetadata{
				EventId:   id62.NewString(),
				Timestamp: timestamppb.New(t3),
				EntityId:  event1.Upsert.EntityId,
			},
			Name: "Evt Old",
		}
		uu.HandleGeneric(ctx, t, event3)
	})

	flow.Step("List", func(ctx context.Context, t flowtest.Asserter) {
		res, err := uu.Query.UpsertList(ctx, &ges_spb.UpsertListRequest{})
		t.NoError(err)

		if len(res.Events) != 1 {
			t.Fatalf("expected 1 event, got %d", len(res.Events))
		}
		evt := res.Events[0]

		t.Log(evt)
		t.Equal(event2.Upsert.EntityId, evt.EntityId)
		// active event should be 2.
		t.Equal(event2.Upsert.EventId, evt.LastEventId)

		fullMsg := &gestest_tpb.FooSummaryMessage{}
		uu.DecodeAnyTo(t, evt.Data, fullMsg)
		t.Equal("Evt2", fullMsg.Name)

		prototest.AssertEqualProto(t, event2, fullMsg)

	})

}
