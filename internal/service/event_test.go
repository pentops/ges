package service

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/pentops/flowtest/prototest"
	"github.com/pentops/ges/internal/gen/gestest/v1/gestest_pb"
	"github.com/pentops/ges/internal/gen/gestest/v1/gestest_tpb"
	"github.com/pentops/j5/gen/j5/state/v1/psm_j5pb"
	"github.com/pentops/j5/lib/id62"
	"github.com/pentops/j5/lib/j5codec"
	"github.com/pentops/o5-messaging/gen/o5/messaging/v1/messaging_pb"
	"github.com/pentops/o5-messaging/o5msg"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestParseReconstruct(t *testing.T) {
	fooMsg := &gestest_tpb.FooEventMessage{
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

	wrapped := wrapMessage(t, fooMsg)
	printJSON(t, "input", wrapped.Body.Value)

	parsed, err := parseEvent(wrapped)
	if err != nil {
		t.Fatalf("failed to parse event: %v", err)
	}

	reconstructed, err := reconstructEvent(parsed)
	if err != nil {
		t.Fatalf("failed to reconstruct event: %v", err)
	}

	printJSON(t, "reconstructed", reconstructed.Body.Value)

	if reconstructed.Body.Encoding != messaging_pb.WireEncoding_J5_JSON {
		t.Fatalf("expected J5_JSON encoding, got %v", reconstructed.Body.Encoding)
	}

	parsedReconstructed := &gestest_tpb.FooEventMessage{}
	err = j5codec.Global.JSONToProto(reconstructed.Body.Value, parsedReconstructed.ProtoReflect())
	if err != nil {
		t.Fatalf("failed to decode reconstructed event: %v", err)
	}

	prototest.AssertEqualProto(t, fooMsg, parsedReconstructed)

}

func printJSON(t testing.TB, label string, input []byte) {
	buffer := &bytes.Buffer{}
	err := json.Indent(buffer, input, "", "  ")
	if err != nil {
		t.Fatalf("failed to indent JSON: %v", err)
	}
	t.Logf("%s:\n%s", label, buffer.String())
}

func wrapMessage(t testing.TB, msg o5msg.Message) *messaging_pb.Message {
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
	return wrapper
}
