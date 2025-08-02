package service

import (
	"context"
	"fmt"

	"github.com/pentops/ges/internal/gen/o5/ges/v1/ges_pb"
	"github.com/pentops/ges/internal/gen/o5/ges/v1/ges_spb"
	"github.com/pentops/golib/gl"
	"github.com/pentops/j5/lib/j5query"
	"github.com/pentops/j5/lib/j5schema"
	"github.com/pentops/o5-messaging/gen/o5/messaging/v1/messaging_pb"
	"github.com/pentops/sqrlx.go/sqrlx"
	"google.golang.org/grpc"
)

type QueryService struct {
	db            sqrlx.Transactor
	eventLister   *j5query.Lister //[*ges_spb.EventsListRequest, *ges_spb.EventsListResponse]
	upsertLister  *j5query.Lister //[*ges_spb.UpsertListRequest, *ges_spb.UpsertListResponse]
	genericLister *j5query.Lister //[*ges_spb.GenericListRequest, *ges_spb.GenericListResponse]

	ges_spb.UnsafeQueryServiceServer
}

func NewQueryService(db sqrlx.Transactor) (*QueryService, error) {

	eventLister, err := j5query.NewLister(
		j5query.ListSpec{
			Method: ges_spb.EventsListJ5MethodSchema(),
			TableSpec: j5query.TableSpec{
				TableName:  "event",
				DataColumn: "data",
				RootObject: j5schema.MustObjectSchema((&ges_pb.Event{}).ProtoReflect().Descriptor()),
				FallbackSortColumns: []j5query.ProtoField{
					j5query.NewJSONField("metadata.timestamp", gl.Ptr("timestamp")),
					j5query.NewJSONField("metadata.eventId", gl.Ptr("id")),
				},
				// Auth:
				// AuthJoin:
			},
			// RequestFilter: smSpec.ListRequestFilter,
		})
	if err != nil {
		return nil, fmt.Errorf("create event lister: %w", err)
	}

	upsertLister, err := j5query.NewLister(
		j5query.ListSpec{
			Method: ges_spb.UpsertListJ5MethodSchema(),
			TableSpec: j5query.TableSpec{
				TableName:  "upsert",
				DataColumn: "data",
				RootObject: j5schema.MustObjectSchema((&ges_pb.Upsert{}).ProtoReflect().Descriptor()),
				FallbackSortColumns: []j5query.ProtoField{
					j5query.NewJSONField("entityId", gl.Ptr("entity_id")),
					j5query.NewJSONField("lastEventTimestamp", gl.Ptr("last_event_timestamp")),
				},
				// Auth:
				// AuthJoin:
			},
			// RequestFilter: smSpec.ListRequestFilter,
		})
	if err != nil {
		return nil, fmt.Errorf("create upsert lister: %w", err)
	}

	genericLister, err := j5query.NewLister(
		j5query.ListSpec{
			Method: ges_spb.GenericListJ5MethodSchema(),
			TableSpec: j5query.TableSpec{
				TableName:  "generic",
				DataColumn: "data",
				RootObject: j5schema.MustObjectSchema((&messaging_pb.Message{}).ProtoReflect().Descriptor()),
				FallbackSortColumns: []j5query.ProtoField{
					j5query.NewJSONField("timestamp", gl.Ptr("timestamp")),
					j5query.NewJSONField("messageId", gl.Ptr("message_id")),
				},
				// Auth:
				// AuthJoin:
			},
			// RequestFilter: smSpec.ListRequestFilter,
		})
	if err != nil {
		return nil, fmt.Errorf("create generic lister: %w", err)
	}

	return &QueryService{
		db:            db,
		eventLister:   eventLister,
		upsertLister:  upsertLister,
		genericLister: genericLister,
	}, nil
}

func (s *QueryService) RegisterGRPC(server grpc.ServiceRegistrar) {
	ges_spb.RegisterQueryServiceServer(server, s)
}

func (s *QueryService) EventsList(ctx context.Context, req *ges_spb.EventsListRequest) (*ges_spb.EventsListResponse, error) {
	res := &ges_spb.EventsListResponse{}
	return res, s.eventLister.List(ctx, s.db, req.J5Object(), res.J5Object())
}

func (s *QueryService) UpsertList(ctx context.Context, req *ges_spb.UpsertListRequest) (*ges_spb.UpsertListResponse, error) {
	res := &ges_spb.UpsertListResponse{}
	return res, s.upsertLister.List(ctx, s.db, req.J5Object(), res.J5Object())
}

func (s *QueryService) GenericList(ctx context.Context, req *ges_spb.GenericListRequest) (*ges_spb.GenericListResponse, error) {
	res := &ges_spb.GenericListResponse{}
	return res, s.genericLister.List(ctx, s.db, req.J5Object(), res.J5Object())

}
