package service

import (
	"context"
	"fmt"

	"github.com/pentops/ges/internal/gen/o5/ges/v1/ges_spb"
	"github.com/pentops/golib/gl"
	"github.com/pentops/protostate/pquery"
	"github.com/pentops/sqrlx.go/sqrlx"
	"google.golang.org/grpc"
)

type QueryService struct {
	db           sqrlx.Transactor
	eventLister  *pquery.Lister[*ges_spb.EventsListRequest, *ges_spb.EventsListResponse]
	upsertLister *pquery.Lister[*ges_spb.UpsertListRequest, *ges_spb.UpsertListResponse]

	ges_spb.UnsafeQueryServiceServer
}

var _ ges_spb.QueryServiceServer = &QueryService{}

func NewQueryService(db sqrlx.Transactor) (*QueryService, error) {

	eventLister, err := pquery.NewLister(
		pquery.ListSpec[*ges_spb.EventsListRequest, *ges_spb.EventsListResponse]{
			TableSpec: pquery.TableSpec{
				TableName:  "event",
				DataColumn: "data",
				FallbackSortColumns: []pquery.ProtoField{
					pquery.NewProtoField("timestamp", gl.Ptr("timestamp")),
					pquery.NewProtoField("id", gl.Ptr("id")),
				},
				//Auth:
				//AuthJoin:
			},
			//RequestFilter: smSpec.ListRequestFilter,
		})

	if err != nil {
		return nil, fmt.Errorf("create event lister: %w", err)
	}

	upsertLister, err := pquery.NewLister(
		pquery.ListSpec[*ges_spb.UpsertListRequest, *ges_spb.UpsertListResponse]{
			TableSpec: pquery.TableSpec{
				TableName:  "upsert",
				DataColumn: "data",
				FallbackSortColumns: []pquery.ProtoField{
					pquery.NewProtoField("entity_id", gl.Ptr("entity_id")),
					pquery.NewProtoField("last_event_timestamp", gl.Ptr("last_event_timestamp")),
				},
				//Auth:
				//AuthJoin:
			},
			//RequestFilter: smSpec.ListRequestFilter,
		})

	if err != nil {
		return nil, fmt.Errorf("create upsert lister: %w", err)
	}
	return &QueryService{
		db:           db,
		eventLister:  eventLister,
		upsertLister: upsertLister,
	}, nil
}

func (s *QueryService) RegisterGRPC(server grpc.ServiceRegistrar) {
	ges_spb.RegisterQueryServiceServer(server, s)
}

func (s *QueryService) EventsList(ctx context.Context, req *ges_spb.EventsListRequest) (*ges_spb.EventsListResponse, error) {
	res := &ges_spb.EventsListResponse{}
	return res, s.eventLister.List(ctx, s.db, req, res)
}

func (s *QueryService) UpsertList(ctx context.Context, req *ges_spb.UpsertListRequest) (*ges_spb.UpsertListResponse, error) {
	res := &ges_spb.UpsertListResponse{}
	return res, s.upsertLister.List(ctx, s.db, req, res)
}
