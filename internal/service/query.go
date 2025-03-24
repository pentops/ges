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
	db          sqrlx.Transactor
	eventLister *pquery.Lister[*ges_spb.EventsListRequest, *ges_spb.EventsListResponse]
	//upsertLister *pquery.Lister[*ges_spb.UpsertListRequest, *ges_spb.UpsertListResponse]

	ges_spb.UnsafeQueryServiceServer
}

var _ ges_spb.QueryServiceServer = &QueryService{}

func NewQueryService(db sqrlx.Transactor) (*QueryService, error) {

	listSpec := pquery.ListSpec[*ges_spb.EventsListRequest, *ges_spb.EventsListResponse]{
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
	}

	lister, err := pquery.NewLister(listSpec)
	if err != nil {
		return nil, fmt.Errorf("failed to create lister: %w", err)
	}

	return &QueryService{
		db:          db,
		eventLister: lister,
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
	return nil, nil
}
