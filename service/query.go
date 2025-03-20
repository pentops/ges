package service

import (
	"github.com/pentops/sqrlx.go/sqrlx"
	"google.golang.org/grpc"
)

type QueryService struct {
	db sqrlx.Transactor
}

func NewQueryService(db sqrlx.Transactor) *QueryService {
	return &QueryService{db}
}

func (s *QueryService) RegisterGRPC(server grpc.ServiceRegistrar) {
}
