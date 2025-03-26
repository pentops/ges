package service

import (
	"fmt"

	"github.com/pentops/grpc.go/protovalidatemw"
	"github.com/pentops/log.go/grpc_log"
	"github.com/pentops/log.go/log"
	"github.com/pentops/sqrlx.go/sqrlx"
	"google.golang.org/grpc"
)

type App struct {
	QueryService *QueryService
	EventWorker  *EventWorker
}

func NewApp(db sqrlx.Transactor) (*App, error) {
	qs, err := NewQueryService(db)
	if err != nil {
		return nil, fmt.Errorf("failed to create query service: %w", err)
	}
	app := &App{
		QueryService: qs,
		EventWorker:  NewEventWorker(db),
	}
	return app, nil
}

func (a *App) RegisterGRPC(server grpc.ServiceRegistrar) {
	a.QueryService.RegisterGRPC(server)
	a.EventWorker.RegisterGRPC(server)
}

func GRPCMiddleware() []grpc.UnaryServerInterceptor {
	return []grpc.UnaryServerInterceptor{
		grpc_log.UnaryServerInterceptor(log.DefaultContext, log.DefaultTrace, log.DefaultLogger),
		protovalidatemw.UnaryServerInterceptor(),
	}
}
