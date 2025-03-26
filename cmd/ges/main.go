package main

import (
	"context"

	"github.com/pentops/ges/internal/service"
	"github.com/pentops/grpc.go/grpcbind"
	"github.com/pentops/runner/commander"
	"github.com/pentops/sqrlx.go/pgenv"
	"github.com/pressly/goose"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var Version = "0.0.0"

func main() {
	cmdGroup := commander.NewCommandSet()

	cmdGroup.Add("serve", commander.NewCommand(runServe))
	cmdGroup.Add("migrate", commander.NewCommand(runMigrate))

	cmdGroup.RunMain("ges", Version)
}

func runMigrate(ctx context.Context, cfg struct {
	MigrationsDir string `env:"MIGRATIONS_DIR" default:"./ext/db"`
	pgenv.DatabaseConfig
}) error {

	db, err := cfg.OpenPostgres(ctx)
	if err != nil {
		return err
	}

	return goose.Up(db, "/migrations")
}

func runServe(ctx context.Context, cfg struct {
	grpcbind.EnvConfig
	pgenv.DatabaseConfig
}) error {

	db, err := cfg.OpenPostgresTransactor(ctx)
	if err != nil {
		return err
	}

	app, err := service.NewApp(db)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer(grpc.ChainUnaryInterceptor(
		service.GRPCMiddleware()...,
	))
	app.RegisterGRPC(grpcServer)
	reflection.Register(grpcServer)

	return cfg.ListenAndServe(ctx, grpcServer)
}
