package main

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/pentops/ges/internal/service"
	"github.com/pentops/grpc.go/grpcbind"
	"github.com/pentops/runner/commander"
	"github.com/pentops/sqrlx.go/pgenv"
	"github.com/pressly/goose"
	"golang.org/x/sync/errgroup"
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

	awsConfig, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	sqsClient := sqs.NewFromConfig(awsConfig)

	listener, err := service.ReplayListener(cfg.DatabaseConfig.URL, sqsClient)
	if err != nil {
		return err
	}

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {

		grpcServer := grpc.NewServer(grpc.ChainUnaryInterceptor(
			service.GRPCMiddleware()...,
		))
		app.RegisterGRPC(grpcServer)
		reflection.Register(grpcServer)

		return cfg.ListenAndServe(ctx, grpcServer)
	})

	eg.Go(func() error {
		return listener.Listen(ctx)
	})

	return eg.Wait()
}
