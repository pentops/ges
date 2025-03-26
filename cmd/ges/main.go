package main

import (
	"context"
	"database/sql"
	"time"

	"github.com/pentops/ges/internal/service"
	"github.com/pentops/grpc.go/grpcbind"
	"github.com/pentops/log.go/log"
	"github.com/pentops/runner/commander"
	"github.com/pentops/sqrlx.go/sqrlx"
	"github.com/pressly/goose"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var Version = "0.0.0"

func main() {
	cmdGroup := commander.NewCommandSet()

	cmdGroup.Add("serve", commander.NewCommand(runServe))
	cmdGroup.Add("migrate", commander.NewCommand(runMigrate))

	cmdGroup.RunMain("registry", Version)
}

func runMigrate(ctx context.Context, cfg struct {
	MigrationsDir string `env:"MIGRATIONS_DIR" default:"./ext/db"`
	DBConfig
}) error {

	db, err := cfg.OpenDatabase(ctx)
	if err != nil {
		return err
	}

	return goose.Up(db, "/migrations")
}

func runServe(ctx context.Context, cfg struct {
	GRPCBind string `env:"GRPC_BIND" default:":8080"`
	DBConfig
}) error {

	dbConn, err := cfg.DBConfig.OpenDatabase(ctx)
	if err != nil {
		return err
	}
	db := sqrlx.NewPostgres(dbConn)

	app, err := service.NewApp(db)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer(grpc.ChainUnaryInterceptor(
		service.GRPCMiddleware()...,
	))
	app.RegisterGRPC(grpcServer)
	reflection.Register(grpcServer)

	return grpcbind.ListenAndServe(ctx, grpcServer, cfg.GRPCBind)
}

type DBConfig struct {
	URL string `env:"POSTGRES_URL"`
}

func (cfg *DBConfig) OpenDatabase(ctx context.Context) (*sql.DB, error) {

	conn, err := sql.Open("postgres", cfg.URL)
	if err != nil {
		return nil, err
	}

	// Default is unlimited connections, use a cap to prevent hammering the database if it's the bottleneck.
	// 10 was selected as a conservative number and will likely be revised later.
	conn.SetMaxOpenConns(10)

	for {
		if err := conn.Ping(); err != nil {
			log.WithError(ctx, err).Error("pinging PG")
			time.Sleep(time.Second)
			continue
		}
		break
	}

	return conn, nil
}
