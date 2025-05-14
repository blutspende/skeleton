package db

import (
	"context"
	"fmt"
	"github.com/blutspende/skeleton/config"
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog/log"
)

type Postgres interface {
	Connect() error
	GetDbConnection() (*sqlx.DB, error)
	Close() error
}

type postgres struct {
	ctx    context.Context
	config *config.Configuration
	pgConn *sqlx.DB
}

func NewPostgres(ctx context.Context, config *config.Configuration) Postgres {
	return &postgres{
		ctx:    ctx,
		config: config,
		pgConn: nil,
	}
}

func (p *postgres) Connect() error {
	url := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s application_name=%s",
		p.config.PostgresDB.Host, p.config.PostgresDB.Port, p.config.PostgresDB.User,
		p.config.PostgresDB.Pass, p.config.PostgresDB.Database, p.config.PostgresDB.SSLMode, p.config.ApplicationName)
	//pgDB, err := sqlx.ConnectContext(p.ctx, "postgres", url)
	pgDB, err := sqlx.ConnectContext(p.ctx, "pgx", url)
	if err != nil {
		return err
	}
	log.Info().Msgf("Postgres available, connected to %s / %s", p.config.PostgresDB.Host, p.config.PostgresDB.Database)
	p.pgConn = pgDB
	return nil
}

func (p *postgres) GetDbConnection() (*sqlx.DB, error) {
	if p.pgConn == nil {
		return nil, fmt.Errorf("postgres connection is not established")
	}
	return p.pgConn, nil
}

func (p *postgres) Close() error {
	if p.pgConn != nil {
		return p.pgConn.Close()
	}
	return nil
}
