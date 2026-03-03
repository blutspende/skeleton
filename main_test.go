package skeleton

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/blutspende/bloodlab-common/db"
	"github.com/blutspende/skeleton/migrator"
	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func TestMain(m *testing.M) {
	postgres := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().Port(5551))
	err := postgres.Start()
	if err != nil {
		log.Error().Err(err).Msg("starting embedded postgres failed")
	}

	configureLogger()

	code := m.Run()

	err = postgres.Stop()
	if err != nil {
		log.Error().Err(err).Msg("stopping embedded postgres failed")
	}

	os.Exit(code)
}

func configureLogger() {
	consoleWriter := zerolog.NewConsoleWriter()
	consoleWriter.TimeFormat = "2006-01-02T15:04:05Z07:00"
	log.Logger = zerolog.New(consoleWriter).With().Caller().Stack().Timestamp().Logger()
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
}

func setupDbConnector(schemaName string) (db.DbConnection, string, db.Postgres, *sqlx.DB) {
	pgConfig := db.PgConfig{
		ApplicationName: "skeleton_test",
		Host:            "localhost",
		Port:            5551,
		User:            "postgres",
		Pass:            "postgres",
		Database:        "postgres",
		SSLMode:         "disable",
	}
	postgres := db.NewPostgres(pgConfig)
	_, _ = postgres.Connect(context.TODO())
	sqlConn, _ := postgres.GetSqlConnection()

	_, _ = sqlConn.Exec(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp" schema public;`)
	_, _ = sqlConn.Exec(fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE;`, schemaName))
	_, _ = sqlConn.Exec(fmt.Sprintf(`CREATE SCHEMA %s;`, schemaName))

	dbConn := db.NewEmptyDbConnection()
	dbConn.SetSqlConnection(sqlConn)

	return dbConn, schemaName, postgres, sqlConn
}

func setupDbConnectorAndRunMigration(schemaName string) (db.DbConnection, string, db.Postgres, *sqlx.DB) {
	dbConn, _, postgres, sqlConn := setupDbConnector(schemaName)

	mig := migrator.NewSkeletonMigrator()
	_ = mig.Run(context.Background(), sqlConn, schemaName)
	_, _ = sqlConn.Exec(fmt.Sprintf(`INSERT INTO %s.sk_supported_protocols (id, "name", description) VALUES ('abb539a3-286f-4c15-a7b7-2e9adf6eab91', 'IH-1000 v5.2', 'IHCOM');`, schemaName))

	return dbConn, schemaName, postgres, sqlConn
}
