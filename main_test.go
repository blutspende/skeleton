package skeleton

import (
	"context"
	"fmt"
	"github.com/blutspende/skeleton/config"
	"github.com/blutspende/skeleton/db"
	"github.com/blutspende/skeleton/migrator"
	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"os"
	"testing"
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

func setupDbConnector(schemaName string) (db.DbConnector, string, db.Postgres, *sqlx.DB) {
	configuration := config.Configuration{}
	configuration.PostgresDB.Host = "localhost"
	configuration.PostgresDB.Port = 5551
	configuration.PostgresDB.User = "postgres"
	configuration.PostgresDB.Pass = "postgres"
	configuration.PostgresDB.Database = "postgres"
	configuration.PostgresDB.SSLMode = "disable"
	postgres := db.NewPostgres(context.Background(), &configuration)
	_ = postgres.Connect()
	sqlConn, _ := postgres.GetSqlConnection()

	_, _ = sqlConn.Exec(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp" schema public;`)
	_, _ = sqlConn.Exec(fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE;`, schemaName))
	_, _ = sqlConn.Exec(fmt.Sprintf(`CREATE SCHEMA %s;`, schemaName))

	dbConn := db.NewDbConnector()
	dbConn.SetDbConnection(sqlConn)

	return dbConn, schemaName, postgres, sqlConn
}

func setupDbConnectorAndRunMigration(schemaName string) (db.DbConnector, string) {
	dbConn, _, _, sqlConn := setupDbConnector(schemaName)

	mig := migrator.NewSkeletonMigrator()
	_ = mig.Run(context.Background(), sqlConn, schemaName)
	_, _ = sqlConn.Exec(fmt.Sprintf(`INSERT INTO %s.sk_supported_protocols (id, "name", description) VALUES ('abb539a3-286f-4c15-a7b7-2e9adf6eab91', 'IH-1000 v5.2', 'IHCOM');`, schemaName))

	return dbConn, schemaName
}
