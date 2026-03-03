package skeleton

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/blutspende/bloodlab-common/db"
	"github.com/blutspende/skeleton/migrator"
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	tcpg "github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestMain(m *testing.M) {
	configureLogger()

	err := setupTestContainers(context.Background())
	if err != nil {
		log.Fatal().Err(err).Msg("starting test containers failed")
	}

	code := m.Run()

	err = teardownTestContainers()
	if err != nil {
		log.Error().Err(err).Msg("stopping test containers failed")
	}

	os.Exit(code)
}

func configureLogger() {
	consoleWriter := zerolog.NewConsoleWriter()
	consoleWriter.TimeFormat = "2006-01-02T15:04:05Z07:00"
	log.Logger = zerolog.New(consoleWriter).With().Caller().Stack().Timestamp().Logger()
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
}

func Recover(t *testing.T) {
	if r := recover(); r != nil {
		assert.Fail(t, "should not panic")
	}
}

// Test containers setup
var postgresContainer *tcpg.PostgresContainer

func setupTestContainers(ctx context.Context) (err error) {
	dbName := "postgres"
	dbUser := "postgres"
	dbPass := "postgres"
	postgresContainer, err = tcpg.Run(ctx,
		"postgres:16-alpine",
		tcpg.WithDatabase(dbName),
		tcpg.WithUsername(dbUser),
		tcpg.WithPassword(dbPass),
		tcpg.BasicWaitStrategies(),
	)
	return err
}
func teardownTestContainers() (err error) {
	if postgresContainer != nil {
		err = testcontainers.TerminateContainer(postgresContainer)
		if err != nil {
			return err
		}
	}
	return nil
}

// Common methods
func setupDbConnector(ctx context.Context, schemaName string) (db.DbConnection, string, db.Postgres, *sqlx.DB, error) {
	dbPort, err := postgresContainer.MappedPort(ctx, "5432")
	if err != nil {
		return nil, "", nil, nil, err
	}
	pgConfig := db.PgConfig{
		ApplicationName: "skeleton_test",
		Host:            "localhost",
		Port:            uint32(dbPort.Int()),
		User:            "postgres",
		Pass:            "postgres",
		Database:        "postgres",
		SSLMode:         "disable",
	}

	postgres := db.NewPostgres(pgConfig)
	sqlConn, err := postgres.Connect(ctx)
	if err != nil {
		return nil, "", nil, nil, err
	}

	_, err = sqlConn.Exec(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp" schema public;`)
	if err != nil {
		return nil, "", nil, nil, err
	}
	_, err = sqlConn.Exec(fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE;`, schemaName))
	if err != nil {
		return nil, "", nil, nil, err
	}
	_, err = sqlConn.Exec(fmt.Sprintf(`CREATE SCHEMA %s;`, schemaName))
	if err != nil {
		return nil, "", nil, nil, err
	}

	dbConn := db.NewDbConnection(sqlConn)

	return dbConn, schemaName, postgres, sqlConn, nil
}

func setupDbConnectorAndRunMigration(ctx context.Context, schemaName string) (db.DbConnection, string, db.Postgres, *sqlx.DB, error) {
	dbConn, _, postgres, sqlConn, err := setupDbConnector(ctx, schemaName)
	if err != nil {
		return nil, "", nil, nil, err
	}

	mig := migrator.NewSkeletonMigrator()
	err = mig.Run(context.Background(), sqlConn, schemaName)
	if err != nil {
		return nil, "", nil, nil, err
	}

	_, err = sqlConn.Exec(fmt.Sprintf(`INSERT INTO %s.sk_supported_protocols (id, "name", description) VALUES ('abb539a3-286f-4c15-a7b7-2e9adf6eab91', 'IH-1000 v5.2', 'IHCOM');`, schemaName))
	if err != nil {
		return nil, "", nil, nil, err
	}

	return dbConn, schemaName, postgres, sqlConn, nil
}

// Reusable code for tests

//defer Recover(t)
//
//ctx := context.Background()
//
//dbConn, schemaName, postgres, sqlConn, err := setupDbConnector(ctx, schemaName)
//if err != nil {
//    assert.Fail(t, err.Error())
//    return
//}
