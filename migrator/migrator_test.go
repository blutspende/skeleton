package migrator_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/blutspende/skeleton/migrator"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	tcpg "github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestSkeletonMigrations(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			assert.Fail(t, "should not panic")
		}
	}()

	ctx := context.Background()

	dbName := "postgres"
	dbUser := "postgres"
	dbPass := "postgres"
	postgresContainer, err := tcpg.Run(ctx,
		"postgres:16-alpine",
		tcpg.WithDatabase(dbName),
		tcpg.WithUsername(dbUser),
		tcpg.WithPassword(dbPass),
		tcpg.BasicWaitStrategies(),
	)
	assert.Nil(t, err)
	defer func() {
		if postgresContainer != nil {
			err = testcontainers.TerminateContainer(postgresContainer)
			assert.Nil(t, err)
		}
	}()

	dbPort, err := postgresContainer.MappedPort(ctx, "5432")
	assert.Nil(t, err)
	connStr := fmt.Sprintf("host=localhost port=%d user=postgres password=postgres dbname=postgres sslmode=disable", dbPort.Num())
	dbConn, err := sqlx.Connect("pgx", connStr)
	assert.Nil(t, err)

	schemaName := "test"
	_, err = dbConn.Exec(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp" schema public;`)
	assert.Nil(t, err)
	_, err = dbConn.Exec(`DROP SCHEMA IF EXISTS test CASCADE;`)
	assert.Nil(t, err)
	_, err = dbConn.Exec(`CREATE SCHEMA test;`)
	assert.Nil(t, err)

	migrator := migrator.NewSkeletonMigrator()
	assert.NotNil(t, migrator)
	err = migrator.Run(context.Background(), dbConn, schemaName)
	assert.Nil(t, err)

	row := dbConn.QueryRowx(fmt.Sprintf("SELECT MAX(version) FROM %s.sk_migrations", schemaName))
	assert.NotNil(t, row)
	assert.Nil(t, row.Err())
	var version int
	err = row.Scan(&version)
	assert.Nil(t, err)

	//MODIFY THE EXPECTED VERSION AFTER ADDING NEW SKELETON MIGRATION!!!
	assert.Equal(t, 34, version)
}
