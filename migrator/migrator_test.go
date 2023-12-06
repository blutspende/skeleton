package migrator_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/DRK-Blutspende-BaWueHe/skeleton/migrator"
	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/jmoiron/sqlx"
	_ "github.com/proullon/ramsql/driver"
	"github.com/stretchr/testify/assert"
)

func TestSkeletonMigrations(t *testing.T) {
	postgres := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().Port(5551))
	postgres.Start()
	defer postgres.Stop()
	dbConn, err := sqlx.Connect("postgres", "host=localhost port=5551 user=postgres password=postgres dbname=postgres sslmode=disable")

	schemaName := "test"
	assert.Nil(t, err)

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
	assert.Equal(t, 12, version)
}
