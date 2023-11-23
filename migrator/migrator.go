package migrator

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
)

// in order! do not skip any number
var migrations = []string{
	migration_1,
	migration_2,
	migration_3,
	migration_4,
	migration_5,
	migration_6,
	migration_7,
	migration_8,
	migration_9,
	migration_10,
	migration_11,
	migration_12,
}

type skeletonMigrator struct {
}

type SkeletonMigrator interface {
	Run(ctx context.Context, db *sqlx.DB, schemaName string) error
}

func (sm *skeletonMigrator) Run(ctx context.Context, db *sqlx.DB, schemaName string) error {
	tx, err := db.Beginx()
	if err != nil {
		return err
	}
	err = sm.createMigrationsTableIfNotExists(ctx, tx, schemaName)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	currentVersion, err := sm.getLastAppliedMigrationVersion(ctx, tx, schemaName)
	if err != nil {
		return err
	}
	for i, query := range migrations {
		version := i + 1
		if version <= currentVersion {
			continue
		}
		query = strings.ReplaceAll(query, "<SCHEMA_PLACEHOLDER>", schemaName)
		_, err = tx.ExecContext(ctx, query)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
		err = sm.insertMigration(ctx, tx, schemaName, version)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	err = tx.Commit()
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	return nil
}

func (sm *skeletonMigrator) createMigrationsTableIfNotExists(ctx context.Context, tx *sqlx.Tx, schemaName string) error {
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.sk_migrations(
		"version" int NOT NULL,
		applied_at timestamp NOT NULL DEFAULT now(),
		description varchar NOT NULL DEFAULT '',
		CONSTRAINT sk_pk_migrations PRIMARY KEY (version)
	);`, schemaName)
	_, err := tx.ExecContext(ctx, query)
	if err != nil {
		return err
	}
	return nil
}

func (sm *skeletonMigrator) insertMigration(ctx context.Context, tx *sqlx.Tx, schemaName string, version int) error {
	query := fmt.Sprintf(`INSERT INTO %s.sk_migrations(version)VALUES($1);`, schemaName)
	_, err := tx.ExecContext(ctx, query, version)
	if err != nil {
		return err
	}
	return nil
}

func (sm *skeletonMigrator) getLastAppliedMigrationVersion(ctx context.Context, tx *sqlx.Tx, schemaName string) (int, error) {
	query := fmt.Sprintf(`SELECT COALESCE(MAX(version),0) FROM %s.sk_migrations;`, schemaName)
	row := tx.QueryRowxContext(ctx, query)
	if row != nil && row.Err() != nil {
		if row.Err() == sql.ErrNoRows {
			return 0, nil
		}
		return -1, row.Err()
	}
	version := 0
	err := row.Scan(&version)
	return version, err
}

func NewSkeletonMigrator() SkeletonMigrator {
	return &skeletonMigrator{}
}
