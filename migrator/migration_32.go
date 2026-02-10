package migrator

const migration_32 = `ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_instruments ADD COLUMN allowresending BOOL NOT NULL DEFAULT false;`
