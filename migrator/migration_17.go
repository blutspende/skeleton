package migrator

const migration_17 = `ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_results ADD COLUMN is_invalid bool NOT NULL DEFAULT false;`
