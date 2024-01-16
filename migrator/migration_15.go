package migrator

const migration_15 = `
ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_results
ADD COLUMN is_processed BOOL NOT NULL DEFAULT TRUE;

ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_results
ALTER COLUMN is_processed SET DEFAULT FALSE;
`
