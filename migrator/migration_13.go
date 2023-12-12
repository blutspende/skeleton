package migrator

const migration_13 = `
ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_requests
ADD COLUMN is_processed BOOL NOT NULL DEFAULT TRUE;

ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_requests
ALTER COLUMN is_processed SET DEFAULT FALSE;
`
