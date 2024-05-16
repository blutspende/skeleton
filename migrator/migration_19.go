package migrator

const migration_19 = `
ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_requests ADD COLUMN modified_at timestamp NULL;
`
