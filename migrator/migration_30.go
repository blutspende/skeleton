package migrator

const migration_30 = `
	ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_requests ADD COLUMN deleted_at TIMESTAMP;
`
