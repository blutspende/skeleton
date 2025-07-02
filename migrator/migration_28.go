package migrator

const migration_28 = `
	ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_results DROP COLUMN batch_id;
`
