package migrator

const migration_11 = `
ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_images
ADD COLUMN sync_to_cerberus_needed BOOL NOT NULL DEFAULT FALSE;
`
