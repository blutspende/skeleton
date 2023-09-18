package migrator

const migration_10 = `
ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_images
ADD COLUMN created_at TIMESTAMP NOT NULL DEFAULT timezone('utc', now());
`
