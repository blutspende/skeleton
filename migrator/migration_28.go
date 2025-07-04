package migrator

const migration_28 = `
	ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_results DROP COLUMN batch_id;
	CREATE INDEX sk_idx_message_in_dea_raw_message_id ON <SCHEMA_PLACEHOLDER>.sk_message_in (dea_raw_message_id);
	CREATE INDEX sk_idx_message_in_sample_codes_sample_code ON <SCHEMA_PLACEHOLDER>.sk_message_in_sample_codes (sample_code);
`
