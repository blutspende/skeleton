package migrator

const migration_34 = `
	ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_cerberus_queue_items ADD COLUMN data_type varchar;
	ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_control_results ADD COLUMN dea_raw_message_id UUID NULL;
	ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_control_results ADD COLUMN message_in_id uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000';
	ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_control_results ADD COLUMN instrument_module varchar NULL;
	ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_results ADD COLUMN instrument_module varchar NULL;
	UPDATE <SCHEMA_PLACEHOLDER>.sk_cerberus_queue_items SET data_type='AnalysisResult' WHERE data_type IS NULL;
	ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_cerberus_queue_items ALTER COLUMN data_type SET NOT NULL;`
