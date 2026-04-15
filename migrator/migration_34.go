package migrator

const migration_34 = `
	ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_cerberus_queue_items ADD COLUMN IF NOT EXISTS data_type varchar NOT NULL;
	ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_control_results ADD COLUMN IF NOT EXISTS dea_raw_message_id UUID NULL;
	ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_control_results ADD COLUMN IF NOT EXISTS message_in_id uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000';
	ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_control_results ADD COLUMN IF NOT EXISTS instrument_module varchar NULL;
	ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_control_results ADD COLUMN IF NOT EXISTS cerberus_id UUID NULL;
	ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_results ADD COLUMN IF NOT EXISTS instrument_module varchar NULL;
	UPDATE <SCHEMA_PLACEHOLDER>.sk_cerberus_queue_items SET data_type='AnalysisResult' WHERE data_type IS NULL;
	ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_cerberus_queue_items ALTER COLUMN data_type SET NOT NULL;
	UPDATE <SCHEMA_PLACEHOLDER>.sk_analysis_result_control_result_relations sarcrr SET is_processed = TRUE FROM <SCHEMA_PLACEHOLDER>.sk_control_results scr WHERE scr.id = sarcrr.control_result_id AND scr.message_in_id::text = '00000000-0000-0000-0000-000000000000' AND sarcrr.is_processed is false;
	UPDATE <SCHEMA_PLACEHOLDER>.sk_reagent_control_result_relations srcrr SET is_processed = TRUE FROM <SCHEMA_PLACEHOLDER>.sk_control_results scr WHERE scr.id = srcrr.control_result_id AND scr.message_in_id::text = '00000000-0000-0000-0000-000000000000' AND srcrr.is_processed is false;

	CREATE INDEX IF NOT EXISTS sk_idx_analysis_results_instrument_module ON <SCHEMA_PLACEHOLDER>.sk_analysis_results(instrument_module);
	CREATE INDEX IF NOT EXISTS sk_idx_control_results_instrument_module ON <SCHEMA_PLACEHOLDER>.sk_control_results(instrument_module);`
