package migrator

const migration_21 = `
	DROP TABLE <SCHEMA_PLACEHOLDER>.sk_instrument_request_upload_log;
	DROP TABLE <SCHEMA_PLACEHOLDER>.sk_cia_http_history_analysis_request_ids;
	DROP TABLE <SCHEMA_PLACEHOLDER>.sk_cia_http_history_analysis_result_ids;
	DROP TABLE <SCHEMA_PLACEHOLDER>.sk_cia_http_history;
	DROP TABLE <SCHEMA_PLACEHOLDER>.sk_request_mapping_sent;`
