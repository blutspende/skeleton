package migrator

const migration_25 = `CREATE INDEX IF NOT EXISTS sk_analysis_request_extra_values_analysis_request_id_idx ON <SCHEMA_PLACEHOLDER>.sk_analysis_request_extra_values (analysis_request_id);`
