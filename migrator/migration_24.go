package migrator

const migration_24 = `ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_results 
    					RENAME COLUMN result_record_id TO dea_raw_message_id;`
