package migrator

const migration_8 = `
ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_results
DROP COLUMN run_counter;
    
ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_results
ALTER COLUMN yielded_at DROP NOT NULL;
    
ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_results
ALTER COLUMN technical_release_datetime DROP NOT NULL;
    
ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_results
ADD COLUMN created_at TIMESTAMP NOT NULL DEFAULT timezone('utc', now());

CREATE INDEX sk_analysis_results_created_at ON <SCHEMA_PLACEHOLDER>.sk_analysis_results USING btree (created_at);
`
