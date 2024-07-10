package migrator

const migration_21 = `
	ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_reagent_infos RENAME TO sk_reagent_infos;  
	CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_reagent_infos (
		analysis_result_id uuid NOT NULL,
		reagent_info_id uuid NOT NULL,
		CONSTRAINT sk_fk_analysis_result_reagent_info_analysis_result FOREIGN KEY (analysis_result_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_analysis_results(id),
		CONSTRAINT sk_fk_analysis_result_reagent_info_reagent_info FOREIGN KEY (reagent_info_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_reagent_infos(id)
	);
	INSERT INTO <SCHEMA_PLACEHOLDER>.sk_analysis_result_reagent_infos ((SELECT analysis_result_id, id FROM <SCHEMA_PLACEHOLDER>.sk_reagent_infos));
	ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_reagent_infos DROP COLUMN analysis_result_id, ADD COLUMN unique_identifier VARCHAR, ALTER COLUMN created_at SET DEFAULT timezone('utc', now()), ADD COLUMN cerberus_id uuid NULL;
	UPDATE <SCHEMA_PLACEHOLDER>.sk_reagent_infos SET unique_identifier = manufacturer || lot_no || serial;
	ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_reagent_infos ALTER COLUMN unique_identifier SET NOT NULL;
	
	CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_control_results (
		id uuid NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
		sample_code VARCHAR NULL, 
		"result" VARCHAR NOT NULL,
		examined_at TIMESTAMP NOT NULL,
		created_at TIMESTAMP NOT NULL DEFAULT timezone('utc', NOW())
	);
	
	CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_control_results (
		analysis_result_id uuid NOT NULL,
		control_result_id uuid NOT NULL,
		CONSTRAINT sk_fk_analysis_result_control_result_analysis_result FOREIGN KEY (analysis_result_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_analysis_results(id),
		CONSTRAINT sk_fk_analysis_result_control_result_control_result FOREIGN KEY (control_result_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_control_results(id)
	);
	
	CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_reagent_info_control_results (
		reagent_info_id uuid NOT NULL,
		control_result_id uuid NOT NULL,
		CONSTRAINT sk_fk_reagent_info_control_result_reagent_info FOREIGN KEY (reagent_info_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_reagent_infos(id),
		CONSTRAINT sk_fk_reagent_info_control_result_control_result FOREIGN KEY (control_result_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_control_results(id)
	);
`
