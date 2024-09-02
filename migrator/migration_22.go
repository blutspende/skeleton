package migrator

const migration_22 = `
ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_reagent_infos
    ALTER COLUMN name SET DEFAULT '';
ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_reagent_infos
    ALTER COLUMN code DROP NOT NULL;
                           
ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_reagent_infos
    ALTER COLUMN serial SET DEFAULT '';
                             
ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_reagent_infos
    ALTER COLUMN lot_no SET DEFAULT '';
        
ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_reagent_infos
    DROP COLUMN analysis_result_id;
                             
ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_reagent_infos
    DROP COLUMN shelf_life;
        
ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_reagent_infos
    RENAME COLUMN use_until TO expiration_date;
ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_reagent_infos
    ALTER COLUMN expiration_date DROP NOT NULL;
        
ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_reagent_infos
    RENAME COLUMN manufacturer_name TO manufacturer;
ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_reagent_infos
    RENAME COLUMN reagent_manufacturer_date TO manufacturing_date;
ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_reagent_infos
    ALTER COLUMN manufacturing_date DROP NOT NULL;
ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_reagent_infos
    RENAME COLUMN reagent_type TO "type";
ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_reagent_infos
    RENAME COLUMN date_created TO created_at;
ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_reagent_infos
    ADD COLUMN cerberus_id UUID NULL;
ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_results
    ADD COLUMN cerberus_id UUID NULL;
ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_reagent_infos RENAME TO sk_reagents;
CREATE INDEX sk_idx_reagents_cerberus_id ON <SCHEMA_PLACEHOLDER>.sk_reagents(cerberus_id);
CREATE INDEX sk_idx_analysis_results_cerberus_id ON <SCHEMA_PLACEHOLDER>.sk_analysis_results(cerberus_id);
CREATE UNIQUE INDEX sk_idx_reagents_unique ON <SCHEMA_PLACEHOLDER>.sk_reagents(manufacturer, serial, lot_no, name);
CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_control_results(
	id UUID NOT NULL DEFAULT UUID_GENERATE_V4(),
	sample_code VARCHAR NULL,
	analyte_code VARCHAR NULL,
    result VARCHAR NOT NULL,
	examined_at TIMESTAMP NOT NULL,
    cerberus_id UUID NULL,
    created_at TIMESTAMP NOT NULL DEFAULT TIMEZONE('UTC', NOW()),
    CONSTRAINT sk_pk_analysis_result_control_results PRIMARY KEY (id)
);
CREATE INDEX sk_idx_control_results_cerberus_id ON <SCHEMA_PLACEHOLDER>.sk_control_results(cerberus_id);
CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_reagent_relations(
    analysis_result_id UUID NOT NULL,
    reagent_id UUID NOT NULL,
    CONSTRAINT sk_pk_analysis_result_reagent_relations PRIMARY KEY (analysis_result_id, reagent_id),
    CONSTRAINT sk_fk_analysis_result_reagent_relations_analysis_result FOREIGN KEY (analysis_result_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_analysis_results(id),
    CONSTRAINT sk_fk_analysis_result_reagent_relations_reagent FOREIGN KEY (reagent_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_reagents(id)
);
CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_reagent_control_result_relations(
    reagent_id UUID NOT NULL,
    control_result_id UUID NOT NULL,
    is_processed BOOL NOT NULL DEFAULT FALSE,
    CONSTRAINT sk_pk_reagent_control_result_relations PRIMARY KEY (reagent_id, control_result_id),
    CONSTRAINT sk_fk_reagent_control_result_relations_reagent FOREIGN KEY (reagent_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_reagents(id),
    CONSTRAINT sk_fk_reagent_control_result_relations_control_result FOREIGN KEY (control_result_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_control_results(id)
);
CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_control_result_relations(
    analysis_result_id UUID NOT NULL,
    control_result_id UUID NOT NULL,
    is_processed BOOL NOT NULL DEFAULT FALSE,
    CONSTRAINT sk_pk_analysis_result_control_result_relations PRIMARY KEY (analysis_result_id, control_result_id),
    CONSTRAINT sk_fk_analysis_result_control_result_relations_analysis_result FOREIGN KEY (analysis_result_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_analysis_results(id),
    CONSTRAINT sk_fk_analysis_result_control_result_relations_control_result FOREIGN KEY (control_result_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_control_results(id)
);
CREATE TYPE <SCHEMA_PLACEHOLDER>.CERBERUS_QUEUE_ITEM_TYPE AS ENUM ('ANALYSIS_RESULT', 'CONTROL_RESULT');
ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_cerberus_queue_items
    ADD COLUMN type <SCHEMA_PLACEHOLDER>.CERBERUS_QUEUE_ITEM_TYPE NOT NULL DEFAULT 'ANALYSIS_RESULT';
CREATE INDEX sk_idx_cerberus_queue_items_type ON <SCHEMA_PLACEHOLDER>.sk_cerberus_queue_items(type);
`
