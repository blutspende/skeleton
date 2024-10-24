package migrator

const migration_23 = `
ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_reagent_infos
    DROP COLUMN name;
ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_reagent_infos
    DROP COLUMN code;
ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_reagent_infos
    DROP COLUMN analysis_result_id;                
ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_reagent_infos
    DROP COLUMN shelf_life;
ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_reagent_infos
    DROP COLUMN use_until;
ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_reagent_infos
    DROP COLUMN reagent_manufacturer_date;
ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_reagent_infos
    ALTER COLUMN serial SET DEFAULT '';                  
ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_reagent_infos
    ALTER COLUMN lot_no SET DEFAULT '';
ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_reagent_infos
    RENAME COLUMN manufacturer_name TO manufacturer;
ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_reagent_infos
    RENAME COLUMN reagent_type TO "type";
ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_reagent_infos
    RENAME COLUMN date_created TO created_at;
ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_reagent_infos RENAME TO sk_reagents;
CREATE UNIQUE INDEX sk_idx_reagents_unique ON <SCHEMA_PLACEHOLDER>.sk_reagents(manufacturer, serial, lot_no);

ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_results
    ADD COLUMN cerberus_id UUID NULL;
CREATE INDEX sk_idx_analysis_results_cerberus_id ON <SCHEMA_PLACEHOLDER>.sk_analysis_results(cerberus_id);

CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_control_results(
	id UUID NOT NULL DEFAULT UUID_GENERATE_V4(),
	sample_code VARCHAR NULL,
	analyte_mapping_id UUID NOT NULL,
	instrument_id UUID NOT NULL,
	expected_control_result_id UUID NULL,
    is_valid BOOL NOT NULL DEFAULT FALSE,
    is_compared_to_expected_result BOOL NOT NULL DEFAULT FALSE,
    result VARCHAR NOT NULL,
	examined_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT TIMEZONE('UTC', NOW()),
    CONSTRAINT sk_pk_analysis_result_control_results PRIMARY KEY (id)
);

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

CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_control_result_channel_results(
    id UUID NOT NULL DEFAULT UUID_GENERATE_V4(),
    control_result_id UUID NOT NULL,
    channel_id UUID NOT NULL,
    qualitative_result VARCHAR NOT NULL,
    qualitative_result_edited BOOL NOT NULL,
    CONSTRAINT sk_pk_control_result_channel_results PRIMARY KEY (id),
    CONSTRAINT sk_fk_control_result_channel_results_control_result FOREIGN KEY (control_result_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_control_results(id)
);
CREATE INDEX idx_sk_control_result_channel_results_control_result_id on <SCHEMA_PLACEHOLDER>.sk_control_result_channel_results (control_result_id);

CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_control_result_channel_result_quantitative_values(
    id UUID NOT NULL DEFAULT UUID_GENERATE_V4(),
    channel_result_id UUID NOT NULL,
    metric VARCHAR NOT NULL,
    value VARCHAR NOT NULL,
    CONSTRAINT sk_pk_control_result_channel_result_quantitative_values PRIMARY KEY (id),
    CONSTRAINT sk_fk_control_result_channel_result_quantitative_values_control_result_channel_result FOREIGN KEY (channel_result_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_control_result_channel_results(id)
);
CREATE INDEX idx_sk_control_result_channel_result_quantitative_values_channel_result_id on <SCHEMA_PLACEHOLDER>.sk_control_result_channel_result_quantitative_values (channel_result_id);

CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_control_result_extravalues(
    id UUID NOT NULL DEFAULT UUID_GENERATE_V4(),
    control_result_id UUID NOT NULL,
    key VARCHAR NOT NULL,
    value VARCHAR NOT NULL,
    CONSTRAINT sk_pk_control_result_extravalues PRIMARY KEY (id),
    CONSTRAINT sk_fk_control_result_extravalues_control_result FOREIGN KEY (control_result_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_control_results(id)
);
CREATE INDEX idx_sk_control_result_extravalues_control_result_id on <SCHEMA_PLACEHOLDER>.sk_control_result_extravalues (control_result_id);

CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_control_result_warnings(
    id UUID NOT NULL DEFAULT UUID_GENERATE_V4(),
    control_result_id UUID NOT NULL,
    warning VARCHAR NOT NULL,
    CONSTRAINT sk_pk_control_result_warnings PRIMARY KEY (id),
    CONSTRAINT sk_fk_control_result_warnings_control_result FOREIGN KEY (control_result_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_control_results(id)
);
CREATE INDEX idx_sk_control_result_warnings_control_result_id on <SCHEMA_PLACEHOLDER>.sk_control_result_warnings (control_result_id);

CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_control_result_images(
    id UUID NOT NULL DEFAULT UUID_GENERATE_V4(),
    control_result_id UUID NOT NULL,
    channel_result_id UUID,
    name VARCHAR NOT NULL,
    description VARCHAR,
    dea_image_id UUID,
    image_bytes BYTEA,
    uploaded_to_dea_at TIMESTAMP,
    upload_retry_count INTEGER DEFAULT 0,
    upload_error VARCHAR,
    created_at TIMESTAMP NOT NULL DEFAULT TIMEZONE('UTC', NOW()),
    sync_to_cerberus_needed BOOL NOT NULL DEFAULT FALSE,
    CONSTRAINT sk_pk_control_result_images PRIMARY KEY (id),
    CONSTRAINT sk_fk_control_result_images_control_result FOREIGN KEY (control_result_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_control_results(id),
    CONSTRAINT sk_fk_control_result_images_channel_result FOREIGN KEY (channel_result_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_channel_results(id)
);
CREATE INDEX sk_un_control_result_images_control_result_id on <SCHEMA_PLACEHOLDER>.sk_control_result_images (control_result_id);
CREATE INDEX sk_un_control_result_images_control_result_id_channel_result on <SCHEMA_PLACEHOLDER>.sk_control_result_images (control_result_id, channel_result_id);

ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analyte_mappings
    ADD COLUMN control_instrument_analyte VARCHAR;
ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analyte_mappings
    ADD COLUMN control_result_required BOOL NOT NULL DEFAULT FALSE;

CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_expected_control_result(
    id UUID NOT NULL DEFAULT UUID_GENERATE_V4(),
    analyte_mapping_id UUID NOT NULL,
    sample_code VARCHAR NOT NULL,
    operator VARCHAR NOT NULL,
    expected_value VARCHAR NOT NULL,
    expected_value2 VARCHAR,
    created_at TIMESTAMP NOT NULL DEFAULT TIMEZONE('UTC', NOW()),
    deleted_at TIMESTAMP,
	created_by UUID NOT NULL,
    deleted_by UUID,
    CONSTRAINT sk_pk_sk_expected_control_result PRIMARY KEY (id),
    CONSTRAINT sk_fk_sk_expected_control_result_analyte_mapping FOREIGN KEY (analyte_mapping_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_analyte_mappings(id)
);
`
