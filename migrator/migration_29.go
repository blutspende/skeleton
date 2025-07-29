package migrator

const migration_29 = `
	CREATE TABLE IF NOT EXISTS <SCHEMA_PLACEHOLDER>.sk_control_mappings(
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    analyte_id uuid NOT NULL,
    instrument_id uuid NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT timezone('utc', NOW()),
    modified_at TIMESTAMP,
    deleted_at TIMESTAMP,
    CONSTRAINT pk_sk_control_mappings PRIMARY KEY (id),
    CONSTRAINT fk_sk_control_mappings_instruments FOREIGN KEY (instrument_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_instruments(id)
	);

	CREATE TABLE IF NOT EXISTS <SCHEMA_PLACEHOLDER>.sk_control_mapping_control_analyte(
		id uuid NOT NULL DEFAULT uuid_generate_v4(),
		control_analyte_id uuid NOT NULL,
		control_mapping_id uuid NOT NULL,
		created_at TIMESTAMP NOT NULL DEFAULT timezone('utc', NOW()),
		modified_at TIMESTAMP,
		deleted_at TIMESTAMP,
		constraint pk_sk_control_mapping_control_analyte PRIMARY KEY (id),
		CONSTRAINT fk_sk_control_mapping_control_analyte_id FOREIGN KEY (control_mapping_id) references <SCHEMA_PLACEHOLDER>.sk_control_mappings(id)
	);

	CREATE UNIQUE INDEX IF NOT EXISTS sk_idx_control_mappings_unique ON <SCHEMA_PLACEHOLDER>.sk_control_mappings(analyte_id, instrument_id) WHERE deleted_at IS NULL;
	CREATE UNIQUE INDEX IF NOT EXISTS sk_idx_control_mapping_control_analyte_unique ON <SCHEMA_PLACEHOLDER>.sk_control_mapping_control_analyte(control_analyte_id, control_mapping_id) WHERE deleted_at IS NULL;

	DROP INDEX IF EXISTS <SCHEMA_PLACEHOLDER>.sk_un_analyte_mapping_instrument_id_control_instrument_analyte;
	ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analyte_mappings DROP COLUMN IF EXISTS control_instrument_analyte;
`
