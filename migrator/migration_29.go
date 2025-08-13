package migrator

const migration_29 = `
	CREATE TABLE IF NOT EXISTS <SCHEMA_PLACEHOLDER>.sk_validated_analytes(
		id uuid NOT NULL DEFAULT uuid_generate_v4(),
		analyte_mapping_id uuid NOT NULL,
		validated_analyte_id uuid NOT NULL,
		created_at TIMESTAMP NOT NULL DEFAULT timezone('utc', NOW()),
		deleted_at TIMESTAMP,
		CONSTRAINT pk_sk_validated_analytes PRIMARY KEY (id),
		CONSTRAINT fk_sk_validated_analytes__analyte_mapping_id FOREIGN KEY (analyte_mapping_id) references <SCHEMA_PLACEHOLDER>.sk_analyte_mappings(id)
	);

	CREATE UNIQUE INDEX IF NOT EXISTS sk_un_validated_analytes_analyte_mapping_id_validated_analyte_id ON <SCHEMA_PLACEHOLDER>.sk_validated_analytes(analyte_mapping_id, validated_analyte_id) WHERE deleted_at IS NULL;

	DROP INDEX IF EXISTS <SCHEMA_PLACEHOLDER>.sk_un_analyte_mapping_instrument_id_control_instrument_analyte;
	DROP INDEX IF EXISTS <SCHEMA_PLACEHOLDER>.sk_un_analyte_mapping_instrument_id_instrument_analyte;
	DROP INDEX IF EXISTS <SCHEMA_PLACEHOLDER>.sk_un_analyte_mapping_analyte_id;

	ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analyte_mappings ADD COLUMN IF NOT EXISTS is_control BOOLEAN NOT NULL DEFAULT false;

	CREATE UNIQUE INDEX IF NOT EXISTS sk_un_analyte_mapping_analyte_id_is_control ON <SCHEMA_PLACEHOLDER>.sk_analyte_mappings USING btree (instrument_id, analyte_id, is_control) WHERE (deleted_at IS NULL);
	CREATE UNIQUE INDEX IF NOT EXISTS sk_un_analyte_mapping_instrument_id_instrument_is_control ON <SCHEMA_PLACEHOLDER>.sk_analyte_mappings(instrument_id, instrument_analyte, is_control) WHERE deleted_at IS NULL;

	ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analyte_mappings DROP COLUMN IF EXISTS control_instrument_analyte;
`
