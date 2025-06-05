package migrator

const migration_27 = `
	CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_message_in_sample_codes (
		id uuid NOT NULL DEFAULT uuid_generate_v4(),
		message_in_id uuid NOT NULL,
		sample_code TEXT NOT NULL,
		uploaded_to_dea_at TIMESTAMP NULL,
		created_at TIMESTAMP NOT NULL DEFAULT timezone('utc', NOW()),
		CONSTRAINT sk_pk_message_in_sample_codes PRIMARY KEY (id),
		CONSTRAINT sk_fk_message_sample_code_message_in_id FOREIGN KEY (message_in_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_message_in (id) ON DELETE CASCADE
	);
	CREATE INDEX sk_idx_message_in_sample_codes_message_in_id ON <SCHEMA_PLACEHOLDER>.sk_message_in_sample_codes (message_in_id);
	CREATE INDEX sk_idx_message_in_sample_codes_created_at ON <SCHEMA_PLACEHOLDER>.sk_message_in_sample_codes (created_at);
	CREATE UNIQUE INDEX sk_un_idx_message_in_sample_codes_message_id_sample_code ON <SCHEMA_PLACEHOLDER>.sk_message_in_sample_codes (message_in_id, sample_code);
	
	CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_message_out_sample_codes (
		id uuid NOT NULL DEFAULT uuid_generate_v4(),
		message_out_id uuid NOT NULL,
		sample_code TEXT NOT NULL,
		uploaded_to_dea_at TIMESTAMP NULL,
		created_at TIMESTAMP NOT NULL DEFAULT timezone('utc', NOW()),
		CONSTRAINT sk_pk_message_out_sample_codes PRIMARY KEY (id),
		CONSTRAINT sk_fk_message_sample_code_message_out_id FOREIGN KEY (message_out_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_message_out (id) ON DELETE CASCADE
	);
	CREATE INDEX sk_idx_message_out_sample_codes_message_out_id ON <SCHEMA_PLACEHOLDER>.sk_message_out_sample_codes (message_out_id);
	CREATE INDEX sk_idx_message_out_sample_codes_created_at ON <SCHEMA_PLACEHOLDER>.sk_message_out_sample_codes (created_at);
	CREATE UNIQUE INDEX sk_un_idx_message_out_sample_codes_message_id_sample_code ON <SCHEMA_PLACEHOLDER>.sk_message_out_sample_codes (message_out_id, sample_code);
`
