package migrator

const migration_36 = `
	CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_samples(
	    id uuid NOT NULL DEFAULT uuid_generate_v4(),
	    sample_code varchar NOT NULL,
	    cerberus_id uuid NULL,
	    external_id varchar NOT NULL,
	    created_at timestamp NOT NULL DEFAULT timezone('utc', now()),
	    CONSTRAINT sk_pk_samples PRIMARY KEY (id)
	);

	CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_plates(
	    id uuid NOT NULL DEFAULT uuid_generate_v4(),
	    plate_identifier varchar NOT NULL,
	    cerberus_id uuid NULL,
	    external_id varchar NOT NULL,
	    plate_created_at timestamp DEFAULT NULL,
	    created_at timestamp NOT NULL DEFAULT timezone('utc', now()),
	    CONSTRAINT sk_pk_plates PRIMARY KEY (id)
	);

	CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_plate_samples(
	    id uuid NOT NULL DEFAULT uuid_generate_v4(),
	    plate_id uuid NOT NULL,
	    sample_id uuid NOT NULL,
	    position_row int NOT NULL,
	    position_column int NOT NULL,
	    CONSTRAINT sk_pk_plate_samples PRIMARY KEY (id),
	    CONSTRAINT sk_fk_plate_samples_plate FOREIGN KEY (plate_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_plates(id),
	    CONSTRAINT sk_fk_plate_samples_sample FOREIGN KEY (sample_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_samples(id)
	);

	CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_pool_types (
	    type varchar NOT NULL,
	    CONSTRAINT sk_pk_pool_types PRIMARY KEY (type)
	);

	INSERT INTO <SCHEMA_PLACEHOLDER>.sk_pool_types (type) VALUES ('TwoStagePool'),('ColumnPool'),('RowPool');

	CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_pools (
		id uuid NOT NULL DEFAULT uuid_generate_v4(),
		sample_code varchar NOT NULL,
	    type varchar NOT NULL,
	    plate_id uuid NOT NULL,
	    cerberus_id uuid NULL,
	    external_id varchar NOT NULL,
	    pool_created_at timestamp DEFAULT NULL,
	    created_at timestamp NOT NULL DEFAULT timezone('utc', now()),
	    CONSTRAINT sk_pk_pools PRIMARY KEY (id),
	    CONSTRAINT sk_fk_pools_plate FOREIGN KEY (plate_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_plates(id),
	    CONSTRAINT sk_fk_pools_pool_type FOREIGN KEY (type) REFERENCES <SCHEMA_PLACEHOLDER>.sk_pool_types(type)
	);

	CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_pool_samples (
	    id uuid NOT NULL DEFAULT uuid_generate_v4(),
	    pool_id uuid NOT NULL,
	    sample_id uuid NOT NULL,
	    CONSTRAINT sk_pk_pool_samples PRIMARY KEY (id),
	    CONSTRAINT sk_fk_pool_samples_pool FOREIGN KEY (pool_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_pools(id),
	    CONSTRAINT sk_fk_pool_samples_sample FOREIGN KEY (sample_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_samples(id)
	);
`
