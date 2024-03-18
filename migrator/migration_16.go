package migrator

const migration_16 = `
	CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_request_extra_values
(
	id uuid NOT NULL DEFAULT uuid_generate_v4(),
	analysis_request_id uuid NOT NULL,
	key VARCHAR NOT NULL,
	value VARCHAR NOT NULL,
	CONSTRAINT "sk_pk_analysis_request_extra_values" PRIMARY KEY (id),
	CONSTRAINT "sk_fk_extra_values_analysis_request_id__id" FOREIGN KEY (analysis_request_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_analysis_requests (id)
);
`
