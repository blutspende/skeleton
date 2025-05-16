package migrator

const migration_26 = `

CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_message_statuses (
	status TEXT NOT NULL,
	CONSTRAINT sk_pk_message_statuses PRIMARY KEY (status)
);

INSERT INTO <SCHEMA_PLACEHOLDER>.sk_message_statuses (status) 
	VALUES ('STORED'), ('PROCESSED'), ('ERROR'), ('SENT');

CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_message_types (
	"type" TEXT NOT NULL,
	CONSTRAINT sk_pk_message_types PRIMARY KEY ("type")
);

INSERT INTO <SCHEMA_PLACEHOLDER>.sk_message_types ("type")
	VALUES ('QUERY'), ('ORDER'), ('RESULT'), ('ACKNOWLEDGEMENT'), ('CANCELLATION'), ('REORDER'), ('DIAGNOSTICS'), ('UNIDENTIFIED');

CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_message_in (
	id uuid NOT NULL DEFAULT uuid_generate_v4(),
	instrument_id uuid NOT NULL,
	status TEXT NOT NULL DEFAULT 'STORED',
	dea_raw_message_id uuid,
	protocol_id uuid NOT NULL,
	"type" TEXT,
	encoding TEXT,
	raw BYTEA NOT NULL,
	error TEXT,
	created_at TIMESTAMP NOT NULL DEFAULT timezone('utc', now()),
	modified_at TIMESTAMP,
	retry_count INT NOT NULL DEFAULT 0,
	CONSTRAINT sk_pk_message_in PRIMARY KEY (id),
	CONSTRAINT sk_fk_message_in_message_statuses FOREIGN KEY (status) REFERENCES <SCHEMA_PLACEHOLDER>.sk_message_statuses (status),
	CONSTRAINT sk_fk_message_in_message_types FOREIGN KEY ("type") REFERENCES <SCHEMA_PLACEHOLDER>.sk_message_types ("type"),
    CONSTRAINT sk_fk_message_in_protocol_id FOREIGN KEY (protocol_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_supported_protocols (id)
);

CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_message_out (
	id uuid NOT NULL DEFAULT uuid_generate_v4(),
	instrument_id uuid NOT NULL,
	status TEXT,
	dea_raw_message_id uuid,
	protocol_id uuid NOT NULL,
	"type" TEXT,
	encoding TEXT,
	raw BYTEA NOT NULL,
	error TEXT,
	trigger_message_in_id uuid,
	response_message_in_id uuid,
	created_at TIMESTAMP NOT NULL DEFAULT timezone('utc', now()),
	modified_at TIMESTAMP,
	retry_count INT NOT NULL DEFAULT 0,
	CONSTRAINT sk_pk_message_out PRIMARY KEY (id),
	CONSTRAINT sk_fk_message_out_message_statuses FOREIGN KEY (status) REFERENCES <SCHEMA_PLACEHOLDER>.sk_message_statuses (status),
	CONSTRAINT sk_fk_message_out_message_types FOREIGN KEY ("type") REFERENCES <SCHEMA_PLACEHOLDER>.sk_message_types ("type"),
	CONSTRAINT sk_fk_message_out_trigger_message_in FOREIGN KEY (trigger_message_in_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_message_in (id),
	CONSTRAINT sk_fk_message_out_response_message_in FOREIGN KEY (response_message_in_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_message_in (id)
);

CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_message_out_orders (
	id uuid NOT NULL DEFAULT uuid_generate_v4(),
	message_out_id uuid NOT NULL,
	request_mapping_id uuid NOT NULL,
	CONSTRAINT sk_pk_message_out_orders PRIMARY KEY (id),
	CONSTRAINT sk_fk_message_out_orders_message_out FOREIGN KEY (message_out_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_message_out (id),
	CONSTRAINT sk_fk_message_out_orders_request_mappings FOREIGN KEY (request_mapping_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_request_mappings (id)
);

CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_message_out_order_analysis_requests (
	id uuid NOT NULL DEFAULT uuid_generate_v4(),
	message_out_order_id uuid NOT NULL,
	analysis_request_id uuid NOT NULL,
	CONSTRAINT sk_pk_message_out_order_analysis_requests PRIMARY KEY (id),
	CONSTRAINT sk_fk_message_out_order_analysis_requests_message_out_orders FOREIGN KEY (message_out_order_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_message_out_orders (id),
	CONSTRAINT sk_fk_message_out_order_analysis_requests_analysis_requests FOREIGN KEY (analysis_request_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_analysis_requests (id)
);

CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_message_in_samplecodes (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    message_in_id uuid NOT NULL,
    sample_code TEXT NOT NULL,
    CONSTRAINT sk_pk_message_in_samplecodes PRIMARY KEY (id),
    CONSTRAINT sk_fk_message_in_samplecodes_message_ins FOREIGN KEY (message_in_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_message_in (id)
);

CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_message_out_samplecodes (
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    message_out_id uuid NOT NULL,
    sample_code TEXT NOT NULL,
    CONSTRAINT sk_pk_message_out_samplecodes PRIMARY KEY (id),
    CONSTRAINT sk_fk_message_out_samplecodes_message_outs FOREIGN KEY (message_out_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_message_out (id)
);

ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_results ALTER COLUMN dea_raw_message_id DROP NOT NULL;
ALTER TABLE <SCHEMA_PLACHOLDER>.sk_analysis_results ADD COLUMN message_in_id uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000';
`
