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
    instrument_module_id uuid NULL,
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
CREATE INDEX sk_idx_message_in_created_at ON <SCHEMA_PLACEHOLDER>.sk_message_in (created_at);
CREATE INDEX sk_idx_message_in_instrument_id ON <SCHEMA_PLACEHOLDER>.sk_message_in (instrument_id);

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
CREATE INDEX sk_idx_message_out_created_at ON <SCHEMA_PLACEHOLDER>.sk_message_out (created_at);
CREATE INDEX sk_idx_message_out_instrument_id ON <SCHEMA_PLACEHOLDER>.sk_message_out (instrument_id);

CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_message_out_orders (
	id uuid NOT NULL DEFAULT uuid_generate_v4(),
	message_out_id uuid NOT NULL,
	sample_code TEXT NOT NULL,
    request_mapping_id uuid NOT NULL,
	CONSTRAINT sk_pk_message_out_orders PRIMARY KEY (id),
    CONSTRAINT sk_fk_message_out_order_message_out_id FOREIGN KEY (message_out_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_message_out (id) ON DELETE CASCADE,
    CONSTRAINT sk_fk_message_out_order_request_mapping_id FOREIGN KEY (request_mapping_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_request_mappings (id)
    );
CREATE INDEX sk_idx_message_out_order_message_out_id ON <SCHEMA_PLACEHOLDER>.sk_message_out_orders (message_out_id);
CREATE INDEX sk_idx_message_out_order_sample_code ON <SCHEMA_PLACEHOLDER>.sk_message_out_orders (sample_code);

CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_message_out_order_analysis_requests (
	id uuid NOT NULL DEFAULT uuid_generate_v4(),
	message_out_order_id uuid NOT NULL,
	analysis_request_id uuid NOT NULL,
	CONSTRAINT sk_pk_message_out_order_analysis_requests PRIMARY KEY (id),
	CONSTRAINT sk_fk_message_out_order_analysis_requests_message_out_orders FOREIGN KEY (message_out_order_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_message_out_orders (id) ON DELETE CASCADE
);
CREATE INDEX sk_idx_sk_message_out_order_analysis_request_message_out_order ON <SCHEMA_PLACEHOLDER>.sk_message_out_order_analysis_requests (message_out_order_id);
CREATE INDEX sk_idx_sk_message_out_order_analysis_request_analysis_request ON <SCHEMA_PLACEHOLDER>.sk_message_out_order_analysis_requests (analysis_request_id);
CREATE UNIQUE INDEX sk_un_idx_message_out_order_analysis_request_id ON <SCHEMA_PLACEHOLDER>.sk_message_out_order_analysis_requests (message_out_order_id, analysis_request_id);

ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_results ALTER COLUMN dea_raw_message_id DROP NOT NULL;
ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_results ADD COLUMN message_in_id uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000';
`
