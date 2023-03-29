package migrator

const migration_4 = `
CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_protocol_settings_types(
    "type" varchar not null,
    constraint sk_pk_protocol_settings_types primary key("type")
);

INSERT INTO <SCHEMA_PLACEHOLDER>.sk_protocol_settings_types("type") VALUES('string'),('int'),('bool');

CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_protocol_settings(
    id uuid not null default(uuid_generate_v4()),
    protocol_id uuid not null,
    "key" varchar not null,
    description varchar,
    "type" varchar,
    created_at timestamp not null default(now()),
    modified_at timestamp,
    deleted_at timestamp,
	constraint sk_pk_protocol_settings primary key(id),
	constraint sk_fk_protocol_settings_protocol_id foreign key(protocol_id) references <SCHEMA_PLACEHOLDER>.sk_supported_protocols(id),
	constraint sk_fk_protocol_settings_type foreign key("type") references <SCHEMA_PLACEHOLDER>.sk_protocol_settings_types("type")
);

CREATE INDEX sk_idx_protocol_settings_protocol_id ON <SCHEMA_PLACEHOLDER>.sk_protocol_settings(protocol_id);

CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_instrument_settings(
    id uuid not null default(uuid_generate_v4()),
    instrument_id uuid not null,
    protocol_setting_id uuid not null,
    "value" varchar not null,
    created_at timestamp not null default(now()),
    modified_at timestamp,
    deleted_at timestamp,
    constraint sk_pk_instrument_settings primary key(id),
    constraint sk_fk_instrument_settings_instrument_id foreign key(instrument_id) references <SCHEMA_PLACEHOLDER>.sk_instruments(id),
    constraint sk_fk_instrument_settings_protocol_setting_id foreign key(protocol_setting_id) references <SCHEMA_PLACEHOLDER>.sk_protocol_settings(id)
);

CREATE INDEX sk_idx_instrument_settings_instrument_id ON <SCHEMA_PLACEHOLDER>.sk_instrument_settings(instrument_id);
CREATE UNIQUE INDEX sk_idx_instrument_settings_unique ON <SCHEMA_PLACEHOLDER>.sk_instrument_settings(instrument_id, protocol_setting_id) WHERE deleted_at IS NULL;
`
