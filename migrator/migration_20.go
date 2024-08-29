package migrator

const migration_20 = `
CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_instrument_ftp_config
(
    id uuid NOT NULL default(uuid_generate_v4()),
    instrument_id uuid NOT NULL,
    username TEXT NOT NULL,
    password TEXT NOT NULL,
    order_path TEXT NOT NULL DEFAULT '/',
    order_file_mask TEXT NOT NULL,
    order_file_suffix TEXT NOT NULL,
    result_path TEXT NOT NULL DEFAULT '/',
    result_file_mask TEXT NOT NULL,
    result_file_suffix TEXT NOT NULL,
    ftp_server_type TEXT NOT NULL,
    created_at timestamp NOT NULL DEFAULT timezone('utc', now()),
    deleted_at TIMESTAMP,
    CONSTRAINT sk_pk_instrument_ftp_config PRIMARY KEY (id),
    CONSTRAINT sk_fk_instrument_id FOREIGN KEY (instrument_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_instruments (id) ON DELETE CASCADE
);
`
