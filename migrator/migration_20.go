package migrator

const migration_20 = `
CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_instrument_ftp_config
(
    id uuid NOT NULL default(uuid_generate_v4()),
    instrument_id uuid NOT NULL,
    username TEXT NOT NULL,
    password TEXT NOT NULL,
    remote_path TEXT NOT NULL,
    file_mask TEXT NOT NULL,
    result_remote_path TEXT NOT NULL,
    file_suffix TEXT NOT NULL,
    ftp_server_type TEXT,
    deleted_at TIMESTAMP,
    CONSTRAINT sk_pk_instrument_ftp_config PRIMARY KEY (id),
    CONSTRAINT sk_fk_instrument_id FOREIGN KEY (instrument_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_instruments (id) ON DELETE CASCADE
);
`
