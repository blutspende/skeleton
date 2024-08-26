package migrator

const migration_20 = `
CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_instrument_ftp_config
(
    instrument_id uuid NOT NULL,
    ftp_server_base_path TEXT NOT NULL,
    ftp_server_file_mask_download TEXT NOT NULL,
    ftp_server_file_mask_upload TEXT NOT NULL,
    ftp_server_host_key TEXT,
    ftp_server_hostname TEXT NOT NULL,
    ftp_server_username TEXT NOT NULL,
    ftp_server_password TEXT NOT NULL,
    ftp_server_port INT NOT NULL,
    ftp_server_public_key TEXT,
    ftp_server_type TEXT,
    CONSTRAINT sk_pk_instrument_ftp_config PRIMARY KEY (instrument_id),
    CONSTRAINT sk_fk_instrument_id FOREIGN KEY (instrument_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_instruments (id) ON DELETE CASCADE
);
`
