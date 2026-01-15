package migrator

const migration_31 = `
	ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_instrument_ftp_config RENAME TO sk_instrument_file_server_config;
	ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_instrument_file_server_config RENAME COLUMN ftp_server_type TO server_type;
	ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_instrument_file_server_config RENAME CONSTRAINT sk_pk_instrument_ftp_config TO sk_pk_instrument_file_config;
	INSERT INTO <SCHEMA_PLACEHOLDER>.sk_connection_modes (name) VALUES ('FILE_SERVER');
	UPDATE <SCHEMA_PLACEHOLDER>.sk_instruments SET connection_mode = 'FILE_SERVER' WHERE connection_mode = 'FTP_SFTP';
	UPDATE <SCHEMA_PLACEHOLDER>.sk_protocol_abilities SET connection_mode = 'FILE_SERVER' WHERE connection_mode = 'FTP_SFTP';
	DELETE FROM <SCHEMA_PLACEHOLDER>.sk_connection_modes WHERE "name" = 'FTP_SFTP';
	CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_file_server_types (
		name TEXT NOT NULL,
		CONSTRAINT sk_pk_file_server_types PRIMARY KEY (name)
	);
	INSERT INTO <SCHEMA_PLACEHOLDER>.sk_file_server_types (name) VALUES ('FTP'), ('SFTP'), ('WEBDAV');
	UPDATE <SCHEMA_PLACEHOLDER>.sk_instrument_file_server_config SET server_type = 'FTP' WHERE server_type = 'ftp';
	UPDATE <SCHEMA_PLACEHOLDER>.sk_instrument_file_server_config SET server_type = 'SFTP' WHERE server_type = 'sftp';
	ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_instrument_file_server_config ADD CONSTRAINT sk_fk_instrument_file_server_config_server_type FOREIGN KEY (server_type) REFERENCES <SCHEMA_PLACEHOLDER>.sk_file_server_types (name);	
`
