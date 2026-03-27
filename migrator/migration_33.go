package migrator

const migration_33 = `
	ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_message_in_sample_codes ADD COLUMN retry_count INT NOT NULL DEFAULT 0;
	ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_message_out_sample_codes ADD COLUMN retry_count INT NOT NULL DEFAULT 0;`
