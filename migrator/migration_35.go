package migrator

const migration_35 = `
	ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_reagents ADD COLUMN IF NOT EXISTS cerberus_id UUID NULL;`
