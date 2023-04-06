package migrator

const migration_7 = `
DROP INDEX <SCHEMA_PLACEHOLDER>.sk_un_protocol_abilities;

CREATE UNIQUE INDEX sk_un_protocol_abilities ON <SCHEMA_PLACEHOLDER>.sk_protocol_abilities (protocol_id, connection_mode) where deleted_at is NULL;
`
