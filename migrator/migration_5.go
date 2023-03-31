package migrator

const migration_5 = `
UPDATE <SCHEMA_PLACEHOLDER>.sk_encodings SET encoding = 'ISO8859-1' WHERE encoding = 'ISO 8859-1';
`
