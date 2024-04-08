package migrator

const migration_18 = `
CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_instrument_type_definitions
(
	name TEXT NOT NULL,
	CONSTRAINT sk_pk_instrument_type_definitions PRIMARY KEY (name)
);

INSERT INTO <SCHEMA_PLACEHOLDER>.sk_instrument_type_definitions VALUES ('ANALYZER'), ('SORTER');

ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_instruments 
    ADD COLUMN type TEXT NOT NULL DEFAULT 'ANALYZER', 
    ADD CONSTRAINT sk_fk_instrument_instrument_type FOREIGN KEY (type) 
    	REFERENCES <SCHEMA_PLACEHOLDER>.sk_instrument_type_definitions (name);
`
