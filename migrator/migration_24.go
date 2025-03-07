package migrator

const migration_24 = `
ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_results RENAME COLUMN result_record_id TO dea_raw_message_id;

CREATE TABLE IF NOT EXISTS <SCHEMA_PLACEHOLDER>.sk_manufacturer_tests(
	id uuid not null default(uuid_generate_v4()),
	test_name text not null,
	channels text,
    valid_result_values text,
    created_at timestamp DEFAULT timezone('utc', now()),
    modified_at timestamp,
    deleted_at timestamp,
    constraint pk_instrument_manufacturer_tests primary key(id)
);
CREATE UNIQUE INDEX IF NOT EXISTS sk_un_manufacturer_tests_name_idx ON <SCHEMA_PLACEHOLDER>.sk_manufacturer_tests (test_name);

ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_instruments DROP COLUMN IF EXISTS sent_to_cerberus;
`
