package migrator

const migration_22 = `
CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_condition_operand_types (
	operand_type VARCHAR NOT NULL,
	PRIMARY KEY (operand_type)
);

INSERT INTO <SCHEMA_PLACEHOLDER>.sk_condition_operand_types (operand_type) VALUES ('constant'), ('extraValue'), ('sampleCode'), ('laboratory'), ('order'), ('target'), ('sample'), ('analyte'), ('default');

CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_condition_operands (
	id uuid NOT NULL DEFAULT uuid_generate_v4(),
	"name" VARCHAR,
	"type" VARCHAR NOT NULL,
	constant_value VARCHAR,
	extra_value_key VARCHAR,
	created_at TIMESTAMP NOT NULL DEFAULT timezone('utc', now()),
	deleted_at TIMESTAMP,
	modified_at TIMESTAMP,
	CONSTRAINT fk_sk_condition_operand_sk_condition_operand_type FOREIGN KEY (type) REFERENCES <SCHEMA_PLACEHOLDER>.sk_condition_operand_types (operand_type),
	PRIMARY KEY(id)
);

INSERT INTO <SCHEMA_PLACEHOLDER>.sk_condition_operands (id, name, type, constant_value, extra_value_key)
	VALUES
	('5ac8b346-8cd6-4382-b889-93976e836ff2', 'Donation Type', 'extraValue', NULL, 'DonationType'),
	('9a8e849f-085e-475d-a3f6-f67666b127d0', 'Customer Facility', 'extraValue', NULL, 'CustomerFacilityID'),
	('9d5034a8-5041-45e7-8304-0e7ac7159bab', 'First Time Donation Constant', 'constant', 'E', NULL),
	('37c57ec1-a2e5-4c75-9889-c1ae1cb460a3', 'Second Time Donation Constant', 'constant', '2', NULL),
	('ee159352-1cc1-46e0-9620-183bc40843ca', 'Multiple Time Donation Constant', 'constant', 'M', NULL),
	('7e9ea1b8-0f6c-41ef-b376-0ee4e08a62ad', NULL, 'constant', 'true', NULL),
	('8050bb22-7d8d-4c43-a24a-6ef91c5dc0ec', NULL, 'constant', 'pos', NULL),
	('4333845e-9b8c-4a31-affb-d442b3b3f811', NULL, 'constant', 'true', NULL),
	('bfc8a380-cfe6-48f4-8b8b-1ec9b164dc85', NULL, 'constant', NULL, NULL),
	('bc390d40-62b8-4e3a-8964-2f2195341b15', NULL, 'constant', 'false', NULL),
	('6fbaf400-fce0-46c6-bbbf-d2b774a3d0af', NULL, 'order', NULL, NULL),
	('a88b82eb-1cb5-40e1-9238-7eafa4f19975', NULL, 'constant', '', NULL),
	('6613faa0-b2bd-4ded-83a0-d3ce8d03c334', NULL, 'constant', 'true', NULL);

CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_condition_operators (
	operator VARCHAR NOT NULL,
	PRIMARY KEY(operator)
);

INSERT INTO <SCHEMA_PLACEHOLDER>.sk_condition_operators (operator)
	VALUES ('and'),
	('or'),
	('=='),
	('!='),
	('contains'),
	('<'),
	('<='),
	('>'),
	('>='),
	('regex'),
	('exists'),
	('notExists'),
	('matchAny'),
	('matchAll'),
	('compareAll'),
	('targetApplied'),
	('targetNotApplied'),
	('notContains'),
	('isNthSample'),
	('hasNPercentProbability'),
	('default');

CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_conditions
	(
		id uuid NOT NULL DEFAULT uuid_generate_v4(),
		name VARCHAR,
		operator VARCHAR NOT NULL,
		subcondition_1_id uuid,
		subcondition_2_id uuid,
		negate_subcondition_1 BOOLEAN NOT NULL DEFAULT false,
		negate_subcondition_2 BOOLEAN NOT NULL DEFAULT false,
		operand_1_id uuid,
		operand_2_id uuid,
		created_at TIMESTAMP NOT NULL DEFAULT timezone('utc', NOW()),
		deleted_at TIMESTAMP,
		modified_at TIMESTAMP,
		CONSTRAINT fk_sk_condition_sk_condition_operator FOREIGN KEY (operator) REFERENCES <SCHEMA_PLACEHOLDER>.sk_condition_operators (operator),
		CONSTRAINT fk_sk_condition_subcondition1 FOREIGN KEY (subcondition_1_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_conditions (id),
		CONSTRAINT fk_sk_condition_subcondition2 FOREIGN KEY (subcondition_2_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_conditions (id),
		CONSTRAINT fk_sk_condition_sk_operand1 FOREIGN KEY (operand_1_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_condition_operands (id),
		CONSTRAINT fk_sk_condition_sk_operand2 FOREIGN KEY (operand_2_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_condition_operands (id),
	PRIMARY KEY(id)
);


INSERT INTO <SCHEMA_PLACEHOLDER>.sk_conditions (id, name, operator, subcondition_1_id, subcondition_2_id, negate_subcondition_1, negate_subcondition_2, operand_1_id, operand_2_id)
	VALUES('34fdb17b-0d60-4676-80c0-0924667272ca', 'First Time Donation', '==', NULL, NULL, false, false, '5ac8b346-8cd6-4382-b889-93976e836ff2', '9d5034a8-5041-45e7-8304-0e7ac7159bab'),
	('19a7ff42-9669-4559-9acd-6616679dc386', 'Second Time Donation', '==', NULL, NULL, false, false, '5ac8b346-8cd6-4382-b889-93976e836ff2', '37c57ec1-a2e5-4c75-9889-c1ae1cb460a3'),
	('84ef19e9-3c88-422a-9c14-841a48e4e5cb', 'Multiple Time Donation', '==', NULL, NULL, false, false, '5ac8b346-8cd6-4382-b889-93976e836ff2', 'ee159352-1cc1-46e0-9620-183bc40843ca'),
	('16a0db9f-21b8-439d-86d4-f00a0bb9ab66', 'Non-Donor Subject', 'notExists', NULL, NULL, false, false, '5ac8b346-8cd6-4382-b889-93976e836ff2', NULL);

CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_sorting_rules (
	id uuid NOT NULL DEFAULT uuid_generate_v4(),
	instrument_id uuid NOT NULL,
	condition_id uuid,
	priority INT NOT NULL,
	target VARCHAR NOT NULL,
	programme VARCHAR NOT NULL,
	created_at TIMESTAMP NOT NULL DEFAULT timezone('utc', NOW()),
	modified_at TIMESTAMP,
	deleted_at TIMESTAMP,
	CONSTRAINT fk_sk_sorting_rule_sk_instrument FOREIGN KEY (instrument_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_instruments (id),
	CONSTRAINT fk_sk_sorting_rule_sk_condition FOREIGN KEY (condition_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_conditions (id),
	PRIMARY KEY (id)
);

CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_applied_sorting_rule_targets (
	id uuid NOT NULL DEFAULT uuid_generate_v4(),
	instrument_id uuid NOT NULL,
	sample_code VARCHAR NOT NULL,
	target VARCHAR NOT NULL,
	programme VARCHAR,
	created_at TIMESTAMP NOT NULL DEFAULT timezone('utc', NOW()),
	valid_until TIMESTAMP NOT NULL,
	CONSTRAINT fk_sk_applied_sorting_rule_target_sk_instrument FOREIGN KEY (instrument_id) REFERENCES <SCHEMA_PLACEHOLDER>.sk_instruments (id),
	PRIMARY KEY (id)
);
`
