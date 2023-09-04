package migrator

const migration_9 = `
INSERT INTO <SCHEMA_PLACEHOLDER>.sk_result_mode_definitions (result_mode, description)
VALUES ('SIMULATION', 'Do process, but do not return the results to cerberus'),
       ('QUALIFICATION', 'Return to workflow, these orders will not be returned via external api''s');

UPDATE <SCHEMA_PLACEHOLDER>.sk_analysis_results
SET result_mode = 'SIMULATION'
WHERE result_mode = 'TEST';

UPDATE <SCHEMA_PLACEHOLDER>.sk_analysis_results
SET result_mode = 'QUALIFICATION'
WHERE result_mode = 'VALIDATION';

DELETE FROM <SCHEMA_PLACEHOLDER>.sk_result_mode_definitions
WHERE result_mode IN ('TEST','VALIDATION');`
