package migrator

const migration_20 = `
ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_reagent_infos
    ALTER COLUMN name DROP NOT NULL;

ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_reagent_infos
    ALTER COLUMN code DROP NOT NULL;
        
ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_reagent_infos
    DROP COLUMN shelf_life;
        
ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_reagent_infos
    RENAME COLUMN use_until TO expiration_date;

ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_reagent_infos
    ALTER COLUMN expiration_date DROP NOT NULL;
        
ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_reagent_infos
    RENAME COLUMN manufacturer_name TO manufacturer;

ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_reagent_infos
    RENAME COLUMN reagent_manufacturer_date TO manufacturing_date;

ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_reagent_infos
    ALTER COLUMN manufacturing_date DROP NOT NULL;

ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_reagent_infos
    RENAME COLUMN reagent_type TO "type";

ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_reagent_infos
    RENAME COLUMN date_created TO created_at;
`
