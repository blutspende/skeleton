package migrator

const migration_3 = `
ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_images
ADD COLUMN dea_image_id uuid default(null);

ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_images
ADD COLUMN image_bytes bytea default(null);

ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_images
ADD COLUMN uploaded_to_dea_at timestamp default(null);

ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_images
ADD COLUMN upload_retry_count int default(0);

ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_result_images
ADD COLUMN upload_error varchar default(null);
`
