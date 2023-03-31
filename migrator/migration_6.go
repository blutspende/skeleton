package migrator

const migration_6 = `
DROP INDEX <SCHEMA_PLACEHOLDER>.sk_un_result_images_result_id;
DROP INDEX <SCHEMA_PLACEHOLDER>.sk_un_result_images_result_id_channel_result;

CREATE INDEX sk_un_result_images_result_id ON <SCHEMA_PLACEHOLDER>.sk_analysis_result_images USING btree (analysis_result_id);
CREATE INDEX sk_un_result_images_result_id_channel_result ON <SCHEMA_PLACEHOLDER>.sk_analysis_result_images USING btree (analysis_result_id, channel_result_id);
`
