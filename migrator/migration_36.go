package migrator

const migration_36 = `create table <SCHEMA_PLACEHOLDER>.sk_material_mappings 
(
    id uuid not null default(uuid_generate_v4()),
    material_id uuid not null,
    instrument_id uuid not null,
    code text,
    volume text, 
    unit text,
    created_at timestamp not null default timezone('utc', now()),
    modified_at timestamp,
    deleted_at timestamp,
    constraint sk_pk_material_mappings primary key (id),
    constraint sk_fk_material_mapping_instrument_id foreign key (instrument_id) references <SCHEMA_PLACEHOLDER>.sk_instruments (id)
);

create unique index sk_un_material_mappings_material_id_instrument_id on <SCHEMA_PLACEHOLDER>.sk_material_mappings (material_id, instrument_id) where deleted_at is null;
`
