package migrator

const migration_2 = `
ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_requests
ADD COLUMN reexamination_requested_count int not null default(0);

ALTER TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_requests
ADD COLUMN sent_to_instrument_count int not null default(0);

CREATE TABLE <SCHEMA_PLACEHOLDER>.sk_analysis_request_instrument_transmissions(
    id uuid not null default(uuid_generate_v4()),
    analysis_request_id uuid not null,
    instrument_id uuid not null,
    created_at timestamp not null default(now()),
    constraint pk_sk_analysis_request_instrument_transmissions primary key (id),
    constraint fk_sk_analysis_request_instrument_transmissions_analysis_request foreign key (analysis_request_id) references <SCHEMA_PLACEHOLDER>.sk_analysis_requests(id),
    constraint fk_sk_analysis_request_instrument_transmissions_instruments foreign key (instrument_id) references <SCHEMA_PLACEHOLDER>.sk_instruments(id)
);
`
