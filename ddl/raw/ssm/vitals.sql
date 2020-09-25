create table raw_ssm.vitals (
    patient_id varchar(255),
    patient_account_number varchar(255),
    clinical_encounter_id varchar(255),
    vitals_captured_by varchar(255),
    vitals_name varchar(255),
    vitals_value numeric(18,2),
    vitals_unit varchar(255),
    created_timestamp timestamp,
    modified_timestamp timestamp
);