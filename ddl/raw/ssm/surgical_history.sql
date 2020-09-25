create table raw_ssm.surgical_history (
    patient_id varchar(255),
    patient_account_number varchar(255),
    clinical_encounter_id varchar(255),
    reason varchar(255),
    surgical_date timestamp,
    created_timestamp timestamp,
    modified_timestamp timestamp
);