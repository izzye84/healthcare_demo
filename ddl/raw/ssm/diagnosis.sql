create table ssm.diagnosis (
    patient_id varchar(255),
    patient_account_number varchar(255),
    clinical_encounter_id varchar(255),
    diagnosis_date timestamp,
    diagnosis_code varchar(255),
    onset_date timestamp,
    resolved_date timestamp,
    code_set varchar(255),
    diagnosis_name varchar(255),
    service_date timestamp,
    created_timestamp timestamp,
    modified_timestamp timestamp
);