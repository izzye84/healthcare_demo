create table ssm.procedure (
    patient_id varchar(255),
    patient_account_number varchar(255),
    clinical_encounter_id varchar(255),
    procedure_code varchar(255),
    procedure_description varchar(255),
    procedure_code_type varchar(255),
    modifier1 varchar(255),
    modifier2 varchar(255),
    modifier3 varchar(255),
    modifier4 varchar(255),
    create_timestamp timestamp,
    modified_timestamp timestamp
);