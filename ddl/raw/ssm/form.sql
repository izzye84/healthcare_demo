create table ssm.form (
    patient_id varchar(255),
    patient_account_number varchar(255),
    clinical_encounter_id varchar(255),
    form_category_id varchar(255),
    form_category_name varchar(255),
    form_id varchar(255),
    form_name varchar(255),
    section_id varchar(255),
    section_name varchar(255),
    question_id varchar(255),
    question varchar(255),
    answer varchar(255),
    created_timestamp timestamp,
    modified_timestamp timestamp
)