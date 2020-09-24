create table ssm.allergies (
    patient_id varchar(255),
    patient_account_number varchar(255),
    clinical_encounter_id varchar(255),
    allergy_verified varchar(255),
    allergen_type varchar(255),
    allergy_description varchar(255),
    allergy_reaction_text varchar(255),
    allergy_status varchar(255),
    allergy_code varchar(255),
    code_set varchar(255),
    onset_date timestamp,
    created_timestamp timestamp,
    modified_timestamp timestamp
);