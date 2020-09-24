create table ssm.encounter (
    patient_id varchar(255),
    patient_account_number varchar(255),
    clinical_encounter_date timestamp,
    clinical_encounter_facility_name varchar(255),
    clinical_encounter_id varchar(255),
    clinical_encounter_reason varchar(255),
    clinical_encounter_begin_time timestamp,
    clinical_encounter_status varchar(255),
    clinical_encounter_visit_type varchar(255),
    clinica_encounter_end_time timestamp,
    provider_id varchar(255),
    provider_npi varchar(255),
    user_id varchar(255),
    place_of_service_code varchar(255),
    place_of_service_description varchar(255),
    created_timestamp timestamp,
    modified_timestamp timestamp
);