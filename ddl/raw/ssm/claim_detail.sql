create table raw_ssm.claim_detail(
    claim_number varchar(255),
    claim_line_number varchar(255),
    claim_sequence_number varchar(255),
    patient_external_id varchar(255),
    subsciber_id varchar(255),
    dependent_number varchar(255),
    place_of_service varchar(255),
    type_of_bill varchar(255),
    service_date_time timestamp,
    claim_from_date_time timestamp,
    claim_thru_date_time timestamp,
    rev_code varchar(255),
    drg varchar(255),
    first_name varchar(255),
    last_name varchar(255),
    gender varchar(255),
    date_of_birth timestamp,
    cpt_hcpcs varchar(255),
    loinc varchar(255),
    snomed varchar(255),
    units varchar(255),
    claim_adjustment_type_code varchar(255),
    claim_adjustment_sequence_number varchar(255),
    claim_adjustment_date_time timestamp,
    performing_npi varchar(255),
    allowed_amount numeric(18,2),
    charge_amount numeric(18,2),
    payment_date_time timestamp,
    plan_payment_amount numeric(18,2),
    member_payment_amount numeric(18,2)
);