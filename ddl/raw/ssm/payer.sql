create table ssm.payer (
    patient_id varchar(255),
    patient_account_number varchar(255),
    payer_id varchar(255),
    payer_name varchar(255),
    payer_code varchar(255),
    contract_number varchar(255),
    medicaid_id varchar(255),
    medicare_id varchar(255),
    insurance_type_flag varchar(255),
    insurance_product_type varchar(255),
    coverage_begin_date timestamp,
    coverage_end_date timestamp,
    insurance_address_line_1 varchar(255),
    insurance_address_line_2 varchar(255),
    insurance_city varchar(255),
    insurance_state varchar(255),
    insurance_zip varchar(255),
    insurance_group_number varchar(255),
    insurance_subscriber_first_name varchar(255),
    insurance_subscriber_last_name varchar(255),
    insurance_subscriber_relation varchar(255),
    created_timestamp timestamp,
    modified_timestamp timestamp
);