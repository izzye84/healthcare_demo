create table member_eligibility (
    first_name varchar(255),
    middle_name varchar(255),
    last_name varchar(255),
    social_security varchar(15),
    dob timestamp,
    patient_id varchar(255),
    relation varchar(255),
    gender varchar(255),
    race varchar(255),
    ethnic_group varchar(255),
    primary_phone varchar(50),
    primary_email varchar(255),
    address_line1 varchar(255),
    address_line2 varchar(255),
    city varchar(255),
    state varchar(50),
    zip varchar(15),
    eff_date timestamp,
    term_date timestamp,
    lob varchar(255),
    contract varchar(255),
    insurance_name varchar(255),
    primary_care_prov_id varchar(255)
);