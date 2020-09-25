create table raw_humana.referral (
    mbr_pers_gen_key varchar(255),
    idcard_mbr_id varchar(255),
    pers_last_name varchar(255),
    pers_first_name varchar(255),
    birth_date timestamp,  -- should it be string or date type?
    gender varchar(255),
    home_phone_nbr varchar(255),
    work_phone_nbr varchar(255),
    work_phone_ext varchar(255),
    primary_email_addr varchar(255),
    ckd_stage varchar(255), -- do we want this as string here?
    latest_gfr varchar(255),  -- do we want this as string here?
    latest_acr_score_over300 varchar(255),  -- do we want this as string here?
    esrd_2clms_last4mo varchar(255),
    dialysis_predictive_model_score varchar(255), -- do we want this as string here?
    state_of_residence varchar(255),
    county_of_residence varchar(255),
    current_zip_code varchar(255),
    addr_line1 varchar(255),
    addr_line2 varchar(255),
    city_name varchar(255),
    zip_cd varchar(255), -- what's the difference between this and 'current_zip_code'?
    zip_plus_cd varchar(255),
    latest_month_neph_visit timestamp, -- should it be string here or a date?
    neph_provider_name varchar(255),
    neph_npi varchar(255),
    neph_practice_name varchar(255),
    neph_first_name varchar(255),
    neph_last_name varchar(255),
    rendering_nephrologist_npi varchar(255),
    last_pcp_attributed_month timestamp, -- should it be string or date type?
    pcp_provider_name varchar(255),
    pcp_npi varchar(255),
    pcp_practice_name varchar(255),
    pcp_first_name varchar(255),
    pcp_last_name varchar(255),
    pcp_tax_id varchar(255),
    latest_month_cardiologist_visit timestamp, -- should it be string or date type?
    cardiologist_practice_name varchar(255),
    cardiologist_first_name varchar(255),
    cardiologist_last_name varchar(255),
    latest_mo_endocrinologist_visit timestamp, -- should it be string or date type?
    endocrinologist_practice_name varchar(255),
    endocrinologist_first_name varchar(255),
    endocrinologist_last_name varchar(255),
    vendor varchar(255),
    lob varchar(255),
    referral_month varchar(255),
    live varchar(255),
    notes varchar(255)
);
