create table raw_humana.demographics (
    age varchar(255), -- do we want this as integer type?
    birth_date timestamp, -- should it be string or date type?
    cov_month timestamp,  -- should it be string or date type?
    ctrct_cat_cd varchar(255),
    esrd_ind varchar(255),
    grpr_comp_name varchar(255),
    grpr_id varchar(255),
    hospice_ind varchar(255),
    lob varchar(255),
    mco_contract_nbr varchar(255),
    medicaid_dual_status_cd varchar(255),
    orig_reas_entitle_cd varchar(255),
    pbp_segment_id varchar(255),
    pcp_assignment varchar(255),
    pers_gen_key varchar(255),
    plan_benefit_package_id varchar(255),
    product_type varchar(255),
    recon_ma_risk_score_nbr varchar(255), -- do we transform to float later?
    sex_cd varchar(255),
    snp_plans varchar(255),
    src_mbr_id varchar(255),
    src_platform_cd varchar(255),
    src_subs_mbr_id varchar(255),
    state_of_residence varchar(255),
    std_rptg_st varchar(255),
    decsd_date timestamp, -- should it be string or date type?
    cov_eff_date timestamp, -- should it be string or date type?
    cov_end_date timestamp, -- should it be string or date type?
    home_phone_nbr varchar(255),
    pers_first_name varchar(255),
    pers_last_name varchar(255)
);
