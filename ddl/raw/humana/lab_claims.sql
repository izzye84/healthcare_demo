create table raw_humana.lab_claims (
    cov_month timestamp, -- should this be string or date type?
    lab_result_key varchar(255),
    lab_results_value varchar(255), -- should not be float here because sometimes it's a string 
    loinc_cd varchar(255),
    pers_gen_key varchar(255),
    service_date timestamp, -- should this be string or date type?
    src_mbr_id varchar(255),
    src_platform_cd varchar(255),
    vendor_loinc_cd varchar(255),
    vendor_results_units_desc varchar(255)
);
