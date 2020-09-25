create table raw_humana.disenrollment (
    mbr_pers_gen_key varchar(255),
    idcard_mbr_id varchar(255),
    pers_first_name varchar(255),
    pers_last_name varchar(255),
    ineligible_date timestamp, -- should it be string or date type?
    inelig_reason varchar(255),
    lob varchar(255)
);
