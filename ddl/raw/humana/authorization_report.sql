create table raw_humana.authorization_report (
    type varchar(255),
    src_auth_id varchar(255),
    initialnotificationdate timestamp,
    additionalrodrequestdate timestamp,
    auth_status varchar(255),
    auth_admission_date timestamp,
    auth_discharge_date timestamp,
    expected_discharge_dt timestamp,
    authorizeddays numeric(18,0),
    rodstartdate timestamp,
    rodenddate timestamp,
    admission_type varchar(255),
    bed_type varchar(255),
    category varchar(255),
    primarydiagcode varchar(255),
    primarydiagdescription varchar(255),
    mbr_consolidatedsellingmarket varchar(255),
    mbr_coverageenddate timestamp,
    mbr_dateofbirth timestamp,
    mbr_subscriber_id varchar(255),
    mbr_memberiddependent_cd varchar(255),
    mbr_employergroupname varchar(255),
    mbr_employergroupnumber varchar(255),
    mbr_firstname varchar(255),
    mbr_lastname varchar(255),
    mbr_grouperid varchar(255),
    mbr_groupername varchar(255),
    mbrpersgenkey varchar(255),
    mbr_state varchar(255),
    fac_dbaname varchar(255),
    fac_taxid varchar(255),
    fac_npiid varchar(255),
    fac_s_state varchar(255),
    tre_dbaname varchar(255),
    tre_taxid varchar(255),
    tre_npiid varchar(255),
    req_dbaname varchar(255),
    req_taxid varchar(255),
    req_npiid varchar(255),
    discharge_disposition varchar(255)
);
