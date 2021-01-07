/*****************************************************************/
/*** Create Redshift User Groups                               ***/
/*****************************************************************/
create group loader;
create group transformer;
create group reporter;

/*****************************************************************/
/*** Create Redshift Users                                     ***/
/*****************************************************************/
create user ejenkins password disable in group transformer;
create user ierekson password disable in group transformer;
create user czuloaga password disable in group transformer;
create user smoran password disable in group transformer;
create user mfaraji password disable in group transformer;
create user dwallace password disable in group transformer;
create user smorgan password disable in group transformer;
create user kdent password disable in group transformer;
create user sstatton password disable in group transformer;
create user mgravina password disable in group transformer;
create user ajain password disable in group transformer;
create user nbyers password disable in group transformer;
create user smancuso password disable in group transformer;
create user lebronsthegoat password disable in group transformer;
create user jcolclough password disable in group transformer;
create user dreiger password disable in group transformer;

/*** Two service account users need to be added with passwords ***/
/*** Passwords are stored in AWS Secrets Manager ***/

--In Secrets Manager search for analytics_warehouse_inference_service_user
create user inference_service password '<get from Secrets Manager>' in group reporter;

-- In Secrets Manager search for AnalyticsWarehouseDataLoadService
create user data_load_service password '<get from Secrets Manager>' in group loader;

/*****************************************************************/
/*** Create Databases   								       ***/
/*****************************************************************/
create database "strive-dev";
create database "strive-prod";

/*****************************************************************/
/*** Create external schemas for Redshift Spectrum queries     ***/
/*****************************************************************/

create external schema opp_assessment
from data catalog
database 'opp_assessment'
region 'us-east-1'
iam_role 'arn:aws:iam::557805935884:role/redshift-glue';

create external schema platform_data_pro
from data catalog
database 'platform_data_pro'
region 'us-east-1'
iam_role 'arn:aws:iam::557805935884:role/redshift-glue';

create external schema platform_reference_data
from data catalog
database 'reference_data'
region 'us-east-1'
iam_role 'arn:aws:iam::557805935884:role/redshift-glue';

create external schema raw_conviva
from data catalog
database 'analytics_raw_conviva'
region 'us-east-1'
iam_role 'arn:aws:iam::557805935884:role/redshift-glue';

create external schema raw_humana
from data catalog
database 'analytics_raw_humana'
region 'us-east-1'
iam_role 'arn:aws:iam::557805935884:role/redshift-glue';

create external schema raw_ssm
from data catalog
database 'analytics_raw_ssm'
region 'us-east-1'
iam_role 'arn:aws:iam::557805935884:role/redshift-glue';

create external schema reference_data
from data catalog
database 'analytics_reference_data'
region 'us-east-1'
iam_role 'arn:aws:iam::557805935884:role/redshift-glue';

create external schema synthea
from data catalog
database 'synthea_pro'
region 'us-east-1'
iam_role 'arn:aws:iam::557805935884:role/redshift-glue';

create external schema datascience_pro
from data catalog
database 'datascience_pro'
region 'us-east-1'
iam_role 'arn:aws:iam::557805935884:role/redshift-glue';

/****************************************************************************/
/*** Grant Permissions on Raw Schemas. Run in strive-dev and strive-prod  ***/
/****************************************************************************/
grant create on database "strive-dev" to group loader;
grant create on database "strive-dev" to group transformer;

grant create on database "strive-prod" to group loader;
grant create on database "strive-prod" to group transformer;

grant select on all tables in schema information_schema to group transformer;
grant select on all tables in schema pg_catalog to group transformer;

grant usage on schema opp_assessment to group transformer;
grant select on all tables in schema opp_assessment to group transformer;

grant usage on schema platform_data_pro to group transformer;
grant select on all tables in schema platform_data_pro to group transformer;

grant usage on schema platform_reference_data to group transformer;
grant select on all tables in schema platform_reference_data to group transformer;

grant usage on schema raw_conviva to group transformer;
grant select on all tables in schema raw_conviva to group transformer;

grant usage on schema raw_humana to group transformer;
grant select on all tables in schema raw_humana to group transformer;

grant usage on schema raw_ssm to group transformer;
grant select on all tables in schema raw_ssm to group transformer;

grant usage on schema reference_data to group transformer;
grant select on all tables in schema reference_data to group transformer;

grant usage on schema datascience_pro to group transformer;
grant select on all tables in schema reference_data to group transformer;

grant usage on schema synthea to group transformer;
grant select on all tables in schema synthea to group transformer;

grant usage on schema opp_assessment to group loader;
grant select on all tables in schema opp_assessment to group loader;

grant usage on schema platform_data_pro to group loader;
grant select on all tables in schema platform_data_pro to group loader;

grant usage on schema platform_reference_data to group loader;
grant select on all tables in schema platform_reference_data to group loader;

grant usage on schema raw_conviva to group loader;
grant select on all tables in schema raw_conviva to group loader;

grant usage on schema raw_humana to group loader;
grant select on all tables in schema raw_humana to group loader;

grant usage on schema raw_ssm to group loader;
grant select on all tables in schema raw_ssm to group loader;

grant usage on schema reference_data to group loader;
grant select on all tables in schema reference_data to group loader;

grant usage on schema synthea to group loader;
grant select on all tables in schema synthea to group loader;

grant usage on schema opp_assessment to group reporter;
grant select on all tables in schema opp_assessment to group reporter;

grant usage on schema platform_data_pro to group reporter;
grant select on all tables in schema platform_data_pro to group reporter;

grant usage on schema platform_reference_data to group reporter;
grant select on all tables in schema platform_reference_data to group reporter;

grant usage on schema raw_conviva to group reporter;
grant select on all tables in schema raw_conviva to group reporter;

grant usage on schema raw_humana to group reporter;
grant select on all tables in schema raw_humana to group reporter;

grant usage on schema raw_ssm to group reporter;
grant select on all tables in schema raw_ssm to group reporter;

grant usage on schema reference_data to group reporter;
grant select on all tables in schema reference_data to group reporter;

grant usage on schema datascience_pro to group reporter;
grant select on all tables in schema reference_data to group reporter;

grant usage on schema synthea to group reporter;
grant select on all tables in schema synthea to group reporter;

grant select on table svv_table_info to group loader;

grant select on table svv_table_info to group reporter;
grant select on table svv_external_tables to group reporter;
grant select on table svv_external_schemas to group reporter;

/*****************************************************************/
/*** Grants for strive-prod DB ONLY   			   			   ***/
/*****************************************************************/
grant usage on schema stage to group transformer;
grant usage on schema stage to group reporter;

grant usage on schema core to group transformer;
grant usage on schema core to group reporter;

grant usage on schema analytics to group transformer;
grant usage on schema analytics to group reporter;

alter default privileges in schema stage
	grant select on tables to group transformer;

alter default privileges in schema core
	grant select on tables to group transformer;

alter default privileges in schema analytics
	grant select on tables to group transformer;

alter default privileges in schema core
	grant select on tables to group reporter;

alter default privileges in schema analytics
	grant select on tables to group reporter;
