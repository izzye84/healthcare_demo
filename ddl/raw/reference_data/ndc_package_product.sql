/*************************************************

The following DDL will create a new directory in S3
that contains parquet files with NDC reference data
 
To refresh the NDC data:
    1. Replace files in s3://strive-analytics-warehouse-pro/reference_data/codes/code_system=ndc/package
        with the latest version from https://www.nber.org/research/data/national-drug-code

    2. Replace files in 3://strive-analytics-warehouse-pro/reference_data/codes/code_system=ndc/product
        with the lates version from https://www.nber.org/research/data/national-drug-code

    3. *** NOTE: DO NOT DELETE THE ENTIRE BUCKET, JUST THE ONE FOLDER WITH PARQUET FILES ***
        Delete the folder `package_product` at s3://strive-analytics-warehouse-pro/reference_data/codes/code_system=ndc/

    4. Delete the table analytics_reference_data.ndc_package_product in the AWS Glue Catalog

    5. Run the following script to create a new version of the reference table

*******************************************************/


create table ndc_package_product
with (
  format = 'PARQUET',
  parquet_compression = 'SNAPPY',
  external_location = 's3://strive-analytics-warehouse-pro/reference_data/codes/code_system=ndc/package_product'
 ) as

with 

code_format as (
	select 
		ndc_product_codes.product_id
		,ndc_product_codes.product_ndc
		,ndc_package_codes.ndc_package_code
		,cast(length(trim(split_part(ndc_package_codes.ndc_package_code,'-',1))) as varchar) || '-' || cast(length(trim(split_part(ndc_package_codes.ndc_package_code,'-',2))) as varchar) || '-' || cast(length(trim(split_part(ndc_package_codes.ndc_package_code,'-',3)))as varchar) as ndc_10_digit_format
		,trim(split_part(ndc_package_codes.ndc_package_code,'-',1)) as first_section
		,trim(split_part(ndc_package_codes.ndc_package_code,'-',2)) as second_section
		,trim(split_part(ndc_package_codes.ndc_package_code,'-',3)) as third_section
		,ndc_package_codes.package_description
		,ndc_product_codes.product_type_name
		,ndc_product_codes.proprietary_name
		,ndc_product_codes.proprietary_name_suffix
		,ndc_product_codes.non_proprietary_name
		,ndc_product_codes.dosage_form_name
		,ndc_product_codes.route_name
		,ndc_product_codes.labeler_name
		,ndc_product_codes.substance_name
		,ndc_product_codes.strength_number
		,ndc_product_codes.strength_unit
		,ndc_product_codes.pharm_classes
		,ndc_product_codes.dea_schedule
		,ndc_package_codes.sample_package
	from analytics_reference_data.ndc_product_codes left join analytics_reference_data.ndc_package_codes
	on ndc_product_codes.product_id = ndc_package_codes.product_id and ndc_product_codes.product_ndc = ndc_package_codes.product_ndc
)

select 
	product_id 
	,product_ndc 
	,ndc_package_code 
	,ndc_10_digit_format 
	,case ndc_10_digit_format 
		when '4-4-2' then '0' || first_section || second_section || third_section
		when '5-3-2' then first_section || '0' || second_section || third_section
		when '5-4-1' then first_section || second_section || '0' || third_section
 	end as ndc_package_code_11_digit
 	,package_description 
 	,product_type_name
 	,proprietary_name
 	,proprietary_name_suffix
 	,non_proprietary_name
 	,dosage_form_name
 	,route_name
 	,labeler_name
 	,substance_name
 	,strength_number
 	,strength_unit
 	,pharm_classes
 	,dea_schedule
 	,sample_package
from code_format
  