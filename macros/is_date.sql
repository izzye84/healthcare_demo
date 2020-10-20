{# Verifies field is a properly formatted date. If validation_errors returns >0 rows, test fails. #}

{% macro test_is_date(model, column_name, date_format, date_type='date') %}

{% if date_type != 'date' and date_type != 'timestamp' %}

{% set error_message = '
Warning: the `is_date` test requires `date_type` to be either `date` or `timestamp`. \
The {}.{} model triggered this warning. Date type was {} \
'.format(model.package_name, model.name,date_type) %}

{% do exceptions.warn(error_message) %}

{% endif %}

with validation as (
    select
       {{ column_name }}::varchar(50) as date_string
    from {{ model }}
),

validation_errors as (
    select
        date_string
    from validation
    where date_string != TO_CHAR(date_string::{{ date_type }},'{{ date_format }}')
        and date_string <> ''
)

select count(*)
from validation_errors

{% endmacro %}
