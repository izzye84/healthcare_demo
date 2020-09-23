{# Verifies field is a properly formatted date. If validation_errors returns >0 rows, test fails. #}

{% macro test_is_date(model, column_name, date_format) %}

with validation as (
    select
       {{ column_name }}::varchar(50) as date_string
    from {{ model }}
),

validation_errors as (
    select
        date_string
    from validation
    where date_string != TO_CHAR(date_string::date,'{{ date_format }}')
)

select count(*)
from validation_errors

{% endmacro %}
