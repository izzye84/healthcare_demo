{# Verifies string field is not blank. If validation_errors returns >0 rows, test fails. #}

{% macro test_not_blank(model, column_name) %}

with validation as (
    select
       {{ column_name }} as check_column
    from {{ model }}
),

validation_errors as (
    select
        check_column
    from validation
    where check_column = ''
)

select count(*)
from validation_errors

{% endmacro %}