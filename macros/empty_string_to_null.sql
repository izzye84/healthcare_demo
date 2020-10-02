{% macro empty_string_to_null(column_name) %}

   case
    when {{ column_name }} = '' then null
    else {{ column_name }}
   end

{% endmacro %}