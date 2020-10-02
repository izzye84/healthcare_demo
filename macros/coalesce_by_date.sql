{%- macro coalesce_by_date(greater_than_date, less_than_date,field_list,default_field) -%}

case
    when {{ greater_than_date }} >= {{ less_than_date }} then
        coalesce(

        {%- for field in field_list -%}
            {{ field }},
        {%- endfor -%} '')
    else {{ default_field }}
end

{%- endmacro -%}
