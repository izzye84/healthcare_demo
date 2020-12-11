{% macro limit_dev_data() %}
  
    {% if target.name == 'dev' %}

        where ingest_date >= date(date_trunc('month',dateadd(month,{{ var('month_interval',-3) }},current_date)))

    {% endif %}

{% endmacro %}