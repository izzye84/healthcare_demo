{{ dbt_utils.date_spine(
    datepart="month",
    start_date="date_trunc('year',dateadd('year',-4,GETDATE()))",
    end_date="dateadd('month',1,GETDATE())"
   )
}}