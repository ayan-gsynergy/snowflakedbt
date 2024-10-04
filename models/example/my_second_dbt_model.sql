
-- Use the `ref` function to select from other models
{{ config(
    query_tag = 'dbt_special'
) }}
select *
from {{ ref('my_first_dbt_model') }}
where id = 1
