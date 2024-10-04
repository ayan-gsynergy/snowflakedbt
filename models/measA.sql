{{ config(materialized='ephemeral') }}

select rn-1 as measA from dbt_source.prodsku