{{ config(materialized='ephemeral') }}

with measA as (
select * from {{ ref('measA') }})
select measA.measA+1 as measB from measA