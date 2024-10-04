{{ config(materialized='table') }}

select measA+measB as measC from {{ ref('measA') }} as measA,{{ ref('measB') }} as measB where measA.measA = measB.measB