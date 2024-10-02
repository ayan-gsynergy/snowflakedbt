create or replace transient table RAW.dbt_ephemeral.measC
         as
    (

    with  __dbt__cte__measA as (    select rn-1 as measA from dbt_source.prodsku    ), 
          __dbt__cte__measB as (


                                    with measA as (
                                    select * from __dbt__cte__measA)
                                    select measA.measA+1 as measB from measA
                                ), 
        measA as (select * from __dbt__cte__measA),
        measB as (select * from __dbt__cte__measB)
    select measA+measB as measC from measA,measB where measA.measA = measB.measB
    );



create or replace transient table RAW.dbt_ephemeral.measC
         as
    with  __dbt__cte__measA as (    select rn-1 as measA from dbt_source.prodsku    ), 
          __dbt__cte__measB as (

                                    select __dbt__cte__measA.measA+1 as measB from __dbt__cte__measA
                                ), 
        measA as (select * from __dbt__cte__measA),
        measB as (select * from __dbt__cte__measB)
    select measA+measB as measC from measA,measB where measA.measA = measB.measB;





create or replace transient table RAW.dbt_ephemeral.measC
         as
    (

    with  __dbt__cte__measA as (    select rn-1 as measA from dbt_source.prodsku    ), 
          __dbt__cte__measB as (


                                    with measA as (
                                    select rn-1 as measA from dbt_source.prodsku)
                                    select measA.measA+1 as measB from measA
                                ), 
        measA as (select * from __dbt__cte__measA),
        measB as (select * from __dbt__cte__measB)
    select measA+measB as measC from measA,measB where measA.measA = measB.measB
    );