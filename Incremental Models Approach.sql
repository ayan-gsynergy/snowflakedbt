--All RHS measures are on same base intersection as LHS

with 
    impacted_keys as (
        select distinct <comma separated base intersection columns> from
        <RHS meas table1> where batchid = <current batch id>
        union
        select distinct <comma separated base intersection columns> from
        <RHS meas table2> where batchid = <current batch id>
    ),
    RHSmeasTabl1 as (
        select * from <RHS meas table1> where (<comma separated base intersection columns>) in (select <comma separated base intersection columns> from impacted_keys)
    ),
    RHSmeasTabl2 as (
        select * from <RHS meas table1> where (<comma separated base intersection columns>) in (select <comma separated base intersection columns> from impacted_keys)
    )
select <tranformation logic on measure columns> from RHSmeasTabl1 full outer join RHSmeasTabl2 on RHSmeasTabl1.key1 = RHSmeasTabl2.key1 and ...;

measA(style,day) = measB(day) + measD(sku) + measE(style,day)
measA 
measB -->16th Sep
measD --> sku1
        but also all the skus that belong to st2
measE --> st2
impacted_keys for measA --> st1 - all days and 16th Sep - all styles.

Even if: 
measA(style) = measD(sku)
measD --> sku1

    Filter first approach:
        we dont have to just filter for sku1 but also for all the skus that map to the style that the sku1 maps to.

        Join measD with dim_sku to find all the siblings to sku1 -- you ended up joining to sku table to find the parent style (st1) to sku1 and then filtering for st1.

    Aggregation first approach: get the measD value for st1 and then filter for st1 based on current batch_id.

So we have to basically look at not just the changed rows but also unchanged rows from a RHS measure table and then join to replicate or aggregate and then calculate the LHS measure.


If: 
measA(style) = measD(sku) + measE(style)
measD --> sku1
measE --> st2

Filter first approach:
    First get st1 from measD, and then get st2 from measE and then take a union of st1 and st2 and then filter the 'measD table joined with sku_dim' for st1,st2 and filter measE for st1,st2


In the filter first approach, the SQL for getting st1 from measD would be:

with 
    impacted_keys as(
        select distinct style from measDtbl_stg,dim_sku where dim_sku.sku = measDtbl.sku
        union
        select distinct style from measEtbl_stg
    )
    measDfiltered as (
        select agg(measD),style from measDtbl, dim_sku where dim_sku.sku = measDtbl.sku and style in (select style from impacted_keys) group by style
    ),
    measEfiltered as (
        select * from measEtbl where style in (select style from impacted_keys)
    )
select <tranformation logic on measure columns> as measA from measDfiltered full outer join measEfiltered on joining keys;


Aggregation first approach SQL:

with
    measDtblBroadcasted as (
        select agg(measD) as measD,max(batch_id) as batch_id,style from measDtbl,dim_sku where dim_sku.sku = measDtbl.sku group by style
    ),
    impacted_keys as (
        select distinct style from measDtblBroadcasted where batch_id = cur_batch_id
        union
        select distinct style from measEtbl where batch_id = cur_batch_id
    ),
    measDtblBroadcastedFiltered as (
        select * from measDtblBroadcasted where style in (select style from impacted_keys)
    ),
    measEtblBroadcastedFiltered as (
        select * from measEtbl where style in (select style from impacted_keys)
    )
select <tranformation logic on measure columns> as as measA from measDtblBroadcastedFiltered full outer join measEtblBroadcastedFiltered on joining keys;

--Filter first approach is doable and wins out

measA(style) = measD(class) + measE(style)
measD --> cl2
measE --> st1

Filter first approach:
    Get styles mapping to cl2, union it with st1 and then use this set of keys to filter measD and measE

with
    impacted_keys as (
        --select distinct style from dim_style where class in (select distinct class from measDtbl_stg)
        select distinct style from measDtbl,dim_style where measDtbl.class = dim_style.class and batch_id = cur_batch_id
        union
        select distinct style from measEtbl_stg
    ),
    measDfiltered as (
        select * from measDtbl, dim_style where dim_style.class = measDtbl.class and dim_style.style in (select style from impacted_keys)
    ),
    measEfiltered as (
        select * from measEtbl where style in (select style from impacted_keys)
    )
select <tranformation logic on measure columns> as as measA from measDfiltered full outer join measEfiltered on joining keys;

Replication first approach SQL:
with
    measDtblBroadcasted as (
        select measD, batch_id,style from measDtbl,dim_style where dim_style.class = measDtbl.class
    ),
    impacted_keys as (
        select distinct style from measDtblBroadcasted where batch_id = cur_batch_id
        union
        select distinct style from measEtbl where batch_id = cur_batch_id
    ),
    measDtblBroadcastedFiltered as (
        select * from measDtblBroadcasted where style in (select style from impacted_keys)
    ),
    measEtblBroadcastedFiltered as (
        select * from measEtbl where style in (select style from impacted_keys)
    )
select <tranformation logic on measure columns> as as measA from measDtblBroadcastedFiltered full outer join measEtblBroadcastedFiltered on joining keys;


select distinct style from dim_style where class in (select distinct class from measDtbl where batch_id = cur_batch_id);
select distinct style from measDtbl, dim_style where dim_style.class = measDtbl.class and batch_id = cur_batch_id;


measA(style,day) = measD(style) + measE(day) + measF(style,day) + measG(sku,day)
measD --> st1
measE --> 17th Sept

with
{% if is_incremental() %}
    impacted_keys as (
        select distinct style,day from measDtbl,dim_day where batch_id = cur_batch_id
        union
        select distinct style,day from measEtbl,dim_style where batch_id = cur_batch_id
        union
        select distinct style,day from measFtbl where batch_id = cur_batch_id
        union
        select distinct style, day from measGtbl,dim_sku where measGtbl.sku = dim_sku.sku and batch_id = cur_batch_id
    ),
{% endif %}
    measDfiltered as (
        select * from measDtbl,dim_day where 1=1
        {% if is_incremental() %}
            and (style,day) in (select style,day from impacted_keys)
        {% endif %}
    ),
    measEfiltered as (
        select * from measEtbl,dim_style where 1=1
        {% if is_incremental() %}
            and (style,day) in (select style,day from impacted_keys)
        {% endif %}
    ),
    measFfiltered as (
        select * from measFtbl where 1=1
        {% if is_incremental() %}    
            and (style,day) in (select style,day from impacted_keys)
        {% endif %}
    ),
    measGfiltered as (
        select * from measGtbl, dim_sku where measGtbl.sku = dim_sku.sku
        {% if is_incremental() %}    
            and (style,day) in (select style,day from impacted_keys)
        {% endif %}
    ),
    measGfilteredAggregated as (
        select style,day,agg(measG) as measG from measGfiltered group by style,day
    )
select <tranformation logic on measure columns> as measA 
from 
impacted_keys left join measDfiltered on <keys> left join measEfiltered on <keys> left join measFfiltered on <keys> left join measGfilteredAggregated on <keys>;
/*measDfiltered 
full outer join measEfiltered on measDfiltered.style = measEfiltered.style and measDfiltered.day=measEfiltered.day
full outer join measFfiltered on joining keys 
full outer join measGfilteredAggregated on joining keys*/;

-- For all the dimension, ordered dimension, timeseries functions we get to broadcasted tables first

-- RHS measures on different base intersections
with

    .
    .
    .

    {% if is_incremental() %}
        impacted_keys as (
            select distinct <comma separated base intersection columns> from
            <RHSmeasTable1Broadcasted> where batchid = <current batch id>
            union
            select distinct <comma separated base intersection columns> from
            <RHSmeasTable2Broadcasted> where batchid = <current batch id>
        ),
    {% endif %} 
    RHSmeasTabl1BroadcastedFiltered as (
        select * from <RHSmeasTable1Broadcasted> 
        {% if is_incremental() %}
            where (<comma separated base intersection columns>) in (select <comma separated base intersection columns> from impacted_keys)
        {% endif %}    
    ),
    RHSmeasTabl2BroadcastedFiltered as (
        select * from <RHSmeasTable1Broadcasted>
        {% if is_incremental() %}
            where (<comma separated base intersection columns>) in (select <comma separated base intersection columns> from impacted_keys)
        {% endif %}
    )
select <tranformation logic on measure columns> as measA from RHSmeasTabl1BroadcastedFiltered full outer join RHSmeasTabl2BroadcastedFiltered on RHSmeasTabl1BroadcastedFiltered.key1 = RHSmeasTabl2BroadcastedFiltered.key1 and ...;