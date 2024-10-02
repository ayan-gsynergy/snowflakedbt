measA(style,day) = measD(style) + measE(day) + measF(class,day) + measG(sku,day)
measD --> st1
measE --> 17th Sept
/*
So it seems only for those RHS measures where both for RHS and LHS we have same number of levels from a dimension but a level in RHS is either lower or above than LHS level we have to consider filtering the dimension table for batch_id = cur_batch_id.
And when RHS level is lower ie we have to do aggregation then we have to also include old ancestors.

Lets say style to class mapping is changed and we have to aggregate from sku to class (class being the grandparent of sku, then need to have a trigger that whenever there's an update to a level cascade down its effects to its direct children and then recursively its children will cascade down to all the descendants.)


*/
with
{% if is_incremental() %}
    impacted_keys as (
        select distinct style,day from measDtbl,dim_day where measDtbl.batch_id = cur_batch_id
        union
        select distinct style,day from measEtbl,dim_style where measEtbl.batch_id = cur_batch_id
        union
        select distinct style,day from measFtbl,dim_style where measFtbl.class = dim_style.class and (measFtbl.batch_id = cur_batch_id or dim_style.batch_id = cur_batch_id)
        union
        select distinct style, day from measGtbl,dim_sku where measGtbl.sku = dim_sku.sku and (measGtbl.batch_id = cur_batch_id or dim_sku.batch_id = cur_batch_id)
        union
        select distinct old_style, day from measGtbl,dim_sku where measGtbl.sku = dim_sku.sku and (measGtbl.batch_id = cur_batch_id or dim_sku.batch_id = cur_batch_id)
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
        select * from measFtbl,dim_style where measFtbl.class = dim_style.class
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