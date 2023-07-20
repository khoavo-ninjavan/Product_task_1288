with route_logs AS (
    SELECT 
       legacy_id as id
       ,driver_id
    FROM vn_views.route_logs_enriched
    WHERE created_month >= date_format(current_date - interval '3' month, '%Y-%m')
    )

,drivers_enriched AS (
    SELECT 
        id
        ,display_name
        ,contact_details
        ,hub_id
        ,hub_name
        
    FROM vn_views.drivers_enriched
    WHERE TRUE
        AND hub_id != 1 
        AND NOT regexp_like(drivers_enriched.display_name, 'RTS-CE|RTS-SS-FS')
    )

select
    transactions.order_id  
    ,transactions.service_end_time + interval '7' hour as attempt_datetime
    ,replace(drivers_enriched.contact_details,' ','' ) as caller
    ,replace(transactions.contact,' ','' ) as callee
    ,transactions.status
    ,transactions.route_id

from datalake_core_prod_vn.transactions  
    join vn_views.order_milestones om on transactions.order_id = om.order_id 
        and transactions.type = 'DD'
        and om.order_type = 'Normal'
    left join route_logs ON transactions.route_id = route_logs.id
    join drivers_enriched ON route_logs.driver_id = drivers_enriched.id        

where TRUE
    and {{attempt_date}}
    
