with task_1 as
(SELECT
  TIMESTAMP_MICROS(event_timestamp)AS timestamp_value
  ,user_pseudo_id as users
  ,event_name
  ,geo.country
  ,device.category
  ,(SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id
  ,case
when traffic_source.medium = '(none)'or traffic_source.medium = '<Other>' then null
when  traffic_source.name = '(data deleted)' then "data deleted"
else traffic_source.medium end as medium 
    ,case
when  traffic_source.source = '<Other>' then null
when  traffic_source.source = '(direct)' then "direct"
when  traffic_source.name = '(data deleted)' then "data deleted"
else traffic_source.source end as  source
    ,case
when  traffic_source.name = '<Other>' then null
when  traffic_source.name = '(direct)' then "direct"
when  traffic_source.name = '(organic)' then "organic"
when  traffic_source.name = '(data deleted)' then "data deleted"
else traffic_source.source end as campaign_name

   FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
   where _TABLE_SUFFIX BETWEEN '20201231' AND '20220101'AND event_name='view_item' 
  or  event_name='add_to_cart' 
  or  event_name= 'add_payment_info' 
  or  event_name='add_shipping_info' 
  or  event_name='purchase'
  or  event_name='begin_checkout'
  or  event_name='session_start'
    group by event_name,user_pseudo_id,geo.country,device.category ,traffic_source.medium,traffic_source.source,traffic_source.name,timestamp_value,ga_session_id
      order by 1 desc
  
  
  --  LIMIT 200
),

---------------------------------------------------------------------------------------1----------
event_convertion_cte as
(select
date(timestamp_value)AS event_date
,source as source 
,medium as medium
,campaign_name as campaign_name
,count(distinct (concat (users,cast(ga_session_id as string)))) as user_ga
,count(distinct(case when event_name='add_to_cart' then ga_session_id else null end))as add_to_cart_cte
,count(distinct(case when event_name='begin_checkout' then ga_session_id else null end)) as begin_checkout_cte
,count(distinct(case when event_name='purchase' then ga_session_id else null end)) as purchase_cte
from task_1
group by event_date,campaign_name,source,medium
),

---------------------------------------------------------------------------------------2---------------
task_2 as
(select
date(timestamp_value)AS event_date
,source as source 
,medium as medium
,campaign_name as campaign_name
,count(distinct (concat (users,cast(ga_session_id as string)))) as user_sessions_count
,count(distinct(case when event_name='add_to_cart' then ga_session_id else null end))as add_to_cart_session
,count(distinct(case when event_name='begin_checkout' then ga_session_id else null end)) as begin_checkout_session
,count(distinct(case when event_name='purchase' then ga_session_id else null end)) as purchase_session
FROM task_1

group by event_date,campaign_name,source,medium

-- LIMIT 200
)

select 
t2.event_date
,t2.source as source 
,t2.medium as medium
,t2.campaign_name as campaign_name
,t2.user_sessions_count as user_sessions_count
,round((t2.add_to_cart_session/t2.user_sessions_count)*100,0) as visit_to_cart
,round((t2.begin_checkout_session/t2.user_sessions_count)*100,0) as visit_to_checkout
,round((t2.purchase_session/t2.user_sessions_count)*100,0) as visit_to_purchase
from task_2 t2
left join event_convertion_cte ecc on t2.user_sessions_count=ecc.user_ga















