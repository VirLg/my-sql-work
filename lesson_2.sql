select distinct campaign_id ,ad_date 
, sum(spend) as spend
, sum(impressions) as impressions
, sum(clicks) as clicks
, sum(value) as value
, sum(spend)/sum(clicks) as CPC
,round((sum(spend)/sum(impressions)::numeric)*1000,2) as CPM
, round(sum(impressions)::numeric/sum(clicks),2) as CTR
, round((sum(value::numeric)/sum(spend))*100,2) as ROMI
from facebook_ads_basic_daily
where campaign_id IS NOT null and clicks !=0
group by campaign_id,ad_date 
;

select *
from  facebook_ads_basic_daily