with h_work_3 as 
	(
select 
	ad_date as ad_date
  , url_parameters  as media_source
  , sum(spend) as spend
  , sum(impressions) as impressions
  , sum(distinct reach) as reach
  , sum(clicks) as clicks
  , sum(leads) as leads
  , sum(value
) as value
from facebook_ads_basic_daily 
group by ad_date, url_parameters 


union

select 
	  ad_date
	, url_parameters
	, sum(spend)
	, sum(impressions) as impressions
	, sum(distinct reach) as reach
	, sum(clicks) as clicks
    , sum(leads) as leads
    , sum(value
) as value
from google_ads_basic_daily
group by ad_date, url_parameters 
	)
select
	ad_date
	,media_source
	,spend
	,impressions
	,clicks
	,value 
from h_work_3
where ad_date is not null 