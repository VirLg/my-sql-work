with hw_6 as
(
select
	ad_date as ad_date
	, url_parameters as url
	,substring(url_parameters, 'utm_campaign=(.+)') as utm_campaign
	, coalesce (sum(spend),0) as spend
	, coalesce(sum (impressions),0) as impressions
	, coalesce (sum(reach),0) as reach
	, coalesce (sum(clicks),0) as clicks
	, coalesce(sum(leads),0) as leads
	, coalesce (sum(value),0) as value
	
from facebook_ads_basic_daily

group by ad_date,url_parameters

union 
select 
ad_date as ad_date
	, url_parameters as url
	,substring(url_parameters, 'utm_campaign=(.+)') as utm_campaign
	, coalesce (sum(spend),0) as spend
	, coalesce(sum (impressions),0) as impressions
	, coalesce (sum(reach),0) as reach
	, coalesce (sum(clicks),0) as clicks
	, coalesce(sum(leads),0) as leads
	, coalesce (sum(value),0) as value
from google_ads_basic_daily

group by ad_date,url_parameters

)

select
	ad_date
	,CASE
     WHEN LOWER(utm_campaign) = 'nan' THEN null  
     ELSE utm_campaign
	END AS utm_campaign	
	,case
		when
		clicks >0 THEN value/clicks
		ELSE null
	end  as CPC
	
	,round((clicks/impressions::numeric)*100,2) as CPM						
	,round((spend/impressions::numeric)*1000,2) as CPM
	,round((value::numeric/spend)*100,2) as ROMI
--	,spend 
--	,impressions 
----	,reach 
--	,clicks 
----	,leads 
--	,value
from  hw_6
where spend!=0
group by ad_date,url,spend, impressions, utm_campaign,
	reach, clicks, leads, value
order by 1
--	(	case
--		when
--		clicks >0 THEN value/1
--		ELSE null
--	end  ) desc
;
	
	

	
	
	
	
	
	


