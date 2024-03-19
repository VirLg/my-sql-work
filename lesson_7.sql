with hw_7 as
(
select
	date( DATE_TRUNC('month', ad_date)) ad_month
	, url_parameters as url
	,substring(url_parameters, 'utm_campaign=(.+)') as utm_campaign
	, coalesce (sum(spend),0) as spend
	, coalesce(sum (impressions),0) as impressions
	, coalesce (sum(reach),0) as reach
	, coalesce (sum(clicks),0) as clicks
	, coalesce(sum(leads),0) as leads
	, coalesce (sum(value),0) as value	
		,case
		when
		clicks >0 THEN value/clicks
		ELSE null
	end  as CPC
	,case
		when
		clicks >0 THEN round(((impressions)::numeric/clicks),2)
		ELSE null
	end  as CTR		
	,case
		when
		impressions >0 THEN round((spend/impressions::numeric)*1000,2) 
		ELSE null
	end as CPM
		,case
		when
		spend >0 THEN round((value::numeric/spend)*100,2) 
		ELSE null
	end as ROMI	
from facebook_ads_basic_daily
group by ad_date,url_parameters,CPC,CTR,CPM,ROMI

union 
select 
date( DATE_TRUNC('month', ad_date)) ad_month
	, url_parameters as url
	,substring(url_parameters, 'utm_campaign=(.+)') as utm_campaign
	, coalesce (sum(spend),0) as spend
	, coalesce(sum (impressions),0) as impressions
	, coalesce (sum(reach),0) as reach
	, coalesce (sum(clicks),0) as clicks
	, coalesce(sum(leads),0) as leads
	, coalesce (sum(value),0) as value
		,case
		when
		clicks >0 THEN value/clicks
		ELSE null
	end  as CPC
	,case
		when
		clicks >0 THEN round(((impressions)::numeric/clicks),2)
		ELSE null
	end  as CTR	
	,case
		when
		impressions >0 THEN round((spend/impressions::numeric)*1000,2) 
		ELSE null
	end as CPM
	,case
		when
		spend >0 THEN round((value/spend)*100,2) 
		ELSE null
	end as ROMI	
	
from google_ads_basic_daily
group by ad_date,url_parameters,CPC,CTR,CPM,ROMI

)

select
	ad_month	

	,CASE
     WHEN LOWER(utm_campaign) = 'nan' THEN null  
     ELSE utm_campaign
	END AS utm_campaign
	,CPC
	,CTR
	,(CTR - LAG (CTR) over 
	(partition by ad_month order by 1 asc)) difference_ctr
	,CPM
	,(CPM - LAG (CPM) over 
	(partition by ad_month order by 1 asc)) difference_cpm	
	,ROMI
	,(ROMI - LAG (ROMI) over 
	(partition by ad_month order by 1 asc)) difference_romi	
	
	,spend 
	,impressions 
--	,reach 
	,clicks 
--	,leads 
	,value
from hw_7
--where spend!=0




		









