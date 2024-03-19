with hw_4_1 as 
	(
	select 
		fabd.ad_date as ad_date
		,fabd.url_parameters as media_source 
		, fa.adset_name  as adset_name
		, fc.campaign_name as campaign_name	
		, sum(fabd.spend) as total_spend
		, sum(fabd.impressions) as total_impressions
		, sum(fabd.reach) as total_reach
		, sum(fabd.clicks) as total_clicks
		, sum(fabd.leads) as total_leads
		, sum(fabd.value) as total_value
		
from facebook_ads_basic_daily fabd
left join facebook_adset fa on fabd.adset_id = fa.adset_id
left join facebook_campaign fc on fabd.campaign_id = fc.campaign_id 
where ad_date is not null	
	group by 
	fabd.ad_date
	, fa.adset_name
	,fc.campaign_name
	,fabd.url_parameters
	)

select 
	ad_date
	,media_source
	,campaign_name
	,adset_name
	,total_spend
	,total_impressions
	,total_reach
	,total_clicks
	,total_leads
	,total_value
from hw_4_1

	union
	
	select 
	
		gabd.ad_date 
		,gabd.url_parameters as media_source 
		, gabd.campaign_name 
		, gabd.adset_name	 
		, sum(gabd.spend) 
		, sum(gabd.impressions)
		, sum(gabd.reach) 
		, sum(gabd.clicks)  as total_clicks
		, sum(gabd.leads) as total_leads
		, sum(gabd.value) as total_value
	
from google_ads_basic_daily gabd
where ad_date is not null
group by 	
	gabd.ad_date
	,gabd.adset_name
	,gabd.campaign_name
	,gabd.url_parameters
--order by 1