select ad_date
, spend
, clicks 
, spend/clicks as activ_users
from facebook_ads_basic_daily
where clicks!=0
order by 1 desc 
limit 20
;


select *
--count(*) as ad_date 
from facebook_ads_basic_daily
WHERE leads  IS NULL;
;
