
select *
from gold.mart_advertiser_engagement
where ctr < 0
   or ctr > 1
