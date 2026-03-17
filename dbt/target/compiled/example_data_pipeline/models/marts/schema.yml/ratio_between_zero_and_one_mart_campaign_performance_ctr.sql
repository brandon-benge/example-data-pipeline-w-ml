
select *
from gold.mart_campaign_performance
where ctr < 0
   or ctr > 1
