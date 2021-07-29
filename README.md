select distinct spend_type from ANALYSTS.FACT_COUPON_V2_GROWTH_MARKETING_SPEND CP 
select * from  ANALYSTS.FACT_COUPON_V2_GROWTH_MARKETING_SPEND CP limit 1

select year(delivered_date_pt) as year,	month(delivered_date_pt) as month, CATEGORY_L2,lifestage,category_attributed,
spend_type,discount_code, discount_policy_id,
sum(spend) as spend 
from ANALYSTS.FACT_COUPON_V2_GROWTH_MARKETING_SPEND CP
where spend_type IN ('express_marketing_coupon','express_trial','first_free','marketing_coupon','monetary''express_annual_discount')
AND delivered_date_pt between '2021-07-01' and '2021-07-31'
group 	by 1,2,3,4,5,6,7,8
order 		by 2,1

select year(delivered_date_pt) as year,	month(delivered_date_pt) as month, CATEGORY_L2,lifestage,category_attributed,
spend_type,discount_code, discount_policy_id,
sum(spend) as spend 
from ANALYSTS.FACT_COUPON_V2_GROWTH_MARKETING_SPEND CP
where spend_type IN ('express_trial','first_free')
AND delivered_date_pt between '2021-06-01' and '2021-06-30'
group 	by 1,2,3,4,5,6,7,8
order 		by 2,1

select year(delivered_date_pt) as year,	month(delivered_date_pt) as month, CATEGORY_L2,lifestage,category_attributed,
spend_type,discount_code, discount_policy_id,
sum(spend) as spend
from ANALYSTS.FACT_COUPON_V2_GROWTH_MARKETING_SPEND CP
where 	delivered_date_pt>='2020-01-01'
group 	by 1,2,3,4,5,6,7,8
order 		by 2,1

SELECT
--       DDI.CALENDAR_YEAR_MONTH AS ISSUED_MONTH_PT,
--       DDI.CALENDAR_YEAR AS ISSUED_YEAR_PT,
      DDR.CALENDAR_YEAR_MONTH AS REDEEMED_MONTH_PT,
      DDR.CALENDAR_YEAR AS REDEEMED_YEAR_PT,
    CATEGORY,COUPON_CODE,
--     DISCOUNT_DESCRIPTION,
    VALUE_CURRENCY,
    COUNT(DISTINCT COUPON_ID) AS DISTINCT_COUNT_COUPON_ID,
    COUNT(CASE WHEN ORDER_ID IS NOT NULL THEN COUPON_ID ELSE NULL END) AS USED_COUPONS_COUNT,
--     SUM(ISSUED_VALUE) AS ISSUED_VALUE,
--     SUM(ISSUED_VALUE_USD) AS ISSUED_VALUE_USD,
    SUM(KNOWN_USED_VALUE) AS KNOWN_USED_VALUE,
    SUM(KNOWN_USED_VALUE_USD) AS KNOWN_USED_VALUE_USD
from tableau.vw_finance_coupons_categorization
-- left join DWH.DIM_DATE DDI ON DDI.FULL_DATE = ISSUED_DATE_PT
left join DWH.DIM_DATE DDR ON DDR.FULL_DATE = REDEEMED_DATE_PT
where 
[COALESCE(CATEGORY,unknown)=Coupon_Categories]
  and [DDR.CALENDAR_YEAR_MONTH BETWEEN '2020-06-10'
  and [DDR.calendar_year='2021']
  and [VALUE_CURRENCY=("USD", 'CAD')]
--   and [DDI.CALENDAR_YEAR_MONTH=coupons_issued_month]
--   and [DDI.calendar_year=coupons_issued_year]
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4

select * from tableau.vw_finance_coupons_categorization limit 1

select year(delivered_date_pt) as year,	month(delivered_date_pt) as month, CATEGORY_L2,lifestage,
spend_type,discount_code, 
sum(spend) as spend
from 		tableau.vw_finance_coupons_categorization
where 	delivered_date_pt>='2020-01-01'
group 	by 1,2,3,4,5,6
order 		by 2,1


---TD bank
with subscriptions_cleanup AS (
  
SELECT *, row_number() over (partition by user_id,subscription_id order by SUBSCRIPTION_START_DATE_TIME_UTC) AS filter_rank
FROM dwh.dim_subscription 

QUALIFY filter_rank = 1
)

,existing_users AS (
  
SELECT 
distinct user_id
  
FROM subscriptions_cleanup
WHERE subscription_start_date_time_utc::date >= '2021-05-01'
AND subscription_start_date_time_utc::date <= '2021-05-31'
AND CHARGE_ID IS NOT NULL -- Excluding users who may had a trial subscription in May

)


SELECT

convert_timezone('UTC','America/Los_Angeles', ec.created_at)::date as coupon_created_date,
convert_timezone('UTC','America/Los_Angeles', ec.used_at)::date as coupon_used_date,
ec.user_id AS user_id,
CASE WHEN eu.user_id IS NOT NULL
     THEN 'existing_member'
     ELSE 'new_member'
     END AS member_type,
sc.subscription_start_date_time_utc AS subscription_start_date_utc_tied_to_coupon,
sc.subscription_end_date_time_utc AS subscription_end_date_utc_tied_to_coupon,
9.99*3 AS coupon_redeemed_value

FROM rds_data.express_coupons ec

JOIN rds_data.coupon_codes c
ON ec.coupon_code_id = c.id

JOIN rds_data.discount_policies dp
ON c.discount_policy_id = dp.id

LEFT JOIN subscriptions_cleanup sc 
ON sc.user_id = ec.user_id
AND sc.subscription_id = ec.subscription_id

LEFT JOIN existing_users eu 
ON eu.user_id = ec.user_id


WHERE ec.subscription_id is not null
AND ec.free_months is not null
AND dp.id = 825354494 -- Discount Policy for TD Bank Promotion
AND convert_timezone('UTC','America/Los_Angeles',ec.created_at)::date>='2021-06-21' -- Start Date of Promotion
AND sc.SUBSCRIPTION_CANCELED_DATE_TIME_UTC is null -- Excluding any canceled subscriptions
AND coupon_created_date < Current_date -- Making sure subscription data is captured properly
  
ORDER BY 1 ASC;


SELECT * FROM DIM_WAGE W limit 1



SELECT
    PAYMENT_METHOD
  , WAGE_TYPE
  , SUM(CASE WHEN PAYMENT_METHOD = 'payroll' THEN WAGE_AMOUNT_USD*1.12 ELSE WAGE_AMOUNT_USD END)   TOTAL_WAGES
FROM DIM_WAGE W
WHERE TRUE
AND date_trunc('week', CAST (W.EARNED_DATE AS DATE )) >= '2020-02-24'
AND NVL(W.WORKFLOW_STATE, 'x') != 'failed'
AND DATE_TRUNC(MONTH,EARNED_DATE) = '2021-06-01'
GROUP BY 1,2



SELECT
    PAYMENT_METHOD
  , WAGE_TYPE
FROM DIM_WAGE W
GROUP BY 1,2

select * from  ANALYSTS.DELIVERY_LEVEL_PL DLPL limit 1
select * from DWH.FACT_BATCH limit 1
select * from DWH.FACT_ORDER_DELIVERY limit 1

SELECT
DATE_TRUNC('month',DELIVERED_DATE_PT) AS CAL_MONTH
, SPEND_TYPE
, ROUND(SUM(COALESCE (SPEND,0)), 2) AS WAIVED_DELIVERY_FEE_AMT
, SUM(COALESCE (service_fee_trial_cost,0)) AS service_fee_trial_cost_amt
FROM ANALYSTS.FACT_COUPON_V2_GROWTH_MARKETING_SPEND CP
WHERE TRUE
AND (LOWER(INITIATIVE) NOT IN ('happiness') OR INITIATIVE IS NULL)
AND SPEND_TYPE IN ( 'marketing_coupon'
                   , 'express_trial'
                   , 'first_free'
                   , 'express_coupon'
                   , 'express_annual_discount')
AND delivered_date_pt >='2020-01-01'
GROUP BY 1, 2
ORDER BY 1, 2 DESC
       
SELECT ISSUED_MONTH,
       COUPON_ID, COUPON_CODE, KNOWN_USED_VALUE_USD
from tableau.vw_finance_coupons_categorization
where  ISSUED_MONTH BETWEEN '2021-06-01'AND '2021-06-30'
GROUP BY 1,2,3,4
order 		by 2,1
   
SELECT
       From tableau.vw_finance_coupons_categorization
       
       
select year(delivered_date_pt) as year,	month(delivered_date_pt) as month, CATEGORY_L2,lifestage,category_attributed,
spend_type,discount_code, discount_policy_id,
sum(spend) as spend 
from ANALYSTS.FACT_COUPON_V2_GROWTH_MARKETING_SPEND CP
where discount_policy_id IN ('825355558', '825355559','825355214','825355215','825355216','825355217')
group 	by 1,2,3,4,5,6,7,8
order 		by 2,1
       
Select * from tableau.vw_finance_coupons_categorization limit 10
