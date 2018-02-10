-- CREATE
-- TABLE
-- 		customer_cln
-- SELECT 
-- 		  cast(CustNo as char(255)) as customer_number
-- 		, trim(CustName) as customer_name
-- 		, trim(Region) as region
-- 		, trim(`Type`) as 'type'
-- 		, trim(Segment) as segment
-- 		, `Customer Since` as date_customer_since
-- FROM 
-- 		maritzcx_raw.customer
-- ;


-- CREATE
-- TABLE
-- 		cx_cln
-- SELECT 
-- 		  cast(`Customer ID` as char(255)) as customer_number
-- 		, `Realationship NPS` as relationship_nps
-- 		, `Transaction NPS` as transaction_nps 
-- 		, case trim(`Text Sentiment`)
-- 			when 'null' then ''
-- 			when 'Null' then ''
-- 		    else trim(`Text Sentiment`) 
-- 		  end as text_sentiment
-- 		, case trim(`SocialCX Class`)
-- 			when 'null' then ''
-- 			when 'Null' then ''
-- 			else trim(`SocialCX Class`)
-- 		  end as socialcx_class
-- FROM 
-- 		maritzcx_raw.cx
-- ;


-- CREATE
-- TABLE
-- 		market_cln
-- SELECT 
-- 		  trim(County) as country
-- 		, trim(Region) as region
-- 		, case trim(`Type`)
-- 			when 'null' then ''
-- 			else trim(`Type`)
-- 		  end as `type`
-- 		, trim(`Economic Condition`) as economic_condition
-- 		, trim(`Competitive Instensity`) as competitive_intensity
-- 		, trim(`Distributor Mystery Shop Performance`) as distributor_mystery_shop_performance
-- FROM 
-- 		maritzcx_raw.market
-- ;


-- CREATE
-- TABLE
-- 		service_history_cln
-- SELECT 
-- 		  cast(`Customer ID` as char(255)) as customer_number
-- 		, trim(`Service Type`) as service_type
-- 		, trim(Resolved) as resolved
-- FROM 
-- 		maritzcx_raw.service_history
-- ;


-- CREATE
-- TABLE
-- 		three_year_order_cln 
-- SELECT 
-- 		  cast(CustNo as char(255)) as customer_number
-- 		, trim(CustName) as customer_name
-- 		, `2015 Quantity` as quantity_2015
-- 		, `2015 Revenue $` as revenue_2015
-- 		, `2015 Discount %` as discount_2015
-- 		, `2016 Quantity`as quantity_2016
-- 		, `2016 Revenue $` as revenue_2016
-- 		, `2016 Discount %` as discount_2016
-- 		, `2017 Quantity` as quantity_2017
-- 		, `2017 Revenue $` as revenue_2017
-- 		, `2017 Discount %` as discount_2017
-- -- 		, `2015 Quantity` as quantity_2015 
-- -- 		, replace(replace(replace(replace(replace(`2015 Revenue $`, '$', ''), '-', ''), ',', ''), ' ', ''), '	', '')
-- -- 		, case replace(replace(replace(replace(replace(`2015 Revenue $`, '$', ''), '-', ''), ',', ''), ' ', ''), '	', '')
-- -- 			when '' then null
-- -- 			else cast(replace(replace(replace(replace(`2015 Revenue $`, '$', ''), '-', ''), ',', ''), ' ', ''), '	', '') as decimal)
-- -- 		  end as revenue_2015
-- -- 		, cast(replace(replace(`2015 Discount %`, "%", ""), ",", "") as double) as discount_2015
-- -- 		, `2016 Quantity` as quantity_2016
-- -- 		, cast(replace(replace(replace(``2016 Revenue $`, "$", ""), "-", ""), ",", "") as double) as revenue_2016
-- -- 		, cast(replace(replace(`2016 Discount %`, "%", ""), ",", "") as double) as discount_2016
-- -- 		, `2017 Quantity` as quantity_2017
-- -- 		, cast(replace(replace(replace(``2017 Revenue $`, "$", ""), "-", ""), ",", "") as double) as revenue_2017
-- -- 		, cast(replace(replace(`2017 Discount %`, "%", ""), ",", "") as double) as discount_2017
-- FROM 
-- 		maritzcx_raw.three_year_order
-- ;


-- select	
-- distinct
-- -- 		  region
-- -- 		, 
-- 		`type`
-- from
-- 		maritzcx.customer_cln
-- order
-- by
-- 		1
-- ;		
-- 
-- 
-- select
-- distinct
-- -- 		  region
-- -- 		, 
-- 		`type`
-- from
-- 		maritzcx.market_cln
-- order
-- by
-- 		1
-- ;


-- -- template
-- CREATE TEMPORARY TABLE core.my_tmp_table 
-- (INDEX my_index_name (tag, time), UNIQUE my_unique_index_name (order_number))
-- SELECT * FROM core.my_big_table
-- WHERE my_val = 1


/* START OF ANALYSIS
 * 
 */ 


-- create supporting tables and indexes so that the final composit query is more readable
drop table if exists s1
;
CREATE 
TABLE 
		s1
		(index index_s1 (customer_number))
select
		  tcust.customer_number
		, tcust.customer_name
		, tcust.region as region_customer
		, tm.region as region_market
		, tcust.`type` as type_customer
		, tm.`type` as type_market
		, tcust.segment
		, tcust.date_customer_since
		, tm.county
		, tm.economic_condition
		, tm.competitive_intensity
		, tm.distributor_mystery_shop_performance
from
		customer_cln as tcust
left
join
		market_cln as tm
on
			tcust.region = tm.region
		and tcust.`type` = tm.`type`
;


-- create 
-- index
-- 		index_service_history_cln
-- on
-- 		service_history_cln (customer_number)
-- ;


drop table if exists `s2`
;
CREATE 
TABLE 
		s2
		(index index_s2 (customer_number_s2))
select
		  case
		  	when s1.customer_number is null and tsh.customer_number is not null then tsh.customer_number
		  	when s1.customer_number is not null and tsh.customer_number is null then s1.customer_number
		  	when s1.customer_number = tsh.customer_number			   			then s1.customer_number
		  	else 'WTF_s1'
		  end customer_number_s2
		, s1.customer_number as customer_number_customer
		, tsh.customer_number as customer_number_sh
		, s1.customer_name
		, s1.region_customer
		, s1.region_market
		, s1.type_customer
		, s1.type_market
		, s1.segment
		, s1.date_customer_since
		, s1.country
		, s1.economic_condition
		, s1.competitive_intensity
		, s1.distributor_mystery_shop_performance
		, tsh.resolved
		, tsh.service_type					
from
		s1
left
outer
join
		service_history_cln as tsh
on
		s1.customer_number = tsh.customer_number
union
all
select
		  case
		  	when s1.customer_number is null and tsh.customer_number is not null then tsh.customer_number
		  	when s1.customer_number is not null and tsh.customer_number is null then s1.customer_number
		  	when s1.customer_number = tsh.customer_number			   			then s1.customer_number
		  	else 'WTF_s1'
		  end customer_number_s2
		, s1.customer_number as customer_number_customer
		, tsh.customer_number as customer_number_sh
		, s1.customer_name
		, s1.region_customer
		, s1.region_market
		, s1.type_customer
		, s1.type_market
		, s1.segment
		, s1.date_customer_since
		, s1.country
		, s1.economic_condition
		, s1.competitive_intensity
		, s1.distributor_mystery_shop_performance
		, tsh.resolved
		, tsh.service_type					
from
		s1
right
outer
join
		service_history_cln as tsh
on
		s1.customer_number = tsh.customer_number
where
		s1.customer_number is null
;


-- create
-- index
-- 		index_three_year_order_cln
-- on
-- 		three_year_order_cln (customer_number)
-- ;


drop table if exists `s3`
;
CREATE 
TABLE 
		s3
		(index index_s3 (customer_number_s3))
select
		  case
		  	when s2.customer_number_s2 is null and t3.customer_number is not null then t3.customer_number
		  	when s2.customer_number_s2 is not null and t3.customer_number is null then s2.customer_number_s2
		  	when s2.customer_number_s2 = t3.customer_number				 		  then s2.customer_number_s2
		  	else 'WTF_s2'
		  end customer_number_s3
		, t3.customer_number as customer_number_3yr_order
		, t3.customer_name as customer_name_3yr_order
		, s2.*
		, t3.quantity_2015
		, t3.revenue_2015
		, t3.discount_2015
		, t3.quantity_2016
		, t3.revenue_2016
		, t3.discount_2016
		, t3.quantity_2017
		, t3.revenue_2017
		, t3.discount_2017
from
		s2
left
join
		three_year_order_cln t3
on
		s2.customer_number_s2 = t3.customer_number
union all
select
		  case
		  	when s2.customer_number_s2 is null and t3.customer_number is not null then t3.customer_number
		  	when s2.customer_number_s2 is not null and t3.customer_number is null then s2.customer_number_s2
		  	when s2.customer_number_s2 = t3.customer_number				 		  then s2.customer_number_s2
		  	else 'WTF_s2'
		  end customer_number_s3
		, t3.customer_number as customer_number_3yr_order
		, t3.customer_name as customer_name_3yr_order
		, s2.*
		, t3.quantity_2015
		, t3.revenue_2015
		, t3.discount_2015
		, t3.quantity_2016
		, t3.revenue_2016
		, t3.discount_2016
		, t3.quantity_2017
		, t3.revenue_2017
		, t3.discount_2017
from
		s2
right
join
		three_year_order_cln t3
on
		s2.customer_number_s2 = t3.customer_number
where
		s2.customer_number_s2 is null
;


-- create
-- index
-- 		index_cx
-- on
-- 		cx_cln (customer_number)
-- ;


-- create the final analysis table (with only client data - no supplementary data)
drop table if exists analysis_panel
;
go
;
create
table
		analysis_panel
select
		  case
		  	when s3.customer_number_s3 is null and tcx.customer_number is not null then tcx.customer_number
		  	when s3.customer_number_s3 is not null and tcx.customer_number is null then s3.customer_number_s3
		  	when s3.customer_number_s3 = tcx.customer_number 					   then s3.customer_number_s3
		  	else 'WTF_s3'
		  end customer_number
		, tcx.customer_number as customer_number_cx
		, s3.*
-- 		, TIMESTAMPDIFF(DAY, date_customer_since, '2018-01-01 00:00:00') as customer_duration_cmdr
		, case
			when revenue_2015 is null and revenue_2016 is null and revenue_2017 is null then null
			when revenue_2017 > 0 then TIMESTAMPDIFF(DAY, date_customer_since, '2018-01-01 00:00:00')
			when revenue_2016 > 0 and revenue_2017 = 0 then TIMESTAMPDIFF(DAY, date_customer_since, '2017-01-01 00:00:00')
			when revenue_2015 > 0 and revenue_2016 = 0 and revenue_2017 = 0 then TIMESTAMPDIFF(DAY, date_customer_since, '2016-01-01 00:00:00') 
			else null
		  end as customer_duration_cmdr
		, case upper(region_customer)
			when 'NORTH' then 1
			when 'NATIONAL' then 1
			else 0
		  end as north
		, case upper(region_customer) 
			when 'SOUTH' then 1
			when 'NATIONAL' then 1
			else 0
		  end as south
		, case upper(region_customer)
			when 'EAST' then 1
			when 'NATIONAL' then 1
			else 0
		  end as east
		, case upper(region_customer)
			when 'WEST' then 1
			when 'NATIONAL' then 1
			else 0
		  end as west
		, case upper(type_customer)
			when 'PAWN' then 1
			else 0
		  end as type_pawn
		, case upper(type_customer)
			when 'RECREATION' then 1
			else 0
		  end as type_recreation
		, case upper(type_customer)
			when 'SPECIALTY' then 1
			else 0
		  end as type_specialty
		, case upper(type_customer)
			when 'MASS' then 1
			else 0
		  end as type_mass
		, case upper(type_customer)
			when 'OTHER' then 1
			else 0
		  end as type_other
		, case upper(segment)
			when 'PRODUCT BREADTH' then 1
			else 0
		  end as segment_product_breadth
		, case upper(segment)
			when 'AVAILABILITY' then 1
			else 0
		  end as segment_availability
		, case upper(segment)
			when 'PRICE POINT' then 1
			else 0
		  end as segment_price_point
		, case upper(segment)
			when 'NOT CLASSIFIED' then 1
			else 0
		  end as segment_not_classified
		, case upper(segment)
			when 'CUSTOMER SERVICE' then 1
			else 0
		  end as segment_customer_service
		, case upper(economic_condition)
			when null then null
			when 'NEGATIVE' then 1
			else 0
		  end as economic_condition_negative
		, case upper(economic_condition)
			when null then null
			when 'NEUTRAL' then 1
			else 0
		  end as economic_condition_neutral
		, case upper(economic_condition)
			when null then null
			when 'POSTIVE' then 1
			else 0
		  end as economic_condition_positive
		, case upper(competitive_intensity)
			when null then null
			when 'HIGH' then 1
			else 0
		  end as competitive_intensity_high
		, case upper(competitive_intensity)
			when null then null
			when 'LOW' then 1
			else 0
		  end as competitive_intensity_low
		, case upper(competitive_intensity)
			when null then null
			when 'MEDIUM' then 1
			else 0
		  end as competitive_intensity_medium
		, case upper(distributor_mystery_shop_performance)
			when null then null
			when 'WORSE' then 1
			else 0
		  end as dmsp_worse
		, case upper(distributor_mystery_shop_performance)
			when null then null
			when 'PARITY' then 1
			else 0
		  end as dmsp_parity
		, case upper(distributor_mystery_shop_performance)
			when null then null
			when 'BETTER' then 1
			else 0
		  end as dmsp_better
		, case upper(resolved)
			when null then null
			when 'YES' then 1
			else 0
		  end as resolved_numeric
		, case upper(service_type)
			when null then null
			when 'RETURN' then 1
			else 0
		  end as service_type_return
		, case upper(service_type)
			when null then null
			when 'TECH SUPPORT' then 1
			else 0
		  end as service_type_tech_support
		, case upper(service_type)
			when null then null
			when 'COMPLAINT' then 1
			else 0
		  end as service_type_complaint
		, case upper(service_type)
			when null then null
			when 'OTHER' then 1
			else 0
		  end as service_type_other
		, case upper(service_type)
			when null then null
			when 'REFUND' then 1
			else 0
		  end as service_type_refund
		, tcx.relationship_nps
		, tcx.transaction_nps
		, tcx.text_sentiment
		, tcx.socialcx_class
		, case
			when revenue_2015 is null or revenue_2016 is null or revenue_2017 is null then null  
			when revenue_2016 * revenue_2017 = 0 then 1
			/* there is no row where all revenue_* columns are equal to 0 (zero) */
			else 0
		  end as churned_cmdr
from	
		s3
left
join
		cx_cln as tcx
on
		s3.customer_number_s3 = tcx.customer_number
union all
select
		  case
		  	when s3.customer_number_s3 is null and tcx.customer_number is not null then tcx.customer_number
		  	when s3.customer_number_s3 is not null and tcx.customer_number is null then s3.customer_number_s3
		  	when s3.customer_number_s3 = tcx.customer_number 					   then s3.customer_number_s3
		  	else 'WTF_s3'
		  end customer_number
		, tcx.customer_number as customer_number_cx
		, s3.*
-- 		, TIMESTAMPDIFF(DAY, date_customer_since, '2018-01-01 00:00:00') as customer_duration_cmdr
		, case
			when revenue_2015 is null and revenue_2016 is null and revenue_2017 is null then null
			when revenue_2017 > 0 then TIMESTAMPDIFF(DAY, date_customer_since, '2018-01-01 00:00:00')
			when revenue_2016 > 0 and revenue_2017 = 0 then TIMESTAMPDIFF(DAY, date_customer_since, '2017-01-01 00:00:00')
			when revenue_2015 > 0 and revenue_2016 = 0 and revenue_2017 = 0 then TIMESTAMPDIFF(DAY, date_customer_since, '2016-01-01 00:00:00') 
			else null
		  end as customer_duration_cmdr
		, case upper(region_customer)
			when 'NORTH' then 1
			when 'NATIONAL' then 1
			else 0
		  end as north
		, case upper(region_customer) 
			when 'SOUTH' then 1
			when 'NATIONAL' then 1
			else 0
		  end as south
		, case upper(region_customer)
			when 'EAST' then 1
			when 'NATIONAL' then 1
			else 0
		  end as east
		, case upper(region_customer)
			when 'WEST' then 1
			when 'NATIONAL' then 1
			else 0
		  end as west
		, case upper(type_customer)
			when 'PAWN' then 1
			else 0
		  end as type_pawn
		, case upper(type_customer)
			when 'RECREATION' then 1
			else 0
		  end as type_recreation
		, case upper(type_customer)
			when 'SPECIALTY' then 1
			else 0
		  end as type_specialty
		, case upper(type_customer)
			when 'MASS' then 1
			else 0
		  end as type_mass
		, case upper(type_customer)
			when 'OTHER' then 1
			else 0
		  end as type_other
		, case upper(segment)
			when 'PRODUCT BREADTH' then 1
			else 0
		  end as segment_product_breadth
		, case upper(segment)
			when 'AVAILABILITY' then 1
			else 0
		  end as segment_availability
		, case upper(segment)
			when 'PRICE POINT' then 1
			else 0
		  end as segment_price_point
		, case upper(segment)
			when 'NOT CLASSIFIED' then 1
			else 0
		  end as segment_not_classified
		, case upper(segment)
			when 'CUSTOMER SERVICE' then 1
			else 0
		  end as segment_customer_service
		, case upper(economic_condition)
			when null then null
			when 'NEGATIVE' then 1
			else 0
		  end as economic_condition_negative
		, case upper(economic_condition)
			when null then null
			when 'NEUTRAL' then 1
			else 0
		  end as economic_condition_neutral
		, case upper(economic_condition)
			when null then null
			when 'POSTIVE' then 1
			else 0
		  end as economic_condition_positive
		, case upper(competitive_intensity)
			when null then null
			when 'HIGH' then 1
			else 0
		  end as competitive_intensity_high
		, case upper(competitive_intensity)
			when null then null
			when 'LOW' then 1
			else 0
		  end as competitive_intensity_low
		, case upper(competitive_intensity)
			when null then null
			when 'MEDIUM' then 1
			else 0
		  end as competitive_intensity_medium
		, case upper(distributor_mystery_shop_performance)
			when null then null
			when 'WORSE' then 1
			else 0
		  end as dmsp_worse
		, case upper(distributor_mystery_shop_performance)
			when null then null
			when 'PARITY' then 1
			else 0
		  end as dmsp_parity
		, case upper(distributor_mystery_shop_performance)
			when null then null
			when 'BETTER' then 1
			else 0
		  end as dmsp_better
		, case upper(resolved)
			when null then null
			when 'YES' then 1
			else 0
		  end as resolved_numeric
		, case upper(service_type)
			when null then null
			when 'RETURN' then 1
			else 0
		  end as service_type_return
		, case upper(service_type)
			when null then null
			when 'TECH SUPPORT' then 1
			else 0
		  end as service_type_tech_support
		, case upper(service_type)
			when null then null
			when 'COMPLAINT' then 1
			else 0
		  end as service_type_complaint
		, case upper(service_type)
			when null then null
			when 'OTHER' then 1
			else 0
		  end as service_type_other
		, case upper(service_type)
			when null then null
			when 'REFUND' then 1
			else 0
		  end as service_type_refund
		, tcx.relationship_nps
		, tcx.transaction_nps
		, tcx.text_sentiment
		, tcx.socialcx_class
		, case
			when revenue_2015 is null or revenue_2016 is null or revenue_2017 is null then null 
			when revenue_2016 * revenue_2017 = 0 then 1
			/* there is no row where all revenue_* columns are equal to 0 (zero) */
			else 0
		  end as churned_cmdr
from	
		s3
right
join
		cx_cln as tcx
on
		s3.customer_number_s3 = tcx.customer_number
where
		s3.customer_number_s3 is null
;


-- US or [NULL]
-- select
-- 		distinct
-- -- 		country
-- -- 		region_customer
-- -- 		type_customer
-- -- 		lower(segment)
-- -- 		lower(economic_condition)
-- -- 		upper(competitive_intensity)
-- -- 		upper(distributor_mystery_shop_performance)
-- -- 		upper(resolved)
-- 		lower(service_type)
-- from
-- 		analysis_panel
-- ;


-- 797 rows 
select
		  quantity_2015
		, revenue_2015
		, discount_2015
		, quantity_2016
		, revenue_2016
		, discount_2016
		, quantity_2017
		, revenue_2017
		, discount_2017
		, customer_duration_cmdr
from
		analysis_panel
where
			revenue_2015 > 0
		and revenue_2016 = 0
		and revenue_2017 = 0
;


-- 0 rows 
select
		  quantity_2015
		, revenue_2015
		, discount_2015
		, quantity_2016
		, revenue_2016
		, discount_2016
		, quantity_2017
		, revenue_2017
		, discount_2017
		, customer_duration_cmdr
from
		analysis_panel
where
			revenue_2015 = 0
		and revenue_2016 = 0
		and revenue_2017 = 0
;


-- 0 rows 
select
		  quantity_2015
		, revenue_2015
		, discount_2015
		, quantity_2016
		, revenue_2016
		, discount_2016
		, quantity_2017
		, revenue_2017
		, discount_2017
		, customer_duration_cmdr
from
		analysis_panel
where
			revenue_2015 = 0
		and revenue_2016 = 0
		and revenue_2017 > 0
;


-- 1,972
select
		  quantity_2015
		, revenue_2015
		, discount_2015
		, quantity_2016
		, revenue_2016
		, discount_2016
		, quantity_2017
		, revenue_2017
		, discount_2017
		, customer_duration_cmdr
from
		analysis_panel
where
		revenue_2015 * revenue_2016 * revenue_2017 = 0
;


-- 1,583
select
		  quantity_2015
		, revenue_2015
		, discount_2015
		, quantity_2016
		, revenue_2016
		, discount_2016
		, quantity_2017
		, revenue_2017
		, discount_2017
		, customer_duration_cmdr
from
		analysis_panel
where
			revenue_2016 * revenue_2017 = 0
		and revenue_2015 > 0
;


-- 786
select
		  quantity_2015
		, revenue_2015
		, discount_2015
		, quantity_2016
		, revenue_2016
		, discount_2016
		, quantity_2017
		, revenue_2017
		, discount_2017
		, customer_duration_cmdr
from
		analysis_panel
where
			revenue_2017 = 0
		and revenue_2015 > 0
		and revenue_2016 > 0
;


-- 786
select
		  quantity_2015
		, revenue_2015
		, discount_2015
		, quantity_2016
		, revenue_2016
		, discount_2016
		, quantity_2017
		, revenue_2017
		, discount_2017
		, customer_duration_cmdr
from
		analysis_panel
where
			revenue_2017 > 0
		and revenue_2015 > 0
		and revenue_2016 > 0
;


-- breakouts of churned
select
		  churned_cmdr
		, count(*)
from
		analysis_panel
group
by
		churned_cmdr
;


select
		  quantity_2015
		, revenue_2015
		, discount_2015
		, quantity_2016
		, revenue_2016
		, discount_2016
		, quantity_2017
		, revenue_2017
		, discount_2017
		, customer_duration_cmdr
		, churned_cmdr
from
		analysis_panel
where
-- 			revenue_2017 is null
--  	or  
 		revenue_2015 is null
-- 		or  
-- 			revenue_2016 is null
;


-- 0 (for all where-conditions)
-- select
-- 		count(*)
-- from
-- 		analysis_panel
-- where
-- -- 		region_customer <> region_market
-- -- 		type_customer != type_market
-- 		customer_name_3yr_order != customer_name
-- ;


-- -- KEY FIELDS
-- customer_number
-- region_customer
-- type_customer
-- customer_duration_cmdr
-- segment
-- country
-- economic_condition
-- competitive_intensity
-- distributor_mystery_shop_performance
-- resolved
-- service_type
-- quantity_2015
-- revenue_2015
-- discount_2015
-- quantity_2016
-- revenue_2016
-- discount_2016
-- quantity_2017
-- revenue_2017
-- discount_2017
-- relationship_nps
-- transaction_nps
-- text_sentiment
-- socialcx_class


-- 16767 
select 
		count(*)
from
		analysis_panel
;		


-- 10209
select 
		count(distinct customer_number)
from
		analysis_panel
;


-- 10209
select
		count(distinct customer_number)
from
		customer_cln
;


-- 0
select
		count(*)
from
		analysis_panel
where
		customer_number_customer is null
		

-- 1780
select
		count(*)
from
		analysis_panel
where
			segment is not null
		and region_customer is not null
		and type_customer is not null
		and date_customer_since is not null
		and country is not null
		and economic_condition is not null
		and competitive_intensity is not null
		and distributor_mystery_shop_performance is not null
		and resolved is not null
		and service_type is not null
		and quantity_2015 is not null
		and revenue_2015 is not null
		and discount_2015 is not null
		and quantity_2016 is not null
		and revenue_2016 is not null
		and discount_2016 is not null
		and quantity_2017 is not null
		and revenue_2017 is not null
		and discount_2017 is not null
;
		
/*
 * Notes/Questions
 * 
 * 1. Positive is spelled "Postive" in value for economic_condition 
 * 2. Is this all of the customer data? A; Yes
 * 3. There are only 1780 records with full data (no null columns) 
 * 4. Which explicit column is churn? A: There is none
 * 5. 2015: 100 lost in first year, 2016: 200 lost in second year
 *		- based on demographics data mining, we should be able to infer which percentage churned
 *		- churn based on presence of quantity/revenue/discount
 * 6. determine why the 200 churned?
 * 		a. create (binary) column churn
 * 		b. cascade
 * 7. Q's:
 * 		- what are the distinct demographic groups in the population?
 * 		- what is the likelihood of each customer to churn?
 * 		- what is the likelihood of each demographic profile?
 * 		 
 * *. ACTION ITEMS:
 * 		1. DEVELOP PRESENTATION
 * 		2. 
 */
 
		