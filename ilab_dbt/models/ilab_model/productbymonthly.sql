{{config(materialized='table')}}

with productbymonthly as (
select 
	o.product_id,
	date_part('month', o.order_date) as month,
	sum(o.total_amount) as total_amount_month,
	count(o.order_id) as count_order
from orders as o 
group by
	o.product_id,
	date_part('month', o.order_date)
order by 2
)

select product_id, month, total_amount_month, count_order
from productbymonthly