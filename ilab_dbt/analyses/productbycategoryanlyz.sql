select p.product_category,
	sum(pbm.total_amount_month) as total_amount_category,
	sum(pbm.count_order) as count_order_category
from productbymonthly as pbm
inner join products as p on pbm.product_id = p.product_id
group by p.product_category