--------------------------------------
-- how long does it take for a repurchase
--------------------------------------
select days_since_prior_order, count(*)
from "Ecommerce_Project".model_data
where target = 1
group by days_since_prior_order

--------------------------------------
-- which popular days & hours for repurchases
--------------------------------------
select order_dow, count(*) count_orders
from "Ecommerce_Project".model_data
where target = 1
group by order_dow
order by count_orders desc

select order_hour_of_day, count(*) count_orders
from "Ecommerce_Project".model_data
where target = 1
group by order_hour_of_day
order by count_orders desc
--------------------------------------
-- how long does it take for a repurchase
-- 1.2% repurchase orders have 3 previous orders (max)
-- lots of repurchase takes 1-13 orders
-- when we start to see repurchase pattern, send coupons asap
--------------------------------------
select order_number, 
      count(*)::float  / (select count(*) from "Ecommerce_Project".model_data) * 100 as order_num_ratio
from "Ecommerce_Project".model_data
where target = 1
group by order_number

-- most purchased item within each department (stock more popular items)

with CTE1 as (
select m.department_x, p.product_name, count(*) count_orders_department_product
from "Ecommerce_Project".model_data_department_product m
join "Ecommerce_Project".products p
on m.product_id = p.product_id
group by m.department_x, p.product_name
order by m.department_x
)
select *
from (select department_x,
    		product_name,
    		count_orders_department_product,
			row_number() over (partition by department_x order by count_orders_department_product desc) rank_top5,
			row_number() over (partition by department_x order by count_orders_department_product) rank_bottom5
from CTE1
where department_x = 'international') as tmp
where rank_top5 <=5 or rank_bottom5 <= 5 

select distinct department
from "Ecommerce_Project".departments

select distinct department_x
from "Ecommerce_Project".model_data_department_product


-- most popular aisle? what does it mean? (add more promotion there)


-- Products placed 1th-7th in cart are the products mostly reordered.
select add_to_cart_order, count(*) count_reorder_per_cart_order
from "Ecommerce_Project".order_products_prior
where reordered = 1
group by add_to_cart_order
--------------------------------------
-- most frequently purchased items among reordered items
--------------------------------------
select p.product_name, count(*) count_popular_item_reordered
from "Ecommerce_Project".model_data_department_product m
join "Ecommerce_Project".products p
on m.product_id = p.product_id
where target = 1 and m.department_x = 'beverages'
group by p.product_name
order by count_popular_item_reordered desc
limit 10

frequent_buy as (
select *
from prior_transaction pt
join "Ecommerce_Project".products p
on pt.product_id = p.product_id
)

select product_name, count(o_order_id) as count_product
from frequent_buy
group by product_name
order by count(o_order_id) desc
limit 10

