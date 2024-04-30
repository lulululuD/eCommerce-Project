--------------------
-- explore tables
--------------------

-- order_id and user_id, no reorder info(one big transaction)
select *
from "Ecommerce_Project".orders

-- only order_id and product_id, tells your whether reordered (which product was re-ordered)
select *
from "Ecommerce_Project".order_products_prior

-- only order_id and product_id, tells your whether reordered (which product was re-ordered)
select *
from "Ecommerce_Project".order_products_train

-- prior transactional information
select *
from "Ecommerce_Project".orders o
join "Ecommerce_Project".order_products_prior p
on o.order_id = p.order_id

-- train transactional information
select *
from "Ecommerce_Project".orders o
join "Ecommerce_Project".order_products_train t
on o.order_id = t.order_id

--------------------------------------
-- create target variable for modeling

-- timeline below
-- prior --now --train
--------------------------------------

-- if user_id from the prior is in orders.eval_set=train, meaning it has previous purchase.
-- subquery use orders table because it contains user_id. orders table does not contain user_id
-- DROP TABLE IF EXISTS model_data;
create table model_data as
select distinct user_id, product_id, order_number, order_dow, order_hour_of_day, days_since_prior_order
from "Ecommerce_Project".orders o
join "Ecommerce_Project".order_products_prior p
on o.order_id = p.order_id
where o.user_id in (
						select distinct user_id
						from "Ecommerce_Project".orders 
						where eval_set = 'train'
)

-- create a new column 'target', target variable for the model
-- target =1 when (user_id, product_id) of model_data in (user_id, product_id) of train data

-- Add the 'target' column to the model_data table
alter table model_data
add column target INT;
-- Update model_data table with the new column 'target'
update model_data
set target = (
	case 
		when (user_id, product_id) in (
			select user_id, product_id
			from "Ecommerce_Project".orders o
			join "Ecommerce_Project".order_products_train t
			on o.order_id = t.order_id
		) then 1
		else 0
	end 
);


--------------------------------------
-- create features
-- 1. user variables
--------------------------------------
-- you have to alter table before CTE, otherwise you will get errors
-- add user_feature columns
alter table model_data
add column user_num_orders INT,
add column user_num_products INT,
add column user_most_order_day INT,
add column user_most_order_hour INT,
add column user_avg_days_since_prior_order FLOAT;

-- CTE to get prior data
with prior_transaction as (
select o.order_id AS o_order_id,  -- Assigning alias to order_id column from "orders" table, if not, there are errors running codes below
       p.order_id AS p_order_id,  -- Assigning alias to order_id column from "order_products_prior" table
       o.*,  -- Selecting all columns from "orders" table
       p.*   -- Selecting all columns from "order_products_prior" table
from "Ecommerce_Project".orders o
join "Ecommerce_Project".order_products_prior p
on o.order_id = p.order_id
),
-- CTE for user_var
	user_var as (
	select user_id,
    count(distinct o_order_id) user_num_orders,
	count(product_id) user_num_products,
	MODE() WITHIN GROUP (ORDER BY order_dow) user_most_order_day,
	MODE() WITHIN GROUP (ORDER BY order_hour_of_day) user_most_order_hour,
	avg(days_since_prior_order) user_avg_days_since_prior_order
	from prior_transaction
	group by user_id
	)
--select *
--from user_var as uv
--join model_data 
--on uv.user_id = model_data.user_id


-- Update model_data table with the new user_var, join user_var and model_data
update model_data 
set user_num_orders = uv.user_num_orders,
    user_num_products = uv.user_num_products,
    user_most_order_day = uv.user_most_order_day,
    user_most_order_hour = uv.user_most_order_hour,
    user_avg_days_since_prior_order = uv.user_avg_days_since_prior_order
from user_var as uv
where uv.user_id = model_data.user_id 
-- if I use join model_data on uv.user_id = model_data.user_id , sql cannot update because it can distinguish between table name and join table name
-- ERROR:  table name "model_data" specified more than once 

--------------------------------------
-- create features
-- 2. product variables
--------------------------------------
-- you have to alter table before CTE, otherwise you will get errors
-- add product_feature columns
alter table model_data
add column product_num_orders INT,
add column product_num_users INT,
add column product_most_order_day INT,
add column product_most_order_hour INT,
add column product_avg_days_since_prior_order FLOAT;

-- CTE to get prior data
with prior_transaction as (
select o.order_id AS o_order_id,  -- Assigning alias to order_id column from "orders" table, if not, there are errors running codes below
       p.order_id AS p_order_id,  -- Assigning alias to order_id column from "order_products_prior" table
       o.*,  -- Selecting all columns from "orders" table
       p.*   -- Selecting all columns from "order_products_prior" table
from "Ecommerce_Project".orders o
join "Ecommerce_Project".order_products_prior p
on o.order_id = p.order_id
),
-- CTE for product_var
product_var as (
	select product_id,
    count(distinct o_order_id) product_num_orders,
	count(distinct user_id) product_num_users,
	MODE() WITHIN GROUP (ORDER BY order_dow) product_most_order_day,
	MODE() WITHIN GROUP (ORDER BY order_hour_of_day) product_most_order_hour,
	avg(days_since_prior_order) product_avg_days_since_prior_order
	from prior_transaction
	group by product_id
	)
--select *
--from user_var as uv
--join model_data 
--on uv.user_id = model_data.user_id


-- Update model_data table with the new user_var, join user_var and model_data
update model_data 
set product_num_orders = pv.product_num_orders,
    product_num_users = pv.product_num_users,
    product_most_order_day = pv.product_most_order_day,
    product_most_order_hour  = pv.product_most_order_hour ,
    product_avg_days_since_prior_order  = pv.product_avg_days_since_prior_order 
from product_var as pv
where pv.product_id = model_data.product_id 
-- if I use join model_data on uv.user_id = model_data.user_id , sql cannot update because it can distinguish between table name and join table name
-- ERROR:  table name "model_data" specified more than once 

--------------------------------------
-- create features
-- 3. user_product variables
--------------------------------------
-- you have to alter table before CTE, otherwise you will get errors
-- add user_product_feature columns
alter table model_data
add column user_product_num_orders INT,
add column user_product_avg_add_to_cart_order FLOAT,
add column user_product_avg_reorder FLOAT,
add column user_product_most_order_day INT,
add column user_product_most_order_hour INT;

-- CTE to get prior data
with prior_transaction as (
select o.order_id AS o_order_id,  -- Assigning alias to order_id column from "orders" table, if not, there are errors running codes below
       p.order_id AS p_order_id,  -- Assigning alias to order_id column from "order_products_prior" table
       o.*,  -- Selecting all columns from "orders" table
       p.*   -- Selecting all columns from "order_products_prior" table
from "Ecommerce_Project".orders o
join "Ecommerce_Project".order_products_prior p
on o.order_id = p.order_id
),
-- CTE for user_product_var
user_product_var as (
	select user_id, product_id,
    count(o_order_id) user_product_num_orders,
	avg(add_to_cart_order) user_product_avg_add_to_cart_order,
	avg(reordered) user_product_avg_reorder,
	MODE() WITHIN GROUP (ORDER BY order_dow) user_product_most_order_day,
	MODE() WITHIN GROUP (ORDER BY order_hour_of_day) user_product_most_order_hour
	from prior_transaction
	group by user_id, product_id
	)
	
-- Update model_data table with the new user_var, join user_var and model_data
update model_data 
set user_product_num_orders = upv.user_product_num_orders,
    user_product_avg_add_to_cart_order  = upv.user_product_avg_add_to_cart_order,
    user_product_avg_reorder  = upv.user_product_avg_reorder,
	user_product_most_order_day = upv.user_product_most_order_day,
    user_product_most_order_hour  = upv.user_product_most_order_hour
from user_product_var as upv
where upv.product_id = model_data.product_id and upv.user_id = model_data.user_id 
-- if I use join model_data on uv.user_id = model_data.user_id , sql cannot update because it can distinguish between table name and join table name
-- ERROR:  table name "model_data" specified more than once


-- save the dataset 'model_data' which will be used in part 3 modeling process
copy model_data TO 'C:/Users/shenl/OneDrive/Documents/eCommerce DS project/model_data.csv' WITH CSV HEADER
-- run above code at psql environment in order to save the dataframe











--------------------------------------
-- update model_data by join products dataset 
-- get some information about department information
-- I will use Python merge() function to join products information











