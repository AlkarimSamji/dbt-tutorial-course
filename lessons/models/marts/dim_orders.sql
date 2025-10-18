WITH

-- Aggregate measures
order_item_measures AS (
	SELECT
		order_id,
		SUM(item_sale_price) AS total_sale_price,
		SUM(product_cost) AS total_product_cost,
		SUM(item_profit) AS total_profit,
		SUM(item_discount) AS total_discount,

		{#{%- set departments = ['Men','Women'] %} get values dynamically see below line#}
		{%- set departments = dbt_utils.get_column_values(table=ref('int_ecommerce__order_items_products'),column='product_department')%}
		{% for department in departments %}
		SUM(IF(product_department = '{{ department }}', item_sale_price, 0)) as total_sold_{{ department.lower() }}swear{{"," if not loop.last}}
		{%- endfor %}
		
	FROM {{ ref('int_ecommerce__order_items_products') }}
	GROUP BY 1
)

SELECT
	--data from staging orders table
	od.order_id,
	od.created_at AS order_created_at,	
	{{ is_weekend('od.created_at')}} AS order_was_created_on_weekend,
	od.shipped_at AS order_shipped_at,
	od.delivered_at AS order_delivered_at,
	od.returned_at AS order_returned_at,
	od.status AS order_status,
	od.num_items_ordered,

	--metrics from order level
	om.total_sale_price,
	om.total_product_cost,
	om.total_profit,
	om.total_discount,	
	TIMESTAMP_DIFF(od.created_at, user_data.first_order_created_at, DAY) AS days_since_first_order,
	{%- for department in departments %}
	total_sold_{{ department.lower() }}swear{{"," if not loop.last}}
	{%- endfor %}
FROM {{ ref('stg_ecommerce__orders') }} AS od
LEFT JOIN order_item_measures AS om
	ON od.order_id = om.order_id
LEFT JOIN {{ ref('int_ecommerce__first_order_created') }} as user_data
	ON user_data.user_id = od.user_id
