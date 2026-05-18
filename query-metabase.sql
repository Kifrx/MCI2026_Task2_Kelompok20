
-- Q1 - Peak Order Times

SELECT
    order_hour_of_day AS hour_of_day,
    uniqExact(order_id) AS total_unique_orders
FROM analytics.order_items
GROUP BY order_hour_of_day
ORDER BY order_hour_of_day ASC;




-- Q2 - Weekday vs Weekend Behavior

SELECT
    if(order_dow < 2, 'Weekend', 'Weekday') AS day_type,
    uniqExact(order_id) AS total_unique_orders
FROM analytics.order_items
GROUP BY day_type
ORDER BY total_unique_orders DESC;




-- Q3 - Top Departments

SELECT
    department,
    count() AS total_items_sold
FROM analytics.order_items
GROUP BY department
ORDER BY total_items_sold DESC
LIMIT 15;


-- Q4 - Most Reordered Products

SELECT
    product_name,
    sum(reordered) AS total_reorders
FROM analytics.order_items
GROUP BY product_name
HAVING total_reorders > 0
ORDER BY total_reorders DESC
LIMIT 15;




-- Q5 - Customer Reorder Behavior

SELECT
    days_since_prior_order,
    uniqExact(order_id) AS total_unique_orders
FROM analytics.order_items
GROUP BY days_since_prior_order
ORDER BY days_since_prior_order ASC;



-- Q6 - Basket Analysis FP-Growth Rules

SELECT
    antecedent,
    consequent,
    confidence,
    lift
FROM analytics.fp_growth_rules
ORDER BY lift DESC, confidence DESC;


