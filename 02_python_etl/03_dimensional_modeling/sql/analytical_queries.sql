/* Analytical Queries for the Dimensional Model */

/* Monthly revenue */
SELECT 
        d.month,
        d.month_name,
        d.year,
        SUM(s.net_revenue) AS monthly_revenue
FROM fact_sales s
JOIN dim_date d ON s.date_key = d.date_key
GROUP BY d.month, d.month_name, d.year
ORDER BY d.year, d.month;

/* Revenue by product category */
SELECT p.category,
       SUM(s.net_revenue) AS revenue_by_category
FROM fact_sales s
JOIN dim_product p ON s.product_key = p.product_key
GROUP BY p.category
ORDER BY p.category;

/* Top 3 products of each store */
WITH product_store_sales AS(
SELECT 	p.product_name,
	p.category,
	p.brand,
	st.store_name,
	st.store_city,
	SUM(s.net_revenue) AS revenue_product_store
FROM fact_sales s
JOIN dim_product p ON s.product_key = p.product_key
JOIN dim_store st ON s.store_key = st.store_key
GROUP BY p.product_name, p.category, p.brand, st.store_name, st.store_city),

ranked_product_store_sales AS (
SELECT 	product_name,
	category,
	brand,
	store_name,
	store_city,
	revenue_product_store,
	ROW_NUMBER() OVER (PARTITION BY store_name
	ORDER BY revenue_product_store DESC) AS position
FROM product_store_sales)

SELECT *
FROM ranked_product_store_sales
WHERE position <= 3
ORDER BY store_name, position;

/* Daily revenue vs previous day revenue by store */
WITH daily_store_revenue AS (
    SELECT st.store_name,
           d.full_date,
           SUM(s.net_revenue) AS daily_revenue
    FROM fact_sales s
    JOIN dim_store st ON s.store_key = st.store_key
    JOIN dim_date d ON s.date_key = d.date_key
    GROUP BY st.store_name, d.full_date
)
SELECT store_name,
       full_date,
       daily_revenue,
       LAG(daily_revenue) OVER (PARTITION BY store_name ORDER BY full_date) AS previous_day_revenue,
       daily_revenue - LAG(daily_revenue) OVER (PARTITION BY store_name ORDER BY full_date) AS revenue_change
FROM daily_store_revenue
ORDER BY store_name, full_date;

/* Revenue accumulated by store */
WITH accumulated_store_revenue AS (
    SELECT st.store_name,
           d.full_date,
           SUM(s.net_revenue) AS daily_revenue
    FROM fact_sales s
    JOIN dim_store st ON s.store_key = st.store_key
    JOIN dim_date d ON s.date_key = d.date_key
    GROUP BY st.store_name, d.full_date
)
SELECT store_name,
       full_date,
       daily_revenue,
       SUM(daily_revenue) OVER (
            PARTITION BY store_name 
            ORDER BY full_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS accumulated_revenue
FROM accumulated_store_revenue
ORDER BY store_name, full_date;

