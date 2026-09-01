/* Exercises to test intermediate SQL skills adquired in DataCamp
For an SQL table such as 

orders
--------------------------------
order_id        INTEGER
customer_id     INTEGER
country         VARCHAR
order_date      DATE
product_category VARCHAR
quantity        INTEGER
unit_price      DECIMAL
discount        DECIMAL
status          VARCHAR
*/

/* Exercise 1: Total sales per country */
SELECT  country,
        SUM(quantity * unit_price) AS total_sales
FROM    orders
GROUP BY country;

/* Exercise 2: Average money spent per order */
SELECT order_id, AVG(unit_price) AS average_unit_price_per_order
FROM orders
GROUP BY order_id;


SELECT order_id, AVG(quantity * unit_price) AS average_money_spent_per_order
FROM orders
GROUP BY order_id;

/* Exercise 3: Countries with total sales greater than 100,000 */

SELECT country
FROM orders
GROUP BY country
HAVING SUM(quantity * unit_price) > 100000;

/* Exercise 4: Total number of orders in 2025 */
SELECT COUNT(DISTINCT(order_id)) AS number_of_orders, SUM(quantity * unit_price) AS total_sales
FROM orders
WHERE status = 'completed' AND order_date >= '2025-01-01' AND order_date < '2026-01-01';


/* Exercise 5: Sales summary per product category */
SELECT  product_category,
        COUNT(order_id) AS number_of_orders,
        SUM(quantity) AS units_sold,
        SUM(quantity * unit_price) AS gross_sales,
        AVG(unit_price) AS average_unit_price
FROM    orders
GROUP BY product_category
ORDER BY gross_sales DESC;

/* Exercise 6: Real sales per country for countries with more than 100 orders */
SELECT country, SUM(quantity * unit_price * (1- discount)) AS real_sales
FROM orders
GROUP BY country
HAVING COUNT(order_id) >= 100
ORDER BY real_sales DESC;